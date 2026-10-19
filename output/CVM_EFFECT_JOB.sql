-- ============================================================================================
-- CVM_EFFECT_JOB — новый отчёт эффекта CVM в заданиях SQL Agent и удаление согласованных старых объектов (13.10.2026).
-- Запустить ОДИН РАЗ целиком в SSMS (F5) после окончания ночного прохода «MCI Regular update», последним из трёх:
--   CVM_DISCOUNT_MART.sql (витрина скидок и её шаги PROMO_CAMPAIGN_LINK, DISCOUNT_MART) → CVM_EFFECT_PROC.sql
--   (витрины I_CVM_EFFECT / I_CVM_EFFECT_CG, новая dbo.CVM_EFFECT_TODAY, правка чистки шага 1) → ручной прогон
--   процедуры с замером → этот файл.
-- Порядок файла: 0 проверки → 1 шаги «MCI Regular update» → 2 вечерний догон истории → 3 удаление → 4 итог.
--
-- Решения Елены, на которых стоит файл (13.10.2026):
--   — ночной расчёт: шаг 25 CVM_EFFECT_TODAY возвращается в задание сразу после витрины скидок (DISCOUNT_MART) и ведёт
--     туда, куда DISCOUNT_MART вёл раньше (сейчас 41 cohort_new); ставится только новая версия процедуры (метка
--     «-- CVM_EFFECT_TODAY: методика 2026-10» в её тексте);
--   — шаги 20 (CVM_EFFECT_E_COMM) и 42 (cohort_new_ss) обходятся (19 → 21, 41 → 43), их команды — пустые; шаги не
--     удаляются: sp_delete_jobstep перенумеровал бы шаги;
--   — история с 01.09.2025 — отдельным заданием раз в день после прохода MCI, порциями, от новых акций к старым; после
--     23:30 новые акции не начинать; задание выключается само, когда история догнана;
--   — удаление — только утверждённый список (раздел 3); I_CVM_EFFECT_SEG, I_CVM_EFFECT_CG_SEG, I_CVM_EFFECT_E_COMM_OFF
--     читает портал — их не удалять до его перевода;
--   — проверки «идёт ли задание MCI» нет.
-- Что меняется: переходы шагов DISCOUNT_MART, CVM_EFFECT_TODAY, UPDATE_TYPE_PROMO, cohort_new и команды шагов
--   I_CVM_EFFECT_E_COMM, cohort_new_ss в «MCI Regular update»; новое задание «CVM EFFECT HISTORY»; DROP 12 процедур и
--   до 41 таблицы. Таблицы MCI только читаются (nolock).
-- Повторный запуск: сделанное в разделе 1 не повторяется, задание догона пересоздаётся (включённым), удалённых
--   объектов раздел 3 уже не находит.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
DECLARE @abort bit = 0;

IF DB_NAME() <> N'mci_model'
BEGIN
    RAISERROR(N'СТОП: файл выполняется не в базе mci_model — ничего не меняю.', 16, 1);
    SET @abort = 1;
END
-- кириллица (метка версии процедуры, тексты шагов) должна дойти до сервера как есть: файл открыт в UTF-8
IF N'Отток' <> NCHAR(1054) + NCHAR(1090) + NCHAR(1090) + NCHAR(1086) + NCHAR(1082)
BEGIN
    RAISERROR(N'СТОП: файл открыт не в UTF-8 — метка версии процедуры не нашлась бы, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
-- новая процедура и витрины эффекта (CVM_EFFECT_PROC.sql)
IF isnull(OBJECT_DEFINITION(OBJECT_ID(N'dbo.CVM_EFFECT_TODAY')), N'') NOT LIKE N'%-- CVM[_]EFFECT[_]TODAY: методика 2026-10%'
BEGIN
    RAISERROR(N'СТОП: в тексте dbo.CVM_EFFECT_TODAY нет метки «-- CVM_EFFECT_TODAY: методика 2026-10» — сначала CVM_EFFECT_PROC.sql; ничего не меняю.', 16, 1);
    SET @abort = 1;
END
IF OBJECT_ID(N'dbo.I_CVM_EFFECT', N'U') IS NULL OR OBJECT_ID(N'dbo.I_CVM_EFFECT_CG', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: нет витрин dbo.I_CVM_EFFECT / dbo.I_CVM_EFFECT_CG — сначала CVM_EFFECT_PROC.sql; ничего не меняю.', 16, 1);
    SET @abort = 1;
END
IF @abort = 1 SET NOEXEC ON;   -- всё ниже не выполняется — до SET NOEXEC OFF в конце файла
GO

-- ============================== 1. ШАГИ «MCI Regular update»: 25 после DISCOUNT_MART, обход 20 и 42 ==============================
-- Было (12–13.10.2026): … 52 DELIVERY_REPORT → [CVM_DISCOUNT_MART.sql: PROMO_CAMPAIGN_LINK → DISCOUNT_MART] → 41 cohort_new
--   → 42 cohort_new_ss → 43 I_TRENDS …; 24 AFFINITIVE_MODEL → 26 (25 CVM_EFFECT_TODAY не вызывается); 19 → 20 → 21.
-- Станет: … DISCOUNT_MART → 25 CVM_EFFECT_TODAY → шаг, куда вёл DISCOUNT_MART (сейчас 41 cohort_new) → 43 I_TRENDS …;
--   19 UPDATE_TYPE_PROMO → 21 CVM_CONTACT. Команда 25 — exec dbo.CVM_EFFECT_TODAY @stop_at = '18:00' (после 18:00 новые
--   акции не начинаются: ночной проход не затягивается, вечерний догон с 18:00 не встречает шаг 25). У 25 и успех, и ошибка ведут дальше: сбой расчёта эффекта не останавливает
--   ночь, ошибка видна в истории задания. Переход 24 → 26 не меняется: 25 вызывается только после витрины скидок.
--   Шаги 20 I_CVM_EFFECT_E_COMM и 42 cohort_new_ss остаются под своими номерами, с пустой командой (строка комментария).
-- Шаги ищутся по именам. Раздел — СТОП (задание не меняется), если: нет метки новой версии в dbo.CVM_EFFECT_TODAY; нет
--   шага DISCOUNT_MART (сначала раздел 6 CVM_DISCOUNT_MART.sql) или другого нужного шага; имена повторяются; DISCOUNT_MART
--   ведёт не на один и тот же шаг при успехе и ошибке; в 25 уже ведёт другой шаг (после правки цепочка пропустила бы
--   шаги); у 20 / 42 другие команды или переходы. Уже сделанное не повторяется.
-- Все правки — одной транзакцией; внутри — повторная проверка, что имена и переходы шагов не изменились после чтения.
USE msdb
GO

SET NOCOUNT ON;

DECLARE @job sysname = N'MCI Regular update', @rc int = 0, @msg nvarchar(2000), @stop nvarchar(1000) = NULL
      , @noop nvarchar(300) = N'-- CVM_EFFECT_JOB: шаг отключён 13.10.2026 по решению Елены — процедура удалена, шаг обходится; номер оставлен, чтобы не сдвигать переходы';
DECLARE @mart int, @next int, @s25 int, @s19 int, @s20 int, @s21 int, @s41 int, @s42 int, @s43 int, @sig int, @sig2 int
      , @do25 bit = 0, @do19 bit = 0, @do20 bit = 0, @do41 bit = 0, @do42 bit = 0;

BEGIN TRY
    DROP TABLE IF EXISTS #ej_steps;
    SELECT s.step_id, s.step_name, s.subsystem, s.command
         , s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
         -- куда шаг ведёт при успехе: 3 — следующий шаг, 4 — шаг N (1 и 2 — выход)
         , SUCC = CASE s.on_success_action WHEN 3 THEN s.step_id + 1 WHEN 4 THEN s.on_success_step_id END
         -- команда уже пустая (или её поставил этот файл)
         , NOOP = CASE WHEN ltrim(rtrim(isnull(s.command, N''))) = N'' OR s.command LIKE N'-- CVM[_]EFFECT[_]JOB:%' THEN 1 ELSE 0 END
    INTO #ej_steps
    FROM msdb.dbo.sysjobsteps AS s (nolock)
         JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
    WHERE j.name = @job;

    SELECT @sig  = CHECKSUM_AGG(CHECKSUM(step_id, step_name, on_success_action, on_success_step_id, on_fail_action, on_fail_step_id))
         , @mart = max(IIF(step_name = N'DISCOUNT_MART',       step_id, NULL))
         , @s25  = max(IIF(step_name = N'CVM_EFFECT_TODAY',    step_id, NULL))
         , @s19  = max(IIF(step_name = N'UPDATE_TYPE_PROMO',   step_id, NULL))
         , @s20  = max(IIF(step_name = N'I_CVM_EFFECT_E_COMM', step_id, NULL))
         , @s21  = max(IIF(step_name = N'CVM_CONTACT',         step_id, NULL))
         , @s41  = max(IIF(step_name = N'cohort_new',          step_id, NULL))
         , @s42  = max(IIF(step_name = N'cohort_new_ss',       step_id, NULL))
         , @s43  = max(IIF(step_name = N'I_TRENDS',            step_id, NULL))
    FROM #ej_steps;

    IF NOT EXISTS (SELECT 1
                   FROM mci_model.sys.sql_modules AS m (nolock)
                   WHERE m.object_id = OBJECT_ID(N'mci_model.dbo.CVM_EFFECT_TODAY')
                     AND m.definition LIKE N'%-- CVM[_]EFFECT[_]TODAY: методика 2026-10%')
        SET @stop = N'в тексте dbo.CVM_EFFECT_TODAY нет метки «-- CVM_EFFECT_TODAY: методика 2026-10» — сначала CVM_EFFECT_PROC.sql';
    ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps)
        SET @stop = N'задания «MCI Regular update» нет';
    ELSE IF EXISTS (SELECT 1
                    FROM #ej_steps
                    WHERE step_name IN (N'DISCOUNT_MART', N'CVM_EFFECT_TODAY', N'UPDATE_TYPE_PROMO', N'I_CVM_EFFECT_E_COMM'
                                      , N'CVM_CONTACT', N'cohort_new', N'cohort_new_ss', N'I_TRENDS')
                    GROUP BY step_name
                    HAVING count(*) > 1)
        SET @stop = N'имена шагов DISCOUNT_MART, CVM_EFFECT_TODAY, UPDATE_TYPE_PROMO, I_CVM_EFFECT_E_COMM, CVM_CONTACT, cohort_new, cohort_new_ss, I_TRENDS повторяются';
    ELSE IF @mart IS NULL
        SET @stop = N'нет шага DISCOUNT_MART — сначала раздел 6 CVM_DISCOUNT_MART.sql';
    ELSE IF @s25 IS NULL OR @s19 IS NULL OR @s20 IS NULL OR @s21 IS NULL OR @s41 IS NULL OR @s42 IS NULL OR @s43 IS NULL
        SET @stop = N'нет одного из шагов CVM_EFFECT_TODAY, UPDATE_TYPE_PROMO, I_CVM_EFFECT_E_COMM, CVM_CONTACT, cohort_new, cohort_new_ss, I_TRENDS';
    ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s25 AND subsystem = N'TSQL' AND command LIKE N'%CVM[_]EFFECT[_]TODAY%')
        SET @stop = N'шаг CVM_EFFECT_TODAY вызывает не dbo.CVM_EFFECT_TODAY';

    -- (а) 25 после DISCOUNT_MART
    IF @stop IS NULL
    BEGIN
        IF EXISTS (SELECT 1 FROM #ej_steps
                   WHERE step_id = @mart AND on_success_action = 4 AND on_success_step_id = @s25
                     AND on_fail_action = 4 AND on_fail_step_id = @s25)
        BEGIN
            -- уже стоит (повторный запуск): 25 и при успехе, и при ошибке ведёт на один и тот же существующий шаг
            IF NOT EXISTS (SELECT 1 FROM #ej_steps AS s
                           WHERE s.step_id = @s25 AND s.on_success_action = 4 AND s.on_fail_action = 4
                             AND s.on_fail_step_id = s.on_success_step_id AND s.on_success_step_id <> @s25
                             AND EXISTS (SELECT 1 FROM #ej_steps AS x WHERE x.step_id = s.on_success_step_id))
                SET @stop = N'DISCOUNT_MART уже ведёт на CVM_EFFECT_TODAY, но CVM_EFFECT_TODAY не ведёт дальше на один и тот же шаг при успехе и ошибке';
        END
        ELSE
        BEGIN
            SELECT @next = on_success_step_id
            FROM #ej_steps
            WHERE step_id = @mart AND on_success_action = 4 AND on_fail_action = 4 AND on_fail_step_id = on_success_step_id;

            IF @next IS NULL OR @next IN (@mart, @s25, @s20, @s42) OR NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @next)
                SET @stop = N'ожидался шаг DISCOUNT_MART с переходом на один и тот же существующий шаг и при успехе, и при ошибке';
            -- в 25 сейчас не ведёт ни один шаг (24 → 26 в обход): иначе после 25 → дальше цепочка пропустила бы шаги
            ELSE IF EXISTS (SELECT 1 FROM #ej_steps
                            WHERE (on_success_action = 4 AND on_success_step_id = @s25)
                               OR (on_fail_action    = 4 AND on_fail_step_id    = @s25)
                               OR (step_id = @s25 - 1 AND (on_success_action = 3 OR on_fail_action = 3)))
                SET @stop = N'в шаг CVM_EFFECT_TODAY уже ведёт другой шаг (ожидался обход 24 → 26) — после правки цепочка пропустила бы шаги';
            ELSE
                SET @do25 = 1;
        END
    END

    -- (б) обход 20: 19 → 21
    IF @stop IS NULL
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s20 AND SUCC = @s21)
            SET @stop = N'шаг I_CVM_EFFECT_E_COMM при успехе ведёт не на CVM_CONTACT';
        ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps
                            WHERE step_id = @s20 AND subsystem = N'TSQL'
                              AND (NOOP = 1 OR command LIKE N'%CVM[_]EFFECT[_]E[_]COMM%'))
            SET @stop = N'у шага I_CVM_EFFECT_E_COMM неожиданная команда (ожидался вызов dbo.CVM_EFFECT_E_COMM)';
        ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s19 AND SUCC IN (@s20, @s21))
            SET @stop = N'шаг UPDATE_TYPE_PROMO при успехе ведёт не на I_CVM_EFFECT_E_COMM и не на CVM_CONTACT';
        ELSE
            SELECT @do19 = CASE WHEN EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s19 AND SUCC = @s20) THEN 1 ELSE 0 END
                 , @do20 = CASE WHEN EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s20 AND NOOP = 0) THEN 1 ELSE 0 END;
    END

    -- (в) обход 42: 41 → 43
    IF @stop IS NULL
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s42 AND SUCC = @s43)
            SET @stop = N'шаг cohort_new_ss при успехе ведёт не на I_TRENDS';
        ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps
                            WHERE step_id = @s42 AND subsystem = N'TSQL'
                              AND (NOOP = 1 OR command LIKE N'%ss[_]cohort[_]new%'))
            SET @stop = N'у шага cohort_new_ss неожиданная команда (ожидался вызов dbo.ss_cohort_new)';
        ELSE IF NOT EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s41 AND SUCC IN (@s42, @s43))
            SET @stop = N'шаг cohort_new при успехе ведёт не на cohort_new_ss и не на I_TRENDS';
        ELSE
            SELECT @do41 = CASE WHEN EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s41 AND SUCC = @s42) THEN 1 ELSE 0 END
                 , @do42 = CASE WHEN EXISTS (SELECT 1 FROM #ej_steps WHERE step_id = @s42 AND NOOP = 0) THEN 1 ELSE 0 END;
    END
END TRY
BEGIN CATCH
    SET @stop = concat(N'не прочитать задания в msdb (', ERROR_MESSAGE(), N')');
END CATCH

IF @stop IS NOT NULL
BEGIN
    SET @msg = concat(N'СТОП по шагам задания: ', @stop, N'. Задание «MCI Regular update» не изменено.');
    RAISERROR(N'%s', 16, 1, @msg);
    GOTO EndJob;
END
IF @do25 = 0 AND @do19 = 0 AND @do20 = 0 AND @do41 = 0 AND @do42 = 0
BEGIN
    RAISERROR(N'Шаги задания: всё уже стоит (25 после DISCOUNT_MART, обход 20 и 42) — задание не меняю.', 10, 1) WITH NOWAIT;
    GOTO EndJob;
END

BEGIN TRANSACTION;

-- повторная проверка внутри транзакции: имена и переходы шагов те же, что при чтении
SELECT @sig2 = CHECKSUM_AGG(CHECKSUM(s.step_id, s.step_name, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id))
FROM msdb.dbo.sysjobsteps AS s (nolock)
     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
WHERE j.name = @job;
IF isnull(@sig2, 0) <> isnull(@sig, 0) GOTO QuitWithRollback;

IF @do25 = 1
BEGIN
    -- 25 → туда, куда вёл DISCOUNT_MART, и при успехе, и при ошибке
    -- команда: после 18:00 новые акции не начинаются — ночной проход не затягивается и не пересекается с вечерним
    -- догоном; недосчитанное берут следующие ночи
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @s25,
         @command = N'exec dbo.CVM_EFFECT_TODAY @stop_at = ''18:00'';',
         @on_success_action = 4, @on_success_step_id = @next,
         @on_fail_action = 4, @on_fail_step_id = @next;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
    -- DISCOUNT_MART → 25 и при успехе, и при ошибке
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @mart,
         @on_success_action = 4, @on_success_step_id = @s25,
         @on_fail_action = 4, @on_fail_step_id = @s25;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END
IF @do19 = 1
BEGIN
    -- 19 → 21 при успехе (переход при ошибке не меняется)
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @s19, @on_success_action = 4, @on_success_step_id = @s21;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END
IF @do20 = 1
BEGIN
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @s20, @command = @noop;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END
IF @do41 = 1
BEGIN
    -- 41 → 43 при успехе (переход при ошибке не меняется)
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @s41, @on_success_action = 4, @on_success_step_id = @s43;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END
IF @do42 = 1
BEGIN
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = @job, @step_id = @s42, @command = @noop;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END

COMMIT TRANSACTION;
SET @msg = concat(N'Шаги задания изменены: '
                , IIF(@do25 = 1, concat(@mart, N' DISCOUNT_MART → ', @s25, N' CVM_EFFECT_TODAY (новые акции — до 18:00) → ', @next, N' (и при успехе, и при ошибке); '), N'')
                , IIF(@do19 = 1, concat(@s19, N' → ', @s21, N' в обход ', @s20, N'; '), N'')
                , IIF(@do20 = 1, concat(N'команда шага ', @s20, N' пустая; '), N'')
                , IIF(@do41 = 1, concat(@s41, N' → ', @s43, N' в обход ', @s42, N'; '), N'')
                , IIF(@do42 = 1, concat(N'команда шага ', @s42, N' пустая; '), N'')
                , N'итог — раздел 4.');
RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
GOTO EndJob;

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;
    RAISERROR(N'СТОП по шагам задания: имена или переходы шагов изменились после чтения, либо ошибка sp_update_jobstep — всё откачено, задание не изменено.', 16, 1);
EndJob:
GO

-- ОТКАТ раздела 1 (не выполняется; выделить и выполнить вручную): шаг 25 снова вне цепочки — DISCOUNT_MART ведёт туда,
--   куда сейчас ведёт 25, у 25 — переходы как до файла (успех — следующий шаг, ошибка — письмо об ошибке). Обход шагов
--   20 и 42 не откатывается: их процедуры удаляет раздел 3.
-- USE msdb;
-- DECLARE @j sysname = N'MCI Regular update', @mart int, @s25 int, @next int, @err int;
-- SELECT @mart = s.step_id FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id WHERE j.name = @j AND s.step_name = N'DISCOUNT_MART';
-- SELECT @s25 = s.step_id, @next = s.on_success_step_id FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id WHERE j.name = @j AND s.step_name = N'CVM_EFFECT_TODAY';
-- SELECT @err = s.step_id FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id WHERE j.name = @j AND s.step_name = N'send error mail';
-- EXEC msdb.dbo.sp_update_jobstep @job_name = @j, @step_id = @mart, @on_success_action = 4, @on_success_step_id = @next, @on_fail_action = 4, @on_fail_step_id = @next;
-- EXEC msdb.dbo.sp_update_jobstep @job_name = @j, @step_id = @s25, @on_success_action = 3, @on_fail_action = 4, @on_fail_step_id = @err;

-- ============================== 2. ВЕЧЕРНИЙ ДОГОН ИСТОРИИ: задание «CVM EFFECT HISTORY» ==============================
-- Отдельное задание SQL Agent, каждый день в 18:00 (проход MCI 10–12.10 кончался в 11:26–16:06). Шаг (текст — @cmd ниже):
--   акции — те же, что считает процедура (ID_COMPANY = 1, старт с 01.09.2025; 101000–101999, 500000–599999 с
--     TYPE_PROMO = 'CVM', 110000–111999), закончившиеся раньше её ночного окна (финиш + 4 раньше «сегодня − 10»;
--     незаконченные считает ночной шаг 25), которых нет в I_CVM_EFFECT;
--   порция — до 31 дня дат старта от самой новой непосчитанной акции к старым: exec dbo.CVM_EFFECT_TODAY @date_from,
--     @date_to (процедура считает акции порции от новых к старым и пишет каждую сама; чеки читает раз на порцию);
--   окно 18:00–23:30: после 23:30 новая порция не начинается, не начинается и порция, которая по оценке (среднее время
--     акции в этом проходе × акций в порции) не успеет до 23:30;
--   dbo.CVM_EFFECT_TODAY уже считает в другом сеансе (ночной шаг 25 или ручной запуск) — ждёт по 5 минут: отказ
--     процедуры из-за двойного запуска не засчитывается как посчитанная порция;
--   процедура вызывается с @only_missing = 1 (посчитанные акции порции не пересчитываются) и @stop_at = 23:30;
--   все акции истории в витрине — задание выключается само; часть процедура не записала (скидка не готова, ошибка) —
--     номера в журнале шага, задание остаётся включённым; порция упала — догон идёт к следующей порции, в конце шаг
--     заканчивается ошибкой (видно в истории задания).
-- Задание не создаётся (СТОП только этого раздела), если у dbo.CVM_EFFECT_TODAY нет параметров @date_from / @date_to
--   или в I_CVM_EFFECT нет колонки номера акции (ID_PROMO или ID) и ID_COMPANY.
-- Время менять вместе: расписание (msdb.dbo.sp_update_schedule) и @start_at в тексте шага. Повторный запуск файла
--   пересоздаёт задание (включённым); если задание сейчас идёт — не трогает.
-- Ход и итог — журнал шага (весь вывод догона и процедуры за последний проход):
--   SELECT l.date_modified, l.log FROM msdb.dbo.sysjobstepslogs AS l (nolock)
--        JOIN msdb.dbo.sysjobsteps AS s (nolock) ON s.step_uid = l.step_uid
--        JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
--   WHERE j.name = N'CVM EFFECT HISTORY';
-- Внеплановый проход (в окне 18:00–23:30): EXEC msdb.dbo.sp_start_job @job_name = N'CVM EFFECT HISTORY';
USE msdb
GO

SET NOCOUNT ON;

DECLARE @hjob sysname = N'CVM EFFECT HISTORY', @owner sysname, @rc int = 0, @msg nvarchar(2000), @cmd nvarchar(max);

-- текст шага задания
SET @cmd = N'-- CVM_EFFECT_JOB: вечерний догон истории отчёта эффекта CVM (решение Елены 13.10.2026; ставит CVM_EFFECT_JOB.sql)
-- exec dbo.CVM_EFFECT_TODAY @date_from, @date_to порциями по датам старта: от новых акций к старым до 01.09.2025.
-- Окно 18:00–23:30: после 23:30 новая порция не начинается, не начинается и порция, которая по оценке не успеет до 23:30.
-- Проход прошёл все непосчитанные акции — задание выключается само. Журнал по акциям — сообщения процедуры в журнале шага.
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

DECLARE @floor        date    = ''20250901''   -- история с 01.09.2025
      , @start_at     time(0) = ''18:00''      -- начало окна — как в расписании задания (менять вместе)
      , @stop_at      time(0) = ''23:30''      -- после 23:30 новые акции не начинаются
      , @portion_days int     = 31           -- порция — до 31 дня дат старта (чеки процедура читает один раз на порцию)
      , @job          sysname = N''CVM EFFECT HISTORY'';
DECLARE @now datetime2(0) = sysdatetime(), @deadline datetime2(0), @fin_lim date, @col sysname, @sql nvarchar(max)
      , @d date, @w date, @from date, @n int, @miss int, @have0 int, @have1 int
      , @written int = 0, @portions int = 0, @sec_all int = 0, @promos_all int = 0, @est int
      , @t0 datetime2(0), @why nvarchar(300) = NULL, @left int, @list nvarchar(max), @msg nvarchar(2000)
      , @busy bit, @sd nvarchar(10), @fd nvarchar(10), @errors int = 0;

-- граница вечера — сегодняшние 23:30 (порция, ушедшая за полночь, следующую уже не начнёт)
SET @deadline = dateadd(second, datediff(second, cast(''00:00'' AS time(0)), @stop_at), cast(cast(@now AS date) AS datetime2(0)));
IF cast(@now AS time(0)) < @start_at OR @now >= @deadline
BEGIN
    SET @msg = concat(N''Догон эффекта: сейчас '', convert(char(5), @now, 108), N'' — вне окна '', convert(char(5), @start_at, 108)
                    , N''–'', convert(char(5), @stop_at, 108), N'', ничего не считаю.'');
    RAISERROR(N''%s'', 0, 1, @msg) WITH NOWAIT;
    RETURN;
END;

-- новая процедура и витрина на месте
IF isnull(OBJECT_DEFINITION(OBJECT_ID(N''dbo.CVM_EFFECT_TODAY'')), N'''') NOT LIKE N''%-- CVM[_]EFFECT[_]TODAY: методика 2026-10%''
    THROW 50001, N''Догон эффекта: в базе не новая dbo.CVM_EFFECT_TODAY (нет метки «методика 2026-10») — ничего не считаю.'', 1;
SET @col = CASE WHEN COL_LENGTH(N''dbo.I_CVM_EFFECT'', N''ID_PROMO'') IS NOT NULL THEN N''ID_PROMO''
                WHEN COL_LENGTH(N''dbo.I_CVM_EFFECT'', N''ID'') IS NOT NULL THEN N''ID'' END;
IF @col IS NULL OR COL_LENGTH(N''dbo.I_CVM_EFFECT'', N''ID_COMPANY'') IS NULL
    THROW 50002, N''Догон эффекта: в dbo.I_CVM_EFFECT нет колонки номера акции (ID_PROMO или ID) или ID_COMPANY — ничего не считаю.'', 1;

-- акции истории — те же, что считает процедура: ID_COMPANY = 1, старт с 01.09.2025, 101000–101999, 500000–599999
-- с TYPE_PROMO = ''CVM'', 110000–111999; закончились раньше ночного окна процедуры (финиш + 4 раньше «сегодня − 10»);
-- у номера с несколькими строками I_PROMO — самый ранний старт по всем строкам, как в процедуре (номер со строкой до
-- 01.09.2025 процедура не считает); акции с ошибкой дат (финиш раньше старта) процедура не считает — их нет и здесь
SET @fin_lim = dateadd(day, -14, cast(@now AS date));
DROP TABLE IF EXISTS #eh_p;
CREATE TABLE #eh_p (ID_PROMO int NOT NULL PRIMARY KEY, START_DATE date NOT NULL, HAVE bit NOT NULL, TRIED bit NOT NULL);
INSERT INTO #eh_p (ID_PROMO, START_DATE, HAVE, TRIED)
SELECT p.ID_PROMO, min(p.START_DATE), 0, 0
FROM dbo.I_PROMO AS p (nolock)
WHERE p.ID_COMPANY = 1
  AND (p.ID_PROMO BETWEEN 101000 AND 101999
       OR (p.ID_PROMO BETWEEN 500000 AND 599999 AND p.TYPE_PROMO = N''CVM'')
       OR p.ID_PROMO BETWEEN 110000 AND 111999)
  AND p.ID_PROMO <> 1013000
GROUP BY p.ID_PROMO
HAVING min(p.START_DATE) >= @floor          -- номер со строкой до 01.09.2025 процедура в истории не считает
   AND max(p.FINISH_DATE) < @fin_lim
   AND max(p.FINISH_DATE) >= min(p.START_DATE);

-- есть ли акция в витрине (колонка номера — как в I_CVM_EFFECT)
SET @sql = N''UPDATE p SET HAVE = CASE WHEN EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT AS e (nolock) WHERE e.ID_COMPANY = 1 AND e.''
         + QUOTENAME(@col) + N'' = p.ID_PROMO) THEN 1 ELSE 0 END FROM #eh_p AS p;'';
EXEC sys.sp_executesql @sql;
SELECT @n = count(*), @miss = sum(CASE WHEN HAVE = 0 THEN 1 ELSE 0 END) FROM #eh_p;
SET @msg = concat(N''Догон эффекта: акций истории '', @n, N'', без расчёта '', isnull(@miss, 0), N''; новые порции — до ''
                , convert(char(5), @stop_at, 108), N''.'');
RAISERROR(N''%s'', 0, 1, @msg) WITH NOWAIT;

WHILE 1 = 1
BEGIN
    IF sysdatetime() >= @deadline
    BEGIN
        SET @why = N''наступило 23:30'';
        BREAK;
    END

    -- самая новая непосчитанная акция, которую этот проход ещё не брал, и порция до 31 дня стартов от неё к старым;
    -- нижняя граница порции — самая старая непосчитанная акция в этих днях (посчитанные между ними пересчитываются)
    SET @d = NULL;
    SELECT @d = max(START_DATE) FROM #eh_p WHERE HAVE = 0 AND TRIED = 0;
    IF @d IS NULL BREAK;                               -- проход прошёл все непосчитанные акции
    SET @w = dateadd(day, 1 - @portion_days, @d);
    IF @w < @floor SET @w = @floor;
    SELECT @from = min(START_DATE) FROM #eh_p WHERE HAVE = 0 AND TRIED = 0 AND START_DATE BETWEEN @w AND @d;
    IF @from IS NULL                                   -- защита от холостого цикла
    BEGIN
        UPDATE #eh_p SET TRIED = 1 WHERE START_DATE = @d;
        CONTINUE;
    END
    SELECT @n = count(*), @miss = sum(CASE WHEN HAVE = 0 THEN 1 ELSE 0 END) FROM #eh_p WHERE START_DATE BETWEEN @from AND @d;

    -- успеет ли порция до 23:30: среднее время акции в этом проходе × непосчитанных акций порции (у первой порции оценки
    -- нет — её останавливает @stop_at процедуры)
    IF @promos_all > 0
    BEGIN
        SET @est = cast(round(1.0 * @sec_all / @promos_all * @miss, 0) AS int);
        IF dateadd(second, @est, sysdatetime()) > @deadline
        BEGIN
            SET @why = concat(N''следующая порция (без расчёта акций '', @miss, N'', по оценке '', @est / 60, N'' мин) не успеет до 23:30'');
            BREAK;
        END
    END

    -- dbo.CVM_EFFECT_TODAY уже считает в другом сеансе (ночной шаг 25 или ручной запуск) — ждём по 5 минут:
    -- отказ процедуры из-за двойного запуска не должен засчитаться как посчитанная порция
    SET @busy = 1;
    WHILE @busy = 1
    BEGIN
        SET @busy = 0;
        BEGIN TRY
            IF EXISTS (SELECT 1
                       FROM sys.dm_exec_requests AS r
                            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
                       WHERE r.session_id <> @@SPID AND t.dbid = DB_ID() AND t.objectid = OBJECT_ID(N''dbo.CVM_EFFECT_TODAY''))
               OR EXISTS (SELECT 1
                          FROM sys.dm_tran_locks AS l
                          WHERE l.resource_type = N''APPLICATION'' AND l.resource_database_id = DB_ID()
                            AND l.request_session_id <> @@SPID
                            AND l.resource_description LIKE N''%CVM[_]EFFECT[_]TODAY%'')
                SET @busy = 1;
        END TRY
        BEGIN CATCH
            SET @busy = 0;   -- нет прав видеть чужие сеансы — не ждём, двойной запуск отсекает сама процедура
        END CATCH
        IF @busy = 1
        BEGIN
            IF dateadd(minute, 5, sysdatetime()) >= @deadline
            BEGIN
                SET @why = N''dbo.CVM_EFFECT_TODAY считала в другом сеансе до 23:30'';
                BREAK;
            END
            RAISERROR(N''Догон эффекта: dbo.CVM_EFFECT_TODAY уже считает в другом сеансе — жду 5 минут.'', 0, 1) WITH NOWAIT;
            WAITFOR DELAY ''00:05:00'';
        END
    END
    IF @why IS NOT NULL BREAK;

    -- порция: процедура считает непосчитанные акции со стартом @from … @d от новых к старым и пишет каждую сама;
    -- после 23:30 новые акции не начинает
    UPDATE #eh_p SET TRIED = 1 WHERE START_DATE BETWEEN @from AND @d;
    SELECT @have0 = count(*) FROM #eh_p WHERE HAVE = 1;
    SELECT @sd = convert(nvarchar(10), @from, 104), @fd = convert(nvarchar(10), @d, 104);
    SET @msg = concat(N''Догон эффекта: порция '', @sd, N'' – '', @fd, N'': акций '', @n, N'', из них без расчёта '', @miss, N'' — считаю.'');
    RAISERROR(N''%s'', 0, 1, @msg) WITH NOWAIT;
    SET @t0 = sysdatetime();
    BEGIN TRY
        EXEC dbo.CVM_EFFECT_TODAY @date_from = @from, @date_to = @d, @only_missing = 1, @stop_at = @deadline;
    END TRY
    BEGIN CATCH
        -- ошибка порции не останавливает догон: акция, которая падает всегда, иначе остановила бы историю навсегда
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @errors += 1;
        SET @msg = concat(N''ВНИМАНИЕ: догон эффекта: порция '', @sd, N'' – '', @fd, N'' — ошибка '', ERROR_NUMBER(), N'': '', ERROR_MESSAGE()
                        , N''. Записанные акции остаются, иду к следующей порции.'');
        RAISERROR(N''%s'', 10, 1, @msg) WITH NOWAIT;
    END CATCH
    SET @sec_all += datediff(second, @t0, sysdatetime());
    SET @promos_all += @miss;
    SET @portions += 1;
    EXEC sys.sp_executesql @sql;
    SELECT @have1 = count(*) FROM #eh_p WHERE HAVE = 1;
    SET @written += @have1 - @have0;
    SET @msg = concat(N''Догон эффекта: порция '', @sd, N'' – '', @fd, N'' — записано новых акций '', @have1 - @have0, N'' за ''
                    , datediff(second, @t0, sysdatetime()) / 60, N'' мин.'');
    RAISERROR(N''%s'', 0, 1, @msg) WITH NOWAIT;
END

EXEC sys.sp_executesql @sql;
SELECT @left = isnull(sum(CASE WHEN HAVE = 0 THEN 1 ELSE 0 END), 0) FROM #eh_p;
SELECT @list = STRING_AGG(CAST(x.ID_PROMO AS nvarchar(max)), N'', '') WITHIN GROUP (ORDER BY x.START_DATE DESC, x.ID_PROMO DESC)
FROM (SELECT TOP (100) ID_PROMO, START_DATE FROM #eh_p WHERE HAVE = 0 ORDER BY START_DATE DESC, ID_PROMO DESC) AS x;

IF @why IS NULL AND @left = 0
BEGIN
    -- все акции истории в витрине — задание выключается
    EXEC msdb.dbo.sp_update_job @job_name = @job, @enabled = 0;
    SET @msg = concat(N''Догон эффекта: история догнана — порций '', @portions, N'', записано новых акций '', @written
                    , N''; все акции истории в витрине. Задание «'', @job, N''» выключено.'');
END
ELSE IF @why IS NULL
    -- проход прошёл все акции, но часть процедура не записала (скидка не готова, ошибка) — задание остаётся включённым:
    -- старше 120 дней их не посчитает никто, кроме догона
    SET @msg = concat(N''Догон эффекта: проход закончен — порций '', @portions, N'', записано новых акций '', @written
                    , N''; без расчёта остались '', @left, N'' (см. сообщения процедуры выше): '', @list, IIF(@left > 100, N'' …'', N'''')
                    , N''. Задание остаётся включённым — следующий проход завтра в '', convert(char(5), @start_at, 108), N''.'');
ELSE
    SET @msg = concat(N''Догон эффекта: остановлено — '', @why, N''; порций '', @portions, N'', записано новых акций '', @written
                    , N'', без расчёта осталось '', @left, N''. Следующий проход — завтра в '', convert(char(5), @start_at, 108), N''.'');
RAISERROR(N''%s'', 0, 1, @msg) WITH NOWAIT;
-- ошибки порций — шаг заканчивается ошибкой, чтобы было видно в истории задания
IF @errors > 0
BEGIN
    SET @msg = concat(N''Догон эффекта: упало порций — '', @errors, N'' (сообщения «ВНИМАНИЕ» выше).'');
    THROW 50003, @msg, 1;
END';

IF NOT EXISTS (SELECT 1
               FROM mci_model.sys.sql_modules AS m (nolock)
               WHERE m.object_id = OBJECT_ID(N'mci_model.dbo.CVM_EFFECT_TODAY')
                 AND m.definition LIKE N'%-- CVM[_]EFFECT[_]TODAY: методика 2026-10%')
    RAISERROR(N'СТОП по вечернему догону: в тексте dbo.CVM_EFFECT_TODAY нет метки новой версии — задание не создано.', 16, 1);
-- шаг вызывает процедуру с @date_from / @date_to (даты старта)
ELSE IF (SELECT count(*)
         FROM mci_model.sys.parameters AS p (nolock)
         WHERE p.object_id = OBJECT_ID(N'mci_model.dbo.CVM_EFFECT_TODAY')
           AND p.name IN (N'@date_from', N'@date_to')
           AND TYPE_NAME(p.user_type_id) IN (N'date', N'datetime', N'datetime2', N'smalldatetime')) <> 2
    RAISERROR(N'СТОП по вечернему догону: у dbo.CVM_EFFECT_TODAY нет параметров @date_from и @date_to с датой — задание не создано.', 16, 1);
-- шаг узнаёт посчитанные акции по номеру в I_CVM_EFFECT (ID_PROMO или ID) и ID_COMPANY
ELSE IF NOT EXISTS (SELECT 1 FROM mci_model.sys.columns AS c (nolock)
                    WHERE c.object_id = OBJECT_ID(N'mci_model.dbo.I_CVM_EFFECT') AND c.name IN (N'ID_PROMO', N'ID'))
     OR NOT EXISTS (SELECT 1 FROM mci_model.sys.columns AS c (nolock)
                    WHERE c.object_id = OBJECT_ID(N'mci_model.dbo.I_CVM_EFFECT') AND c.name = N'ID_COMPANY')
    RAISERROR(N'СТОП по вечернему догону: в dbo.I_CVM_EFFECT нет колонки номера акции (ID_PROMO или ID) или ID_COMPANY — задание не создано.', 16, 1);
ELSE IF EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name = @hjob
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(ss.session_id) FROM msdb.dbo.syssessions AS ss (nolock)))
    RAISERROR(N'ВНИМАНИЕ: задание «CVM EFFECT HISTORY» сейчас идёт — не пересоздаю, текст шага прежний.', 10, 1) WITH NOWAIT;
ELSE
BEGIN
    BEGIN TRY
        -- повторный запуск файла: прежнее задание удаляется и создаётся заново с текущим текстом шага
        IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs AS j (nolock) WHERE j.name = @hjob)
        BEGIN
            EXEC @rc = msdb.dbo.sp_delete_job @job_name = @hjob;
            IF @rc <> 0 THROW 50011, N'sp_delete_job вернула ошибку', 1;
        END

        -- владелец — как у ночного задания MCI
        SET @owner = (SELECT TOP (1) SUSER_SNAME(j.owner_sid) FROM msdb.dbo.sysjobs AS j (nolock) WHERE j.name = N'MCI Regular update');
        IF @owner IS NULL SET @owner = SUSER_SNAME();

        BEGIN TRANSACTION;
        EXEC @rc = msdb.dbo.sp_add_job @job_name = @hjob, @enabled = 1, @owner_login_name = @owner,
             @description = N'Вечерний догон истории отчёта эффекта CVM (CVM_EFFECT_JOB.sql, решение Елены 13.10.2026): dbo.CVM_EFFECT_TODAY порциями по датам старта, от новых акций к старым до 01.09.2025; новые порции 18:00–23:30; выключается само, когда история догнана. Журнал — msdb.dbo.sysjobstepslogs.';
        IF @rc <> 0 THROW 50012, N'sp_add_job вернула ошибку', 1;
        -- вывод шага: в историю задания (4) и целиком за последний проход в msdb.dbo.sysjobstepslogs (8)
        EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @hjob, @step_id = 1, @step_name = N'effect history portions',
             @subsystem = N'TSQL', @database_name = N'mci_model', @command = @cmd,
             @on_success_action = 1, @on_fail_action = 2, @flags = 12;
        IF @rc <> 0 THROW 50013, N'sp_add_jobstep вернула ошибку', 1;
        EXEC @rc = msdb.dbo.sp_add_jobschedule @job_name = @hjob, @name = N'CVM EFFECT HISTORY 18:00', @enabled = 1,
             @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1, @active_start_time = 180000;   -- каждый день, один раз в 18:00
        IF @rc <> 0 THROW 50014, N'sp_add_jobschedule вернула ошибку', 1;
        EXEC @rc = msdb.dbo.sp_add_jobserver @job_name = @hjob, @server_name = N'(local)';
        IF @rc <> 0 THROW 50015, N'sp_add_jobserver вернула ошибку', 1;
        COMMIT TRANSACTION;

        RAISERROR(N'Вечерний догон: задание «CVM EFFECT HISTORY» создано — каждый день в 18:00, новые порции до 23:30; выключится само, когда история догнана.', 10, 1) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = concat(N'СТОП по вечернему догону: ошибка ', ERROR_NUMBER(), N' — ', ERROR_MESSAGE(), N'. Задание «CVM EFFECT HISTORY» не создано.');
        RAISERROR(N'%s', 16, 1, @msg);
    END CATCH
END
GO

-- ОТКАТ раздела 2 (не выполняется): EXEC msdb.dbo.sp_delete_job @job_name = N'CVM EFFECT HISTORY';

-- ============================== 3. УДАЛЕНИЕ СОГЛАСОВАННЫХ ОБЪЕКТОВ ==============================
-- Только утверждённый Еленой список (13.10.2026), ничего сверх и ничего по шаблону имени:
--   А. процедуры (12): CVM_EFFECT_MONTH, effect_day, effect_event_processing, effect_promo_act, effect_promo_month,
--      mass_promo_effect, promo_effect, CVM_EFFECT_E_COMM, ss_cohort_new, ss_for_global_cg, ss_base_trend_price_segments,
--      ss_for_i_report; таблицы (36): 18 таблиц эффекта без писателя (I_MASS_EFFECT снята 16.10) и все 20 dbo.ss_* (ss_effect_month и
--      ss_effect_month_fplots — в обоих списках);
--   Б. I_CVM_EFFECT_MONTH, I_CVM_EFFECT_MONTH_CG, I_PROMO_EEFECT, I_PROMO_EEFECT_CG — только когда clear_data_update и
--      reload_clear_data их больше не упоминают (строки удаления из них убирает CVM_EFFECT_PROC.sql);
--   В. I_CVM_EFFECT_SEG, I_CVM_EFFECT_CG_SEG, I_CVM_EFFECT_E_COMM_OFF — НЕ удаляются: их читает портал; строка группы В
--      закомментирована — после перевода портала на I_CVM_EFFECT раскомментировать и выполнить раздел 3 ещё раз.
-- Перед удалением — проверка каждого объекта группы: зависимости (sys.sql_expression_dependencies), текст всех модулей
--   базы, команды шагов всех заданий SQL Agent, внешние ключи. Не считаются сами объекты группы и триггеры её таблиц,
--   а также упоминание в закомментированной строке (до имени на той же строке есть «--»); упоминание внутри /* */ —
--   считается. Имя ищется целиком: соседние символы — не буква, цифра или «_» (и не «#», «@» перед ним — временная
--   таблица или переменная). Объект группы где-то упоминается — СТОП по группе: из неё ничего не удаляется, где
--   упоминается — в таблице результата.
-- Группа удаляется одной транзакцией (объект занят дольше минуты — откат группы). Б проверяется после А: старые
--   процедуры эффекта к этому времени удалены и Б больше не держат.
-- Удаление необратимо. Тексты удаляемых процедур на 11.10.2026 — new projects/cvm_mci/data/procedures (Mac).
USE mci_model
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

DROP TABLE IF EXISTS #ej_obj, #ej_mod, #ej_hit, #ej_ref;
CREATE TABLE #ej_obj (
      GRP      char(1)       NOT NULL                              -- A — группа А, B — Б, C — В
    , ORD      int           NOT NULL                              -- порядок внутри группы: процедуры, затем таблицы
    , OBJ_TYPE char(1)       NOT NULL                              -- P — процедура, U — таблица
    , NAME     sysname       COLLATE DATABASE_DEFAULT NOT NULL
    , OBJ_ID   int           NULL                                  -- NULL — объекта уже нет
    , ROWS_NOW bigint        NULL                                  -- строк до удаления (таблицы)
    , PAT      nvarchar(400) COLLATE Latin1_General_BIN2 NULL      -- образец поиска имени целиком (в верхнем регистре)
    , STATUS   nvarchar(200) COLLATE DATABASE_DEFAULT NULL);
INSERT INTO #ej_obj (GRP, ORD, OBJ_TYPE, NAME) VALUES
    -- А. процедуры (12)
      ('A',   1, 'P', N'CVM_EFFECT_MONTH')
    , ('A',   2, 'P', N'effect_day')
    , ('A',   3, 'P', N'effect_event_processing')
    , ('A',   4, 'P', N'effect_promo_act')
    , ('A',   5, 'P', N'effect_promo_month')
    , ('A',   6, 'P', N'mass_promo_effect')
    , ('A',   7, 'P', N'promo_effect')
    , ('A',   8, 'P', N'CVM_EFFECT_E_COMM')
    , ('A',   9, 'P', N'ss_cohort_new')
    , ('A',  10, 'P', N'ss_for_global_cg')
    , ('A',  11, 'P', N'ss_base_trend_price_segments')
    , ('A',  12, 'P', N'ss_for_i_report')
    -- А. таблицы эффекта без писателя (18 + I_MASS_EFFECT, снята 16.10; из них две ss_)
    , ('A', 101, 'U', N'I_CVM_EFFECT_202501')
    , ('A', 102, 'U', N'I_CVM_EFFECT_E_COMM')
    , ('A', 103, 'U', N'I_CVM_EFFECT_E_COMM_CG')
    , ('A', 104, 'U', N'I_CVM_EFFECT_E_COMM_DATA')
    , ('A', 105, 'U', N'I_CVM_EFFECT_E_COMM_MONTH')
    , ('A', 106, 'U', N'I_CVM_EFFECT_E_COMM_MONTH_archive')
    , ('A', 107, 'U', N'I_CVM_EFFECT_E_COMM_OFF_CG')
    , ('A', 108, 'U', N'I_CVM_EFFECT_E_COMM_SEGMENT')
    , ('A', 109, 'U', N'I_CVM_EFFECT_MONTH_ONLINE')
    , ('A', 110, 'U', N'I_CVM_EFFECT_TODAY')
    , ('A', 111, 'U', N'I_CVM_EFFECT_TODAY_CG')
    , ('A', 112, 'U', N'I_EFFECT_EVENT')
    , ('A', 113, 'U', N'I_EFFECT_PROMO')
--  , ('A', 114, 'U', N'I_MASS_EFFECT')   -- 16.10.2026 Елена: «масс эффект не трогай пока, потом разберёмся» — её читает
                                         -- прод-портал («Массовые промо»); удалена 14.10, пустая возвращена CVM_MASS_EFFECT_EMPTY.sql
    , ('A', 115, 'U', N'I_MASS_EFFECT_CAT')
    , ('A', 116, 'U', N'I_MASS_EFFECT_CG')
    , ('A', 117, 'U', N'I_MASS_EFFECT_PRODUCTS')
    , ('A', 118, 'U', N'ss_effect_month')
    , ('A', 119, 'U', N'ss_effect_month_fplots')
    -- А. остальные dbo.ss_* (18; всего ss_ — 20)
    , ('A', 120, 'U', N'ss_basic_indicators_LFL_serg')
    , ('A', 121, 'U', N'ss_bonus_info_for_segmentation')
    , ('A', 122, 'U', N'ss_churn_model')
    , ('A', 123, 'U', N'ss_cohort_1')
    , ('A', 124, 'U', N'ss_cohort_2')
    , ('A', 125, 'U', N'ss_global_cg')
    , ('A', 126, 'U', N'ss_i_report')
    , ('A', 127, 'U', N'ss_i_report_serg')
    , ('A', 128, 'U', N'ss_i_trend_price_segment_serg')
    , ('A', 129, 'U', N'ss_mass_promo_max_data')
    , ('A', 130, 'U', N'ss_mass_promo_week_sort')
    , ('A', 131, 'U', N'ss_promo_report')
    , ('A', 132, 'U', N'ss_segment_price_index_serg')
    , ('A', 133, 'U', N'ss_segment_redemption_share_weekend')
    , ('A', 134, 'U', N'ss_segment_trend')
    , ('A', 135, 'U', N'ss_segment_trend_serg')
    , ('A', 136, 'U', N'ss_test_distribution')
    , ('A', 137, 'U', N'ss_trend_price_segment_serg_segments')
    -- Б. старые таблицы эффекта из чистки шага 1
    , ('B', 201, 'U', N'I_CVM_EFFECT_MONTH')
    , ('B', 202, 'U', N'I_CVM_EFFECT_MONTH_CG')
    , ('B', 203, 'U', N'I_PROMO_EEFECT')
    , ('B', 204, 'U', N'I_PROMO_EEFECT_CG');
-- В. после перевода портала на I_CVM_EFFECT (сейчас их читает портал — НЕ удалять): раскомментировать и выполнить раздел 3 ещё раз
-- INSERT INTO #ej_obj (GRP, ORD, OBJ_TYPE, NAME) VALUES ('C', 301, 'U', N'I_CVM_EFFECT_SEG'), ('C', 302, 'U', N'I_CVM_EFFECT_CG_SEG'), ('C', 303, 'U', N'I_CVM_EFFECT_E_COMM_OFF');

UPDATE #ej_obj
SET OBJ_ID = OBJECT_ID(N'dbo.' + QUOTENAME(NAME), OBJ_TYPE)
  , PAT    = N'%[^0-9A-Z_#@$]' + REPLACE(UPPER(NAME), N'_', N'[_]') + N'[^0-9A-Z_$]%';

UPDATE o
SET ROWS_NOW = (SELECT sum(p.rows) FROM sys.partitions AS p (nolock) WHERE p.object_id = o.OBJ_ID AND p.index_id IN (0, 1))
FROM #ej_obj AS o
WHERE o.OBJ_TYPE = 'U' AND o.OBJ_ID IS NOT NULL;

-- тексты модулей и команд шагов (верхний регистр, двоичное сравнение) и найденные упоминания
CREATE TABLE #ej_mod (REF_NAME nvarchar(400) COLLATE DATABASE_DEFAULT NOT NULL, TXT nvarchar(max) COLLATE Latin1_General_BIN2 NOT NULL);
CREATE TABLE #ej_hit (ID int IDENTITY(1, 1) NOT NULL PRIMARY KEY, NAME sysname COLLATE DATABASE_DEFAULT NOT NULL
                    , PAT nvarchar(400) COLLATE Latin1_General_BIN2 NOT NULL, REF_NAME nvarchar(400) COLLATE DATABASE_DEFAULT NOT NULL
                    , TXT nvarchar(max) COLLATE Latin1_General_BIN2 NOT NULL, LIVE bit NULL);
CREATE TABLE #ej_ref (NAME sysname COLLATE DATABASE_DEFAULT NOT NULL, KIND nvarchar(30) COLLATE DATABASE_DEFAULT NOT NULL
                    , REF_NAME nvarchar(400) COLLATE DATABASE_DEFAULT NOT NULL);

DECLARE @g int = 0, @grp char(1), @gname nchar(1), @n int, @unread int, @sql nvarchar(max), @msg nvarchar(2000), @refs nvarchar(max)
      , @id int, @txt nvarchar(max), @pat nvarchar(400), @pos bigint, @p bigint, @at bigint, @lf bigint, @ls bigint, @live bit;

WHILE @g < 3
BEGIN
    SET @g += 1;
    SELECT @grp = CHOOSE(@g, 'A', 'B', 'C'), @gname = CHOOSE(@g, N'А', N'Б', N'В');
    IF NOT EXISTS (SELECT 1 FROM #ej_obj WHERE GRP = @grp) CONTINUE;

    -- что есть сейчас (группа А могла удалить что-то до Б; объекты, удалённые раньше, — «уже нет»)
    UPDATE #ej_obj SET OBJ_ID = OBJECT_ID(N'dbo.' + QUOTENAME(NAME), OBJ_TYPE) WHERE GRP = @grp;
    UPDATE #ej_obj SET STATUS = N'уже нет' WHERE GRP = @grp AND OBJ_ID IS NULL;
    IF NOT EXISTS (SELECT 1 FROM #ej_obj WHERE GRP = @grp AND OBJ_ID IS NOT NULL)
    BEGIN
        RAISERROR(N'Удаление, группа %s: объектов группы уже нет.', 10, 1, @gname) WITH NOWAIT;
        CONTINUE;
    END

    DELETE FROM #ej_ref;
    DELETE FROM #ej_hit;
    DELETE FROM #ej_mod;

    -- текст модуля не прочитать (шифрование или нет прав) — проверить нельзя
    SET @unread = (SELECT count(*) FROM sys.sql_modules AS m (nolock) WHERE m.definition IS NULL);
    IF @unread > 0
        INSERT INTO #ej_ref (NAME, KIND, REF_NAME)
        VALUES (N'*', N'нет текста', concat(@unread, N' модулей базы без текста — проверить упоминания нельзя'));

    -- 1) зависимости (без самих объектов группы и триггеров её таблиц)
    INSERT INTO #ej_ref (NAME, KIND, REF_NAME)
    SELECT DISTINCT o.NAME, N'зависимость'
         , isnull(OBJECT_SCHEMA_NAME(d.referencing_id) + N'.' + OBJECT_NAME(d.referencing_id), concat(N'объект ', d.referencing_id))
    FROM #ej_obj AS o
         JOIN sys.sql_expression_dependencies AS d (nolock)
           ON d.referenced_id = o.OBJ_ID
           OR (d.referenced_id IS NULL
               AND d.referenced_server_name IS NULL
               AND isnull(d.referenced_database_name, DB_NAME()) COLLATE DATABASE_DEFAULT = DB_NAME()
               AND isnull(d.referenced_schema_name, N'dbo') COLLATE DATABASE_DEFAULT = N'dbo'
               AND d.referenced_entity_name COLLATE DATABASE_DEFAULT = o.NAME)
    WHERE o.GRP = @grp AND o.OBJ_ID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM #ej_obj AS x WHERE x.GRP = @grp AND x.OBJ_ID = d.referencing_id)
      AND NOT EXISTS (SELECT 1
                      FROM sys.objects AS t (nolock)
                           JOIN #ej_obj AS x ON x.GRP = @grp AND x.OBJ_ID = t.parent_object_id
                      WHERE t.object_id = d.referencing_id);

    -- 2) текст модулей базы (без объектов группы и триггеров её таблиц) и команды шагов всех заданий SQL Agent
    INSERT INTO #ej_mod (REF_NAME, TXT)
    SELECT concat(OBJECT_SCHEMA_NAME(m.object_id), N'.', OBJECT_NAME(m.object_id)), UPPER(N' ' + m.definition + N' ')
    FROM sys.sql_modules AS m (nolock)
    WHERE m.definition IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM #ej_obj AS x WHERE x.GRP = @grp AND x.OBJ_ID = m.object_id)
      AND NOT EXISTS (SELECT 1
                      FROM sys.objects AS t (nolock)
                           JOIN #ej_obj AS x ON x.GRP = @grp AND x.OBJ_ID = t.parent_object_id
                      WHERE t.object_id = m.object_id);
    INSERT INTO #ej_mod (REF_NAME, TXT)
    SELECT concat(N'задание «', j.name, N'», шаг ', s.step_id, N' ', s.step_name), UPPER(N' ' + s.command + N' ')
    FROM msdb.dbo.sysjobsteps AS s (nolock)
         JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
    WHERE s.command IS NOT NULL;

    INSERT INTO #ej_hit (NAME, PAT, REF_NAME, TXT)
    SELECT o.NAME, o.PAT, t.REF_NAME, t.TXT
    FROM #ej_obj AS o
         JOIN #ej_mod AS t ON t.TXT LIKE o.PAT
    WHERE o.GRP = @grp AND o.OBJ_ID IS NOT NULL;

    -- каждое вхождение: если до имени на той же строке есть «--», оно закомментировано и не считается
    SET @id = 0;
    WHILE 1 = 1
    BEGIN
        SELECT TOP (1) @id = ID, @txt = TXT, @pat = PAT FROM #ej_hit WHERE ID > @id ORDER BY ID;
        IF @@ROWCOUNT = 0 BREAK;
        SELECT @pos = 1, @live = 0;
        WHILE @live = 0
        BEGIN
            SET @p = PATINDEX(@pat, SUBSTRING(@txt, @pos, 2147483647) COLLATE Latin1_General_BIN2);
            IF @p = 0 BREAK;
            SET @at = @pos + @p - 1;                                      -- символ перед именем
            SET @lf = CHARINDEX(NCHAR(10), REVERSE(LEFT(@txt, @at)));
            SET @ls = CASE WHEN @lf = 0 THEN 1 ELSE @at - @lf + 2 END;    -- начало строки
            IF CHARINDEX(N'--', SUBSTRING(@txt, @ls, @at - @ls + 1)) = 0
                SET @live = 1;
            SET @pos = @at + 1;
        END
        UPDATE #ej_hit SET LIVE = @live WHERE ID = @id;
    END
    INSERT INTO #ej_ref (NAME, KIND, REF_NAME)
    SELECT DISTINCT NAME, N'текст', REF_NAME FROM #ej_hit WHERE LIVE = 1;

    -- 3) внешние ключи из таблиц вне группы
    INSERT INTO #ej_ref (NAME, KIND, REF_NAME)
    SELECT o.NAME, N'внешний ключ', concat(OBJECT_SCHEMA_NAME(f.parent_object_id), N'.', OBJECT_NAME(f.parent_object_id), N' (', f.name, N')')
    FROM #ej_obj AS o
         JOIN sys.foreign_keys AS f (nolock) ON f.referenced_object_id = o.OBJ_ID
    WHERE o.GRP = @grp AND o.OBJ_ID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM #ej_obj AS x WHERE x.GRP = @grp AND x.OBJ_ID = f.parent_object_id);

    IF EXISTS (SELECT 1 FROM #ej_ref)
    BEGIN
        UPDATE #ej_obj SET STATUS = N'СТОП: в группе есть упоминаемые объекты — не удалена' WHERE GRP = @grp AND OBJ_ID IS NOT NULL;
        SELECT [Группа] = @gname, [Объект] = r.NAME, [Где упоминается] = r.REF_NAME, [Как] = r.KIND
        FROM #ej_ref AS r
        ORDER BY r.NAME, r.REF_NAME;
        SELECT @refs = STRING_AGG(CAST(concat(r.NAME, N' ← ', r.REF_NAME) AS nvarchar(max)), N'; ')
        FROM (SELECT DISTINCT NAME, REF_NAME FROM #ej_ref) AS r;
        SET @msg = concat(N'СТОП по удалению, группа ', @gname, N': объекты ещё упоминаются — ', left(@refs, 1400)
                        , IIF(len(@refs) > 1400, N' …', N''), N'. Из группы ничего не удалено.'
                        , IIF(@grp = 'B' AND @refs LIKE N'%clear[_]data%'
                            , N' В процедурах чистки шага 1 остались строки удаления этих таблиц — сначала правка чистки из CVM_EFFECT_PROC.sql.', N''));
        RAISERROR(N'%s', 16, 1, @msg);
        CONTINUE;
    END

    -- 4) удаление группы одной транзакцией: процедуры, затем таблицы
    SELECT @n = count(*)
         , @sql = STRING_AGG(CAST(concat(IIF(OBJ_TYPE = 'P', N'DROP PROCEDURE dbo.', N'DROP TABLE dbo.'), QUOTENAME(NAME), N';') AS nvarchar(max)), NCHAR(10))
                  WITHIN GROUP (ORDER BY ORD)
    FROM #ej_obj
    WHERE GRP = @grp AND OBJ_ID IS NOT NULL;
    BEGIN TRY
        SET LOCK_TIMEOUT 60000;   -- объект занят дольше минуты — ошибка и откат группы, а не ожидание без конца
        BEGIN TRANSACTION;
        EXEC sys.sp_executesql @sql;
        COMMIT TRANSACTION;
        SET LOCK_TIMEOUT -1;
        UPDATE #ej_obj SET STATUS = N'удалена' WHERE GRP = @grp AND OBJ_ID IS NOT NULL;
        SET @msg = concat(N'Удаление, группа ', @gname, N': удалено объектов — ', @n, N'.');
        RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET LOCK_TIMEOUT -1;
        UPDATE #ej_obj SET STATUS = N'СТОП: ошибка при удалении — группа откачена' WHERE GRP = @grp AND OBJ_ID IS NOT NULL;
        SET @msg = concat(N'СТОП по удалению, группа ', @gname, N': ошибка ', ERROR_NUMBER(), N' — ', ERROR_MESSAGE()
                        , N'. Всё откачено, из группы ничего не удалено.');
        RAISERROR(N'%s', 16, 1, @msg);
    END CATCH
END

-- итог раздела
SELECT [Группа]     = CASE GRP WHEN 'A' THEN N'А' WHEN 'B' THEN N'Б' ELSE N'В' END
     , [Тип]        = IIF(OBJ_TYPE = 'P', N'процедура', N'таблица')
     , [Объект]     = N'dbo.' + NAME
     , [Строк было] = ROWS_NOW
     , [Итог]       = STATUS
FROM #ej_obj
ORDER BY GRP, ORD;
GO

-- ============================== 4. ИТОГ ==============================
-- Шаги «MCI Regular update» вокруг правок (переходы: 1 — выход с успехом, 2 — выход с ошибкой, 3 — следующий шаг,
--   4 — на шаг N) и задание вечернего догона (next_run_* SQL Agent обновляет не сразу).
USE msdb
GO

SELECT s.step_id, s.step_name, [command] = left(s.command, 120)
     , s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s (nolock)
     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update'
  AND (s.step_id BETWEEN 19 AND 26 OR s.step_id BETWEEN 40 AND 43 OR s.step_id >= 49
       OR s.step_name IN (N'DISCOUNT_MART', N'PROMO_CAMPAIGN_LINK', N'DELIVERY_REPORT', N'CVM_EFFECT_TODAY'))
ORDER BY s.step_id;

SELECT j.name, j.enabled, [schedule] = sc.name, sc.active_start_time, js.next_run_date, js.next_run_time
FROM msdb.dbo.sysjobs AS j (nolock)
     LEFT JOIN msdb.dbo.sysjobschedules AS js (nolock) ON js.job_id = j.job_id
     LEFT JOIN msdb.dbo.sysschedules AS sc (nolock) ON sc.schedule_id = js.schedule_id
WHERE j.name = N'CVM EFFECT HISTORY';
GO

SET NOEXEC OFF;
GO
