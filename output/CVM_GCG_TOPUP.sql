-- ============================================================================================
-- CVM_GCG_TOPUP — выделение ГКГ: замена процедуры dbo.NEW_FOR_GLOBAL_CG, разовая догонка сейчас
-- и включение её шага 33 «GLOBAL_CG» в «MCI Regular update».
-- Запустить целиком в SSMS (F5) ПОСЛЕ CVM_GCG_REPORT.sql (нужны партия 1 и процедура gcg_batch_add)
-- и ДО сборки списков акций со стартом 14.10 — тогда выборки уже исключат новую ГКГ.
--
--   1. dbo.NEW_FOR_GLOBAL_CG заменяется: нарезка ГКГ из клиентов, которых ещё не разыгрывали
--      (первая покупка — со дня после последней партии журнала I_GCG_BATCH), и заведение их партией отчёта ГКГ.
--      Логика нарезки — скрипт июньской нарезки (data/global cg_new_june.sql), без изменений:
--        когорты по месяцу последней покупки; внутри когорты клиенты сортируются по LTV (до 100 р.),
--        бюджету (до 10 р.), неделе первой покупки (по убыванию) и неделе последней покупки; режутся на блоки
--        по 35 (RN / 35); из каждого блока 1 случайный клиент в ГКГ; цикл идёт, пока расхождение средних ГКГ
--        и остальных и по бюджету ≥ 0,05 %, и по LTV ≥ 0,07 % (как в июньском скрипте; бюджет и LTV здесь одна сумма).
--      Прежняя процедура каждую ночь заново тянула 1/34 из когорты прошлого месяца без учёта уже вытянутых
--      и ставила I_CONTACT.CONTROL_GROUP = 1 (признак обнуляет интеграция). Новая режет каждого клиента один раз,
--      I_CONTACT не трогает. Признаки канала и пуша из I_CONTACT не читаются (в нарезке не участвуют).
--      Запись ГКГ в I_GLOBAL_CG и заведение партии — одной транзакцией.
--   2. Догонка сейчас: клиенты с первой покупкой с 02.07.2026 по последний день данных; в отчёте ГКГ — с 14.10.2026.
--   3. Шаг 33 «GLOBAL_CG» (exec NEW_FOR_GLOBAL_CG) снова включается в цепочку сразу после 32:
--      32 → 33 → (куда 32 ведёт сейчас — 53 CLUSTERS → 34); при ошибке шага 33 — тоже дальше по цепочке.
--      Выбор набора (решение 13.10.2026): до 200 случайных наборов на когорту, берётся лучший по расхождению средних
--      бюджета и LTV; если расхождение до 0,07 % попалось раньше — перебор останавливается сразу. Так и в догонке, и ночью.
--      Процедура режет только тогда, когда в данных появился полный прошлый месяц (обычно в ночь на 1-е):
--      клиенты с первой покупкой со дня после последней партии по конец прошлого месяца; в остальные ночи
--      ничего не делает (одно чтение журнала и последнего дня данных).
-- Нагрузка на каждую нарезку: два прохода по истории I_CHECKHEADER (нарезка и заведение партии).
-- ============================================================================================

USE mci_model
GO

-- ============================== 1. ПРОЦЕДУРА ВЫДЕЛЕНИЯ ГКГ ==============================
CREATE OR ALTER PROCEDURE dbo.NEW_FOR_GLOBAL_CG
      @FIRST_FROM date = NULL   -- первая покупка с; NULL — день после последней партии
    , @FIRST_TO   date = NULL   -- первая покупка по; NULL — конец последнего полного месяца данных
    , @DRAW_DATE  date = NULL   -- с этой даты партия в отчёте ГКГ; NULL — сегодня
    , @PRE_TO     date = NULL   -- «ДО» и сегмент партии — покупки до этой даты; NULL — день после @FIRST_TO
    , @max_tries  int  = 200    -- лучший из 200 наборов на когорту (решение 13.10.2026); NULL — без предела, как в июньском скрипте
    , @fail_if_not_converged bit = 0   -- 1 = когорта не сошлась за @max_tries — ничего не писать, упасть (ночной шаг)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;

    -- последний день данных (с нижней границей — без полного скана; если за 40 дней чеков нет — прежний запрос)
    DECLARE @last date = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock)
                          WHERE ID_COMPANY = @idc AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
    IF @last IS NULL
        SET @last = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc);

    DECLARE @prev_to date = (SELECT max(FIRST_TO) FROM dbo.I_GCG_BATCH (nolock));
    IF @prev_to IS NULL
        THROW 50030, N'NEW_FOR_GLOBAL_CG: в журнале I_GCG_BATCH нет ни одной партии — сначала CVM_GCG_REPORT.sql (июльская партия)', 1;

    SET @FIRST_FROM = isnull(@FIRST_FROM, dateadd(day, 1, @prev_to));
    SET @FIRST_TO   = isnull(@FIRST_TO, IIF(@last = eomonth(@last), @last, eomonth(dateadd(month, -1, @last))));
    SET @DRAW_DATE  = isnull(@DRAW_DATE, cast(getdate() AS date));
    SET @PRE_TO     = isnull(@PRE_TO, dateadd(day, 1, @FIRST_TO));

    IF @FIRST_FROM > @FIRST_TO
    BEGIN
        PRINT N'NEW_FOR_GLOBAL_CG: резать нечего — последняя партия покрывает первые покупки по ' + convert(nvarchar(10), @prev_to, 104);
        RETURN;
    END
    IF @FIRST_FROM <> dateadd(day, 1, @prev_to)
        THROW 50031, N'NEW_FOR_GLOBAL_CG: диапазон первой покупки должен начинаться со дня после последней партии — иначе часть клиентов останется неразыгранной или попадёт в жеребьёвку дважды', 1;
    IF @FIRST_TO > @last
        THROW 50032, N'NEW_FOR_GLOBAL_CG: конец диапазона первой покупки позже последнего дня данных', 1;

    -- 1. Клиенты диапазона: профиль за всю историю (#first_purch июньского скрипта)
    DROP TABLE IF EXISTS #tu_first;
    SELECT a.ID_CONTACT
         , MIN_DATA       = min(a.DATA)
         , MAX_YM         = max(a.YEAR_MONTH)
         , LTV            = sum(a.COST_DISCOUNT)
         , LTV_100        = round(sum(a.COST_DISCOUNT) / 100, 0) * 100
         , BUDGET         = sum(a.COST_DISCOUNT)
         , BUDGET_10      = round(sum(a.COST_DISCOUNT) / 10, 0) * 10
         , COUNT_CHECK    = count(a.ID_CHECK)
         , MIN_WEEKNUMBER = min(a.WEEKNUMBER)
         , MAX_WEEKNUMBER = max(a.WEEKNUMBER)
    INTO #tu_first
    FROM dbo.I_CHECKHEADER AS a (nolock)
    WHERE a.ID_COMPANY = @idc
      AND a.ID_CONTACT <> 0
      AND a.DATA <= @last
    GROUP BY a.ID_CONTACT
    HAVING min(a.DATA) >= @FIRST_FROM
       AND min(a.DATA) <= @FIRST_TO
    OPTION (RECOMPILE);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_tu_first ON #tu_first;

    IF NOT EXISTS (SELECT 1 FROM #tu_first)
    BEGIN
        PRINT N'NEW_FOR_GLOBAL_CG: новых клиентов в диапазоне нет — партия не заводится';
        RETURN;
    END

    DROP TABLE IF EXISTS #tu_gcg;
    SELECT ID_CONTACT
    INTO #tu_gcg
    FROM dbo.I_GLOBAL_CG (nolock)
    WHERE ID_COMPANY = @idc AND CONTROL_GROUP = 1
    GROUP BY ID_CONTACT;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_tu_gcg ON #tu_gcg;

    -- нарезка и партия пишутся одной транзакцией (ниже), поэтому «ГКГ записана, партии нет» не бывает.
    -- Клиенты диапазона уже в ГКГ = диапазон резали в обход процедуры: партию без жеребьёвки не заводим.
    DECLARE @in_gcg int = (SELECT count(*) FROM #tu_first AS f JOIN #tu_gcg AS g ON g.ID_CONTACT = f.ID_CONTACT);
    IF @in_gcg > 0
    BEGIN
        DECLARE @msg nvarchar(400) = concat(N'NEW_FOR_GLOBAL_CG: ', @in_gcg, N' клиентов диапазона уже в ГКГ — нарезка и партия не делаются, нужно решение');
        THROW 50033, @msg, 1;
    END

    DROP TABLE IF EXISTS #tu_cg_new;
    CREATE TABLE #tu_cg_new (ID_CONTACT int NOT NULL, MAX_YM int NOT NULL);

    DROP TABLE IF EXISTS #tu_log;
    CREATE TABLE #tu_log (MAX_YM int NOT NULL, CLIENTS int NULL, GCG int NULL, TRIES int NULL,
                          ERR_BUDGET float NULL, ERR_LTV float NULL, CONVERGED bit NULL);

    -- 2. Нарезка по когортам — логика июньского скрипта
    DROP TABLE IF EXISTS #tu_ym;
    SELECT MAX_YM, RN = row_number() OVER (ORDER BY MAX_YM DESC)
    INTO #tu_ym
    FROM #tu_first
    GROUP BY MAX_YM;

    DECLARE @i int = 1, @ym int, @error float, @error1 float, @tries int, @best float, @maxerr float, @conv bit;
    SET @ym = (SELECT MAX_YM FROM #tu_ym WHERE RN = @i);

    WHILE @ym IS NOT NULL
    BEGIN
        DROP TABLE IF EXISTS #tu_interval;
        SELECT ID_CONTACT
             , RN = row_number() OVER (ORDER BY LTV_100, BUDGET_10, MIN_WEEKNUMBER DESC, MAX_WEEKNUMBER)
        INTO #tu_interval
        FROM #tu_first
        WHERE MAX_YM = @ym;

        SET @error  = 1.0;
        SET @error1 = 1.0;
        SET @tries  = 0;
        SET @best   = 999;
        DROP TABLE IF EXISTS #tu_best;
        CREATE TABLE #tu_best (ID_CONTACT int NOT NULL);

        WHILE @error >= 0.0005
          AND @error1 >= 0.0007
          AND (@max_tries IS NULL OR @tries < @max_tries)
        BEGIN
            SET @tries += 1;

            DROP TABLE IF EXISTS #tu_cg;
            WITH t AS (SELECT ID_CONTACT, SEGMENT = RN / 35 FROM #tu_interval)
               , a AS (SELECT ID_CONTACT, NI = row_number() OVER (PARTITION BY SEGMENT ORDER BY newid()) FROM t)
            SELECT ID_CONTACT
            INTO #tu_cg
            FROM a
            WHERE NI <= 1;

            DROP TABLE IF EXISTS #tu_stat;
            SELECT CG            = IIF(b.ID_CONTACT IS NOT NULL, 1, 0)
                 , COST_DISCOUNT = sum(a.BUDGET) / count(a.ID_CONTACT)
                 , LTV           = sum(a.LTV) / count(a.ID_CONTACT)
            INTO #tu_stat
            FROM #tu_first AS a
                 LEFT JOIN #tu_cg AS b ON b.ID_CONTACT = a.ID_CONTACT
            WHERE a.MAX_YM = @ym
            GROUP BY IIF(b.ID_CONTACT IS NOT NULL, 1, 0);

            SET @error  = (SELECT abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
                           FROM #tu_stat AS a JOIN #tu_stat AS b ON a.CG = 0 AND b.CG = 1);
            SET @error1 = (SELECT abs(a.LTV - b.LTV) / a.LTV
                           FROM #tu_stat AS a JOIN #tu_stat AS b ON a.CG = 0 AND b.CG = 1);

            -- лучший набор — нужен только при пределе попыток
            SET @maxerr = IIF(@error > @error1, @error, @error1);
            IF @max_tries IS NOT NULL AND @maxerr < @best
            BEGIN
                SET @best = @maxerr;
                TRUNCATE TABLE #tu_best;
                INSERT INTO #tu_best (ID_CONTACT) SELECT ID_CONTACT FROM #tu_cg;
            END
        END

        SET @conv = IIF(@error >= 0.0005 AND @error1 >= 0.0007, 0, 1);

        -- ночной шаг: не сошлось за предел попыток — ничего не записано (запись ниже, после всех когорт), шаг падает,
        -- следующей ночью нарезка повторится с новыми случайными наборами; критерий 0,07 % не ослабляется
        IF @conv = 0 AND @fail_if_not_converged = 1
        BEGIN
            DECLARE @msg_conv nvarchar(400) = concat(N'NEW_FOR_GLOBAL_CG: когорта ', @ym, N' не сошлась за ', @tries,
                                                     N' попыток — ничего не записано, следующая ночь повторит нарезку');
            THROW 50034, @msg_conv, 1;
        END

        IF @conv = 1 OR @max_tries IS NULL
            INSERT INTO #tu_cg_new (ID_CONTACT, MAX_YM) SELECT ID_CONTACT, @ym FROM #tu_cg GROUP BY ID_CONTACT;
        ELSE
            INSERT INTO #tu_cg_new (ID_CONTACT, MAX_YM) SELECT ID_CONTACT, @ym FROM #tu_best GROUP BY ID_CONTACT;

        INSERT INTO #tu_log (MAX_YM, CLIENTS, GCG, TRIES, ERR_BUDGET, ERR_LTV, CONVERGED)
        SELECT @ym, (SELECT count(*) FROM #tu_interval), (SELECT count(*) FROM #tu_cg_new WHERE MAX_YM = @ym)
             , @tries, @error, @error1, @conv;

        RAISERROR(N'NEW_FOR_GLOBAL_CG: когорта %d готова, попыток: %d', 0, 1, @ym, @tries) WITH NOWAIT;

        SET @i += 1;
        SET @ym = (SELECT MAX_YM FROM #tu_ym WHERE RN = @i);
    END

    -- 3–4. Запись ГКГ и заведение партии — одной транзакцией: при ошибке или отмене не остаётся ни нарезки, ни партии,
    --      повторный запуск режет заново; параллельный запуск откатится на проверке пересечения партий / PK BATCH_ID
    DECLARE @batch int = (SELECT max(BATCH_ID) + 1 FROM dbo.I_GCG_BATCH (nolock));
    DECLARE @note nvarchar(200) = N'донарезка ГКГ: первая покупка ' + convert(nvarchar(10), @FIRST_FROM, 104)
                                + N'–' + convert(nvarchar(10), @FIRST_TO, 104);
    BEGIN TRANSACTION;

    INSERT INTO dbo.I_GLOBAL_CG (ID_CONTACT, CONTROL_GROUP, ID_COMPANY)
    SELECT ID_CONTACT, 1, @idc
    FROM #tu_cg_new
    GROUP BY ID_CONTACT;

    EXEC dbo.gcg_batch_add @BATCH_ID = @batch, @DRAW_DATE = @DRAW_DATE, @FIRST_FROM = @FIRST_FROM, @FIRST_TO = @FIRST_TO
                         , @PRE_TO = @PRE_TO, @NOTE = @note;

    COMMIT TRANSACTION;

    -- 5. Итог по когортам
    SELECT [Когорта (месяц последней покупки)] = MAX_YM
         , [Клиентов]                = CLIENTS
         , [В ГКГ]                   = GCG
         , [Доля ГКГ, %]             = cast(100.0 * GCG / nullif(CLIENTS, 0) AS decimal(8, 3))
         , [Попыток]                 = TRIES
         , [Расхождение бюджета, %]  = cast(100.0 * ERR_BUDGET AS decimal(18, 4))
         , [Расхождение LTV, %]      = cast(100.0 * ERR_LTV AS decimal(18, 4))
         , [Сошлось]                 = IIF(CONVERGED = 1, N'да', N'нет — взят лучший набор')
    FROM #tu_log
    ORDER BY MAX_YM DESC;
END
GO

-- ============================== 2. ДОГОНКА СЕЙЧАС ==============================
-- клиенты с первой покупкой с 02.07.2026 по последний день данных; в отчёте ГКГ — с 14.10.2026
SET NOCOUNT ON;

DECLARE @act date = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock)
                     WHERE ID_COMPANY = 1 AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
IF @act IS NULL
    SET @act = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock) WHERE ID_COMPANY = 1);
DECLARE @pre  date = dateadd(day, 1, @act);
DECLARE @draw date = IIF(cast(getdate() AS date) > '20261014', cast(getdate() AS date), '20261014');

-- лучший из 200 наборов на когорту (остановка раньше, если расхождение до 0,07 %); в итоге по когортам видно
-- «Сошлось» и достигнутое расхождение
IF NOT EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock) WHERE FIRST_FROM = '20260702')
    EXEC dbo.NEW_FOR_GLOBAL_CG @FIRST_FROM = '20260702', @FIRST_TO = @act, @DRAW_DATE = @draw, @PRE_TO = @pre
                             , @max_tries = 200;

-- журнал партий
SELECT [Партия] = BATCH_ID, [Дата нарезки] = DRAW_DATE, [Первая покупка с] = FIRST_FROM, [по] = FIRST_TO
     , [«ДО» до] = PRE_TO, [Клиентов] = CLIENTS, [В ГКГ] = GCG_CLIENTS
     , [Доля ГКГ, %] = cast(100.0 * GCG_CLIENTS / nullif(CLIENTS, 0) AS decimal(8, 3)), [Примечание] = NOTE
FROM dbo.I_GCG_BATCH (nolock)
ORDER BY BATCH_ID;
GO

-- ============================== 3. ШАГ 33 «GLOBAL_CG» СНОВА В ЦЕПОЧКЕ ==============================
-- Сейчас шаг 32 при успехе переходит на 53 (CLUSTERS → 34) — шаг 33 (exec NEW_FOR_GLOBAL_CG) не выполняется.
-- Включаем: 32 → 33 → 53; при ошибке шага 33 цепочка тоже идёт дальше (раньше — письмо об ошибке и остановка).
-- Номера шагов не меняются. Шаг включается только после догонки: иначе ночью процедура нарезала бы
-- 02.07–30.09 с «ДО» до 01.10 вместо догонки по последний день данных.
USE msdb
GO

SET NOCOUNT ON;

DECLARE @rc int = 0, @next int;

IF NOT EXISTS (SELECT 1 FROM mci_model.dbo.I_GCG_BATCH (nolock) WHERE FIRST_FROM = '20260702')
BEGIN
    RAISERROR(N'СТОП: догонка (раздел 2) не заведена — шаг 33 не включаю.', 16, 1);
    GOTO EndSave;
END

-- куда шаг 32 ведёт сейчас при успехе (сейчас — 53 CLUSTERS, дальше 34); туда же поведёт шаг 33
SELECT @next = s.on_success_step_id
FROM msdb.dbo.sysjobsteps AS s (nolock)
     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update' AND s.step_id = 32 AND s.step_name = N'I_CONTACT_PRICE_SEGMENT'
  AND s.on_success_action = 4;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
               WHERE j.name = N'MCI Regular update' AND s.step_id = 33 AND s.step_name = N'GLOBAL_CG'
                 AND s.command LIKE N'%NEW[_]FOR[_]GLOBAL[_]CG%')
   OR @next IS NULL
   OR (@next <> 33 AND NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
                                   WHERE j.name = N'MCI Regular update' AND s.step_id = @next))
BEGIN
    RAISERROR(N'СТОП: шаги 32/33 не те, что ожидались (32 I_CONTACT_PRICE_SEGMENT с переходом на шаг, 33 GLOBAL_CG = NEW_FOR_GLOBAL_CG) — задание не меняю.', 16, 1);
    GOTO EndSave;
END

-- 32 уже ведёт в 33: команду ставим, только если шаг 33 при ошибке идёт туда же, куда при успехе
-- (так его ставит этот раздел); иначе любая ошибка нарезки уведёт задание на 47 и остановит цепочку
IF @next = 33 AND NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
                              WHERE j.name = N'MCI Regular update' AND s.step_id = 33
                                AND s.on_success_action = 4 AND s.on_fail_action = 4
                                AND s.on_fail_step_id = s.on_success_step_id)
BEGIN
    RAISERROR(N'СТОП: 32 уже ведёт в 33, но шаг 33 при ошибке идёт не туда же, куда при успехе — задание не меняю.', 16, 1);
    GOTO EndSave;
END

BEGIN TRANSACTION;

-- в цепочку — только если ещё не включён (32 уже ведёт в 33 — переходы не трогаем)
IF @next <> 33
BEGIN
    -- 32 → 33 (переход 32 при ошибке не меняется)
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = N'MCI Regular update', @step_id = 32,
         @on_success_action = 4, @on_success_step_id = 33;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

    -- 33 → туда, куда раньше вёл 32, и при успехе, и при ошибке
    EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = N'MCI Regular update', @step_id = 33,
         @on_success_action = 4, @on_success_step_id = @next,
         @on_fail_action = 4, @on_fail_step_id = @next;
    IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;
END

-- команда шага: лучший из 200 наборов на когорту (остановка раньше при расхождении до 0,07 %)
EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = N'MCI Regular update', @step_id = 33,
     @command = N'exec [dbo].[NEW_FOR_GLOBAL_CG] @max_tries = 200;';
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

COMMIT TRANSACTION;
GOTO EndSave;

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;
    RAISERROR(N'СТОП: переходы не изменены (ошибка sp_update_jobstep) — всё откачено, задание не изменено.', 16, 1);
EndSave:

-- шаги 31–35 и 53
SELECT s.step_id, s.step_name, s.command, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s (nolock)
     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update' AND (s.step_id BETWEEN 31 AND 35 OR s.step_name = N'CLUSTERS')
ORDER BY s.step_id;
GO
