-- ============================================================================================
-- CVM_TRENDS_HISTORY_20261031 — пересчёт истории трендов 202401–202609 (I_TREND, I_TREND_PRICE_SEGMENT) исправленной
-- процедурой dbo.trends (CVM_TRENDS_FIX_20261031.sql) порциями по вечерам, от новых месяцев к старым.
-- Выполняется заданием CVM_API_nightly из обменника ПОСЛЕ CVM_TRENDS_FIX_20261031 (по имени он идёт раньше);
-- пока процедура не исправлена — «СТОП (подождать)», повтор через 10 минут. Повтор после успеха безопасен.
--
-- Почему порциями: месяц трендов — ~25 мин расчёта плюс ~40 мин подготовки на вызов (недели покупок за 56 недель,
--   чеки прошлого месяца), 33 месяца — ~15–20 ч. Днём идут ночные джобы MCI (перезагрузка с 01:00, «MCI Regular
--   update» ~05:30–18:05) — одним запуском расчёт пересёкся бы с ними. Окно — вечер, после конца ночного прохода до 00:30.
-- Что создаёт (временно, на время пересчёта):
--   1. dbo.CVM_TRENDS_REBUILD — одна строка отметки хода: до какого месяца уже пересчитано, оценки времени, заметка.
--   2. dbo.cvm_trends_rebuild_step — вечерний проход: порция — до MAX_CHUNK месяцев подряд, от новых к старым,
--      столько, сколько успеет до 00:30 с запасом 30 мин по оценке (подготовка PREP_MIN + месяцы × MONTH_MIN; оценка
--      месяца уточняется по факту каждой порции). Первая порция — один месяц (202609), дальше — до 6. Порция не
--      начинается, если не вечер (00:30–17:00) или идёт ночной джоб MCI / CVM REBUILD ONCE. Отметка сдвигается после
--      посчитанной порции (упала — пересчитается целиком в следующий раз; после 3 сбоев подряд задание выключается,
--      текст ошибки — в LAST_NOTE). Второй экземпляр не стартует (замок). Дошёл до 202401 — выключает своё задание.
--   3. Задание «CVM TRENDS REBUILD» — один запуск в день, в 17:00; пока идёт ночной проход MCI, проход ждёт его конца
--      (проверка раз в 5 минут), лишних запусков нет.
--   4. Разовое задание «CVM REBUILD ONCE» (07.10, шаг 1 — trends с 202401 одним куском ~20 ч) выключается.
-- После окончания пересчёта задание, процедура прохода и таблица удаляются отдельным скриптом.
-- Сроки (оценка, месяц ~25 мин + ~45 мин подготовки на порцию): ~8 месяцев за вечер — 31.10 февраль–сентябрь 2026,
--   01.11 — до середины 2025, к утру 04.11 — вся история. Задание CVM_CLIENT_ITEM_HISTORY (идёт с 23.10) не ждём.
--   Пока порция считается, её ещё не посчитанных месяцев в отчёте нет (удаляются в начале порции, пишутся по месяцу).
-- Ход: SELECT * FROM dbo.CVM_TRENDS_REBUILD (nolock);
-- Сменить размер порции: UPDATE dbo.CVM_TRENDS_REBUILD SET MAX_CHUNK = 3;
-- Внеплановый проход: EXEC msdb.dbo.sp_start_job N'CVM TRENDS REBUILD';  (не EXEC процедуры в окне SSMS: прерванный
--   по Stop, он держит замок прохода, пока окно открыто, и вечерние проходы пропускаются)
-- ============================================================================================

USE mci_model
GO
SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
-- исправленная процедура (параметр @YM_STOP) — иначе подождать: CVM_TRENDS_FIX ещё не выполнен
IF NOT EXISTS (SELECT 1 FROM sys.parameters (nolock) WHERE object_id = OBJECT_ID('dbo.trends') AND name = N'@YM_STOP')
   OR ISNULL(OBJECT_DEFINITION(OBJECT_ID('dbo.trends')), N'') NOT LIKE N'%знаменатель — покупатели своего сегмента%'
BEGIN
    RAISERROR(N'СТОП (подождать): dbo.trends ещё не исправлена (CVM_TRENDS_FIX_20261031) — ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
-- разовый догон идёт — его шаг 1 удаляет и пересчитывает все месяцы с 202401; порции шли бы параллельно ему
ELSE IF EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name = N'CVM REBUILD ONCE'
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
BEGIN
    RAISERROR(N'СТОП: выполняется разовое задание «CVM REBUILD ONCE» — ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
-- повтор скрипта во время прохода: CREATE OR ALTER переписал бы выполняющуюся процедуру — всё уже заведено, выходим
ELSE IF EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name = N'CVM TRENDS REBUILD'
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
BEGIN
    PRINT N'Идёт проход «CVM TRENDS REBUILD» — пересчёт уже заведён, ничего не меняю.';
    SET NOEXEC ON;
END
GO

-- ============================== 1. ОТМЕТКА ХОДА ==============================
IF OBJECT_ID(N'dbo.CVM_TRENDS_REBUILD', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.CVM_TRENDS_REBUILD (
          FROM_YM   int           NOT NULL   -- самый ранний месяц пересчёта
        , NEXT_STOP int           NOT NULL   -- следующая порция — месяцы до него (не включается); пересчитано [NEXT_STOP; 202610)
        , MAX_CHUNK int           NOT NULL   -- месяцев в порции, не больше
        , PREP_MIN  int           NOT NULL   -- оценка подготовки на вызов, мин
        , MONTH_MIN int           NOT NULL   -- оценка месяца, мин (уточняется по факту)
        , FAILS     int           NOT NULL   -- сбоев подряд; на третьем задание выключается
        , DONE      bit           NOT NULL
        , UPDATED   datetime2(0)  NOT NULL
        , LAST_NOTE nvarchar(400) NULL
    );
    -- 202609 тоже: посчитан 01–10.10 со старым знаменателем.
    -- Первая порция — один месяц (MAX_CHUNK = 1, после неё 6): trends в начале порции удаляет все её месяцы, и сбой
    -- первой порции оставил бы последним месяцем витрины 202603 — утренний шаг 54 досчитывал бы 202604–202610 (~4 ч)
    -- внутри MCI Regular update. Сбой первой порции из одного месяца — только 202609–202610.
    INSERT INTO dbo.CVM_TRENDS_REBUILD (FROM_YM, NEXT_STOP, MAX_CHUNK, PREP_MIN, MONTH_MIN, FAILS, DONE, UPDATED, LAST_NOTE)
    VALUES (202401, 202610, 1, 45, 30, 0, 0, sysdatetime(), N'заведено');
END
ELSE
    PRINT N'Таблица dbo.CVM_TRENDS_REBUILD уже есть — отметка хода не меняется.';
GO

-- ============================== 2. ВЕЧЕРНИЙ ПРОХОД ==============================
CREATE OR ALTER PROCEDURE dbo.cvm_trends_rebuild_step
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from int, @stop int, @max_chunk int, @prep int, @mon int, @done bit
          , @avail int, @k int, @start int, @idx int, @lock int, @left int, @sec int, @n int = 0
          , @now datetime2(0), @deadline datetime2(0), @t0 datetime2(0)
          , @msg nvarchar(400), @note nvarchar(400);

    -- второй экземпляр (ручной EXEC при идущем задании) не начинает: та же порция посчиталась бы дважды.
    -- Замок сессионный: EXEC, прерванный в окне SSMS (Stop — отмена, CATCH её не ловит), держит его, пока окно открыто.
    EXEC @lock = sp_getapplock @Resource = N'cvm_trends_rebuild_step', @LockMode = N'Exclusive',
                               @LockOwner = N'Session', @LockTimeout = 0;
    IF @lock < 0
    BEGIN
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET UPDATED = sysdatetime(), LAST_NOTE = N'пропуск: идёт другой проход (задание или EXEC в окне SSMS)';
        RETURN;
    END

    -- граница вечера: ближайшие 00:30 — за полчаса до перезагрузки «MCI Reload every day» в 01:00
    SET @now = sysdatetime();
    SET @deadline = dateadd(minute, 30, convert(datetime2(0), convert(date, @now)));   -- 00:30 сегодня
    IF @deadline <= @now SET @deadline = dateadd(day, 1, @deadline);                   -- прошло → 00:30 завтра

    WHILE 1 = 1
    BEGIN
        SELECT TOP (1) @from = FROM_YM, @stop = NEXT_STOP, @max_chunk = MAX_CHUNK, @prep = PREP_MIN, @mon = MONTH_MIN,
                       @done = DONE
        FROM dbo.CVM_TRENDS_REBUILD (nolock);
        IF @done = 1 OR @stop <= @from BREAK;

        -- Проверки и расчёт — в одном TRY: любая ошибка обрывает проход, отметка не сдвигается, шаг задания падает,
        -- текст — в LAST_NOTE (без TRY ошибка в условии IF, например нечитаемая msdb, пропустила бы проверку MCI)
        BEGIN TRY
            SET @now = sysdatetime();
            -- с 00:30 до 17:00 идут ночные джобы MCI (перезагрузка с 01:00, ночной проход ~05:30–18:05)
            IF convert(time(0), @now) >= '00:30' AND convert(time(0), @now) < '17:00'
            BEGIN
                SET @note = N'остановлено: не вечер (порции считаются с 17:00 до 00:30)';
                BREAK;
            END
            -- ночной джоб MCI или разовый догон идёт — ждём его конца, проверка раз в 5 минут (шаг 54 и CVM REBUILD ONCE
            -- пишут те же таблицы; задания MCI Reload шагом 3 запускают MCI Regular update). Задание запускается один раз
            -- в день в 17:00 и ждёт здесь, а не перезапускается по расписанию. Не дождались, пока порция ещё успевает
            -- до 00:30, — проход заканчивается до завтра
            WHILE EXISTS (SELECT 1
                          FROM msdb.dbo.sysjobactivity AS a (nolock)
                               JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                          WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                                           N'CVM REBUILD ONCE')
                            AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                            AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
            BEGIN
                IF datediff(minute, sysdatetime(), @deadline) < @prep + 30 + @mon
                BEGIN
                    SET @note = N'остановлено: ночной джоб MCI не закончился, порция не успеет до 00:30';
                    BREAK;
                END
                UPDATE dbo.CVM_TRENDS_REBUILD
                SET UPDATED = sysdatetime(), LAST_NOTE = N'ждёт конца ночного джоба MCI (проверка раз в 5 минут)';
                WAITFOR DELAY '00:05:00';
            END
            IF @note IS NOT NULL BREAK;
            SET @now = sysdatetime();

            -- сколько месяцев успеет до 00:30 с запасом 30 мин на ошибку оценки (идущую порцию не прервать, а в 01:00 —
            -- перезагрузка MCI): (осталось − подготовка − 30) / месяц; не больше MAX_CHUNK и оставшихся месяцев
            SET @left  = datediff(minute, @now, @deadline);
            SET @avail = (@stop / 100 * 12 + @stop % 100) - (@from / 100 * 12 + @from % 100);
            SET @k = (@left - @prep - 30) / NULLIF(@mon, 0);
            IF @k > @max_chunk SET @k = @max_chunk;
            IF @k > @avail SET @k = @avail;
            IF ISNULL(@k, 0) < 1
            BEGIN
                SET @note = N'остановлено: следующая порция не успеет до 00:30';
                BREAK;
            END

            -- порция [@start; @stop): @k месяцев перед @stop
            SET @idx = (@stop / 100) * 12 + (@stop % 100 - 1) - @k;
            SET @start = (@idx / 12) * 100 + @idx % 12 + 1;
            SET @t0 = sysdatetime();
            EXEC dbo.trends @YM_START = @start, @YM_STOP = @stop;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @msg = concat(N'ошибка в порции ', @start, N'–', @stop, N' (не включая): ', ERROR_MESSAGE());
            -- постоянная ошибка повторялась бы каждый вечер (удаление месяцев порции, ~40 мин подготовки, сбой) —
            -- на третьем сбое подряд задание выключается
            UPDATE dbo.CVM_TRENDS_REBUILD
            SET FAILS = FAILS + 1, UPDATED = sysdatetime(),
                LAST_NOTE = LEFT(IIF(FAILS + 1 >= 3, N'ЗАДАНИЕ ВЫКЛЮЧЕНО после 3 сбоев подряд; ', N'') + @msg, 400);
            IF EXISTS (SELECT 1 FROM dbo.CVM_TRENDS_REBUILD (nolock) WHERE FAILS >= 3)
                EXEC msdb.dbo.sp_update_job @job_name = N'CVM TRENDS REBUILD', @enabled = 0;
            EXEC sp_releaseapplock @Resource = N'cvm_trends_rebuild_step', @LockOwner = N'Session';
            THROW;
        END CATCH

        -- отметка сдвигается только после посчитанной порции; оценка месяца — по факту этой порции, не меньше 15 мин;
        -- после первой порции (один месяц) — порции по 6 месяцев
        SET @sec = datediff(second, @t0, sysdatetime());
        SET @n += @k;
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET NEXT_STOP = @start,
            DONE      = IIF(@start <= FROM_YM, 1, 0),
            MONTH_MIN = IIF(CEILING((@sec / 60.0 - PREP_MIN) / @k) < 15, 15, CEILING((@sec / 60.0 - PREP_MIN) / @k)),
            MAX_CHUNK = IIF(@stop = 202610 AND MAX_CHUNK = 1, 6, MAX_CHUNK),
            FAILS     = 0,
            UPDATED   = sysdatetime(),
            LAST_NOTE = concat(N'посчитаны ', @start, N'–', @stop, N' (не включая), ', @k, N' мес. за ', @sec / 60, N' мин');
    END

    IF @done = 1 OR @stop <= @from
    BEGIN
        UPDATE dbo.CVM_TRENDS_REBUILD SET DONE = 1, UPDATED = sysdatetime(), LAST_NOTE = N'пересчёт закончен';
        EXEC msdb.dbo.sp_update_job @job_name = N'CVM TRENDS REBUILD', @enabled = 0;
    END
    ELSE IF @note IS NOT NULL
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET UPDATED = sysdatetime(),
            LAST_NOTE = concat(N'за проход месяцев: ', @n, N'; следующая порция — до ', @stop, N' (не включая); ', @note);

    EXEC sp_releaseapplock @Resource = N'cvm_trends_rebuild_step', @LockOwner = N'Session';
END
GO

-- ============================== 3. ЗАДАНИЕ: один запуск в день, 17:00 ==============================
-- владелец — как у работающего ночного джоба (Windows-владельца Agent проверяет в домене при каждом запуске).
-- Задание уже есть (повтор скрипта) — не пересоздаётся: могло бы оборвать идущий проход.
-- Заведение атомарно: упади шаг или расписание — повтор скрипта иначе решил бы «задание уже есть» и не доделал бы его.
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM TRENDS REBUILD')
BEGIN
    -- msdb-процедуры сообщают об ошибке RAISERROR + RETURN(1), XACT_ABORT на это не срабатывает — код возврата
    -- проверяется у каждого вызова, THROW откатывает транзакцию и обрывает батч
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @rc int;
    DECLARE @owner sysname = (SELECT TOP (1) SUSER_SNAME(owner_sid) FROM msdb.dbo.sysjobs (nolock) WHERE name = N'MCI Regular update');
    IF @owner IS NULL SET @owner = suser_sname();
    EXEC @rc = msdb.dbo.sp_add_job @job_name = N'CVM TRENDS REBUILD', @enabled = 1, @owner_login_name = @owner,
         @description = N'Пересчёт истории трендов 202401–202609 порциями по вечерам (CVM_TRENDS_HISTORY_20261031.sql). Сам выключается в конце.';
    IF @rc <> 0 THROW 50001, N'СТОП: sp_add_job — откат, задание не заведено.', 1;
    EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = N'CVM TRENDS REBUILD', @step_id = 1, @step_name = N'trends portion',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cvm_trends_rebuild_step;',
         @on_success_action = 1, @on_fail_action = 2;
    IF @rc <> 0 THROW 50002, N'СТОП: sp_add_jobstep — откат, задание не заведено.', 1;
    EXEC @rc = msdb.dbo.sp_add_jobschedule @job_name = N'CVM TRENDS REBUILD', @name = N'CVM TRENDS REBUILD daily 17:00',
         @enabled = 1, @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1,
         @active_start_time = 170000;   -- ежедневно, один раз в 17:00
    IF @rc <> 0 THROW 50003, N'СТОП: sp_add_jobschedule — откат, задание не заведено.', 1;
    EXEC @rc = msdb.dbo.sp_add_jobserver @job_name = N'CVM TRENDS REBUILD';
    IF @rc <> 0 THROW 50004, N'СТОП: sp_add_jobserver — откат, задание не заведено.', 1;
    COMMIT;
    SET XACT_ABORT OFF;
END
ELSE
    PRINT N'Задание «CVM TRENDS REBUILD» уже есть — не пересоздаю.';
GO

-- ============================== 4. РАЗОВЫЙ «CVM REBUILD ONCE» — ВЫКЛЮЧИТЬ ==============================
-- его шаг 1 (trends с 202401 без границы) удалил бы и пересчитал все месяцы одним куском параллельно порциям
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM REBUILD ONCE' AND enabled = 1)
    EXEC msdb.dbo.sp_update_job @job_name = N'CVM REBUILD ONCE', @enabled = 0;
GO

-- ============================== 5. ПЕРВЫЙ ПРОХОД — СРАЗУ, ЕСЛИ СЕЙЧАС ВЕЧЕР И MCI НЕ ИДЁТ ==============================
-- иначе — по расписанию; успеет ли порция до 00:30, проход проверяет сам
IF convert(time(0), sysdatetime()) >= '00:30' AND convert(time(0), sysdatetime()) < '17:00'
    PRINT N'Сейчас не вечер — первый проход по расписанию с 17:00, после конца ночного джоба MCI.';
ELSE IF EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                                 N'CVM REBUILD ONCE', N'CVM TRENDS REBUILD')
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
    PRINT N'Идёт ночной джоб MCI или проход уже идёт — первый проход по расписанию.';
ELSE IF EXISTS (SELECT 1 FROM dbo.CVM_TRENDS_REBUILD (nolock) WHERE DONE = 0)
BEGIN
    EXEC msdb.dbo.sp_start_job @job_name = N'CVM TRENDS REBUILD';
    PRINT N'Первый проход запущен. Ход: SELECT * FROM dbo.CVM_TRENDS_REBUILD;';
END
GO

SET NOEXEC OFF;
GO

-- ============================== 6. ИТОГ ==============================
SELECT j.name, j.enabled, s.name AS schedule_name, s.active_start_time, s.active_end_time
FROM msdb.dbo.sysjobs AS j (nolock)
     LEFT JOIN msdb.dbo.sysjobschedules AS js (nolock) ON js.job_id = j.job_id
     LEFT JOIN msdb.dbo.sysschedules AS s (nolock) ON s.schedule_id = js.schedule_id
WHERE j.name IN (N'CVM TRENDS REBUILD', N'CVM REBUILD ONCE');
GO
-- отдельным батчем: после «СТОП» таблицы может не быть — ошибка 208 оборвала бы батч и скрыла итог выше
IF OBJECT_ID(N'dbo.CVM_TRENDS_REBUILD', N'U') IS NULL
    RAISERROR(N'СТОП: таблицы dbo.CVM_TRENDS_REBUILD нет — пересчёт не заведён.', 16, 1);
GO
SELECT * FROM dbo.CVM_TRENDS_REBUILD (nolock);
GO
