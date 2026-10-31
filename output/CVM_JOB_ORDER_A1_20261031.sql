-- ============================================================================================
-- CVM_JOB_ORDER_A1_20261031 — новый порядок шагов 18–56 задания «MCI Regular update» (вариант А1 аудита
--   ночного задания 31.10.2026; бэклог Т8; решение Елены 31.10 «2 а1»).
-- Зачем: эффект акций (CVM_EFFECT_TODAY) стартует около 15:30 вместо 17:30 и успевает пересчитать все выбранные акции
--   до 18:00; доставка, ценовой сегмент, ГКГ и кластеры готовы раньше; CLIENT_ITEM — в конце (около 17:50).
-- Что делает: номера шагов не меняются — на место каждого номера 18–56 переписываются имя, команда, «при ошибке» и
--   остальные настройки шага, который туда переезжает (sp_update_jobstep). Каждый шаг при успехе — следующий, как сейчас.
--   Шаги 1–17, письма 57/58, процедуры и таблицы не трогаются. Команда переезжает без пробелов в начале
--   (у LFL_contacts_shops было « exec …» — sp_update_jobstep их снимает; выполняется то же самое).
-- Порядок по старым номерам: 18,19,20,21,46,24,25,26,27,30,32,35,36,37,39,47,48,49,50,51,52,38,40,23,44,22,31,33,34,
--   28,29,41,42,43,45,53,54,55,56. Читатель каждой таблицы остаётся после её писателя, а шаг, который читает вчерашнюю
--   версию, — до писателя: 21, 25, 26, 27 — до 35; 24, 25, 26 — до 36; 21, 25, 26, 37 — до 47; 50 — до 52.
-- Все шаги до правки — одной строкой JSON в mci_model.dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME = 'job MCI Regular update').
-- Откат — этот же скрипт с @target = N'A0': шаги возвращаются на прежние номера с теми же проверками.
-- Защиты: пока идёт «MCI Regular update» или загрузка MCI — ошибка без изменений (обменник повторит через 10 минут);
--   шаги уже в целевом порядке — сообщение без изменений; шаги не совпадают ни с одним из двух порядков — стоп;
--   правка — одной транзакцией, после неё сверка всех настроек каждого шага, при расхождении — откат.
-- ============================================================================================

USE msdb;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @target nvarchar(2) = N'A1';      -- N'A0' — вернуть порядок до 31.10.2026

DECLARE @job sysname = N'MCI Regular update', @job_id uniqueidentifier, @start_step int, @err_step int, @rc int, @k int
      , @state nvarchar(5), @bad int, @diff nvarchar(max), @msg nvarchar(2048)
      , @name sysname, @subsystem nvarchar(40), @command nvarchar(max), @add_par nvarchar(max), @exec_code int
      , @fail_act tinyint, @fail_step int, @server sysname, @db sysname, @db_user sysname
      , @retry_att int, @retry_int int, @prio int, @out_file nvarchar(200), @flags int, @proxy int;

IF @target NOT IN (N'A0', N'A1')
BEGIN
    RAISERROR(N'СТОП: @target — только N''A1'' или N''A0''.', 16, 1);
    RETURN;
END

SELECT @job_id = job_id, @start_step = start_step_id FROM msdb.dbo.sysjobs (nolock) WHERE name = @job;
IF @job_id IS NULL
BEGIN
    RAISERROR(N'СТОП: задание «MCI Regular update» не найдено.', 16, 1);
    RETURN;
END

-- 1. задание и загрузка MCI не идут
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a (nolock)
                JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
           WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                            N'CVM REBUILD ONCE')
             AND a.session_id = (SELECT max(s.session_id) FROM msdb.dbo.syssessions AS s (nolock))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'Идёт «MCI Regular update» или загрузка MCI — порядок шагов не меняю, обменник повторит через 10 минут.', 16, 1);
    RETURN;
END

-- 2. два порядка шагов 18–56: A0 — до 31.10.2026, A1 — новый (в комментарии — прежний номер шага A1)
DROP TABLE IF EXISTS #ord;
CREATE TABLE #ord (STEP_ID int NOT NULL PRIMARY KEY, NAME_A0 sysname NOT NULL UNIQUE, NAME_A1 sysname NOT NULL UNIQUE);
INSERT INTO #ord (STEP_ID, NAME_A0, NAME_A1) VALUES
      (18, N'fav_cat_prod_load',       N'fav_cat_prod_load')         -- 18
    , (19, N'fav_cat_prod_ins',        N'fav_cat_prod_ins')          -- 19
    , (20, N'UPDATE_TYPE_PROMO',       N'UPDATE_TYPE_PROMO')         -- 20
    , (21, N'CVM_CONTACT',             N'CVM_CONTACT')               -- 21
    , (22, N'CLIENT_ITEM',             N'DELIVERY_REPORT')           -- 46
    , (23, N'PRODUCT_SCORE_REGION',    N'GCG_REPORT')                -- 24
    , (24, N'GCG_REPORT',              N'CVM_CONTACT_DATA')          -- 25
    , (25, N'CVM_CONTACT_DATA',        N'CVM_CONTACT_DATA_W')        -- 26
    , (26, N'CVM_CONTACT_DATA_W',      N'AFFINITIVE_MODEL')          -- 27
    , (27, N'AFFINITIVE_MODEL',        N'I_ASSORT_WEEK')             -- 30
    , (28, N'Shop time',               N'I_cont_assort_w')           -- 32
    , (29, N'for_i_segment',           N'I_CONTACT_PRICE_SEGMENT')   -- 35
    , (30, N'I_ASSORT_WEEK',           N'GLOBAL_CG')                 -- 36
    , (31, N'CVM_LIVE_ASSORT',         N'CLUSTERS')                  -- 37
    , (32, N'I_cont_assort_w',         N'PP treatment')              -- 39
    , (33, N'assort_month',            N'PROMO_CAMPAIGN_LINK')       -- 47
    , (34, N'cont_assort_m',           N'DISCOUNT_MART')             -- 48
    , (35, N'I_CONTACT_PRICE_SEGMENT', N'DISC_MODEL_MART')           -- 49
    , (36, N'GLOBAL_CG',               N'DISC_EFFECT')               -- 50
    , (37, N'CLUSTERS',                N'BONUS_MART')                -- 51
    , (38, N'CLUSTER_SCORE',           N'CVM_EFFECT_TODAY')          -- 52
    , (39, N'PP treatment',            N'CLUSTER_SCORE')             -- 38
    , (40, N'PP_EFFECT_MONTH',         N'PP_EFFECT_MONTH')           -- 40
    , (41, N'I_CH_INDEXEX',            N'PRODUCT_SCORE_REGION')      -- 23
    , (42, N'LFL_contacts_shops',      N'CVM_BASE_MONTH')            -- 44
    , (43, N'I_REPORT',                N'CLIENT_ITEM')               -- 22
    , (44, N'CVM_BASE_MONTH',          N'CVM_LIVE_ASSORT')           -- 31
    , (45, N'PRODUCT_DEMAND',          N'assort_month')              -- 33
    , (46, N'DELIVERY_REPORT',         N'cont_assort_m')             -- 34
    , (47, N'PROMO_CAMPAIGN_LINK',     N'Shop time')                 -- 28
    , (48, N'DISCOUNT_MART',           N'for_i_segment')             -- 29
    , (49, N'DISC_MODEL_MART',         N'I_CH_INDEXEX')              -- 41
    , (50, N'DISC_EFFECT',             N'LFL_contacts_shops')        -- 42
    , (51, N'BONUS_MART',              N'I_REPORT')                  -- 43
    , (52, N'CVM_EFFECT_TODAY',        N'PRODUCT_DEMAND')            -- 45
    , (53, N'cohort_new',              N'cohort_new')                -- 53
    , (54, N'I_TRENDS',                N'I_TRENDS')                  -- 54
    , (55, N'Mission report',          N'Mission report')            -- 55
    , (56, N'churn_report',            N'churn_report');             -- 56

-- 3. шаги задания сейчас и в каком они порядке
DROP TABLE IF EXISTS #cur;
SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
     , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
     , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
     , s.flags, s.proxy_id
INTO #cur
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id;

SELECT @err_step = step_id FROM #cur (nolock) WHERE step_name = N'send error mail';

SET @state = CASE WHEN NOT EXISTS (SELECT 1 FROM #ord AS o (nolock) LEFT JOIN #cur AS c (nolock) ON c.step_id = o.STEP_ID AND c.step_name = o.NAME_A0
                                   WHERE c.step_id IS NULL) THEN N'A0'
                  WHEN NOT EXISTS (SELECT 1 FROM #ord AS o (nolock) LEFT JOIN #cur AS c (nolock) ON c.step_id = o.STEP_ID AND c.step_name = o.NAME_A1
                                   WHERE c.step_id IS NULL) THEN N'A1'
                  ELSE N'OTHER' END;

IF (SELECT count(*) FROM #cur (nolock)) <> 58
   OR (SELECT count(DISTINCT step_name) FROM #cur (nolock)) <> 58
   OR NOT EXISTS (SELECT 1 FROM #cur (nolock) WHERE step_id = 57 AND step_name = N'send success mail')
   OR isnull(@err_step, 0) <> 58
   OR @state = N'OTHER'
BEGIN
    RAISERROR(N'СТОП: шаги задания не совпадают ни с прежним, ни с новым порядком (или шагов не 58) — задание не изменено.', 16, 1);
    RETURN;
END

IF @state = @target
BEGIN
    RAISERROR(N'Шаги 18–56 уже в порядке %s — не меняю.', 10, 1, @target) WITH NOWAIT;
    RETURN;
END

-- переходы шагов 18–56: при успехе — следующий; при ошибке — следующий или «send error mail»;
-- и ни один шаг задания (1–58) не переходит «на шаг N» внутрь 18–56: после перестановки такой переход вёл бы на другой шаг
IF EXISTS (SELECT 1 FROM #cur (nolock)
           WHERE step_id BETWEEN 18 AND 56
             AND (on_success_action <> 3 OR NOT (on_fail_action = 3 OR (on_fail_action = 4 AND on_fail_step_id = @err_step))))
   OR EXISTS (SELECT 1 FROM #cur (nolock)
              WHERE (on_success_action = 4 AND on_success_step_id BETWEEN 18 AND 56)
                 OR (on_fail_action = 4 AND on_fail_step_id BETWEEN 18 AND 56))
BEGIN
    RAISERROR(N'СТОП: у шагов 18–56 переходы не «при успехе — следующий, при ошибке — следующий или письмо об ошибке» (или есть переход «на шаг» внутрь 18–56) — задание не изменено.', 16, 1);
    RETURN;
END

-- 4. бэкап и перестановка — одной транзакцией
BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO mci_model.dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    SELECT N'job MCI Regular update', sysdatetime(), (SELECT * FROM #cur (nolock) ORDER BY step_id FOR JSON PATH, INCLUDE_NULL_VALUES);

    -- 4.1 временные имена: имя шага в задании уникально, а шаги меняются местами
    SET @k = 18;
    WHILE @k <= 56
    BEGIN
        SET @name = concat(N'~reorder~', @k);
        EXEC @rc = msdb.dbo.sp_update_jobstep @job_id = @job_id, @step_id = @k, @step_name = @name;
        IF @rc <> 0 THROW 50001, N'sp_update_jobstep (временное имя) — откат, задание не изменено.', 1;
        SET @k += 1;
    END

    -- 4.2 на место каждого номера — шаг целевого порядка со всеми своими настройками; команда — без пробелов в начале
    --     (31.10 18:00 и 18:10 сверка 50004 откатила правку; вероятная причина — sp_update_jobstep снимает пробел в начале
    --     команды LFL_contacts_shops « exec …»)
    SET @k = 18;
    WHILE @k <= 56
    BEGIN
        SELECT @name = c.step_name, @subsystem = c.subsystem, @command = LTRIM(c.command), @add_par = c.additional_parameters
             , @exec_code = c.cmdexec_success_code, @fail_act = c.on_fail_action, @fail_step = c.on_fail_step_id
             , @server = c.server, @db = c.database_name, @db_user = c.database_user_name
             , @retry_att = c.retry_attempts, @retry_int = c.retry_interval, @prio = c.os_run_priority
             , @out_file = c.output_file_name, @flags = c.flags, @proxy = c.proxy_id
        FROM #ord AS o (nolock)
             JOIN #cur AS c (nolock) ON c.step_name = IIF(@target = N'A1', o.NAME_A1, o.NAME_A0)
        WHERE o.STEP_ID = @k;
        -- шаг не найден — переменные остались бы от прошлого витка; стоп до записи
        IF @@ROWCOUNT <> 1 THROW 50007, N'шаг целевого порядка не найден в задании — откат, задание не изменено.', 1;

        EXEC @rc = msdb.dbo.sp_update_jobstep @job_id = @job_id, @step_id = @k, @step_name = @name, @subsystem = @subsystem
             , @command = @command, @additional_parameters = @add_par, @cmdexec_success_code = @exec_code
             , @on_success_action = 3, @on_success_step_id = 0, @on_fail_action = @fail_act, @on_fail_step_id = @fail_step
             , @server = @server, @database_name = @db, @database_user_name = @db_user
             , @retry_attempts = @retry_att, @retry_interval = @retry_int, @os_run_priority = @prio
             , @output_file_name = @out_file, @flags = @flags, @proxy_id = @proxy;
        IF @rc <> 0 THROW 50002, N'sp_update_jobstep — откат, задание не изменено.', 1;
        SET @k += 1;
    END

    -- 5. сверка: имена 18–56 — по целевому порядку; настройки каждого шага — те же, что до правки (переехали вместе с
    --    именем; NULL в sp_update_jobstep значит «не менять», поэтому проверяется итог); 1–17, 57, 58 и начальный шаг — те же
    DROP TABLE IF EXISTS #new;
    SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
         , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
         , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
         , s.flags, s.proxy_id
    INTO #new
    FROM msdb.dbo.sysjobsteps AS s (nolock)
    WHERE s.job_id = @job_id;

    SELECT @bad = count(*)
    FROM #ord AS o (nolock)
         LEFT JOIN #new AS n (nolock) ON n.step_id = o.STEP_ID AND n.step_name = IIF(@target = N'A1', o.NAME_A1, o.NAME_A0)
                             AND n.on_success_action = 3
    WHERE n.step_id IS NULL;
    IF @bad > 0 OR (SELECT count(*) FROM #new (nolock)) <> 58
        THROW 50003, N'после правки номера или имена шагов 18–56 не совпали с порядком — откат, задание не изменено.', 1;

    -- команды сравниваются без пробелов в начале; имена расходящихся шагов — в текст ошибки (латиница видна в журнале обменника)
    SELECT @bad = count(*), @diff = string_agg(CAST(d.step_name AS nvarchar(max)), N', ')
    FROM (SELECT step_name, subsystem, command = LTRIM(command), additional_parameters, cmdexec_success_code, on_fail_action
               , on_fail_step_id, server, database_name, database_user_name, retry_attempts, retry_interval, os_run_priority
               , output_file_name, flags, proxy_id
          FROM #cur (nolock)
          EXCEPT
          SELECT step_name, subsystem, command = LTRIM(command), additional_parameters, cmdexec_success_code, on_fail_action
               , on_fail_step_id, server, database_name, database_user_name, retry_attempts, retry_interval, os_run_priority
               , output_file_name, flags, proxy_id
          FROM #new (nolock)) AS d;
    IF @bad > 0
    BEGIN
        SET @msg = concat(N'steps differ: ', left(@diff, 1500), N' — после правки команды или настройки шагов не совпали с прежними, откат, задание не изменено.');
        THROW 50004, @msg, 1;
    END

    SELECT @bad = count(*)
    FROM (SELECT * FROM #cur (nolock) WHERE step_id NOT BETWEEN 18 AND 56
          EXCEPT
          SELECT * FROM #new (nolock) WHERE step_id NOT BETWEEN 18 AND 56) AS d;
    IF @bad > 0 THROW 50005, N'после правки изменились шаги 1–17, 57 или 58 — откат, задание не изменено.', 1;

    IF (SELECT start_step_id FROM msdb.dbo.sysjobs (nolock) WHERE job_id = @job_id) <> @start_step
        THROW 50006, N'после правки изменился начальный шаг задания — откат, задание не изменено.', 1;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH

RAISERROR(N'Готово: шаги 18–56 «MCI Regular update» — в порядке %s; шаги до правки — в mci_model.dbo.I_DISCOUNT_PROC_BACKUP.', 10, 1, @target) WITH NOWAIT;

-- 6. итог
SELECT [Шаг] = s.step_id, [Имя] = s.step_name, [Команда] = left(s.command, 80)
     , [При ошибке] = CASE s.on_fail_action WHEN 1 THEN N'выход (успех)' WHEN 2 THEN N'выход (ошибка)' WHEN 3 THEN N'следующий'
                                            ELSE concat(N'шаг ', s.on_fail_step_id) END
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id AND s.step_id BETWEEN 18 AND 56
ORDER BY s.step_id;
GO
