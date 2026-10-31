-- ============================================================================================
-- CVM_JOB_WEEKLY_T9_20261101 — раз в неделю шесть шагов «MCI Regular update» (бэклог ночного задания Т9; решение Елены
--   31.10.2026 «3 да»: кластеры со скорами, модель скидок, ценовой сегмент, DATA_W — раз в неделю).
-- Зачем: в обычный день задание короче на ≈ 3 ч 38 мин (эффект акций стартует ≈ на 3 ч 20 мин раньше).
-- Что делает: в команду шага ставится условие запуска, процедуры не трогаются. Шаги ищутся по имени, номера не меняются:
--   CVM_CONTACT_DATA_W      — по пятницам (в первую пятницу месяца шаг 1 стирает ≈ 13 недельных снимков — они досчитываются
--                             в ту же ночь, как сейчас; снимок недели окончательный, когда неделя вышла из окна перезаливки);
--   I_CONTACT_PRICE_SEGMENT,
--   CLUSTERS                — по понедельникам; и в любой день, если окно кластеров офлайна не запускалось 7 дней
--                             (пропущенный понедельник догоняется на следующую ночь);
--   CLUSTER_SCORE           — в день, когда сегодня присвоены кластеры офлайна;
--   DISC_MODEL_MART         — по понедельникам; и в любой день, если в днях модели от сегодня−120 до сегодня−17 посчитаны
--                             не все (догон после трёхмесячной перезаливки первой пятницы — 31 день за ночь, как сейчас;
--                             пропущенный понедельник);
--   DISC_EFFECT             — в день, когда сегодня считалась модель скидок (эффект скидок и прогноз ДЦО идут от последнего
--                             посчитанного дня модели; без этого в будни они пересчитывались бы на 9 дней назад).
--   В остальные дни шаг печатает «пропуск» и сразу идёт дальше. Понедельник = DATEDIFF(day, '19000101', дата) % 7 = 0
--   (не DATEPART(weekday) — зависит от @@DATEFIRST); пятница = 4.
-- Применяется не раньше 01.11.2026 17:00 — после прогона 01.11, по которому проверяется Т8 (порядок А1).
-- Все шаги до правки — одной строкой JSON в mci_model.dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME = 'job MCI Regular update').
-- Откат — этот же скрипт с @target = N'A1': команды шагов возвращаются к прежним (каждую ночь) из самих команд.
-- Защиты: пока идёт «MCI Regular update» или загрузка MCI — ошибка без изменений (обменник повторит через 10 минут);
--   шаги уже с этими условиями — сообщение без изменений; шаги с прежней редакцией условий Т9 — условия заменяются;
--   команда шага не та, что ожидается, или в журналах нет колонок условий — стоп; правка — одной транзакцией, после
--   неё сверка всех настроек всех шагов, при расхождении — откат.
-- ============================================================================================

USE msdb;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @target     nvarchar(2) = N'T9';            -- N'A1' — вернуть прежние команды (шаги каждую ночь)
DECLARE @not_before datetime    = '20261101 17:00';  -- раньше не применять: прогон 01.11 — проверка Т8 на порядке А1

DECLARE @job sysname = N'MCI Regular update', @job_id uniqueidentifier, @start_step int, @rc int, @k int, @n int
      , @state nvarchar(5), @bad int, @diff nvarchar(max), @msg nvarchar(2048), @step_id int, @new nvarchar(max)
      , @o1 nvarchar(20) = N'/*<orig>*/', @o2 nvarchar(20) = N'/*</orig>*/', @mark nvarchar(20) = N'/* T9 */';

IF @target NOT IN (N'T9', N'A1')
BEGIN
    RAISERROR(N'СТОП: @target — только N''T9'' или N''A1''.', 16, 1);
    RETURN;
END

IF @target = N'T9' AND GETDATE() < @not_before
BEGIN
    RAISERROR(N'Wait: T9 applies after 01.11.2026 17:00 (run 01.11 checks T8) — ждём конца прогона 01.11, обменник повторит через 10 минут.', 16, 1);
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
    RAISERROR(N'Job running: идёт «MCI Regular update» или загрузка MCI — команды шагов не меняю, обменник повторит через 10 минут.', 16, 1);
    RETURN;
END

-- 2. шаги задачи: имя; прежняя команда без пробелов и переводов строк (для сверки); условие запуска; текст пропуска
DROP TABLE IF EXISTS #t9;
CREATE TABLE #t9 (NAME sysname NOT NULL PRIMARY KEY, PLAIN nvarchar(400) NOT NULL, COND nvarchar(max) NOT NULL
                , SKIP_MSG nvarchar(400) NOT NULL, STEP_ID int NULL, CMD nvarchar(max) NULL, ORIG nvarchar(max) NULL
                , WANT nvarchar(max) NULL, NEW_CMD nvarchar(max) NULL);
INSERT INTO #t9 (NAME, PLAIN, COND, SKIP_MSG) VALUES
  (N'CVM_CONTACT_DATA_W', N'exec[dbo].[cvm_contact_data_w]'
 , N'-- Т9 (31.10.2026): недельный снимок — по пятницам
IF DATEDIFF(day, ''19000101'', GETDATE()) % 7 = 4'
 , N'CVM_CONTACT_DATA_W: не пятница — пропуск (Т9, раз в неделю)')
, (N'I_CONTACT_PRICE_SEGMENT', N'execdbo.cont_price_segment'
 , N'-- Т9 (31.10.2026): по понедельникам; догон — если окно кластеров офлайна не запускалось 7 дней
IF DATEDIFF(day, ''19000101'', GETDATE()) % 7 = 0
   OR NOT EXISTS (SELECT 1 FROM dbo.I_FOR_CLUSTER_LOG WITH (NOLOCK)
                  WHERE ID_ORGANIZATION = 1 AND STEP = N''window'' AND STATUS = ''OK''
                    AND STARTED >= DATEADD(day, -7, CAST(GETDATE() AS date))
                    AND REFERENCE_DATE >= DATEADD(day, -14, CAST(GETDATE() AS date)))'
 , N'cont_price_segment: не понедельник — пропуск (Т9, раз в неделю)')
, (N'CLUSTERS', N'execdbo.clusters@mode=''current'';execdbo.cluster_assign@org=1,@skip_if_done=1;execdbo.cluster_assign@org=3,@skip_if_done=1;'
 , N'-- Т9 (31.10.2026): по понедельникам; догон — если окно кластеров офлайна не запускалось 7 дней
IF DATEDIFF(day, ''19000101'', GETDATE()) % 7 = 0
   OR NOT EXISTS (SELECT 1 FROM dbo.I_FOR_CLUSTER_LOG WITH (NOLOCK)
                  WHERE ID_ORGANIZATION = 1 AND STEP = N''window'' AND STATUS = ''OK''
                    AND STARTED >= DATEADD(day, -7, CAST(GETDATE() AS date))
                    AND REFERENCE_DATE >= DATEADD(day, -14, CAST(GETDATE() AS date)))'
 , N'clusters: не понедельник — пропуск (Т9, раз в неделю)')
, (N'DISC_MODEL_MART', N'execdbo.disc_model_mart;'
 , N'-- Т9 (31.10.2026): по понедельникам; догон — если из 104 дней модели от сегодня−120 до сегодня−17 посчитаны не все
IF DATEDIFF(day, ''19000101'', GETDATE()) % 7 = 0
   OR (SELECT count(*) FROM dbo.I_DISC_MODEL_STATE WITH (NOLOCK)
       WHERE ID_COMPANY = 1 AND STATUS IN (''OK'', ''NO_CHECKS'')
         AND DATA >= DATEADD(day, -120, CAST(GETDATE() AS date))
         AND DATA <= DATEADD(day, -17, CAST(GETDATE() AS date))) < 104'
 , N'disc_model_mart: не понедельник — пропуск (Т9, раз в неделю)')
, (N'DISC_EFFECT', N'execdbo.disc_effect_mart;'
 , N'-- Т9 (31.10.2026): вслед за моделью скидок — в день, когда она сегодня считалась
IF EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_LOG WITH (NOLOCK)
           WHERE RUN_MODE = ''NIGHT'' AND STARTED >= CAST(GETDATE() AS date))'
 , N'disc_effect_mart: модель скидок сегодня не считалась — пропуск (Т9, раз в неделю)')
, (N'CLUSTER_SCORE', N'execdbo.cluster_score;'
 , N'-- Т9 (31.10.2026): вслед за кластерами — в день, когда сегодня присвоены кластеры офлайна
IF EXISTS (SELECT 1 FROM dbo.I_CLUSTER_ASSIGN_LOG WITH (NOLOCK)
           WHERE ID_ORGANIZATION = 1 AND STATUS = ''OK'' AND STARTED >= CAST(GETDATE() AS date))'
 , N'cluster_score: кластеры сегодня не присваивались — пропуск (Т9, раз в неделю)');

-- 3. шаги задания сейчас
DROP TABLE IF EXISTS #cur;
SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
     , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
     , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
     , s.flags, s.proxy_id
INTO #cur
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id;

UPDATE t SET STEP_ID = c.step_id, CMD = c.command
FROM #t9 AS t JOIN #cur AS c (nolock) ON c.step_name = t.NAME;

IF (SELECT count(*) FROM #cur (nolock)) <> 58
   OR (SELECT count(DISTINCT step_name) FROM #cur (nolock)) <> 58
   OR EXISTS (SELECT 1 FROM #t9 (nolock) WHERE STEP_ID IS NULL)
   OR EXISTS (SELECT 1 FROM #t9 AS t (nolock) JOIN #cur AS c (nolock) ON c.step_id = t.STEP_ID
              WHERE c.subsystem <> N'TSQL' OR c.database_name <> N'mci_model' OR c.on_success_action <> 3)
BEGIN
    RAISERROR(N'СТОП: в задании не 58 шагов, нет одного из шести шагов задачи или шаг не T-SQL в mci_model с переходом «при успехе — следующий» — задание не изменено.', 16, 1);
    RETURN;
END

-- порядок по графу: ценовой сегмент → кластеры → скоры; модель скидок → эффект скидок (условие шага-читателя смотрит журнал
-- шага-писателя этой же ночи)
IF (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'I_CONTACT_PRICE_SEGMENT') >= (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'CLUSTERS')
   OR (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'CLUSTERS') >= (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'CLUSTER_SCORE')
   OR (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'DISC_MODEL_MART') >= (SELECT STEP_ID FROM #t9 (nolock) WHERE NAME = N'DISC_EFFECT')
BEGIN
    RAISERROR(N'СТОП: порядок шагов не «ценовой сегмент → кластеры → скоры» и «модель скидок → эффект скидок» — задание не изменено.', 16, 1);
    RETURN;
END

-- колонки журналов, которые читают условия: пропавшая колонка дала бы ошибку компиляции шага (у 25 и 29 — стоп задания)
IF @target = N'T9'
   AND (   COL_LENGTH(N'mci_model.dbo.I_FOR_CLUSTER_LOG', N'ID_ORGANIZATION') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_FOR_CLUSTER_LOG', N'STEP') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_FOR_CLUSTER_LOG', N'STATUS') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_FOR_CLUSTER_LOG', N'REFERENCE_DATE') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_FOR_CLUSTER_LOG', N'STARTED') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_DISC_MODEL_STATE', N'ID_COMPANY') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_DISC_MODEL_STATE', N'DATA') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_DISC_MODEL_STATE', N'STATUS') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_DISC_MODEL_LOG', N'RUN_MODE') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_DISC_MODEL_LOG', N'STARTED') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_CLUSTER_ASSIGN_LOG', N'ID_ORGANIZATION') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_CLUSTER_ASSIGN_LOG', N'STATUS') IS NULL
        OR COL_LENGTH(N'mci_model.dbo.I_CLUSTER_ASSIGN_LOG', N'STARTED') IS NULL)
BEGIN
    RAISERROR(N'STOP: no log columns — в журналах кластеров или модели скидок нет колонок, которые читают условия, — задание не изменено.', 16, 1);
    RETURN;
END

-- прежняя команда (ORIG): у шага без условия — сама команда, у шага с условием Т9 — между маркерами;
-- команда с условием этой редакции (WANT)
UPDATE #t9 SET ORIG = CASE WHEN CMD NOT LIKE @mark + N'%' THEN LTRIM(RTRIM(CMD))
                           WHEN CHARINDEX(@o1, CMD) > 0 AND CHARINDEX(@o2, CMD) > CHARINDEX(@o1, CMD)
                                THEN SUBSTRING(CMD, CHARINDEX(@o1, CMD) + DATALENGTH(@o1) / 2
                                             , CHARINDEX(@o2, CMD) - CHARINDEX(@o1, CMD) - DATALENGTH(@o1) / 2) END;

UPDATE #t9 SET WANT = concat(@mark, NCHAR(13), NCHAR(10), COND, NCHAR(13), NCHAR(10)
                           , N'BEGIN', NCHAR(13), NCHAR(10), @o1, ORIG, @o2, NCHAR(13), NCHAR(10), N'END', NCHAR(13), NCHAR(10)
                           , N'ELSE', NCHAR(13), NCHAR(10), N'    PRINT N''', SKIP_MSG, N''';')
WHERE ORIG IS NOT NULL;

-- состояние: A1 — все шесть без условия; T9 — все шесть с условием этой редакции; T9OLD — все шесть с условием Т9 другой
-- редакции (повторная выкладка исправленных условий); OTHER — прежняя команда не та или часть шагов с условием, часть без
SET @state = CASE WHEN EXISTS (SELECT 1 FROM #t9 (nolock)
                               WHERE isnull(lower(replace(replace(replace(replace(ORIG, N' ', N''), NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N'')), N'') <> PLAIN)
                       THEN N'OTHER'
                  WHEN NOT EXISTS (SELECT 1 FROM #t9 (nolock) WHERE CMD LIKE @mark + N'%') THEN N'A1'
                  WHEN EXISTS (SELECT 1 FROM #t9 (nolock) WHERE CMD NOT LIKE @mark + N'%') THEN N'OTHER'
                  WHEN EXISTS (SELECT 1 FROM #t9 (nolock) WHERE LTRIM(RTRIM(CMD)) <> WANT) THEN N'T9OLD'
                  ELSE N'T9' END;

IF @state = N'OTHER'
BEGIN
    SELECT @diff = string_agg(CAST(NAME AS nvarchar(max)), N', ') FROM #t9 (nolock)
    WHERE isnull(lower(replace(replace(replace(replace(ORIG, N' ', N''), NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N'')), N'') <> PLAIN;
    SET @msg = concat(N'STOP: steps ', isnull(@diff, N'(mixed T9/A1)'), N' — команды шагов не прежние и не с условием Т9 (или часть шагов уже с условием) — задание не изменено.');
    RAISERROR(@msg, 16, 1);
    RETURN;
END

IF @state = @target
BEGIN
    RAISERROR(N'Шаги задачи уже в состоянии %s — не меняю.', 10, 1, @target) WITH NOWAIT;
    RETURN;
END

UPDATE #t9 SET NEW_CMD = CASE WHEN @target = N'T9' THEN WANT ELSE ORIG END;

-- 4. бэкап и правка команд — одной транзакцией
BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO mci_model.dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    SELECT N'job MCI Regular update', sysdatetime(), (SELECT * FROM #cur (nolock) ORDER BY step_id FOR JSON PATH, INCLUDE_NULL_VALUES);

    SET @n = (SELECT count(*) FROM #t9 (nolock));
    SET @k = 1;
    WHILE @k <= @n
    BEGIN
        SELECT @step_id = STEP_ID, @new = NEW_CMD
        FROM (SELECT STEP_ID, NEW_CMD, RN = row_number() OVER (ORDER BY STEP_ID) FROM #t9 (nolock)) AS x
        WHERE x.RN = @k;
        IF @@ROWCOUNT <> 1 OR @new IS NULL THROW 50001, N'шаг задачи не найден — откат, задание не изменено.', 1;

        EXEC @rc = msdb.dbo.sp_update_jobstep @job_id = @job_id, @step_id = @step_id, @command = @new;
        IF @rc <> 0 THROW 50002, N'sp_update_jobstep — откат, задание не изменено.', 1;
        SET @k += 1;
    END

    -- 5. сверка: команды шести шагов — целевые (без пробелов по краям — sp_update_jobstep их снимает); у всех 58 шагов все
    --    остальные настройки и команды прочих шагов — те же; начальный шаг — тот же
    DROP TABLE IF EXISTS #new;
    SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
         , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
         , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
         , s.flags, s.proxy_id
    INTO #new
    FROM msdb.dbo.sysjobsteps AS s (nolock)
    WHERE s.job_id = @job_id;

    SELECT @bad = count(*), @diff = string_agg(CAST(t.NAME AS nvarchar(max)), N', ')
    FROM #t9 AS t (nolock)
         LEFT JOIN #new AS n (nolock) ON n.step_id = t.STEP_ID AND n.step_name = t.NAME
    WHERE n.step_id IS NULL OR LTRIM(RTRIM(n.command)) <> LTRIM(RTRIM(t.NEW_CMD));
    IF @bad > 0
    BEGIN
        SET @msg = concat(N'steps differ: ', left(@diff, 1500), N' — после правки команды шагов задачи не совпали с целевыми, откат, задание не изменено.');
        THROW 50003, @msg, 1;
    END

    SELECT @bad = count(*), @diff = string_agg(CAST(d.step_name AS nvarchar(max)), N', ')
    FROM (SELECT step_id, step_name, subsystem, additional_parameters, cmdexec_success_code, on_success_action, on_success_step_id
               , on_fail_action, on_fail_step_id, server, database_name, database_user_name, retry_attempts, retry_interval
               , os_run_priority, output_file_name, flags, proxy_id
               , command = CASE WHEN step_name IN (SELECT NAME FROM #t9 (nolock)) THEN NULL ELSE command END
          FROM #cur (nolock)
          EXCEPT
          SELECT step_id, step_name, subsystem, additional_parameters, cmdexec_success_code, on_success_action, on_success_step_id
               , on_fail_action, on_fail_step_id, server, database_name, database_user_name, retry_attempts, retry_interval
               , os_run_priority, output_file_name, flags, proxy_id
               , command = CASE WHEN step_name IN (SELECT NAME FROM #t9 (nolock)) THEN NULL ELSE command END
          FROM #new (nolock)) AS d;
    IF @bad > 0 OR (SELECT count(*) FROM #new (nolock)) <> 58
    BEGIN
        SET @msg = concat(N'steps differ: ', left(isnull(@diff, N'(count)'), 1500), N' — после правки изменились другие настройки или другие шаги, откат, задание не изменено.');
        THROW 50004, @msg, 1;
    END

    IF (SELECT start_step_id FROM msdb.dbo.sysjobs (nolock) WHERE job_id = @job_id) <> @start_step
        THROW 50005, N'после правки изменился начальный шаг задания — откат, задание не изменено.', 1;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH

RAISERROR(N'Готово: шаги задачи Т9 «MCI Regular update» — в состоянии %s; шаги до правки — в mci_model.dbo.I_DISCOUNT_PROC_BACKUP.', 10, 1, @target) WITH NOWAIT;

-- 6. итог
SELECT [Шаг] = s.step_id, [Имя] = s.step_name, [Команда] = s.command
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id AND s.step_name IN (SELECT NAME FROM #t9 (nolock))
ORDER BY s.step_id;
GO
