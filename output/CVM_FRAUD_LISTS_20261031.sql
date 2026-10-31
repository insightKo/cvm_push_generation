-- ============================================================================================
-- CVM_FRAUD_LISTS_20261031 — чёрные списки Manzana в нашу таблицу фрода dbo.I_FRAUD_CONTACT каждую ночь
--   (задача Елены 31.10.2026: «добавляй все ид в нашу таблицу — проверяй, что в таблице фрода только уники»).
-- Списки (по имени в crmdata__List): «Черный список ФРОД» (ListId 35270), «Черный список карт кассиров 01.2023» (33190),
--   «Обновляемый черный список кассиров 06.24» (83944).
-- Участники — loyalty__list_member_a (list_id, contact_id = ID_CONTACT) и crmdata__ListMember (ListId, EntityId = ID_CONTACT):
--   последняя версия участника, не удалённые.
-- Что делает (идемпотентно, для обменника MCI — повторный запуск безопасен):
--   1. Процедура dbo.fraud_lists_load: в I_FRAUD_CONTACT одна строка на клиента (лишние копии удаляются), участники списков,
--      которых в таблице нет, добавляются (SEGMENT = -1, как у всех строк таблицы); в конце — проверка уникальности,
--      при дублях — ошибка и откат. Только добавление: клиент, которого убрали из списка, из фрода не удаляется.
--   2. Таблица до первого запуска — одной строкой JSON в dbo.I_DISCOUNT_PROC_BACKUP
--      (PROC_NAME = 'table I_FRAUD_CONTACT before fraud_lists_load'), один раз.
--   3. Первый запуск процедуры сразу (не ждём ночи).
--   4. Шаг FRAUD_LISTS «MCI Regular update» — перед CVM_CONTACT (первым читателем фрода): exec dbo.fraud_lists_load;
--      при ошибке — следующий шаг (CVM_CONTACT считает по таблице как есть). Номера шагов с CVM_CONTACT и ниже — +1,
--      переходы на шаг (письмо об ошибке 58 → 59) SQL Agent сдвигает сам; после вставки — сверка всех настроек всех шагов.
--      Шаги задания до правки — JSON в dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME = 'job MCI Regular update').
-- Время шага ≈ 5–10 с (участники трёх списков: list_member_a — 0,3 с, ListMember — 5,5 с по замеру 31.10).
-- Защиты: пока идёт «MCI Regular update» или загрузка MCI — ошибка без изменений (обменник повторит через 10 минут);
--   шаг уже стоит — сообщение без изменений; в задании не 58 шагов или нет CVM_CONTACT — стоп.
-- Откат шага: EXEC msdb.dbo.sp_delete_jobstep @job_name = N'MCI Regular update', @step_id = <номер FRAUD_LISTS>;
--   (номера и переходы SQL Agent сдвигает обратно). Откат порядка А1 (@target = 'A0') рассчитан на 58 шагов — с этим
--   шагом он остановится без изменений; Т9 (CVM_JOB_WEEKLY_T9_20261101) принимает 58 шагов или 59 с FRAUD_LISTS.
-- ============================================================================================
USE mci_model
GO
SET NOCOUNT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
SET LOCK_TIMEOUT 300000;   -- CREATE OR ALTER и правка таблицы ждут блокировку не дольше 5 минут: ошибка 1222 → обменник повторит
IF DB_NAME() <> N'mci_model'
BEGIN
    RAISERROR(N'СТОП: не база mci_model — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO
-- ============================== 0. ЗАДАНИЕ И ЗАГРУЗКА MCI НЕ ИДУТ, ИСТОЧНИКИ НА МЕСТЕ ==============================
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a (nolock)
                JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
           WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                            N'CVM REBUILD ONCE')
             AND a.session_id = (SELECT max(s.session_id) FROM msdb.dbo.syssessions AS s (nolock))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'Job running: идёт «MCI Regular update» или загрузка MCI — ничего не меняю, обменник повторит через 10 минут.', 16, 1);
    SET NOEXEC ON;
END
GO
IF COL_LENGTH('dbo.I_FRAUD_CONTACT', 'ID_CONTACT') IS NULL OR COL_LENGTH('dbo.I_FRAUD_CONTACT', 'SEGMENT') IS NULL
   OR COL_LENGTH('dbo.I_FRAUD_CONTACT', 'ID_COMPANY') IS NULL
   OR COL_LENGTH('dbo.crmdata__List', 'ListName') IS NULL OR COL_LENGTH('dbo.crmdata__List', 'last_version') IS NULL
   OR COL_LENGTH('dbo.loyalty__list_member_a', 'contact_id') IS NULL OR COL_LENGTH('dbo.loyalty__list_member_a', 'list_id') IS NULL
   OR COL_LENGTH('dbo.crmdata__ListMember', 'EntityId') IS NULL OR COL_LENGTH('dbo.crmdata__ListMember', 'ListMemberId') IS NULL
   OR COL_LENGTH('dbo.I_DISCOUNT_PROC_BACKUP', 'DEFINITION') IS NULL
BEGIN
    RAISERROR(N'СТОП: в I_FRAUD_CONTACT, crmdata__List, loyalty__list_member_a, crmdata__ListMember или I_DISCOUNT_PROC_BACKUP нет нужных колонок — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO
-- ============================== 1. ПРОЦЕДУРА dbo.fraud_lists_load ==============================
-- Временные таблицы — #fl_*: процедура видит временные таблицы вызвавшей сессии.
CREATE OR ALTER PROCEDURE dbo.fraud_lists_load
AS
-- Чёрные списки Manzana → dbo.I_FRAUD_CONTACT (31.10.2026). Шаг FRAUD_LISTS «MCI Regular update» перед CVM_CONTACT.
-- Списки — по имени, последняя версия списка, не удалённые; участники — последняя версия участника, не удалённые.
-- Только добавление. В I_FRAUD_CONTACT одна строка на (ID_COMPANY, ID_CONTACT): лишние копии удаляются, новые добавляются,
-- если их ещё нет; в конце — проверка уникальности, при дублях — ошибка и откат.
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1, @lists int, @members int, @dup int, @added int, @total int
          , @msg nvarchar(2048), @missing nvarchar(1000);

    -- 1. Списки по имени (справочник лежит копией на каждом экземпляре — одна строка на ListId)
    DROP TABLE IF EXISTS #fl_name;
    CREATE TABLE #fl_name (LIST_NAME nvarchar(400) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY);
    INSERT INTO #fl_name (LIST_NAME) VALUES
        (N'Черный список ФРОД'), (N'Черный список карт кассиров 01.2023'), (N'Обновляемый черный список кассиров 06.24');

    DROP TABLE IF EXISTS #fl_list;
    SELECT x.ListId, ListName = x.ListName COLLATE DATABASE_DEFAULT
    INTO #fl_list
    FROM (SELECT l.ListId, l.ListName, l.sys_change_operation,
                 rn = row_number() OVER (PARTITION BY l.ListId ORDER BY l.last_version DESC, l.dt_load DESC)
          FROM dbo.crmdata__List AS l WITH (NOLOCK)
          WHERE l.ID_COMPANY = @idc) AS x
    WHERE x.rn = 1
      AND isnull(x.sys_change_operation, 'I') <> 'D'
      AND x.ListName IN (SELECT LIST_NAME FROM #fl_name);

    SET @lists = (SELECT count(*) FROM #fl_list);
    SELECT @missing = string_agg(n.LIST_NAME, N'; ')
    FROM #fl_name AS n
    WHERE NOT EXISTS (SELECT 1 FROM #fl_list AS l WHERE l.ListName = n.LIST_NAME);

    IF @lists = 0
        THROW 50001, N'fraud_lists_load: в crmdata__List нет ни одного из трёх чёрных списков — I_FRAUD_CONTACT не изменена.', 1;
    IF @missing IS NOT NULL
    BEGIN
        SET @msg = concat(N'fraud_lists_load: в crmdata__List нет списка — ', @missing, N'; остальные добавляю.');
        RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
    END

    -- 2. Участники. Ключ участника — id (ListMemberId) + instance_id; строка удаления может прийти без номера списка,
    --    поэтому сначала ключи участников списков, затем последняя версия каждого ключа
    DROP TABLE IF EXISTS #fl_c;
    CREATE TABLE #fl_c (ID_CONTACT int NOT NULL PRIMARY KEY);

    -- 2а. loyalty__list_member_a
    DROP TABLE IF EXISTS #fl_ka;
    SELECT DISTINCT m.id, m.instance_id
    INTO #fl_ka
    FROM dbo.loyalty__list_member_a AS m WITH (NOLOCK)
    WHERE m.ID_COMPANY = @idc
      AND m.list_id IN (SELECT ListId FROM #fl_list);

    INSERT INTO #fl_c (ID_CONTACT)
    SELECT DISTINCT x.contact_id
    FROM (SELECT m.list_id, m.contact_id, m.sys_change_operation,
                 rn = row_number() OVER (PARTITION BY m.id, m.instance_id ORDER BY m.last_version DESC, m.dt_load DESC)
          FROM dbo.loyalty__list_member_a AS m WITH (NOLOCK)
               JOIN #fl_ka AS k ON k.id = m.id AND k.instance_id = m.instance_id
          WHERE m.ID_COMPANY = @idc) AS x
    WHERE x.rn = 1
      AND isnull(x.sys_change_operation, 'I') <> 'D'
      AND x.list_id IN (SELECT ListId FROM #fl_list)
      AND x.contact_id > 0
    OPTION (MAXDOP 2);

    -- 2б. crmdata__ListMember (участники CRM-списков)
    DROP TABLE IF EXISTS #fl_km;
    SELECT DISTINCT m.ListMemberId, m.instance_id
    INTO #fl_km
    FROM dbo.crmdata__ListMember AS m WITH (NOLOCK)
    WHERE m.ID_COMPANY = @idc
      AND m.ListId IN (SELECT ListId FROM #fl_list);

    INSERT INTO #fl_c (ID_CONTACT)
    SELECT DISTINCT x.EntityId
    FROM (SELECT m.ListId, m.EntityId, m.sys_change_operation,
                 rn = row_number() OVER (PARTITION BY m.ListMemberId, m.instance_id ORDER BY m.last_version DESC, m.dt_load DESC)
          FROM dbo.crmdata__ListMember AS m WITH (NOLOCK)
               JOIN #fl_km AS k ON k.ListMemberId = m.ListMemberId AND k.instance_id = m.instance_id
          WHERE m.ID_COMPANY = @idc) AS x
    WHERE x.rn = 1
      AND isnull(x.sys_change_operation, 'I') <> 'D'
      AND x.ListId IN (SELECT ListId FROM #fl_list)
      AND x.EntityId > 0
      AND NOT EXISTS (SELECT 1 FROM #fl_c AS c WHERE c.ID_CONTACT = x.EntityId)
    OPTION (MAXDOP 2);

    SET @members = (SELECT count(*) FROM #fl_c);

    -- 3. I_FRAUD_CONTACT: одна строка на клиента, новые участники — добавить; таблица заблокирована на время правки
    BEGIN TRY
        BEGIN TRANSACTION;

        WITH d AS (SELECT rn = row_number() OVER (PARTITION BY f.ID_COMPANY, f.ID_CONTACT ORDER BY f.SEGMENT)
                   FROM dbo.I_FRAUD_CONTACT AS f WITH (TABLOCKX)
                   WHERE f.ID_CONTACT IS NOT NULL)
        DELETE FROM d WHERE rn > 1;
        SET @dup = @@ROWCOUNT;

        INSERT INTO dbo.I_FRAUD_CONTACT (ID_CONTACT, SEGMENT, ID_COMPANY)
        SELECT c.ID_CONTACT, -1, @idc
        FROM #fl_c AS c
        WHERE NOT EXISTS (SELECT 1 FROM dbo.I_FRAUD_CONTACT AS f WHERE f.ID_COMPANY = @idc AND f.ID_CONTACT = c.ID_CONTACT);
        SET @added = @@ROWCOUNT;

        IF EXISTS (SELECT 1 FROM dbo.I_FRAUD_CONTACT AS f
                   WHERE f.ID_CONTACT IS NOT NULL
                   GROUP BY f.ID_COMPANY, f.ID_CONTACT
                   HAVING count(*) > 1)
            THROW 50002, N'fraud_lists_load: в I_FRAUD_CONTACT остались дубли клиента — откат, таблица не изменена.', 1;

        SET @total = (SELECT count(*) FROM dbo.I_FRAUD_CONTACT);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH

    SET @msg = concat(N'fraud_lists_load: списков ', @lists, N' из 3, участников ', @members,
                      N', добавлено в I_FRAUD_CONTACT ', @added, N', удалено дублей ', @dup, N', строк в таблице ', @total, N'.');
    RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
END
GO
IF OBJECT_ID('dbo.fraud_lists_load', 'P') IS NULL
BEGIN
    RAISERROR(N'СТОП: процедура dbo.fraud_lists_load не создалась — дальше не иду.', 16, 1);
    SET NOEXEC ON;
END
GO
-- ============================== 2. ТАБЛИЦА ДО ПЕРВОГО ЗАПУСКА — В БЭКАП (ОДИН РАЗ) ==============================
IF NOT EXISTS (SELECT 1 FROM dbo.I_DISCOUNT_PROC_BACKUP (nolock) WHERE PROC_NAME = N'table I_FRAUD_CONTACT before fraud_lists_load')
BEGIN
    INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    SELECT N'table I_FRAUD_CONTACT before fraud_lists_load', sysdatetime(),
           isnull((SELECT ID_CONTACT, SEGMENT, ID_COMPANY FROM dbo.I_FRAUD_CONTACT (nolock) FOR JSON PATH, INCLUDE_NULL_VALUES), N'[]');
    RAISERROR(N'I_FRAUD_CONTACT до первого запуска — в I_DISCOUNT_PROC_BACKUP.', 10, 1) WITH NOWAIT;
END
GO
-- ============================== 3. ПЕРВЫЙ ЗАПУСК ==============================
EXEC dbo.fraud_lists_load;
GO
-- ============================== 4. ШАГ FRAUD_LISTS В «MCI Regular update» ПЕРЕД CVM_CONTACT ==============================
USE msdb;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @job sysname = N'MCI Regular update', @name sysname = N'FRAUD_LISTS', @before sysname = N'CVM_CONTACT'
      , @cmd nvarchar(max) = N'exec dbo.fraud_lists_load;'
      , @job_id uniqueidentifier, @start_step int, @pos int, @next int, @rc int, @bad int, @diff nvarchar(max), @msg nvarchar(2048);

SELECT @job_id = job_id, @start_step = start_step_id FROM msdb.dbo.sysjobs (nolock) WHERE name = @job;
IF @job_id IS NULL
BEGIN
    RAISERROR(N'СТОП: задание «MCI Regular update» не найдено.', 16, 1);
    RETURN;
END

-- шаг уже стоит
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps (nolock) WHERE job_id = @job_id AND step_name = @name)
BEGIN
    IF EXISTS (SELECT 1
               FROM msdb.dbo.sysjobsteps AS s (nolock)
                    JOIN msdb.dbo.sysjobsteps AS c (nolock)
                      ON c.job_id = s.job_id AND c.step_name = @before AND c.step_id = s.step_id + 1
               WHERE s.job_id = @job_id AND s.step_name = @name AND LTRIM(RTRIM(s.command)) = @cmd
                 AND s.subsystem = N'TSQL' AND s.database_name = N'mci_model'
                 AND s.on_success_action = 3 AND s.on_fail_action = 3)
        RAISERROR(N'Шаг FRAUD_LISTS уже стоит перед CVM_CONTACT — задание не меняю.', 10, 1) WITH NOWAIT;
    ELSE
        RAISERROR(N'СТОП: шаг FRAUD_LISTS есть, но не перед CVM_CONTACT или с другими настройками — задание не меняю.', 16, 1);
    RETURN;
END

-- задание и загрузка MCI не идут (проверка ещё раз — перед правкой)
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a (nolock)
                JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
           WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                            N'CVM REBUILD ONCE')
             AND a.session_id = (SELECT max(s.session_id) FROM msdb.dbo.syssessions AS s (nolock))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'Job running: идёт «MCI Regular update» или загрузка MCI — шаг не ставлю, обменник повторит через 10 минут.', 16, 1);
    RETURN;
END

-- шаги задания сейчас
DROP TABLE IF EXISTS #cur;
SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
     , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
     , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
     , s.flags, s.proxy_id
INTO #cur
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id;

SELECT @pos = step_id FROM #cur (nolock) WHERE step_name = @before;

IF (SELECT count(*) FROM #cur (nolock)) <> 58
   OR (SELECT count(DISTINCT step_name) FROM #cur (nolock)) <> 58
   OR @pos IS NULL OR @pos <= @start_step
BEGIN
    RAISERROR(N'СТОП: в задании не 58 шагов или нет шага CVM_CONTACT — задание не изменено.', 16, 1);
    RETURN;
END

-- бэкап и вставка шага — одной транзакцией
BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO mci_model.dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    SELECT N'job MCI Regular update', sysdatetime(), (SELECT * FROM #cur (nolock) ORDER BY step_id FOR JSON PATH, INCLUDE_NULL_VALUES);

    EXEC @rc = msdb.dbo.sp_add_jobstep
          @job_id            = @job_id
        , @step_id           = @pos
        , @step_name         = @name
        , @subsystem         = N'TSQL'
        , @database_name     = N'mci_model'
        , @command           = @cmd
        , @on_success_action = 3      -- следующий шаг
        , @on_fail_action    = 3      -- ошибка → тоже следующий: CVM_CONTACT считает по таблице как есть
        , @retry_attempts    = 0
        , @retry_interval    = 0;
    IF @rc <> 0 THROW 50001, N'sp_add_jobstep — откат, задание не изменено.', 1;

    -- сверка: новый шаг — на месте CVM_CONTACT с нужными настройками
    DROP TABLE IF EXISTS #new;
    SELECT s.step_id, s.step_name, s.subsystem, s.command, additional_parameters = CAST(s.additional_parameters AS nvarchar(max))
         , s.cmdexec_success_code, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id, s.server
         , s.database_name, s.database_user_name, s.retry_attempts, s.retry_interval, s.os_run_priority, s.output_file_name
         , s.flags, s.proxy_id
    INTO #new
    FROM msdb.dbo.sysjobsteps AS s (nolock)
    WHERE s.job_id = @job_id;

    IF NOT EXISTS (SELECT 1 FROM #new (nolock)
                   WHERE step_id = @pos AND step_name = @name AND subsystem = N'TSQL' AND database_name = N'mci_model'
                     AND LTRIM(RTRIM(command)) = @cmd AND on_success_action = 3 AND on_fail_action = 3)
        THROW 50002, N'после вставки шаг FRAUD_LISTS не на месте CVM_CONTACT или с другими настройками — откат, задание не изменено.', 1;

    -- сверка: у остальных 58 шагов номер и переходы «на шаг N» с места вставки — +1, всё остальное — то же
    SELECT @bad = count(*), @diff = string_agg(CAST(d.step_name AS nvarchar(max)), N', ')
    FROM (SELECT step_id = CASE WHEN step_id >= @pos THEN step_id + 1 ELSE step_id END
               , step_name, subsystem, command, additional_parameters, cmdexec_success_code, on_success_action
               , on_success_step_id = CASE WHEN on_success_step_id >= @pos THEN on_success_step_id + 1 ELSE on_success_step_id END
               , on_fail_action
               , on_fail_step_id = CASE WHEN on_fail_step_id >= @pos THEN on_fail_step_id + 1 ELSE on_fail_step_id END
               , server, database_name, database_user_name, retry_attempts, retry_interval, os_run_priority, output_file_name
               , flags, proxy_id
          FROM #cur (nolock)
          EXCEPT
          SELECT step_id, step_name, subsystem, command, additional_parameters, cmdexec_success_code, on_success_action
               , on_success_step_id, on_fail_action, on_fail_step_id, server, database_name, database_user_name
               , retry_attempts, retry_interval, os_run_priority, output_file_name, flags, proxy_id
          FROM #new (nolock)
          WHERE step_name <> @name) AS d;
    IF @bad > 0 OR (SELECT count(*) FROM #new (nolock)) <> 59
    BEGIN
        SET @msg = concat(N'steps differ: ', left(isnull(@diff, N'(count)'), 1500), N' — после вставки у других шагов изменились номера, переходы или настройки, откат, задание не изменено.');
        THROW 50003, @msg, 1;
    END

    IF (SELECT start_step_id FROM msdb.dbo.sysjobs (nolock) WHERE job_id = @job_id) <> @start_step
        THROW 50004, N'после вставки изменился начальный шаг задания — откат, задание не изменено.', 1;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH

SET @next = @pos + 1;
RAISERROR(N'Готово: шаг FRAUD_LISTS (exec dbo.fraud_lists_load) — шаг %d «MCI Regular update», CVM_CONTACT — %d; шаги до правки — в mci_model.dbo.I_DISCOUNT_PROC_BACKUP.', 10, 1, @pos, @next) WITH NOWAIT;

-- итог
SELECT [Шаг] = s.step_id, [Имя] = s.step_name, [Команда] = s.command, [При ошибке] = s.on_fail_action, [На шаг] = s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s (nolock)
WHERE s.job_id = @job_id AND s.step_id BETWEEN @pos - 1 AND @pos + 1
ORDER BY s.step_id;
GO
SET NOEXEC OFF;
GO
