-- ============================================================================================
-- CVM_CLUSTER_SCORE_STEP — перенос шага CLUSTER_SCORE в «MCI Regular update» сразу ПОСЛЕ шага CLUSTERS (20.10.2026):
--   скоры кластеров берут последнее готовое присвоение кластеров, а CLUSTERS присваивает их в том же проходе — после переноса
--   в день нового присвоения скоры считаются уже по нему, а не по прошлому.
-- Запустить целиком в SSMS (F5), когда «MCI Regular update» НЕ идёт. Повторный запуск ничего не меняет.
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   msdb: задание «MCI Regular update» — шаг CLUSTER_SCORE удаляется со своего места и добавляется сразу после CLUSTERS
--         с той же командой, базой и настройками шага (журнал, повторы), при успехе и при ошибке — следующий шаг; номера
--         шагов между ними сдвигаются на 1, переходы «на шаг N» msdb пересчитывает сам. Если CLUSTER_SCORE уже после
--         CLUSTERS — ничего не делается.
--         СТОП без изменений, если: задание идёт; шага нет или он не один; у CLUSTERS при успехе не «следующий шаг»;
--         какой-то шаг переходит «на шаг N» прямо на CLUSTER_SCORE (msdb превратил бы такой переход в «завершить задание»)
--         или на шаг сразу после CLUSTERS (такой переход стал бы перепрыгивать новый CLUSTER_SCORE). Переходы «на шаг N»
--         всего задания выводятся в конце.
--   НЕ ТРОГАЕТСЯ: остальные шаги задания, процедуры и таблицы mci_model.
-- ============================================================================================
USE msdb
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
DECLARE @job sysname = N'MCI Regular update';
DECLARE @job_id uniqueidentifier, @cs int, @cl int, @rc int = 0,
        @n_steps int, @n_cs int, @n_cl int, @cl_ok_act tinyint, @bad_jumps int,
        @cmd nvarchar(max), @db sysname, @db_user sysname, @out nvarchar(200), @flags int,
        @retry_att int, @retry_int int, @proxy_id int;

SELECT @job_id = job_id FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = @job;

SELECT @n_steps   = COUNT(*),
       @n_cs      = SUM(CASE WHEN step_name = N'CLUSTER_SCORE' THEN 1 ELSE 0 END),
       @n_cl      = SUM(CASE WHEN step_name = N'CLUSTERS' THEN 1 ELSE 0 END),
       @cs        = MAX(CASE WHEN step_name = N'CLUSTER_SCORE' THEN step_id END),
       @cl        = MAX(CASE WHEN step_name = N'CLUSTERS' THEN step_id END),
       @cl_ok_act = MAX(CASE WHEN step_name = N'CLUSTERS' THEN on_success_action END)
FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
WHERE job_id = @job_id;

-- настройки шага как есть — новый шаг повторяет их, а не только команду
SELECT @cmd = command, @db = database_name, @db_user = database_user_name, @out = output_file_name, @flags = flags,
       @retry_att = retry_attempts, @retry_int = retry_interval, @proxy_id = proxy_id
FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
WHERE job_id = @job_id AND step_id = @cs;

-- переходы «на шаг N» (action 4), которые перенос сломает:
--   на CLUSTER_SCORE — sp_delete_jobstep заменяет их на «завершить задание» (успех/ошибка);
--   на шаг сразу после CLUSTERS — sp_add_jobstep сдвигает их вместе с целью, и они перепрыгивают новый CLUSTER_SCORE
SELECT @bad_jumps = COUNT(*)
FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
WHERE job_id = @job_id
  AND (   (on_success_action = 4 AND on_success_step_id IN (@cs, @cl + 1))
       OR (on_fail_action    = 4 AND on_fail_step_id    IN (@cs, @cl + 1)));

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
           WHERE a.job_id = @job_id
             AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
    RAISERROR(N'СТОП: «MCI Regular update» сейчас идёт — номера шагов не сдвигаю, задание не изменено. Запусти после окончания.', 16, 1);
ELSE IF @job_id IS NULL OR @cs IS NULL OR @cl IS NULL
    RAISERROR(N'СТОП: нет задания или в нём нет шага CLUSTER_SCORE или CLUSTERS — задание не изменено.', 16, 1);
ELSE IF @n_cs > 1 OR @n_cl > 1
    RAISERROR(N'СТОП: шаг CLUSTER_SCORE или CLUSTERS в задании не один — задание не изменено.', 16, 1);
ELSE IF @cs > @cl
    RAISERROR(N'CLUSTER_SCORE уже стоит после CLUSTERS — задание не изменено.', 10, 1);
ELSE IF @cl_ok_act <> 3
    RAISERROR(N'СТОП: у CLUSTERS при успехе не «следующий шаг» — новый CLUSTER_SCORE не выполнялся бы. Задание не изменено.', 16, 1);
ELSE IF @bad_jumps > 0
    RAISERROR(N'СТОП: есть переходы «на шаг N» на CLUSTER_SCORE или на шаг сразу после CLUSTERS (список ниже) — задание не изменено.', 16, 1);
ELSE
BEGIN
    BEGIN TRAN;
        EXEC @rc = msdb.dbo.sp_delete_jobstep @job_id = @job_id, @step_id = @cs;
        IF @rc <> 0
        BEGIN
            IF @@TRANCOUNT > 0 ROLLBACK;
            RAISERROR(N'СТОП: шаг CLUSTER_SCORE не удалён — задание не изменено.', 16, 1);
            RETURN;
        END
        -- после удаления CLUSTERS сдвинулся на 1 вверх (номер @cl − 1) — новый шаг сразу за ним получает номер @cl
        EXEC @rc = msdb.dbo.sp_add_jobstep @job_id = @job_id, @step_id = @cl, @step_name = N'CLUSTER_SCORE',
             @subsystem = N'TSQL', @database_name = @db, @database_user_name = @db_user, @command = @cmd,
             @output_file_name = @out, @flags = @flags, @retry_attempts = @retry_att, @retry_interval = @retry_int,
             @proxy_id = @proxy_id, @on_success_action = 3, @on_fail_action = 3;
        IF @rc <> 0
        BEGIN
            IF @@TRANCOUNT > 0 ROLLBACK;
            RAISERROR(N'СТОП: шаг CLUSTER_SCORE не добавлен — изменения откачены, задание как было.', 16, 1);
            RETURN;
        END
        -- контроль до фиксации: CLUSTERS на @cl − 1, CLUSTER_SCORE на @cl, число шагов прежнее
        IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
                       WHERE job_id = @job_id AND step_id = @cl AND step_name = N'CLUSTER_SCORE')
           OR NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
                          WHERE job_id = @job_id AND step_id = @cl - 1 AND step_name = N'CLUSTERS')
           OR (SELECT COUNT(*) FROM msdb.dbo.sysjobsteps WITH (NOLOCK) WHERE job_id = @job_id) <> @n_steps
        BEGIN
            IF @@TRANCOUNT > 0 ROLLBACK;
            RAISERROR(N'СТОП: после переноса номера шагов не сошлись — изменения откачены, задание как было.', 16, 1);
            RETURN;
        END
    COMMIT;
END
GO

-- Шаги вокруг: CVM_CONTACT … CLUSTERS, CLUSTER_SCORE и соседние
SELECT s.step_id, s.step_name, s.command, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update'
  AND s.step_name IN (N'PRODUCT_SCORE_REGION', N'GCG_REPORT', N'I_CONTACT_PRICE_SEGMENT', N'GLOBAL_CG', N'CLUSTERS',
                      N'CLUSTER_SCORE', N'PP treatment')
ORDER BY s.step_id;

-- Все переходы «на шаг N» в задании — куда ведут после переноса
SELECT s.step_id, s.step_name, s.on_success_action, s.on_success_step_id, t1.step_name AS on_success_step_name,
       s.on_fail_action, s.on_fail_step_id, t2.step_name AS on_fail_step_name
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
LEFT JOIN msdb.dbo.sysjobsteps AS t1 WITH (NOLOCK) ON t1.job_id = s.job_id AND t1.step_id = s.on_success_step_id
LEFT JOIN msdb.dbo.sysjobsteps AS t2 WITH (NOLOCK) ON t2.job_id = s.job_id AND t2.step_id = s.on_fail_step_id
WHERE j.name = N'MCI Regular update'
  AND (s.on_success_action = 4 OR s.on_fail_action = 4)
ORDER BY s.step_id;
GO
