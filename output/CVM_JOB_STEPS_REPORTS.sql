-- ============================================================================================
-- CVM_JOB_STEPS_REPORTS — три новые процедуры отчётов в ночной джоб «MCI Regular update».
-- Запустить ОДИН РАЗ после CVM_GCG_REPORT.sql, CVM_BASE_MONTH.sql и CVM_PRODUCT_DEMAND.sql.
--
-- Куда встают шаги:
--   49 GCG_REPORT — сразу после шага 21 (CVM_CONTACT), до шага 22 (CVM_CONTACT_DATA):
--      отчёт ГКГ важный и идёт раньше (решение Елены 09.10.2026); чеки к этому месту загружены (шаг 8).
--   50 CVM_BASE_MONTH, 51 PRODUCT_DEMAND — сразу после шага 40 (I_REPORT), до шага 41 (cohort_new):
--      базе нужны снимки I_CVM_CONTACT_DATA (шаг 22), спросу — список LFL-магазинов (шаг 39);
--      и это раньше тяжёлых шагов 43–45 (тренды, миссии, churn).
-- Как: номера существующих шагов НЕ сдвигаются. Новые шаги дописываются в конец (49, 50, 51),
--   в цепочку их включают переходы:
--      21 → 49 GCG_REPORT → 22
--      40 → 50 CVM_BASE_MONTH → 51 PRODUCT_DEMAND → 41
--   У шагов 21 и 40 меняется только переход при успехе; при ошибке они по-прежнему идут на 47.
-- При падении нового шага цепочка идёт дальше (как у шагов 17–18 любимых): сбой ГКГ не останавливает
--   CVM_CONTACT_DATA и дальше, сбой базы/спроса — тренды, миссии и churn; ошибка видна в истории джоба.
-- Повторный запуск не задвоит шаги: если шаг с таким именем уже есть — СТОП, ничего не меняется.
-- Если джоб сейчас выполняется — СТОП: запустить после его окончания.
-- Шаги 49–51 и переходы 21 → 49, 40 → 50 пишутся одной транзакцией: при любой ошибке всё откатывается.
-- ============================================================================================

USE msdb
GO

SET NOCOUNT ON;

DECLARE @job sysname = N'MCI Regular update';

-- ============================== 0. ПРОВЕРКИ ==============================
DROP TABLE IF EXISTS #job_steps;
SELECT s.step_id, s.step_name, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
INTO #job_steps
FROM msdb.dbo.sysjobsteps AS s
     JOIN msdb.dbo.sysjobs AS j ON j.job_id = s.job_id
WHERE j.name = @job;

IF (SELECT max(step_id) FROM #job_steps) <> 48
   OR NOT EXISTS (SELECT 1 FROM #job_steps WHERE step_id = 21 AND step_name = N'CVM_CONTACT' AND on_success_action = 3)
   OR NOT EXISTS (SELECT 1 FROM #job_steps WHERE step_id = 22 AND step_name = N'CVM_CONTACT_DATA')
   OR NOT EXISTS (SELECT 1 FROM #job_steps WHERE step_id = 40 AND step_name = N'I_REPORT' AND on_success_action = 3)
   OR NOT EXISTS (SELECT 1 FROM #job_steps WHERE step_id = 41 AND step_name = N'cohort_new')
BEGIN
    RAISERROR(N'СТОП: в джобе не 48 шагов или шаги 21/22/40/41 не CVM_CONTACT/CVM_CONTACT_DATA/I_REPORT/cohort_new — джоб изменился, ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
-- шаг 48 с «next step» после появления шага 49 провалился бы в 49 → 22 и прогнал бы цепочку заново;
-- «go to step» на номер > 48 (ссылка вперёд на несуществующий шаг) после добавления попал бы в новый шаг
IF EXISTS (SELECT 1 FROM #job_steps WHERE step_id = 48 AND (on_success_action = 3 OR on_fail_action = 3))
   OR EXISTS (SELECT 1 FROM #job_steps WHERE (on_success_action = 4 AND on_success_step_id > 48)
                                          OR (on_fail_action = 4 AND on_fail_step_id > 48))
BEGIN
    RAISERROR(N'СТОП: шаг 48 переходит на «следующий» или есть переход на шаг > 48 — новые шаги встроились бы не туда, ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
IF EXISTS (SELECT 1 FROM #job_steps WHERE step_name IN (N'GCG_REPORT', N'CVM_BASE_MONTH', N'PRODUCT_DEMAND'))
BEGIN
    RAISERROR(N'СТОП: шаги отчётов уже добавлены — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a
                JOIN msdb.dbo.sysjobs AS j ON j.job_id = a.job_id
           WHERE j.name = @job
             AND a.start_execution_date IS NOT NULL
             AND a.stop_execution_date IS NULL
             AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions))
BEGIN
    RAISERROR(N'СТОП: джоб «MCI Regular update» сейчас выполняется — запустить после его окончания, ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1–2. НОВЫЕ ШАГИ 49–51 И ПЕРЕХОДЫ 21 → 49, 40 → 50 — одной транзакцией ==============================
-- Без транзакции ошибка на одном из шагов не прерывает батч: переход на недобавленный шаг оборвал бы джоб
-- (например, 40 → 50 без шага 51 — «следующего» нет, джоб закончился бы без 41–46).
DECLARE @rc int = 0;
BEGIN TRANSACTION;

-- повторная проверка внутри батча: если батч запущен отдельно от проверок, sp_add_jobstep с @step_id = 49
-- при уже существующем 49 сдвинул бы шаги 49+ и ссылки на них
IF isnull((SELECT max(s.step_id) FROM msdb.dbo.sysjobsteps AS s JOIN msdb.dbo.sysjobs AS j ON j.job_id = s.job_id
           WHERE j.name = N'MCI Regular update'), 0) <> 48
   GOTO QuitWithRollback;

EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = N'MCI Regular update', @step_id = 49, @step_name = N'GCG_REPORT',
     @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.gcg_report;',
     @on_success_action = 4, @on_success_step_id = 22,                    -- обратно в цепочку: CVM_CONTACT_DATA
     @on_fail_action = 4, @on_fail_step_id = 22;
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = N'MCI Regular update', @step_id = 50, @step_name = N'CVM_BASE_MONTH',
     @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cvm_base_month;',
     @on_success_action = 3, @on_fail_action = 3;                         -- дальше: шаг 51
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = N'MCI Regular update', @step_id = 51, @step_name = N'PRODUCT_DEMAND',
     @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.product_demand;',
     @on_success_action = 4, @on_success_step_id = 41,                    -- обратно в цепочку: cohort_new
     @on_fail_action = 4, @on_fail_step_id = 41;
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

-- включить в цепочку: после 21 → 49 (переход 21 при ошибке → 47 не меняется)
EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = N'MCI Regular update', @step_id = 21,
     @on_success_action = 4, @on_success_step_id = 49;
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

-- включить в цепочку: после 40 → 50 (переход 40 при ошибке → 47 не меняется)
EXEC @rc = msdb.dbo.sp_update_jobstep @job_name = N'MCI Regular update', @step_id = 40,
     @on_success_action = 4, @on_success_step_id = 50;
IF (@@ERROR <> 0 OR @rc <> 0) GOTO QuitWithRollback;

COMMIT TRANSACTION;
GOTO EndSave;

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;
    RAISERROR(N'СТОП: шаги не добавлены (джоб изменился или ошибка sp_add_jobstep / sp_update_jobstep) — всё откачено, джоб не изменён.', 16, 1);
EndSave:
GO

SET NOEXEC OFF;
GO

-- ============================== 3. ИТОГ: шаги 19–23 и 38–51 ==============================
SELECT s.step_id, s.step_name, s.command, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s
     JOIN msdb.dbo.sysjobs AS j ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update' AND (s.step_id BETWEEN 19 AND 23 OR s.step_id >= 38)
ORDER BY s.step_id;
