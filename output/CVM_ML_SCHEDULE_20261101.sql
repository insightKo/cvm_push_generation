-- Задание «MCI List to ML» (заливка списков в Manzana Online): кроме ежедневного запуска в 21:00 («every evening») —
-- проверка каждые полчаса с 21:30 до 23:30, нет ли новых списков (Елена 01.11.2026: «поменяй расписание заливки на ML,
-- чтобы оно каждые полчаса до 00 проверяло, нет ли новых списков для заливки»). Без новых списков задание отрабатывает за секунды.
-- Действующее расписание «every evening» не меняется — добавляется второе расписание к этому заданию.
-- Скрипт для обменника: повторный прогон ничего не задвоит (расписание с таким именем уже есть — выход).

SET NOCOUNT ON
GO

USE msdb
GO

declare @job_id uniqueidentifier = (select job_id from dbo.sysjobs where name = N'MCI List to ML')
if @job_id is null
begin
	raiserror(N'Нет задания «MCI List to ML» — расписание не добавлено', 16, 1)
	return
end

if exists (select 1 from dbo.sysjobschedules as js
			inner join dbo.sysschedules as s on s.schedule_id = js.schedule_id
		   where js.job_id = @job_id and s.name = N'every 30 min 21:30-23:30')
begin
	raiserror(N'Расписание «every 30 min 21:30-23:30» у «MCI List to ML» уже есть — ничего не меняю', 10, 1)
	return
end

exec dbo.sp_add_jobschedule
	  @job_id = @job_id
	, @name = N'every 30 min 21:30-23:30'
	, @enabled = 1
	, @freq_type = 4				-- ежедневно
	, @freq_interval = 1
	, @freq_subday_type = 4			-- каждые N минут
	, @freq_subday_interval = 30
	, @active_start_date = 20261101
	, @active_start_time = 213000
	, @active_end_time = 235959

select	s.name, s.enabled, s.freq_subday_type, s.freq_subday_interval, s.active_start_time, s.active_end_time
from dbo.sysjobschedules as js
		inner join dbo.sysschedules as s on s.schedule_id = js.schedule_id
where js.job_id = @job_id
GO
