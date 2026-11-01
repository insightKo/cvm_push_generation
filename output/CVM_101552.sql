-- CVM · ноябрь 2026 · 101552 «Вернём 150р. монетами с покупки от 1000р.» · 03–26.11.2026, монеты тратить до 29.11.2026 23:59
-- Аудитория (согласовано Еленой 01.11.2026, файл «ноябрь общие сегменты»): Активные и Активные LFL (I_CVM_CONTACT SEGMENT 2, 3),
--   организация 1, ID_SALE_CHANNEL 1 офлайн и 2 омни; низкий ценовой сегмент — готовый PRICE_SEGMENT = 3 MCI (I_CONTACT_PRICE_SEGMENT,
--   процедура cont_price_segment; решение Елены «1 а» — старый сегмент как есть); средний чек за 6 недель AVG_CHECK <= 750р.
--   Замер 01.11.2026 по I_CVM_CONTACT на 31.10: 3 183 037 клиентов, из них с пушем 2 138 995.
-- Стандартная сборка как CVM_101538_101549.sql: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG;
--   КГ 5% (SEGMENT 2–3, RN/20 по BUDGET).
-- Механика 5 справочника «Кешбэк X монет за покупку» (MCI 9), без активации, порог 1000р., не более 3 начислений за акцию.
--   LOAD_TO_ML = 0 (акция в Manzana Online — заберёт «MCI List to ML»), LOAD_TO_MC = 1 (PUSH).
--   Период «после»: с 27.11 до финиша + 4 (30.11) — позже даты сгорания монет 29.11.
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после 26.11.2026 — ничего не делается;
--   · шапка I_PROMO заводится, только если её нет;
--   · список уже есть — сборка пропускается (повторный прогон не задвоит);
--   · идёт «MCI Regular update» и не прошёл шаг GLOBAL_CG (до него пересчитываются I_CONTACT, I_CVM_CONTACT,
--     I_CONTACT_PRICE_SEGMENT) — до чтения и после чтения; или прочитано меньше 2,5 млн клиентов аудитории — ошибка 16,
--     раннер повторит через 10 минут, ничего не записано;
--   · подбор КГ — до ошибки < 0,05% или 1000 попыток, берётся лучшая попытка.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
SET XACT_ABORT ON		-- любая ошибка выполнения обрывает пакет: после сбоя до INSERT в I_PROMO_OFFER дело не дойдёт
GO

USE mci_model
GO


-- ==================================================================================
-- 1. ШАПКА АКЦИИ В I_PROMO (заводится, только если её нет)
-- ==================================================================================

if cast(getdate() as date) > '2026-11-26'
begin
	raiserror(N'101552: акция прошла (26.11.2026) — шапка не заводится, файл можно убрать из папки', 10, 1)
	return
end

declare @mech nvarchar(200) = (select MECHANICS from I_MECHANICS (nolock) where ID_MECHANICS = 9)

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, v.D), dateadd(day, -1, v.D), v.D, v.FD, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, v.FD), v.AFTER_TO
	 , N'PUSH', N'Активные, низкий ЦИ', 8, null, @mech, 9, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101552, N'Вернём 150р. монетами с покупки от 1000р.', N'101552_Вернём 150р монетами покупки', cast('2026-11-03' as date), cast('2026-11-26' as date), cast('2026-11-30' as date))
) as v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML, D, FD, AFTER_TO)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)
GO


-- ==================================================================================
-- 2. СПИСОК И КОНТРОЛЬНАЯ ГРУППА
-- ==================================================================================

if cast(getdate() as date) > '2026-11-26'
begin
	raiserror(N'101552: акция прошла (26.11.2026) — список не собирается', 10, 1)
	return
end
if not exists (select 1 from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO = 101552)
begin
	raiserror(N'101552: нет шапки в I_PROMO — список не собран', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101552)
begin
	raiserror(N'101552: список уже в I_PROMO_OFFER — сборка пропущена', 10, 1)
	return
end

declare @msg nvarchar(400)

-- защита от чтения во время перезаливки — по состоянию задания (как CVM_CONTACT_T2 / CVM_BUILD_OFFERS): «MCI Regular update»
-- от старта до конца шага GLOBAL_CG перезаписывает I_CONTACT, I_CVM_CONTACT (CVM_CONTACT: TRUNCATE + INSERT),
-- I_CONTACT_PRICE_SEGMENT (cont_price_segment по понедельникам: DELETE + INSERT) и I_GLOBAL_CG. Порог 2,5 млн ниже ловит только
-- недобор больше 21 % — чтение на хвосте INSERT прошло бы. Шаг ищется по имени; не найден — ждём конца всего задания.
declare @st_end int = isnull((select max(s.step_id)
							  from msdb.dbo.sysjobsteps as s (nolock)
									inner join msdb.dbo.sysjobs as j (nolock) on j.job_id = s.job_id
							  where j.name = N'MCI Regular update' and s.step_name = N'GLOBAL_CG'), 1000)
if exists (select 1
		   from msdb.dbo.sysjobactivity as a (nolock)
				inner join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
		   where j.name = N'MCI Regular update'
			 and a.session_id = (select max(s.session_id) from msdb.dbo.syssessions as s (nolock))
			 and a.start_execution_date is not null and a.stop_execution_date is null
			 and isnull(a.last_executed_step_id, 0) < @st_end)
begin
	raiserror(N'101552: идёт «MCI Regular update», шаг GLOBAL_CG не пройден (пересчёт I_CVM_CONTACT / I_CONTACT_PRICE_SEGMENT) — список не собран, раннер повторит', 16, 1)
	return
end

-- аудитория: Активные и Активные LFL, низкий ЦИ, средний чек до 750р.
drop table if exists #a
select	a.ID_CONTACT
		, SEGMENT = max(a.SEGMENT)
into #a
from I_CVM_CONTACT as a (nolock)
		inner join I_CONTACT_PRICE_SEGMENT as s (nolock)
		on s.ID_CONTACT = a.ID_CONTACT
		and s.ID_COMPANY = a.ID_COMPANY
		and s.ID_ORGANIZATION = a.ID_ORGANIZATION
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT <> 0
	and a.SEGMENT in (2, 3)
	and a.ID_SALE_CHANNEL in (1, 2)
	and s.PRICE_SEGMENT = 3
	and a.AVG_CHECK <= 750
group by a.ID_CONTACT

create clustered columnstore index cci_a on #a

-- защита: I_CVM_CONTACT («MCI Regular update») или I_CONTACT_PRICE_SEGMENT (cont_price_segment, DELETE + INSERT) пересчитываются —
-- неполное чтение (замер 01.11: 3,18 млн)
declare @n_a bigint = (select count(*) from #a)
if @n_a < 2500000
begin
	set @msg = concat(N'101552: клиентов аудитории ', @n_a, N' — меньше 2,5 млн (идёт пересчёт I_CVM_CONTACT или I_CONTACT_PRICE_SEGMENT?), список не собран')
	raiserror(@msg, 16, 1)
	return
end

-- список: пуш, минус I_GLOBAL_CG
drop table if exists #x

select	ID_PROMO = 101552
		, g.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, CRM_GUID = max(b.CRM_GUID)
		, LOAD_TO_MC = 1
into #x
from	  #a as g
		inner join I_CONTACT as b (nolock)
		on g.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on g.ID_CONTACT = c.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
	and c.ID_CONTACT is null
group by g.ID_CONTACT

create clustered columnstore index cci_x on #x

set @msg = concat(N'Клиентов аудитории ', @n_a, N', строк списка ', (select count(*) from #x))
raiserror(@msg, 0, 1) with nowait


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT = round(sum(b.BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(b.BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION = 1
where b.ID_CONTACT <> 0
group by a.ID_CONTACT

create clustered columnstore index cci_t on #t

-- защита: I_CVM_CONTACT пересчитали между чтением #a и #t — у части строк списка нет строки для подбора КГ
if (select count(*) from #t) <> (select count(*) from #x)
begin
	raiserror(N'101552: не у всех клиентов списка прочитана строка I_CVM_CONTACT (идёт пересчёт?) — список не собран, раннер повторит', 16, 1)
	return
end

-- защита: задание стартовало или дошло до перезаливки, пока читались #a, #x, #t
if exists (select 1
		   from msdb.dbo.sysjobactivity as a (nolock)
				inner join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
		   where j.name = N'MCI Regular update'
			 and a.session_id = (select max(s.session_id) from msdb.dbo.syssessions as s (nolock))
			 and a.start_execution_date is not null and a.stop_execution_date is null
			 and isnull(a.last_executed_step_id, 0) < @st_end)
begin
	raiserror(N'101552: «MCI Regular update» стартовало во время чтения аудитории — список не собран, раннер повторит', 16, 1)
	return
end


-- КГ 5% (SEGMENT 2–3, RN/20): страта по MIN_DATA, COST_DISCOUNT, LAST_DATA, случайный клиент в страте
drop table if exists #best_cg
create table #best_cg (ID_CONTACT int)

declare @div int = 20, @error float = 1.0, @best float = null, @iter int = 0

drop table if exists #tp
select ID_CONTACT, COST_DISCOUNT1
		, SEGMENT = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA) / @div
into #tp
from #t

create clustered columnstore index cci_tp on #tp	-- 2,1 млн строк, каждая попытка читает целиком (пункт 12: не куча)

while @error >= 0.0005 and @iter < 1000
begin

	drop table if exists #local_cg

	;with n as (
	select ID_CONTACT
			, R = ROW_NUMBER() over (partition by SEGMENT order by newid())
	from #tp
	)
	select ID_CONTACT
			, CONTROL_GROUP=1
	into #local_cg
	from n
	where R=1

	drop table if exists #stat;
	;with t as (
	select CG = IIF(b.ID_CONTACT is not NULL, 1,0)
			, c.COST_DISCOUNT1
			, c.ID_CONTACT
	from	#tp as c
			left join #local_cg as b
			on c.ID_CONTACT=b.ID_CONTACT
	)
	select	CG
			, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
			, COUNT_CLIENT = count(*)		-- ID_CONTACT в #tp уникален: то же число без второй сортировки 2,1 млн строк в попытке
	into #stat
	from t
	group by CG
	;

	set @error = isnull((select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
			from #stat a
				join #stat b
					on a.CG = 0
					and b.CG = 1
			), 1.0)

	if @best is null or @error < @best
	begin
		set @best = @error
		truncate table #best_cg
		insert #best_cg (ID_CONTACT) select ID_CONTACT from #local_cg
	end

	set @iter += 1
end

set @msg = concat(N'101552: КГ ', (select count(*) from #best_cg), N', попыток ', @iter, N', ошибка ', @best)
raiserror(@msg, 0, 1) with nowait


UPDATE d
SET CONTROL_GROUP=1
from	#best_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


-- список записал другой запуск, пока подбиралась КГ
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101552)
begin
	raiserror(N'101552: список записан другим запуском — вставка пропущена', 10, 1)
	return
end

INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
from #x


-- сводка — только в запуске, который записал список
select	p.ID_PROMO
		, [Название списка] = p.PROMO_NAME_ML
		, [Клиентов на отправку] = sum(iif(x.CONTROL_GROUP = 0, 1, 0))
		, [КГ] = sum(iif(x.CONTROL_GROUP = 1, 1, 0))
		, [Дата старта] = p.START_DATE
		, [Загрузка в Манзана Онлайн] = N'да'
from #x as x
		inner join I_PROMO as p (nolock)
		on p.ID_PROMO = x.ID_PROMO
		and p.ID_COMPANY = 1
group by p.ID_PROMO, p.PROMO_NAME_ML, p.START_DATE
GO
