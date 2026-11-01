-- CVM · ноябрь 2026 · 101556 «Дарим 200р. монетами на неделю» — Спящие (I_CVM_CONTACT SEGMENT 6), организация 1, офлайн и омни.
--   Каждый понедельник 02, 09, 16, 23.11 — 200 монет на счёт без активации, монеты до воскресенья 23:59 (101556_1–_4 в Google);
--   одна шапка и один список на весь ноябрь, как 101426. Старт 02.11 — по слову Елены 01.11.2026 («почему не завтра»).
--   Замер 01.11: 3 881 614 клиентов, на отправку до КГ 682 241. Сборка: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG,
--   КГ 10% (RN/10) по LTV. Механика «Начисление монет» (MCI 15), LOAD_TO_ML = 0, LOAD_TO_MC = 1. Период «после» 30.11–03.12.
--   Спящие не входят ни в одну другую общую акцию ЖЦ. Шапка — если нет; список есть — пропуск; прочитано меньше
--   3 млн Спящих — ошибка 16, раннер повторит.

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
-- 1. ШАПКА В I_PROMO (заводится, только если её нет)
-- ==================================================================================

if cast(getdate() as date) > '2026-11-29'
begin
	raiserror(N'101556: акция прошла — шапка не заводится, файл можно убрать из папки', 10, 1)
	return
end

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, v.D), dateadd(day, -1, v.D), v.D, v.FD, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, v.FD), v.AFTER_TO
	 , N'PUSH', v.SEG, v.ID_SEG, null, (select MECHANICS from I_MECHANICS as m (nolock) where m.ID_MECHANICS = v.ID_MECH), v.ID_MECH, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101556, N'Дарим 200р. монетами на неделю', N'101556_Дарим 200р монетами неделю', cast('2026-11-02' as date), cast('2026-11-29' as date), cast('2026-12-03' as date), N'Спящие', 3, 15)
) as v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML, D, FD, AFTER_TO, SEG, ID_SEG, ID_MECH)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)

GO


-- ==================================================================================
-- 2. СПИСОК И КОНТРОЛЬНАЯ ГРУППА
-- ==================================================================================

if cast(getdate() as date) > '2026-11-29'
begin
	raiserror(N'101556: акция прошла — список не собирается', 10, 1)
	return
end
if not exists (select 1 from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO = 101556)
begin
	raiserror(N'101556: нет шапки в I_PROMO — список не собран', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101556)
begin
	raiserror(N'101556: список уже в I_PROMO_OFFER — сборка пропущена', 10, 1)
	return
end

declare @msg nvarchar(400)

-- Спящие офлайна и омни
drop table if exists #a
select	ID_CONTACT, SEGMENT = max(SEGMENT), ID_PROMO = 101556
into #a
from I_CVM_CONTACT (nolock)
where ID_COMPANY = 1 and ID_ORGANIZATION = 1
	and ID_CONTACT <> 0
	and SEGMENT = 6
	and ID_SALE_CHANNEL in (1, 2)
group by ID_CONTACT

create clustered columnstore index cci_a on #a

declare @n_a bigint = (select count(*) from #a)
if @n_a < 3000000
begin
	set @msg = concat(N'101556: Спящих ', @n_a, N' — меньше 3 млн (неполное чтение?), список не собран')
	raiserror(@msg, 16, 1)
	return
end

-- список: пуш, минус I_GLOBAL_CG
drop table if exists #x

select	a.ID_PROMO
		, a.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, CRM_GUID = max(b.CRM_GUID)
		, LOAD_TO_MC = 1
		, PART = iif(a.SEGMENT >= 4, 4, 1)
into #x
from	  #a as a
		inner join I_CONTACT as b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on a.ID_CONTACT = c.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
	and c.ID_CONTACT is null
group by a.ID_PROMO, a.ID_CONTACT, a.SEGMENT

create clustered columnstore index cci_x on #x


set @msg = concat(N'Клиентов аудиторий ', @n_a, N', строк списков ', (select count(*) from #x))
raiserror(@msg, 0, 1) with nowait


drop table if exists #t

select	a.ID_PROMO
		, a.PART
		, a.ID_CONTACT
		, COST_DISCOUNT = round(sum(iif(a.PART = 4, b.LTV, b.BUDGET))/10,0)
		, COST_DISCOUNT1 = sum(iif(a.PART = 4, b.LTV, b.BUDGET))
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION = 1
where b.ID_CONTACT <> 0
group by a.ID_PROMO, a.PART, a.ID_CONTACT

create clustered columnstore index cci_t on #t

-- защита: I_CVM_CONTACT пересчитали между чтениями — у части строк списка нет строки для подбора КГ
if (select count(*) from #t) <> (select count(*) from #x)
begin
	raiserror(N'101556: не у всех клиентов списков прочитана строка I_CVM_CONTACT — списки не собраны, раннер повторит', 16, 1)
	return
end


-- КГ 10% (SEGMENT 6, RN/10 по LTV)
drop table if exists #cg
create table #cg (ID_PROMO int, ID_CONTACT int)
drop table if exists #best_cg
create table #best_cg (ID_CONTACT int)

declare @p int, @part int, @div int, @error float, @best float, @iter int

drop table if exists #parts
select distinct ID_PROMO, PART into #parts from #t

declare parts cursor local fast_forward for
	select ID_PROMO, PART from #parts
open parts
fetch next from parts into @p, @part

while @@fetch_status = 0
begin
	set @div = iif(@part = 4, 10, 20)
	set @error = 1.0
	set @best = null
	set @iter = 0
	truncate table #best_cg

	drop table if exists #tp
	select ID_CONTACT, COST_DISCOUNT1
			, SEGMENT = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA) / @div
	into #tp
	from #t
	where ID_PROMO = @p and PART = @part

	create clustered columnstore index cci_tp on #tp

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
	from	#tp as c
			left join #local_cg as b
			on c.ID_CONTACT=b.ID_CONTACT
	)
	select	CG
			, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(*)
			, COUNT_CLIENT = count(*)
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

	set @msg = concat(@p, N' часть ', @part, N': КГ ', (select count(*) from #best_cg), N', попыток ', @iter, N', ошибка ', @best)
	raiserror(@msg, 0, 1) with nowait

	insert into #cg (ID_PROMO, ID_CONTACT)
	select @p, ID_CONTACT from #best_cg

	fetch next from parts into @p, @part
end
close parts
deallocate parts


UPDATE d
SET CONTROL_GROUP=1
from	#cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT
		and a.ID_PROMO=d.ID_PROMO


-- списки записал другой запуск, пока подбиралась КГ
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101556))
begin
	raiserror(N'101556: списки записаны другим запуском — вставка пропущена', 10, 1)
	return
end

INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
from #x


-- сводка — только в запуске, который записал списки
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
order by p.ID_PROMO
GO
