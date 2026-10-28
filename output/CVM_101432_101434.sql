-- CVM · чековая лестница · 101432 / 101433 / 101434 · чт 29.10 – ср 04.11.2026 · Активные и Новые по ступеням среднего чека
-- Правило — строки лестницы в CVM_BUILD_OFFERS.sql (ОК Елены 05.10.2026; окупаемость ступени A <= T − D/0,3 из расчёта лестницы
-- августа 2026; верхнюю ступень берём всю — решение Елены 12.08.2026):
--   ступень 1 · 101432 «Активируй 50р. на чек от 500р.»   — средний чек за 6 недель (I_CVM_CONTACT.AVG_CHECK) меньше 333р.;
--   ступень 2 · 101433 «Активируй 100р. на чек от 1000р.» — от 333р. до 667р. (667 не включая);
--   ступень 3 · 101434 «Активируй 300р. на чек от 3000р.» — от 667р., минус получатели и КГ списка 101427 (то же предложение
--                                                          300р. от 3000р. до 31.10; ОК Елены 07.10.2026).
--   Клиенты — I_CVM_CONTACT (организация 1, SEGMENT 1, 2, 3), с пушем (I_CONTACT: HAS_PUSH = 1, TOKENS = 1), минус I_GLOBAL_CG;
--   клиент — ровно в одной ступени; у кого AVG_CHECK пуст — ни в одной (правило по порогу не проверить).
--   КГ 5% (RN/20) по BUDGET отдельно в каждой ступени, подбор до ошибки среднего BUDGET < 0,05%. Правило чеков в день
--   (решение Елены 23.10.2026): клиенты, у которых за 8 недель до последнего дня чеков в какой-то день больше 5 чеков,
--   в розыгрыш КГ не попадают (в списке на отправку остаются).
--   LOAD_TO_ML = 0 (акция с выгодой, настраивается в Манзана Онлайн), LOAD_TO_MC = 1 (PUSH).
--   Шапки акций заведены месячным скриптом CVM_october_2026_I_PROMO.sql; PROMO_NAME_ML — из I_PROMO.
--
-- Скрипт для задания CVM_API_nightly на SQL01 (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после окончания акции (позже 04.11.2026) → ничего не делается, таблицы не читаются;
--   · нет шапки хотя бы одной из трёх акций или нет списка 101427 → ошибка, ничего не пишется;
--   · список уже есть хотя бы у одной из трёх акций → сборка пропускается (повторный прогон не задвоит);
--   · клиентов всех ступеней меньше 80% списка 101420 с КГ (та же аудитория «все Активные и Новые с пушем», собран 26.10.2026:
--     2 862 759, числом в скрипте) → идёт пересчёт I_CVM_CONTACT, ошибка, раннер повторит через 10 минут;
--   · КГ какой-то ступени не подобралась за 200 попыток → ошибка, ничего не пишется.
-- Выгрузки CRM_GUID здесь нет: списки на SFTP для Campaign забирает задание cvm-campaign-lists на ru (каждые 15 минут,
-- акции 101432–101434 в портале согласованы).

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
GO

USE mci_model
GO


-- ==================================================================================
-- СБОРКА СПИСКОВ 101432, 101433, 101434 И КОНТРОЛЬНЫХ ГРУПП
-- ==================================================================================

declare @msg nvarchar(300)

-- защита: акция закончилась — блок пропускается
if cast(getdate() as date) > '2026-11-04'
begin
	raiserror(N'101432–101434: акция прошла (04.11.2026) — блок пропущен, файл можно убрать из папки', 10, 1)
	return
end
-- защита: шапки трёх акций должны быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO in (101432, 101433, 101434)) <> 3
begin
	raiserror(N'101432–101434: в I_PROMO не все три шапки — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: списки уже собраны — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101432, 101433, 101434))
begin
	raiserror(N'101432–101434: списки уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end
-- защита: список 101427 нужен для исключения из ступени 3
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101427)
begin
	raiserror(N'101432–101434: нет списка 101427 — ступень 3 не из чего исключать, списки не собраны', 16, 1)
	return
end

-- последний день чеков — опорная дата окна правила чеков в день (8 недель)
declare @maxd date = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- получатели и КГ списка 101427 «Активируй 300р. на чек от 3000р.» (05.10–31.10) — исключаются из ступени 3
drop table if exists #ex
select ID_CONTACT
into #ex
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101427 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT

-- кандидаты: ступень по среднему чеку; BUDGET, FIRST_DATA, LAST_DATA берём тем же проходом — для подбора КГ
drop table if exists #x

;with t as
(
select	a.ID_CONTACT
		, ID_PROMO = case when max(a.AVG_CHECK) < 333 then 101432
						  when max(a.AVG_CHECK) < 667 then 101433
						  else 101434 end
		, AVG_CHECK = max(a.AVG_CHECK)
		, BUDGET = sum(a.BUDGET)
		, MIN_DATA = min(a.FIRST_DATA)
		, LAST_DATA = max(a.LAST_DATA)
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT <> 0
	and a.SEGMENT in (1, 2, 3)
	and a.AVG_CHECK is not null
group by a.ID_CONTACT
)
select	t.ID_PROMO
		, t.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, b.CRM_GUID
		, LOAD_TO_MC = 1
		, t.AVG_CHECK
		, t.BUDGET
		, t.MIN_DATA
		, t.LAST_DATA
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT = c.ID_CONTACT
		left join #ex as e
		on t.ID_CONTACT = e.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
	and c.ID_CONTACT is null
	and not (t.ID_PROMO = 101434 and e.ID_CONTACT is not null)		-- ступень 3 минус список 101427
group by t.ID_PROMO, t.ID_CONTACT, b.CRM_GUID, t.AVG_CHECK, t.BUDGET, t.MIN_DATA, t.LAST_DATA

create clustered columnstore index cci_x on #x

-- защита: неполное чтение I_CVM_CONTACT (идёт «MCI Regular update») — сверка с 101420 (та же аудитория, 26.10.2026)
declare @n bigint = (select count(*) from #x)
declare @n_ref bigint = 2862759		-- 101420 с КГ на 26.10.2026
if @n < 0.8 * @n_ref
begin
	set @msg = concat(N'101432–101434: клиентов ', @n, N' — меньше 80%% списка 101420 (', @n_ref, N'), списки не записаны')
	raiserror(@msg, 16, 1)
	return
end
set @msg = concat(N'101432–101434: кандидатов ', @n, N', последний день чеков ', @maxd)
raiserror(@msg, 0, 1) with nowait

-- правило чеков в день (23.10.2026): клиенты списка с > 5 чеков в какой-то день за 8 недель — не в розыгрыш КГ
drop table if exists #multi
create table #multi (ID_CONTACT int not null)
insert #multi (ID_CONTACT)
select	h.ID_CONTACT
from I_CHECKHEADER as h (nolock)
where h.ID_COMPANY = 1 and h.ID_ORGANIZATION = 1 and h.ID_CONTACT <> 0
	and h.DATA between dateadd(week, -8, @maxd) and @maxd
	and exists (select 1 from #x as x where x.ID_CONTACT = h.ID_CONTACT)
group by h.ID_CONTACT, h.DATA
having count(*) > 5
option (recompile, max_grant_percent = 10)
create clustered columnstore index cci_multi on #multi

set @msg = concat(N'101432–101434: клиентов с > 5 чеков в день (не в КГ) ', (select count(distinct ID_CONTACT) from #multi))
raiserror(@msg, 0, 1) with nowait

-- пул для розыгрыша КГ: по ступеням, без клиентов правила чеков в день
drop table if exists #t
select	a.ID_PROMO
		, a.ID_CONTACT
		, COST_DISCOUNT = round(a.BUDGET / 10, 0)
		, COST_DISCOUNT1 = a.BUDGET
		, a.MIN_DATA
		, a.LAST_DATA
into #t
from #x as a
where not exists (select 1 from #multi as m where m.ID_CONTACT = a.ID_CONTACT)

create clustered columnstore index cci_t on #t


-- КГ по каждой ступени: страты по 20 клиентов в порядке MIN_DATA, COST_DISCOUNT, LAST_DATA, из каждой — один случайный;
-- подбор повторяется, пока средний BUDGET КГ не сойдётся с остальными до 0,05%; берётся лучшая попытка
drop table if exists #cg
create table #cg (ID_PROMO int not null, ID_CONTACT int not null)
drop table if exists #best_cg
create table #best_cg (ID_CONTACT int not null)
drop table if exists #steps
create table #steps (ID_PROMO int not null, DONE bit not null)
insert #steps (ID_PROMO, DONE) values (101432, 0), (101433, 0), (101434, 0)

declare @p int, @error float, @best float, @iter int, @bad bit = 0

while @bad = 0 and exists (select 1 from #steps where DONE = 0)
begin
	select top (1) @p = ID_PROMO from #steps where DONE = 0 order by ID_PROMO
	update #steps set DONE = 1 where ID_PROMO = @p

	set @error = 1.0
	set @best = null
	set @iter = 0
	truncate table #best_cg

	-- клиенты этой ступени — каждая попытка читает только их; страта (по 20 в порядке MIN_DATA, COST_DISCOUNT, LAST_DATA)
	-- считается один раз, как в CVM_BUILD_OFFERS
	drop table if exists #tp
	select ID_CONTACT, COST_DISCOUNT1
		 , STRATUM = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA) / 20
	into #tp
	from #t
	where ID_PROMO = @p
	create clustered columnstore index cci_tp on #tp

	while @error >= 0.0005 and @iter < 200
	begin

	drop table if exists #local_cg

	;with n as (
	select ID_CONTACT
			, R = ROW_NUMBER() over (partition by STRATUM order by newid())
	from #tp
	)
	select ID_CONTACT
			, CONTROL_GROUP = 1
	into #local_cg
	from n
	where R = 1

	drop table if exists #stat;
	;with t as (
	select CG = IIF(b.ID_CONTACT is not NULL, 1, 0)
			, c.COST_DISCOUNT1
			, c.ID_CONTACT
	from	#tp as c
			left join #local_cg as b
			on c.ID_CONTACT = b.ID_CONTACT
	)
	select	CG
			, COST_DISCOUNT = sum(COST_DISCOUNT1) / count(ID_CONTACT)
			, COUNT_CLIENT = count(distinct ID_CONTACT)
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

	set @msg = concat(@p, N': КГ ', (select count(*) from #best_cg), N' из пула ', (select count(*) from #tp), N', попыток ', @iter, N', ошибка ', @best)
	raiserror(@msg, 0, 1) with nowait

	-- защита: КГ ступени не подобралась — ничего не пишем (ни одной из трёх)
	if @best is null or @best >= 0.0005
	begin
		set @bad = 1
		set @msg = concat(@p, N': КГ не подобралась за ', @iter, N' попыток (ошибка ', @best, N'), списки не записаны')
		raiserror(@msg, 16, 1)
	end
	else
		insert #cg (ID_PROMO, ID_CONTACT)
		select @p, ID_CONTACT from #best_cg
end

if @bad = 1
	return

update d
set CONTROL_GROUP = 1
from	#cg as a
		inner join #x as d
		on a.ID_CONTACT = d.ID_CONTACT
		and a.ID_PROMO = d.ID_PROMO


-- защита: списки записал другой запуск, пока подбиралась КГ
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101432, 101433, 101434))
begin
	raiserror(N'101432–101434: списки записаны другим запуском — вставка пропущена', 10, 1)
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
		, [Средний чек от] = min(x.AVG_CHECK)
		, [Средний чек до] = max(x.AVG_CHECK)
		, [Дата старта] = p.START_DATE
		, [Загрузка в Манзана Онлайн] = N'да'
from #x as x
		inner join I_PROMO as p (nolock)
		on p.ID_PROMO = x.ID_PROMO
		and p.ID_COMPANY = 1
group by p.ID_PROMO, p.PROMO_NAME_ML, p.START_DATE
order by p.ID_PROMO
GO


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #ex
drop table if exists #x
drop table if exists #multi
drop table if exists #t
drop table if exists #cg
drop table if exists #best_cg
drop table if exists #steps
drop table if exists #tp
drop table if exists #local_cg
drop table if exists #stat
GO
