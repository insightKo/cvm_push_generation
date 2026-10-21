-- CVM · неделя 43 · «Вернём 100 монет с покупки категории» · чт 22.10 – вс 25.10.2026 · 30 акций 101445–101474
-- Сегменты — дерево «Планирования» офлайн 22–25.10 (ОК Елены 21.10.2026): кластер уровня 10 → офлайн / омни → покупали
-- трафиковую категорию CVM за 6 недель; клиент — в одной акции: первая по порядку категория своего кластера, которую он покупал.
--   Кластер — I_CLUSTER_CONTACT.CLUSTER_DETAIL, модель how6-2026-10-09, разметка 16.10.2026 (та же, что экран «Кластеры» и дерево).
--   Клиенты — I_CVM_CONTACT, организация 1, SEGMENT 1–6 (без фрода), ID_SALE_CHANNEL 1 офлайн и 2 омни.
--   Покупки — I_CLIENT_ITEM_KEY_DAY, категория CVM (KEY_TYPE 2), организация 1, окно (якорь − 42 дня, якорь], якорь — последний
--   загруженный день витрины по CVM_CLIENT_ITEM_LOG (как «Покупки» конструктора и дерево).
-- Стандартная сборка как CVM_101416.sql / CVM_42_26: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG;
--   КГ по каждой акции и части: SEGMENT 1–3 — 5% (RN/20) по BUDGET, SEGMENT 4–6 — 10% (RN/10) по LTV.
-- Механика 4 справочника «Кешбэк X р. с покупки категории» (MCI 8, «Кэшбек X баллов»), без активации, 1 раз за акцию,
--   сгорание 31.10.2026 23:59. LOAD_TO_ML = 0 (акция в Manzana Online — вечером заберёт «MCI List to ML»), LOAD_TO_MC = 1 (PUSH).
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после окончания акции (позже 25.10.2026) — ничего не делается;
--   · шапки I_PROMO заводятся только отсутствующие;
--   · списки уже есть хотя бы у одной из 30 акций — сборка пропускается (повторный прогон не задвоит);
--   · подбор КГ — до ошибки < 0,05% или 1000 попыток на часть, берётся лучшая попытка.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
GO

USE mci_model
GO


-- ==================================================================================
-- 1. ШАПКИ 30 АКЦИЙ В I_PROMO (заводятся только отсутствующие)
-- ==================================================================================

if cast(getdate() as date) > '2026-10-25'
begin
	raiserror(N'101445–101474: акция прошла (25.10.2026) — шапки не заводятся, файл можно убрать из папки', 10, 1)
	return
end

declare @d date = '2026-10-22'		-- старт (чт)
declare @fd date = '2026-10-25'		-- финиш (вс)

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, @d), dateadd(day, -1, @d), @d, @fd, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, @fd), '2026-10-30'
	 , N'PUSH', N'Активные', 4, null, N'Кэшбек X баллов', 8, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101445, N'Вернём 100р. монетами с покупки шоколадных батончиков', N'101445_100 монет шоколадных батончиков')
	, (101446, N'Вернём 100р. монетами с покупки чипсов', N'101446_100 монет чипсов')
	, (101447, N'Вернём 100р. монетами с покупки шоколада', N'101447_100 монет шоколада')
	, (101448, N'Вернём 100р. монетами с покупки печенья', N'101448_100 монет печенья')
	, (101449, N'Вернём 100р. монетами с покупки винограда', N'101449_100 монет винограда')
	, (101450, N'Вернём 100р. монетами с покупки весовых конфет', N'101450_100 монет весовых конфет')
	, (101451, N'Вернём 100р. монетами с покупки куриных яиц', N'101451_100 монет куриных яиц')
	, (101452, N'Вернём 100р. монетами с покупки куриной разделки', N'101452_100 монет куриной разделки')
	, (101453, N'Вернём 100р. монетами с покупки макарон', N'101453_100 монет макарон')
	, (101454, N'Вернём 100р. монетами с покупки творога', N'101454_100 монет творога')
	, (101455, N'Вернём 100р. монетами с покупки слив', N'101455_100 монет слив')
	, (101456, N'Вернём 100р. монетами с покупки сметаны', N'101456_100 монет сметаны')
	, (101457, N'Вернём 100р. монетами с покупки твёрдого и полутвёрдого сыра', N'101457_100 монет твёрдого полутвёрдого')
	, (101458, N'Вернём 100р. монетами с покупки томатов', N'101458_100 монет томатов')
	, (101459, N'Вернём 100р. монетами с покупки варёной колбасы', N'101459_100 монет варёной колбасы')
	, (101460, N'Вернём 100р. монетами с покупки влажного корма для кошек', N'101460_100 монет влажного корма кошек')
	, (101461, N'Вернём 100р. монетами с покупки сухого корма для кошек', N'101461_100 монет сухого корма кошек')
	, (101462, N'Вернём 100р. монетами с покупки туалетной бумаги', N'101462_100 монет туалетной бумаги')
	, (101463, N'Вернём 100р. монетами с покупки товаров для уборки', N'101463_100 монет товаров уборки')
	, (101464, N'Вернём 100р. монетами с покупки зубной пасты', N'101464_100 монет зубной пасты')
	, (101465, N'Вернём 100р. монетами с покупки средств для посуды', N'101465_100 монет средств посуды')
	, (101466, N'Вернём 100р. монетами с покупки сдобы и булочек', N'101466_100 монет сдобы булочек')
	, (101467, N'Вернём 100р. монетами с покупки негазированной воды', N'101467_100 монет негазированной воды')
	, (101468, N'Вернём 100р. монетами с покупки сытной выпечки', N'101468_100 монет сытной выпечки')
	, (101469, N'Вернём 100р. монетами с покупки светлого пива', N'101469_100 монет светлого пива')
	, (101470, N'Вернём 100р. монетами с покупки батонов и багетов', N'101470_100 монет батонов багетов')
	, (101471, N'Вернём 100р. монетами с покупки прокладок', N'101471_100 монет прокладок')
	, (101472, N'Вернём 100р. монетами с покупки водки', N'101472_100 монет водки')
	, (101473, N'Вернём 100р. монетами с покупки растворимого кофе', N'101473_100 монет растворимого кофе')
	, (101474, N'Вернём 100р. монетами с покупки красного вина', N'101474_100 монет красного вина')
) as v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)
GO


-- ==================================================================================
-- 2. СПИСКИ И КОНТРОЛЬНЫЕ ГРУППЫ
-- ==================================================================================

if cast(getdate() as date) > '2026-10-25'
begin
	raiserror(N'101445–101474: акция прошла (25.10.2026) — списки не собираются', 10, 1)
	return
end
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO between 101445 and 101474) <> 30
begin
	raiserror(N'101445–101474: в I_PROMO не все 30 шапок — списки не собраны', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101445, 101446, 101447, 101448, 101449, 101450, 101451, 101452, 101453, 101454, 101455, 101456, 101457, 101458, 101459, 101460, 101461, 101462, 101463, 101464, 101465, 101466, 101467, 101468, 101469, 101470, 101471, 101472, 101473, 101474))
begin
	raiserror(N'101445–101474: списки уже в I_PROMO_OFFER — сборка пропущена', 10, 1)
	return
end

-- акция → кластер, категория CVM и её порядок в кластере
drop table if exists #p
create table #p (ID_PROMO int not null, CLUSTER_DETAIL int not null, ID_CVM_CATEGORY int not null, PRIO int not null)
insert #p (ID_PROMO, CLUSTER_DETAIL, ID_CVM_CATEGORY, PRIO)
values
	  (101445, 1, 1281, 1)	-- Продукты по мелочи · 1 · Шоколадные батончики
	, (101446, 1, 1041, 2)	-- Продукты по мелочи · 2 · Чипсы
	, (101447, 1, 1279, 3)	-- Продукты по мелочи · 3 · Шоколад
	, (101448, 2, 1277, 1)	-- Редкие · 1 · Печенье
	, (101449, 2, 1213, 2)	-- Редкие · 2 · Виноград
	, (101450, 2, 1278, 3)	-- Редкие · 3 · Конфеты весовые
	, (101451, 3, 1003, 1)	-- Частые с большой корзиной · 1 · Яйца куриные
	, (101452, 3, 1178, 2)	-- Частые с большой корзиной · 2 · Курица разделка
	, (101453, 3, 1044, 3)	-- Частые с большой корзиной · 3 · Макароны
	, (101454, 4, 1004, 1)	-- Регулярные за продуктами · 1 · Творог
	, (101455, 4, 1216, 2)	-- Регулярные за продуктами · 2 · Слива
	, (101456, 4, 1006, 3)	-- Регулярные за продуктами · 3 · Сметана
	, (101457, 5, 1002, 1)	-- Большая закупка продуктов · 1 · Сыр твёрдый и полутвёрдый
	, (101458, 5, 1211, 2)	-- Большая закупка продуктов · 2 · Томаты
	, (101459, 5, 1180, 3)	-- Большая закупка продуктов · 3 · Колбаса варёная
	, (101460, 6, 1401, 1)	-- Много нон-фуда · 1 · Влажный корм для кошек
	, (101461, 6, 1402, 2)	-- Много нон-фуда · 2 · Сухой корм для кошек
	, (101462, 6, 1412, 3)	-- Много нон-фуда · 3 · Туалетная бумага
	, (101463, 7, 1603, 1)	-- Большая закупка с нон-фудом · 1 · Уборка и хозинвентарь
	, (101464, 7, 1413, 2)	-- Большая закупка с нон-фудом · 2 · Зубная паста
	, (101465, 7, 1574, 3)	-- Большая закупка с нон-фудом · 3 · Средства для посуды
	, (101466, 8, 1343, 1)	-- За хлебом и водой · 1 · Сдоба и булочки
	, (101467, 8, 1318, 2)	-- За хлебом и водой · 2 · Вода негазированная
	, (101468, 8, 1337, 3)	-- За хлебом и водой · 3 · Выпечка сытная
	, (101469, 9, 1148, 1)	-- Ультра-частые · 1 · Пиво светлое
	, (101470, 9, 1333, 2)	-- Ультра-частые · 2 · Батоны и багеты
	, (101471, 9, 1415, 3)	-- Ультра-частые · 3 · Прокладки
	, (101472, 10, 1149, 1)	-- Вино, крепкое, кофе · 1 · Водка
	, (101473, 10, 1387, 2)	-- Вино, крепкое, кофе · 2 · Кофе растворимый
	, (101474, 10, 1151, 3)	-- Вино, крепкое, кофе · 3 · Вино красное

-- якорь витрины покупок — последний загруженный день
declare @anchor date = (select max(DATE_TO) from CVM_CLIENT_ITEM_LOG (nolock) where STATUS = 'OK' and MODE in ('night', 'init'))
declare @from date = dateadd(day, -42, @anchor)
declare @msg nvarchar(300)
if @anchor is null
begin
	raiserror(N'101445–101474: нет загруженных дней в CVM_CLIENT_ITEM_LOG — списки не собраны', 16, 1)
	return
end
set @msg = concat(N'Покупки за (', @from, N', ', @anchor, N']')
raiserror(@msg, 0, 1) with nowait

-- клиенты кластеров уровня 10, офлайн и омни
drop table if exists #cl

select	a.ID_CONTACT
		, SEGMENT = max(a.SEGMENT)
		, CLUSTER_DETAIL = max(k.CLUSTER_DETAIL)
into #cl
from I_CVM_CONTACT as a (nolock)
		inner join I_CLUSTER_CONTACT as k (nolock)
		on k.ID_CONTACT = a.ID_CONTACT
		and k.ID_ORGANIZATION = 1
		and k.MODEL_VERSION = 'how6-2026-10-09'
		and k.REFERENCE_DATE = '2026-10-16'
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT <> 0
	and a.SEGMENT between 1 and 6
	and a.ID_SALE_CHANNEL in (1, 2)
	and k.CLUSTER_DETAIL between 1 and 10
group by a.ID_CONTACT

create clustered columnstore index cci_cl on #cl

-- защита: I_CVM_CONTACT пересчитывается («MCI Regular update») — неполное чтение; сверка с деревом
-- (клиентов кластеров 1–10 офлайн и омни на 16.10.2026 — 7 394 893). Меньше 90% — ошибка, раннер повторит позже.
declare @n_cl bigint = (select count(*) from #cl)
if @n_cl < 0.9 * 7394893
begin
	set @msg = concat(N'101445–101474: клиентов кластеров ', @n_cl, N' — меньше 90%% от 7 394 893 (идёт пересчёт I_CVM_CONTACT?), списки не собраны')
	raiserror(@msg, 16, 1)
	return
end

-- покупатели 30 категорий за 6 недель
drop table if exists #b
create table #b (ID_CONTACT int not null, ID_KEY int not null, index cci_b clustered columnstore)

insert #b with (tablock) (ID_CONTACT, ID_KEY)
select	k.ID_CONTACT
		, ID_KEY = cast(k.ID_KEY as int)
from I_CLIENT_ITEM_KEY_DAY as k (nolock)
where k.ID_COMPANY = 1 and k.ID_ORGANIZATION = 1 and k.ID_CONTACT > 0
	and k.KEY_TYPE = cast(2 as tinyint)
	and k.ID_KEY in (1281, 1041, 1279, 1277, 1213, 1278, 1003, 1178, 1044, 1004, 1216, 1006, 1002, 1211, 1180, 1401, 1402, 1412, 1603, 1413, 1574, 1343, 1318, 1337, 1148, 1333, 1415, 1149, 1387, 1151)
	and k.DATA > @from and k.DATA <= @anchor
group by k.ID_CONTACT, k.ID_KEY
option (recompile)

-- акция клиента: первая по порядку категория своего кластера
drop table if exists #pr

select	ID_CONTACT, SEGMENT, ID_PROMO
into #pr
from (
	select	n.ID_CONTACT
			, n.SEGMENT
			, p.ID_PROMO
			, RN = row_number() over (partition by n.ID_CONTACT order by p.PRIO)
	from #cl as n
			inner join #p as p
			on p.CLUSTER_DETAIL = n.CLUSTER_DETAIL
			inner join #b as b
			on b.ID_CONTACT = n.ID_CONTACT
			and b.ID_KEY = p.ID_CVM_CATEGORY
	) as x
where RN = 1

-- список: пуш, минус I_GLOBAL_CG
drop table if exists #x

select	ID_PROMO = t.ID_PROMO
		, t.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, CRM_GUID
		, LOAD_TO_MC = 1
		, PART = iif(t.SEGMENT >= 4, 4, 1)
into #x
from	  #pr as t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT = c.ID_CONTACT
where HAS_PUSH = 1 and TOKENS = 1
and c.ID_CONTACT is null
group by t.ID_PROMO
		, t.ID_CONTACT
		, t.SEGMENT
		, CRM_GUID

create clustered columnstore index cci_x on #x


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


-- КГ по каждой акции и части (1 — SEGMENT 1–3, RN/20; 4 — SEGMENT 4–6, RN/10)
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

	-- клиенты этой части — каждая попытка читает только их
	drop table if exists #tp
	select ID_CONTACT, MIN_DATA, COST_DISCOUNT, LAST_DATA, COST_DISCOUNT1
	into #tp
	from #t
	where ID_PROMO = @p and PART = @part

	while @error >= 0.0005 and @iter < 1000
	begin

	drop table if exists #local_cg

	;with x as
	(
	select ID_CONTACT
			, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
	from #tp
	), a as (
	select ID_CONTACT
			, SEGMENT = RN/@div
	from x
	), n as (
	select ID_CONTACT
			, R = ROW_NUMBER() over (partition by SEGMENT order by newid())
	from a
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
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101445, 101446, 101447, 101448, 101449, 101450, 101451, 101452, 101453, 101454, 101455, 101456, 101457, 101458, 101459, 101460, 101461, 101462, 101463, 101464, 101465, 101466, 101467, 101468, 101469, 101470, 101471, 101472, 101473, 101474))
begin
	raiserror(N'101445–101474: списки записаны другим запуском — вставка пропущена', 10, 1)
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


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #p
drop table if exists #cl
drop table if exists #b
drop table if exists #pr
drop table if exists #x
drop table if exists #t
drop table if exists #cg
drop table if exists #best_cg
drop table if exists #tp
drop table if exists #parts
drop table if exists #local_cg
drop table if exists #stat
GO
