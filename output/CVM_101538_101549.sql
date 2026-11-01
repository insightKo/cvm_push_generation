-- CVM · ноябрь 2026 · кластер 1 «Перекусы на ходу» (в справочнике пока «Продукты по мелочи», ID_SEGMENT 201) · 12 акций 101538–101549
-- «Вернём 50/100р. монетами с покупки …», вт–вс 03–08, 10–15, 17–22, 24–29.11.2026, пуши вт и чт. План согласован Еленой 01.11.2026.
-- Аудитория — клиенты кластера 1 (I_CLUSTER_CONTACT.CLUSTER_DETAIL = 1, последнее готовое присвоение офлайна по I_CLUSTER_ASSIGN_LOG),
--   I_CVM_CONTACT: организация 1, SEGMENT 1–6, ID_SALE_CHANNEL 1 офлайн и 2 омни.
-- Три группы по покупкам за 6 недель (I_CLIENT_ITEM_KEY_DAY, категория CVM KEY_TYPE 2, окно (якорь − 42 дня, якорь], якорь — последний
--   загруженный день витрины по CVM_CLIENT_ITEM_LOG): клиент — в одной группе, где у него больше дней с покупками категорий группы;
--   при равенстве — по порядку групп 1, 2, 3. Не покупал ни одну категорию групп — в списки не входит.
--   1 «Готовая еда» — группа 15 справочника CVM + 1339 «Пицца и пироги» (пицца — по категории целиком, отдельных SKU в витрине нет);
--   2 «Фрукты, шоколад, батончики» — фрукты (без лимонов, лайма и авокадо) + 1279 шоколад, 1281 шоколадные батончики, 1062 злаковые батончики;
--   3 «Вредный перекус» — 1041 чипсы, 1049 сухарики, 1043 семечки, 1201 мясные снеки, 1066 рыбные снеки, 1098 попкорн, 1659 нори.
--   Каждый клиент группы получает все 4 акции своей группы (по одной на неделю).
-- Стандартная сборка как CVM_101445_101474.sql: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG;
--   КГ по каждой акции и части: SEGMENT 1–3 — 5% (RN/20) по BUDGET, SEGMENT 4–6 — 10% (RN/10) по LTV.
-- Механика 4 справочника «Кешбэк X р. с покупки категории» (MCI 8), без активации, 1 раз за акцию.
--   LOAD_TO_ML = 0 (акция в Manzana Online — заберёт «MCI List to ML»), LOAD_TO_MC = 1 (PUSH).
--   Период «после»: с финиша + 1 до даты сгорания монет (15.11, 22.11, 29.11), у последней недели — финиш + 4 (03.12).
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после 29.11.2026 — ничего не делается;
--   · шапки I_PROMO заводятся только отсутствующие;
--   · списки уже есть хотя бы у одной из 12 акций — сборка пропускается (повторный прогон не задвоит);
--   · витрина покупок занята загрузкой (вечер/ночь) — ошибка 16, раннер повторит через 10 минут, ничего не записано;
--   · подбор КГ — до ошибки < 0,05% или 1000 попыток на часть, берётся лучшая попытка.

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
-- 1. ШАПКИ 12 АКЦИЙ В I_PROMO (заводятся только отсутствующие)
-- ==================================================================================

if cast(getdate() as date) > '2026-11-29'
begin
	raiserror(N'101538–101549: акции прошли (29.11.2026) — шапки не заводятся, файл можно убрать из папки', 10, 1)
	return
end

declare @mech nvarchar(200) = (select MECHANICS from I_MECHANICS (nolock) where ID_MECHANICS = 8)

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, v.D), dateadd(day, -1, v.D), v.D, v.FD, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, v.FD), v.AFTER_TO
	 , N'PUSH', N'Продукты по мелочи', 201, null, @mech, 8, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101538, N'Вернём 100р. монетами с покупки сэндвичей и бургеров', N'101538_100 монет сэндвичей бургеров', cast('2026-11-03' as date), cast('2026-11-08' as date), cast('2026-11-15' as date))
	, (101539, N'Вернём 100р. монетами с покупки бананов', N'101539_100 монет бананов', '2026-11-03', '2026-11-08', '2026-11-15')
	, (101540, N'Вернём 50р. монетами с покупки мясных снеков', N'101540_50 монет мясных снеков', '2026-11-03', '2026-11-08', '2026-11-15')
	, (101541, N'Вернём 100р. монетами с покупки роллов и онигири', N'101541_100 монет роллов онигири', '2026-11-10', '2026-11-15', '2026-11-22')
	, (101542, N'Вернём 100р. монетами с покупки хурмы', N'101542_100 монет хурмы', '2026-11-10', '2026-11-15', '2026-11-22')
	, (101543, N'Вернём 100р. монетами с покупки чипсов', N'101543_100 монет чипсов', '2026-11-10', '2026-11-15', '2026-11-22')
	, (101544, N'Вернём 100р. монетами с покупки лапши и пасты', N'101544_100 монет лапши пасты', '2026-11-17', '2026-11-22', '2026-11-29')
	, (101545, N'Вернём 100р. монетами с покупки винограда', N'101545_100 монет винограда', '2026-11-17', '2026-11-22', '2026-11-29')
	, (101546, N'Вернём 100р. монетами с покупки газировки', N'101546_100 монет газировки', '2026-11-17', '2026-11-22', '2026-11-29')
	, (101547, N'Вернём 100р. монетами с покупки пиццы и газировки', N'101547_100 монет пиццы газировки', '2026-11-24', '2026-11-29', '2026-12-03')
	, (101548, N'Вернём 100р. монетами с покупки шоколада и батончиков', N'101548_100 монет шоколада батончиков', '2026-11-24', '2026-11-29', '2026-12-03')
	, (101549, N'Вернём 100р. монетами с покупки семечек', N'101549_100 монет семечек', '2026-11-24', '2026-11-29', '2026-12-03')
) as v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML, D, FD, AFTER_TO)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)
GO


-- ==================================================================================
-- 2. СПИСКИ И КОНТРОЛЬНЫЕ ГРУППЫ
-- ==================================================================================

if cast(getdate() as date) > '2026-11-29'
begin
	raiserror(N'101538–101549: акции прошли (29.11.2026) — списки не собираются', 10, 1)
	return
end
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO between 101538 and 101549) <> 12
begin
	raiserror(N'101538–101549: в I_PROMO не все 12 шапок — списки не собраны', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101538, 101539, 101540, 101541, 101542, 101543, 101544, 101545, 101546, 101547, 101548, 101549))
begin
	raiserror(N'101538–101549: списки уже в I_PROMO_OFFER — сборка пропущена', 10, 1)
	return
end
-- витрину покупок сейчас грузят (client_item_load / client_item_rebuild_keys / история держат этот замок, внутри — TRUNCATE секций, Sch-M):
-- сразу ошибка, без сборки #cl и без ожидания; раннер повторит через 10 минут
if APPLOCK_TEST(N'public', N'CVM_CLIENT_ITEM', N'Exclusive', N'Session') = 0
begin
	raiserror(N'101538–101549: идёт загрузка витрины покупок (замок CVM_CLIENT_ITEM) — списки не собраны, раннер повторит', 16, 1)
	return
end

declare @msg nvarchar(400)

-- акция → группа
drop table if exists #p
create table #p (ID_PROMO int not null, GRP tinyint not null)
insert #p (ID_PROMO, GRP)
values
	  (101538, 1), (101541, 1), (101544, 1), (101547, 1)	-- Готовая еда: сэндвичи и бургеры, роллы и онигири, лапша и паста, пицца и газировка
	, (101539, 2), (101542, 2), (101545, 2), (101548, 2)	-- Фрукты, шоколад, батончики: бананы, хурма, виноград, шоколад и батончики
	, (101540, 3), (101543, 3), (101546, 3), (101549, 3)	-- Вредный перекус: мясные снеки, чипсы, газировка, семечки

-- категория CVM → группа
drop table if exists #k
create table #k (ID_CVM_CATEGORY int not null primary key, GRP tinyint not null)
insert #k (ID_CVM_CATEGORY, GRP)
values
	-- 1 Готовая еда: группа 15 справочника + пицца и пироги
	  (1484, 1), (1485, 1), (1486, 1), (1487, 1), (1488, 1), (1489, 1), (1490, 1), (1491, 1), (1492, 1), (1493, 1), (1494, 1), (1498, 1), (1500, 1), (1339, 1)
	-- 2 Фрукты (без лимонов, лайма, авокадо), шоколад, батончики
	, (1212, 2), (1213, 2), (1216, 2), (1217, 2), (1218, 2), (1220, 2), (1227, 2), (1228, 2), (1229, 2), (1231, 2), (1232, 2), (1235, 2)
	, (1237, 2), (1238, 2), (1242, 2), (1246, 2), (1252, 2), (1260, 2), (1264, 2), (1275, 2), (1279, 2), (1281, 2), (1062, 2)
	-- 3 Вредный перекус
	, (1041, 3), (1049, 3), (1043, 3), (1201, 3), (1066, 3), (1098, 3), (1659, 3)

-- якорь витрины покупок — последний загруженный день
declare @anchor date = (select max(DATE_TO) from CVM_CLIENT_ITEM_LOG (nolock) where STATUS = 'OK' and MODE in ('night', 'init'))
declare @from date = dateadd(day, -42, @anchor)
if @anchor is null
begin
	raiserror(N'101538–101549: нет загруженных дней в CVM_CLIENT_ITEM_LOG — списки не собраны', 16, 1)
	return
end

-- последнее готовое присвоение кластеров офлайна (после него нет перезаписи RUN того же окна и версии)
declare @cl_date date, @cl_ver varchar(64)
declare @cl_log_max int = (select max(ID) from I_CLUSTER_ASSIGN_LOG (nolock))	-- журнал до выбора: для проверки после чтения #cl
select top (1) @cl_date = l.REFERENCE_DATE, @cl_ver = l.MODEL_VERSION
from I_CLUSTER_ASSIGN_LOG as l (nolock)
where l.ID_ORGANIZATION = 1 and l.STATUS = 'OK'
	and not exists (select 1 from I_CLUSTER_ASSIGN_LOG as r (nolock)
					where r.ID_ORGANIZATION = l.ID_ORGANIZATION and r.REFERENCE_DATE = l.REFERENCE_DATE
						and r.MODEL_VERSION = l.MODEL_VERSION and r.STATUS = 'RUN' and r.ID > l.ID)
order by l.REFERENCE_DATE desc, l.ID desc
if @cl_date is null
begin
	raiserror(N'101538–101549: нет готового присвоения кластеров офлайна — списки не собраны', 16, 1)
	return
end
set @msg = concat(N'Покупки за (', @from, N', ', @anchor, N'], кластеры на ', @cl_date, N' ', @cl_ver)
raiserror(@msg, 0, 1) with nowait

-- защита: номер кластера 1 «Перекусы на ходу» — номер модели how6-2026-10-09; у другой версии нумерация может быть другой
if @cl_ver <> 'how6-2026-10-09'
begin
	set @msg = concat(N'101538–101549: последнее присвоение кластеров на другой модели (', @cl_ver, N') — номер кластера 1 не проверен, списки не собраны')
	raiserror(@msg, 16, 1)
	return
end

-- клиенты кластера 1, офлайн и омни
drop table if exists #cl
select	a.ID_CONTACT
		, SEGMENT = max(a.SEGMENT)
into #cl
from I_CVM_CONTACT as a (nolock)
		inner join I_CLUSTER_CONTACT as k (nolock)
		on k.ID_CONTACT = a.ID_CONTACT
		and k.ID_ORGANIZATION = 1
		and k.REFERENCE_DATE = @cl_date
		and k.MODEL_VERSION = @cl_ver
		and k.CLUSTER_DETAIL = 1
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT <> 0
	and a.SEGMENT between 1 and 6
	and a.ID_SALE_CHANNEL in (1, 2)
group by a.ID_CONTACT

create clustered columnstore index cci_cl on #cl

-- защита: cluster_assign начал перезапись этого окна после выбора присвоения (строка журнала RUN пишется до DELETE + INSERT) —
-- чтение NOLOCK могло попасть на удалённые строки; ошибка, раннер повторит. Старые строки ERROR окна (откаченные перезаписи) не мешают.
if exists (select 1 from I_CLUSTER_ASSIGN_LOG (nolock)
		   where ID_ORGANIZATION = 1 and REFERENCE_DATE = @cl_date and MODEL_VERSION = @cl_ver and ID > @cl_log_max)
begin
	raiserror(N'101538–101549: присвоение кластеров перезаписывается — списки не собраны, раннер повторит', 16, 1)
	return
end

-- защита: I_CVM_CONTACT пересчитывается («MCI Regular update») — неполное чтение (на портале в кластере 910 тыс.)
declare @n_cl bigint = (select count(*) from #cl)
if @n_cl < 600000
begin
	set @msg = concat(N'101538–101549: клиентов кластера ', @n_cl, N' — меньше 600 тыс. (идёт пересчёт I_CVM_CONTACT?), списки не собраны')
	raiserror(@msg, 16, 1)
	return
end

-- дни с покупками категорий каждой группы за 6 недель; витрина занята загрузкой — ошибка, раннер повторит
drop table if exists #b
create table #b (ID_CONTACT int not null, GRP tinyint not null, DAYS int not null, index cci_b clustered columnstore)

-- запрос к витрине — через sp_executesql: он компилируется уже под lock_timeout и на уровень ниже TRY, поэтому CATCH ловит и
-- ожидание Sch-M при компиляции (иначе 1222 при отложенной компиляции оператора в этом же пакете мимо CATCH, а пакет при
-- компиляции мог встать на Sch-M ещё до set lock_timeout), и 1222 / 601 при чтении
begin try
	set lock_timeout 120000
	exec sys.sp_executesql N'
	insert #b with (tablock) (ID_CONTACT, GRP, DAYS)
	select	k.ID_CONTACT
			, c.GRP
			, DAYS = count(distinct k.DATA)
	from dbo.I_CLIENT_ITEM_KEY_DAY as k with (nolock)
			inner join #k as c
			on c.ID_CVM_CATEGORY = k.ID_KEY
			inner join #cl as n
			on n.ID_CONTACT = k.ID_CONTACT
	where k.ID_COMPANY = 1 and k.ID_ORGANIZATION = 1 and k.ID_CONTACT > 0
		and k.KEY_TYPE = cast(2 as tinyint)
		and k.ID_KEY in (1484, 1485, 1486, 1487, 1488, 1489, 1490, 1491, 1492, 1493, 1494, 1498, 1500, 1339,
						 1212, 1213, 1216, 1217, 1218, 1220, 1227, 1228, 1229, 1231, 1232, 1235, 1237, 1238, 1242, 1246, 1252, 1260, 1264, 1275, 1279, 1281, 1062,
						 1041, 1049, 1043, 1201, 1066, 1098, 1659)
		and k.DATA > @from and k.DATA <= @anchor
	group by k.ID_CONTACT, c.GRP
	option (recompile)'
		, N'@from date, @anchor date', @from = @from, @anchor = @anchor
	set lock_timeout -1
end try
begin catch
	set lock_timeout -1
	set @msg = concat(N'101538–101549: витрина покупок недоступна (', error_number(), N': ', replace(error_message(), N'%', N'%%'), N') — списки не собраны, раннер повторит')
	raiserror(@msg, 16, 1)
	return
end catch

-- группа клиента: больше дней с покупками, при равенстве — меньший номер группы
drop table if exists #g
select	ID_CONTACT, GRP
into #g
from (
	select	ID_CONTACT, GRP
			, RN = row_number() over (partition by ID_CONTACT order by DAYS desc, GRP)
	from #b
	) as x
where RN = 1

-- список: 4 акции группы, пуш, минус I_GLOBAL_CG
drop table if exists #x

select	ID_PROMO = p.ID_PROMO
		, g.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, CRM_GUID = max(b.CRM_GUID)
		, LOAD_TO_MC = 1
		, PART = iif(n.SEGMENT >= 4, 4, 1)
into #x
from	  #g as g
		inner join #cl as n
		on n.ID_CONTACT = g.ID_CONTACT
		inner join #p as p
		on p.GRP = g.GRP
		inner join I_CONTACT as b (nolock)
		on g.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on g.ID_CONTACT = c.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
	and c.ID_CONTACT is null
group by p.ID_PROMO, g.ID_CONTACT, n.SEGMENT

create clustered columnstore index cci_x on #x

-- защита: группы не пересекаются — клиент получает акции только одной группы
if exists (select 1 from #x as x inner join #p as p on p.ID_PROMO = x.ID_PROMO group by x.ID_CONTACT having count(distinct p.GRP) > 1)
begin
	raiserror(N'101538–101549: клиент попал в акции двух групп — списки не собраны', 16, 1)
	return
end

set @msg = concat(N'Клиентов кластера ', @n_cl, N', с покупками групп ', (select count(*) from #g), N', строк списков ', (select count(*) from #x))
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

-- защита: I_CVM_CONTACT пересчитали между чтением #cl и #t — у части строк списка нет строки для подбора КГ (остались бы все в ЦГ)
if (select count(*) from #t) <> (select count(*) from #x)
begin
	raiserror(N'101538–101549: не у всех клиентов списка прочитана строка I_CVM_CONTACT (идёт пересчёт?) — списки не собраны, раннер повторит', 16, 1)
	return
end


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

	-- клиенты этой части — каждая попытка читает только их; страта (RN/@div по MIN_DATA, COST_DISCOUNT, LAST_DATA) от попытки
	-- не зависит — считается один раз, в попытке остаётся одна сортировка (случайный клиент в страте)
	drop table if exists #tp
	select ID_CONTACT, COST_DISCOUNT1
			, SEGMENT = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA) / @div
	into #tp
	from #t
	where ID_PROMO = @p and PART = @part

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
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101538, 101539, 101540, 101541, 101542, 101543, 101544, 101545, 101546, 101547, 101548, 101549))
begin
	raiserror(N'101538–101549: списки записаны другим запуском — вставка пропущена', 10, 1)
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
