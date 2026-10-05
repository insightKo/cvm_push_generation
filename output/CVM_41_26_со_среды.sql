-- CVM неделя 41 · вторая половина недели, со среды 07.10 по вс 11.10.2026
-- Собираются только списки, которых ещё нет: 101407, 101423, 101422, 101421, 101410.
-- Шапки акций заведены месячным скриптом CVM_october_2026_I_PROMO.sql.
-- Понедельник и вторник (101427, 101426, 101397–101400, 101403, слипы 101393–101396) собраны ранее — здесь их нет.
-- 101428_4 (капуста, ср 07.10) и 101428_5 (алкоголь, чт 08–10.10) выборки не требуют: это дни одного списка 101428,
-- он собран на весь октябрь скриптом недели 40.
--
-- Акции по дням: ср 07.10 — 101407 · чт 08.10 — 101423, 101422, 101421 · вс 11.10 — 101410.
-- Три алкогольные акции четверга идут каскадом пиво → крепкий → вино, чтобы клиент получил ровно одну (приоритет Елены).
-- Аудитории: 101407 и 101410 — все Активные и Новые; алкогольные — Активные и Новые с покупкой своей категории за 8 недель.
-- Контрольная группа 5% (RN/20), балансировка по BUDGET. Каждый блок пропускается, если список уже собран.
-- После сводки — блок с готовыми строками для письма Анастасии Андриановой (anda@manzanagroup.ru) о сформированных списках.


-- ==================================================================================
-- ЧАСТЬ 1. СРЕДА 07.10
-- ==================================================================================

-- 101407 · Активируй 20% на капусту, свёклу и морковь · Активные и Новые · ср 07.10
-- 101407_Активируй 20 на капусту свёклу и морковь

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101407 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101407 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101407)
begin
	raiserror(N'101407 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
select ID_PROMO = 101407
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0, @msg nvarchar(200)

while @error >= 0.0005
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
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
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101407: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


INSERT INTO I_PROMO_OFFER
SELECT *
from #x

GO


-- ==================================================================================
-- ЧАСТЬ 2. ЧЕТВЕРГ 08.10 — АЛКОГОЛЬНЫЙ КАСКАД, покупатели категории за 8 недель
-- Порядок важен: пиво → крепкий → вино, каждый следующий минус уже отобранных.
-- ==================================================================================

-- 101423 · Активируй 20% кешбэка на пиво · Активные и Новые — покупатели пива за 8 недель · чт 08.10–10.10
-- 101423_Активируй 20 кешбэка на пиво

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101423 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101423 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101423)
begin
	raiserror(N'101423 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end
-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1)
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- категории миссии один раз во временную таблицу
drop table if exists #cat_beer
select ID_CATEGORY_5_ext = cast(ID_CATEGORY_5_ext as nvarchar(50))
into #cat_beer
from I_MISSION (nolock)
where MISSION in (N'Пиво') and MAIN is not null
group by cast(ID_CATEGORY_5_ext as nvarchar(50))

-- защита: пустой набор категорий дал бы молча пустой список и сорвал каскад
if not exists (select 1 from #cat_beer)
begin
	raiserror(N'101423: пустой набор категорий миссии «Пиво» — блок остановлен', 16, 1)
	return
end

drop table if exists #x

;with t1 as
(
select	b.ID_CONTACT		-- достаточно одной покупки категории за 8 недель: считать чеки не нужно
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		inner join #cat_beer as k
		on c.ID_CATEGORY_5_ext = k.ID_CATEGORY_5_ext
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT
)
, t as (
select a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
		inner join t1
		on a.ID_CONTACT = t1.ID_CONTACT
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
select ID_PROMO = 101423
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0, @msg nvarchar(200)

while @error >= 0.0005
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
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
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101423: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


INSERT INTO I_PROMO_OFFER
SELECT *
from #x

GO

-- 101422 · Активируй 20% кешбэка на крепкий алкоголь · Активные и Новые — покупатели крепкого за 8 недель, кроме взятых в пиво · чт 08.10–10.10
-- 101422_Активируй 20 кешбэка на крепкий алкоголь

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101422 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101422 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101422)
begin
	raiserror(N'101422 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end
-- защита каскада: без списка пива вычитать нечего — 101422 не строим
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101423)
begin
	raiserror(N'Нет списка 101423 — 101422 не строим', 16, 1)
	return
end
-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1)
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

drop table if exists #x

;with t1 as
(
select	b.ID_CONTACT		-- достаточно одной покупки категории за 8 недель: считать чеки не нужно
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_3 in (77)
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT
)
, t as (
select a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
		inner join t1
		on a.ID_CONTACT = t1.ID_CONTACT
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, ex as (
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101423)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101422
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
		left join ex
		on t.ID_CONTACT=ex.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and ex.ID_CONTACT is null
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0, @msg nvarchar(200)

while @error >= 0.0005
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
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
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101422: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


INSERT INTO I_PROMO_OFFER
SELECT *
from #x

GO

-- 101421 · Активируй 20% кешбэка на вино и игристое · Активные и Новые — покупатели вина за 8 недель, кроме взятых в пиво и крепкий · чт 08.10–10.10
-- 101421_Активируй 20 кешбэка на вино и игристое

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101421 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101421 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101421)
begin
	raiserror(N'101421 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end
-- защита каскада: нужны оба предыдущих списка, иначе вычет будет неполным
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101423)
	or not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101422)
begin
	raiserror(N'Нет списка 101423 или 101422 — 101421 не строим', 16, 1)
	return
end
-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1)
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- категории миссий один раз во временную таблицу
drop table if exists #cat_wine
select ID_CATEGORY_5_ext = cast(ID_CATEGORY_5_ext as nvarchar(50))
into #cat_wine
from I_MISSION (nolock)
where MISSION in (N'Вино', N'Просекко (игристое)') and MAIN is not null
group by cast(ID_CATEGORY_5_ext as nvarchar(50))

-- защита: пустой набор категорий дал бы молча пустой список
if not exists (select 1 from #cat_wine)
begin
	raiserror(N'101421: пустой набор категорий миссий «Вино» и «Просекко (игристое)» — блок остановлен', 16, 1)
	return
end

drop table if exists #x

;with t1 as
(
select	b.ID_CONTACT		-- достаточно одной покупки категории за 8 недель: считать чеки не нужно
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		inner join #cat_wine as k
		on c.ID_CATEGORY_5_ext = k.ID_CATEGORY_5_ext
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT
)
, t as (
select a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
		inner join t1
		on a.ID_CONTACT = t1.ID_CONTACT
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, ex as (
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101423, 101422)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101421
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
		left join ex
		on t.ID_CONTACT=ex.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and ex.ID_CONTACT is null
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0, @msg nvarchar(200)

while @error >= 0.0005
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
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
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101421: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


INSERT INTO I_PROMO_OFFER
SELECT *
from #x

GO


-- ==================================================================================
-- ЧАСТЬ 3. ВОСКРЕСЕНЬЕ 11.10
-- ==================================================================================

-- 101410 · Скидка 20% на крем для рук и тела · Активные и Новые · вс 11.10
-- 101410_Скидка 20 на крем для рук и тела

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101410 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101410 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101410)
begin
	raiserror(N'101410 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
select ID_PROMO = 101410
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0, @msg nvarchar(200)

while @error >= 0.0005
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
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
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101410: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


INSERT INTO I_PROMO_OFFER
SELECT *
from #x

GO


-- ==================================================================================
-- СВОДКА ПО КЛИЕНТАМ
-- ==================================================================================

;with p as (
select ID_PROMO = 101407, LIST_NAME = N'101407_Активируй 20 на капусту свёклу и морковь', DATA_START = cast('2026-10-07' as date), MANZANA_ONLINE = N'да'
union all select 101423, N'101423_Активируй 20 кешбэка на пиво', '2026-10-08', N'да'
union all select 101422, N'101422_Активируй 20 кешбэка на крепкий алкоголь', '2026-10-08', N'да'
union all select 101421, N'101421_Активируй 20 кешбэка на вино и игристое', '2026-10-08', N'да'
union all select 101410, N'101410_Скидка 20 на крем для рук и тела', '2026-10-11', N'да'
)
select ID_PROMO = p.ID_PROMO
		, [Название списка] = p.LIST_NAME
		, [Клиентов на отправку] = isnull(o.CNT, 0)
		, [Дата старта] = p.DATA_START
		, [Загрузка в Манзана Онлайн] = p.MANZANA_ONLINE
from p
		left join (
		select ID_PROMO, CNT = count(distinct ID_CONTACT)
		from I_PROMO_OFFER (nolock)
		where ID_PROMO in (101407, 101423, 101422, 101421, 101410) and CONTROL_GROUP = 0
			and ID_COMPANY = 1 and ID_ORGANIZATION = 1
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
order by p.DATA_START, p.ID_PROMO
GO


-- Контроль каскада: ни один клиент не должен попасть в две алкогольные акции четверга

select [Клиентов в 2+ алко-списках] = count(*)
from (
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101421, 101422, 101423)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
having count(distinct ID_PROMO) > 1
) a
GO


-- ==================================================================================
-- ПИСЬМО О ФОРМИРОВАНИИ СПИСКОВ — Анастасия Андрианова, anda@manzanagroup.ru
-- Строки ниже копируются в письмо как есть: номер списка, название, клиентов, старт, загрузка.
-- ==================================================================================

;with p as (
select ID_PROMO = 101407, LIST_NAME = N'101407_Активируй 20 на капусту свёклу и морковь', DATA_START = cast('2026-10-07' as date), MANZANA_ONLINE = N'да'
union all select 101423, N'101423_Активируй 20 кешбэка на пиво', '2026-10-08', N'да'
union all select 101422, N'101422_Активируй 20 кешбэка на крепкий алкоголь', '2026-10-08', N'да'
union all select 101421, N'101421_Активируй 20 кешбэка на вино и игристое', '2026-10-08', N'да'
union all select 101410, N'101410_Скидка 20 на крем для рук и тела', '2026-10-11', N'да'
)
select [Строка для письма] = concat(
		p.ID_PROMO, N' · ', p.LIST_NAME,
		N' · клиентов на отправку: ', format(isnull(o.CNT, 0), 'N0', 'ru-RU'),
		N' · старт ', format(p.DATA_START, 'dd.MM.yyyy'),
		N' · загрузка в Манзана Онлайн: ', p.MANZANA_ONLINE)
from p
		left join (
		select ID_PROMO, CNT = count(distinct ID_CONTACT)
		from I_PROMO_OFFER (nolock)
		where ID_PROMO in (101407, 101423, 101422, 101421, 101410) and CONTROL_GROUP = 0
			and ID_COMPANY = 1 and ID_ORGANIZATION = 1
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
order by p.DATA_START, p.ID_PROMO
GO


-- ==================================================================================
-- ВЫГРУЗКА СПИСКОВ НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================

-- 101407_Активируй 20 на капусту свёклу и морковь
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101407 and CONTROL_GROUP = 0
GO

-- 101423_Активируй 20 кешбэка на пиво
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101423 and CONTROL_GROUP = 0
GO

-- 101422_Активируй 20 кешбэка на крепкий алкоголь
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101422 and CONTROL_GROUP = 0
GO

-- 101421_Активируй 20 кешбэка на вино и игристое
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101421 and CONTROL_GROUP = 0
GO

-- 101410_Скидка 20 на крем для рук и тела
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101410 and CONTROL_GROUP = 0
GO


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #cat_beer
drop table if exists #cat_wine
drop table if exists #x
drop table if exists #t
drop table if exists #local_cg
drop table if exists #stat
GO
