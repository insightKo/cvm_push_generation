-- CVM неделя 36 (31.08–06.09.2026) — один скрипт на всю неделю
-- Акции 101360–101362 (slip) и 101369–101374 (push) должны быть заведены месячным скриптом
-- CVM_september_2026_I_PROMO.sql. Здесь только выборки клиентов в I_PROMO_OFFER.
--
-- Порядок блоков важен: алкогольный каскад идёт пиво → крепкий → вино, отдельно для push и для slip.
-- Клиент попадает ровно в одну алкогольную акцию своего канала.
-- По алкоголю всегда берём только тех, у кого были покупки категории за последние 52 недели.
--
-- Контрольная группа: 5% (RN/20) и оценка по BUDGET для Активных/Новых,
--                     10% (RN/10) и оценка по LTV для Оттока/Спящих.


-- ==================================================================================
-- ЧАСТЬ 1. PUSH · ВТОРНИК 01.09
-- ==================================================================================

-- 101369 · 1 сентября — день Kinder: активируй 20% кешбэка на Kinder · Активные, Новые · вт 01.09
-- 101369_1 сентября — день Kinder

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT	
)
select ID_PROMO = 101369
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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
-- ЧАСТЬ 2. PUSH · ЧЕТВЕРГ 03.09 — АЛКОГОЛЬНЫЙ КАСКАД, АКТИВНЫЕ И НОВЫЕ
-- Только покупатели категории за 52 недели. Приоритет: пиво → крепкий → вино.
-- ==================================================================================

-- 101372 · Активируй 20% кешбэка на пиво · Активные, Новые — покупатели пива · чт 03.09
-- 101372_Активируй 20 кешбэка на пиво

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_5_ext in (select distinct cast(ID_CATEGORY_5_ext as nvarchar(50)) from I_MISSION where MISSION in ('Пиво') and MAIN is not null)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
select ID_PROMO = 101372
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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

-- 101371 · Активируй 20% кешбэка на крепкий алкоголь · Активные, Новые — покупатели крепкого, кроме взятых в пиво · чт 03.09
-- 101371_Активируй 20 кешбэка на крепкий алкоголь

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_3 in (77)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101372)
group by ID_CONTACT
)
select ID_PROMO = 101371
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
		left join x
		on t.ID_CONTACT=x.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and x.ID_CONTACT is null
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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

-- 101370 · Активируй 20% кешбэка на вино и игристое · Активные, Новые — покупатели вина, кроме взятых в пиво и крепкое · чт 03.09
-- 101370_Активируй 20 кешбэка на вино и игристое

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_5_ext in (select distinct cast(ID_CATEGORY_5_ext as nvarchar(50)) from I_MISSION where MISSION in ('Вино', 'Просекко (игристое)') and MAIN is not null)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101372, 101371)
group by ID_CONTACT
)
select ID_PROMO = 101370
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
		left join x
		on t.ID_CONTACT=x.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and x.ID_CONTACT is null
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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
-- ЧАСТЬ 3. PUSH · ЧЕТВЕРГ 03.09 — ОТТОК И СПЯЩИЕ, КГ 10%, оценка по LTV
-- Акция без разбивки по видам алкоголя: берём покупателей любого алкоголя.
-- ==================================================================================

-- 101373 · 50% кешбэка на алкоголь · Отток, Спящие — покупатели алкоголя · чт 03.09
-- 101373_50 кешбэка на алкоголь

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and (c.ID_CATEGORY_3 in (77)
		or c.ID_CATEGORY_5_ext in (select distinct cast(ID_CATEGORY_5_ext as nvarchar(50)) from I_MISSION where MISSION in ('Пиво', 'Вино', 'Просекко (игристое)') and MAIN is not null))
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (5, 6)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
select ID_PROMO = 101373
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
		, COST_DISCOUNT= round(sum(LTV)/10,0)
		, COST_DISCOUNT1 = sum(LTV)
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


declare @error float = 1.0

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
		, SEGMENT = RN/10
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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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
-- ЧАСТЬ 4. PUSH · ВОСКРЕСЕНЬЕ 06.09
-- ==================================================================================

-- 101374 · Скидка 20% на средства для мытья посуды · Активные, Новые · вс 06.09
-- 101374_Скидка 20 на средства для мытья посуды

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT	
)
select ID_PROMO = 101374
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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
-- ЧАСТЬ 5. SLIP · ВЫДАЧА 03.09–30.09 — КУПОНЫ 100 р. НА КАТЕГОРИЮ
-- Аудитория: Активные и Новые БЕЗ пушей, покупатели категории за 52 недели.
-- Тот же каскад: пиво → крепкий → вино, один купон на клиента.
-- ==================================================================================

-- 101362 · Пиво купон на 100р. на 7 дней · slip · Активные, Новые без пушей — покупатели пива · выдача 03.09–30.09
-- 101362_Пиво купон на 100р на 7 дней

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_5_ext in (select distinct cast(ID_CATEGORY_5_ext as nvarchar(50)) from I_MISSION where MISSION in ('Пиво') and MAIN is not null)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
select ID_PROMO = 101362
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
where (HAS_PUSH!=1 or TOKENS!=1)
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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

-- 101361 · Крепкий алкоголь купон на 100р. на 7 дней · slip · без пушей — покупатели крепкого, кроме взятых в пиво · выдача 03.09–30.09
-- 101361_Крепкий алкоголь купон на 100р на 7 дней

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_3 in (77)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101362)
group by ID_CONTACT
)
select ID_PROMO = 101361
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
		left join x
		on t.ID_CONTACT=x.ID_CONTACT
where (HAS_PUSH!=1 or TOKENS!=1)
and x.ID_CONTACT is null
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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

-- 101360 · Вина купон на 100р. на 7 дней · slip · без пушей — покупатели вина, кроме взятых в пиво и крепкое · выдача 03.09–30.09
-- 101360_Вина купон на 100р на 7 дней

drop table if exists #x

;with t1 as
(
select	a.ID_CONTACT
	, checks = count(distinct ID_CHECK)
from I_CVM_CONTACT as a (nolock)
		inner join I_CHECK as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		join I_PRODUCT c
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		and c.ID_CATEGORY_5_ext in (select distinct cast(ID_CATEGORY_5_ext as nvarchar(50)) from I_MISSION where MISSION in ('Вино', 'Просекко (игристое)') and MAIN is not null)
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT<>0
	and DATA between dateadd(week,-52,  (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)) and (select max(DATA) from I_CHECK(nolock) where ID_COMPANY=1)
	and SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
, t as (
select t1.ID_CONTACT
from t1
where checks >= 1
group by t1.ID_CONTACT
)
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101362, 101361)
group by ID_CONTACT
)
select ID_PROMO = 101360
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
		left join x
		on t.ID_CONTACT=x.ID_CONTACT
where (HAS_PUSH!=1 or TOKENS!=1)
and x.ID_CONTACT is null
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


declare @error float = 1.0

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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)
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
-- ЧАСТЬ 6. СВОДКА ПО КЛИЕНТАМ
-- ==================================================================================

;with p as (
select ID_PROMO = 101369, LIST_NAME = N'101369_1 сентября — день Kinder', DATA_START = cast('2026-09-01' as date), MANZANA_ONLINE = N'да'
union all select 101372, N'101372_Активируй 20 кешбэка на пиво', '2026-09-03', N'да'
union all select 101371, N'101371_Активируй 20 кешбэка на крепкий алкоголь', '2026-09-03', N'да'
union all select 101370, N'101370_Активируй 20 кешбэка на вино и игристое', '2026-09-03', N'да'
union all select 101373, N'101373_50 кешбэка на алкоголь', '2026-09-03', N'да'
union all select 101374, N'101374_Скидка 20 на средства для мытья посуды', '2026-09-06', N'да'
union all select 101362, N'101362_Пиво купон на 100р на 7 дней', '2026-09-03', N'нет'
union all select 101361, N'101361_Крепкий алкоголь купон на 100р на 7 дней', '2026-09-03', N'нет'
union all select 101360, N'101360_Вина купон на 100р на 7 дней', '2026-09-03', N'нет'
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
		where ID_PROMO in (101360, 101361, 101362, 101369, 101370, 101371, 101372, 101373, 101374) and CONTROL_GROUP = 0
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
order by p.DATA_START, p.ID_PROMO
GO


-- Контроль каскада: ни один клиент не должен попасть в две алкогольные акции своего канала

select [Канал] = N'PUSH', [Клиентов в 2+ акциях] = count(*)
from (
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101370, 101371, 101372)
group by ID_CONTACT
having count(distinct ID_PROMO) > 1
) a
union all
select N'SLIP', count(*)
from (
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101360, 101361, 101362)
group by ID_CONTACT
having count(distinct ID_PROMO) > 1
) b
GO


-- ==================================================================================
-- ЧАСТЬ 7. ВЫГРУЗКА СПИСКОВ НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================


-- 101369_1 сентября — день Kinder
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101369 and CONTROL_GROUP = 0
GO


-- 101372_Активируй 20 кешбэка на пиво
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101372 and CONTROL_GROUP = 0
GO


-- 101371_Активируй 20 кешбэка на крепкий алкоголь
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101371 and CONTROL_GROUP = 0
GO


-- 101370_Активируй 20 кешбэка на вино и игристое
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101370 and CONTROL_GROUP = 0
GO


-- 101373_50 кешбэка на алкоголь
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101373 and CONTROL_GROUP = 0
GO


-- 101374_Скидка 20 на средства для мытья посуды
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101374 and CONTROL_GROUP = 0
GO


-- 101362_Пиво купон на 100р на 7 дней
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101362 and CONTROL_GROUP = 0
GO


-- 101361_Крепкий алкоголь купон на 100р на 7 дней
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101361 and CONTROL_GROUP = 0
GO


-- 101360_Вина купон на 100р на 7 дней
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101360 and CONTROL_GROUP = 0
GO
