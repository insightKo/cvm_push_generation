-- CVM · тестовый список 1013000_Test · ср 26.08.2026
-- Логика отбора полностью повторяет акцию 101355 (Активируй 20% на конфеты в коробках):
--   часть 1 — SEGMENT (1, 2, 3) активные и новые, КГ 5%, балансировка по BUDGET
--   часть 2 — SEGMENT (5, 6) отток и спящие, КГ 10%, балансировка по LTV
-- Отличие: LOAD_TO_ML = 0 — список в ML-обучение не отдаём.
-- Прогонять сверху вниз. Блок 0 нужен только если акции 1013000 ещё нет в I_PROMO.


-- ==================================================================================
-- БЛОК 0. ЗАВЕДЕНИЕ АКЦИИ В I_PROMO (пропустить, если строка уже есть)
-- ==================================================================================

DECLARE @d date = '2026-08-26'
DECLARE @fd date = '2026-08-26'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 1013000
	  , 'Test'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
	  , NULL
	  , 'Активируемая скидка'
	  , 4
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO


-- ==================================================================================
-- ЧАСТЬ 1. АКТИВНЫЕ И НОВЫЕ — SEGMENT (1, 2, 3), контрольная группа 5%, оценка по BUDGET
-- ==================================================================================

-- 1013000 · часть 1 (активные и новые, КГ 5%) · Test · Активные, Новые, Спящие, Отток · ср 26.08
-- 1013000_Test

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
select ID_PROMO = 1013000
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
-- ЧАСТЬ 2. ОТТОК И СПЯЩИЕ — SEGMENT (5, 6), контрольная группа 10%, оценка по LTV
-- ==================================================================================

-- 1013000 · часть 2 (отток и спящие, КГ 10%) · Test · Активные, Новые, Спящие, Отток · ср 26.08
-- 1013000_Test

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and SEGMENT in (5, 6)
group by a.ID_CONTACT	
)
select ID_PROMO = 1013000
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
-- ЧАСТЬ 3. СВОДКА ПО КЛИЕНТАМ (обе части вместе)
-- ==================================================================================

;with p as (
select ID_PROMO = 1013000, LIST_NAME = N'1013000_Test', DATA_START = cast('2026-08-26' as date), MANZANA_ONLINE = N'нет'
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
		where ID_PROMO = 1013000 and CONTROL_GROUP = 0
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
GO


-- ==================================================================================
-- ЧАСТЬ 4. ВЫГРУЗКА СПИСКА НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================

-- 1013000_Test
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 1013000 and CONTROL_GROUP = 0
GO
