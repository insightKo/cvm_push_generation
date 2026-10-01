-- CVM · акция 101392 «Активируй 100 монет» · сб 26.09 – ср 30.09.2026 · один скрипт: шапка в I_PROMO + выборка + выгрузка
-- Добавлена 25.09.2026 к неделе 39. Прогонять сверху вниз в SSMS.
-- Аудитория: Активные и Новые (SEGMENT 1, 2, 3) — решение Елены 25.09. КГ 5% (RN/20), балансировка по BUDGET.
-- Из выборки исключается список 101380 «Купи 2 раза от 1000р.» — у них до 30.09 своя акция (решение Елены 25.09).
-- Шапка: сегмент «Активные» (4), окно истории 6 недель, механика 17 «Предначисление бонусов с активацией», окно «после» 4 дня (01.10–04.10).
-- Блок 2 удаляет текущий список 101392 из I_PROMO_OFFER, блок 3 собирает его заново — файл можно прогонять целиком повторно.
-- Шапка в I_PROMO заводится один раз: если она уже есть, блок 1 пропускается.


-- ==================================================================================
-- БЛОК 1. ШАПКА АКЦИИ В I_PROMO
-- ==================================================================================

if exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101392 and ID_COMPANY = 1)
begin
	raiserror(N'101392 уже заведена в I_PROMO — блок пропущен', 10, 1)
	return
end

DECLARE @d date = '2026-09-26'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101392
	  , 'Активируй 100 монет'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Предначисление бонусов с активацией'
	  , 17
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- контроль шапки: одна строка, окно «после» 01.10–04.10
select ID_PROMO, PROMO_NAME, START_DATE, FINISH_DATE, START_DATE_AFTER, FINISH_DATE_AFTER
		, [Дней после] = datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1
from I_PROMO (nolock)
where ID_PROMO = 101392 and ID_COMPANY = 1
GO


-- ==================================================================================
-- БЛОК 2. УДАЛЕНИЕ ТЕКУЩЕГО СПИСКА 101392 ИЗ I_PROMO_OFFER
-- Список, собранный раньше без исключения 101380, удаляется целиком — вместе с контрольной группой.
-- ==================================================================================

select [Строк 101392 в I_PROMO_OFFER до удаления] = count(*)
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101392

delete from I_PROMO_OFFER
where ID_PROMO = 101392

select [Удалено строк] = @@ROWCOUNT
GO


-- ==================================================================================
-- БЛОК 3. ВЫБОРКА — Активные и Новые, КГ 5% по BUDGET
-- ==================================================================================

-- 101392 · Активируй 100 монет · Активные, Новые · сб 26.09–ср 30.09, кроме списка 101380 «Купи 2 раза от 1000р.»
-- 101392_Активируй 100 монет

-- защита: список акции уже собран — блок пропускаем (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101392)
begin
	raiserror(N'101392 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

-- из выборки исключается список 101380: без него исключение не сработает
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101380)
begin
	raiserror(N'Нет списка 101380 — 101392 не строим', 16, 1)
	return
end


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
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101380)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101392
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 1
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
set @msg = concat(N'101392 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
-- БЛОК 4. СВОДКА И ВЫГРУЗКА СПИСКА НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================

select ID_PROMO = 101392
		, [Название списка] = N'101392_Активируй 100 монет'
		, [Клиентов на отправку] = count(distinct ID_CONTACT)
		, [Дата старта] = cast('2026-09-26' as date)
		, [Загрузка в Манзана Онлайн] = N'да'
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101392 and CONTROL_GROUP = 0
GO

-- 101392_Активируй 100 монет
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101392 and CONTROL_GROUP = 0
GO
