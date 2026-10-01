-- CVM неделя 40 (28.09–04.10.2026) — один скрипт на всю неделю, запускать целиком (F5)
-- Акции недели по датам: ср 30.09 — 101390 · чт 01.10 — 101404, 101428 · пт 02.10 — 101402, 101401 · вс 04.10 — 101406.
-- Шапки в I_PROMO: 101390 — CVM_september_2026_I_PROMO.sql, остальные — CVM_october_2026_I_PROMO.sql (прогнать до этого файла).
-- 101390: обе части в один список, минус список 101392 «Активируй 100 монет» (решение Елены 25.09).
-- 101428 — один список на весь октябрь: Отток и Спящие (SEGMENT 5, 6) на дату сборки; по нему идут дни 101428_1…_17 (решение Елены 29.09.2026).
-- Серии пиво и вино — Активные и Новые (1, 2, 3), 2+ офлайн-покупок миссии за 8 недель; вино — минус попавшие в пиво.
-- Контрольная группа: 5% (RN/20) и BUDGET для Активных/Новых, 10% (RN/10) и LTV для Оттока и Спящих.
-- Каждый блок пропускается, если его список уже есть в I_PROMO_OFFER, — файл можно перезапускать целиком, ничего не задвоится.
-- Ход подбора КГ виден на вкладке «Сообщения».


-- ==================================================================================
-- ЧАСТЬ 0. МАРКЕР ЧАСТЕЙ ТЕКУЩЕГО ПРОГОНА (для 101390, которая собирается двумя блоками)
-- ==================================================================================

drop table if exists #done
create table #done (ID_PROMO int, PART tinyint)
GO


-- ==================================================================================
-- ЧАСТЬ 1. СРЕДА 30.09 — АКТИВАЦИЯ НА КАШИ И ХЛОПЬЯ, обе части в один список 101390
-- ==================================================================================


-- 101390 · часть 1 (активные и новые, КГ 5%) · Активируй 20% на каши и хлопья · ср 30.09, кроме списка 101392
-- 101390_Активируй 20 на каши и хлопья

-- защита: список 101390 уже собран — пропускаем обе части (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101390)
begin
	raiserror(N'101390 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end


-- из выборки исключается список 101392 «Активируй 100 монет»: без него исключение не сработает
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101392)
begin
	raiserror(N'Нет списка 101392 — 101390 не строим', 16, 1)
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
where ID_PROMO in (101392)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101390
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

insert into #done values (101390, 1)

GO

-- 101390 · часть 2 (отток и спящие, КГ 10%) · Активируй 20% на каши и хлопья · ср 30.09, кроме списка 101392
-- 101390_Активируй 20 на каши и хлопья

-- защита: вторая часть идёт только следом за первой частью этого же прогона
if not exists (select 1 from #done where ID_PROMO = 101390 and PART = 1)
begin
	raiserror(N'101390: первая часть в этом прогоне не строилась — вторая часть пропущена', 10, 1)
	return
end


-- из выборки исключается список 101392 «Активируй 100 монет»: без него исключение не сработает
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101392)
begin
	raiserror(N'Нет списка 101392 — 101390 не строим', 16, 1)
	return
end

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
, x as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101392)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101390
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
-- ЧАСТЬ 2. ЧЕТВЕРГ 01.10
-- ==================================================================================

-- 101404 · Активируй 20% кешбэка на конфеты в коробках и шоколад · Активные, Новые · чт 01.10 – вс 04.10
-- 101404_Активируй 20 кешбэка на конфеты в коробках и шоколад

-- защита: шапка акции должна быть заведена (CVM_october_2026_I_PROMO.sql); список уже собран — блок пропускается
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101404 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101404 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101404)
begin
	raiserror(N'101404 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
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
select ID_PROMO = 101404
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
set @msg = concat(N'101404 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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

-- 101428 · Отток 50% кешбэка по дням · Отток и Спящие · список на месяц 01.10–31.10 (дни 101428_1…_17)
-- 101428_Отток 50 кешбэка по дням

-- защита: шапка акции должна быть заведена (CVM_october_2026_I_PROMO.sql); список уже собран — блок пропускается
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101428 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101428 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101428)
begin
	raiserror(N'101428 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end


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
select ID_PROMO = 101428
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

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101428 (сегменты 5, 6): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
-- ЧАСТЬ 3. ПЯТНИЦА 02.10 — ТЕМАТИЧЕСКИЕ СЕРИИ ОКТЯБРЯ, каскад пиво → вино
-- ==================================================================================

-- 101402 · Тематическая рассылка пиво · Активные и Новые, 2+ покупок «Пиво», «Снеки» за 8 недель · с пт 02.10, серия октября
-- 101402_Тематическая рассылка пиво

-- защита: шапка акции должна быть заведена (CVM_october_2026_I_PROMO.sql); список уже собран — блок пропускается
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101402 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101402 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101402)
begin
	raiserror(N'101402 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1, решение Елены 14.09.2026); окно 8 недель
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- категории миссий один раз во временную таблицу, чтобы не гонять подзапрос по чекам
drop table if exists #cat
select ID_CATEGORY_5_ext = cast(ID_CATEGORY_5_ext as nvarchar(50))
into #cat
from I_MISSION (nolock)
where MISSION in (N'Пиво', N'Снеки') and MAIN is not null
group by cast(ID_CATEGORY_5_ext as nvarchar(50))

drop table if exists #x

;with t1 as
(
select	b.ID_CONTACT
		, checks = count(distinct b.ID_CHECK)
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		inner join #cat as k
		on c.ID_CATEGORY_5_ext = k.ID_CATEGORY_5_ext
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0                         -- анонимные чеки не агрегируем (ниже всё равно ID_CONTACT<>0)
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT
having count(distinct b.ID_CHECK) >= 2          -- 2+ покупок миссии за 8 недель (порог тематических пиво/вино, решение Елены)
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
select ID_PROMO = 101402
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
set @msg = concat(N'101402 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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

-- 101401 · Тематическая рассылка вино и просекко · Активные и Новые, 2+ покупок «Вино», «Просекко (игристое)» за 8 недель, минус список 101402 · с пт 02.10
-- 101401_Тематическая рассылка вино и просекко

-- защита: шапка акции должна быть заведена (CVM_october_2026_I_PROMO.sql); список уже собран — блок пропускается
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101401 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101401 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101401)
begin
	raiserror(N'101401 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

-- каскад: из списка исключаются попавшие в 101402; без списка 101402 блок не строим
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101402)
begin
	raiserror(N'Нет списка 101402 — 101401 не строим', 16, 1)
	return
end

-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1, решение Елены 14.09.2026); окно 8 недель
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- категории миссий один раз во временную таблицу, чтобы не гонять подзапрос по чекам
drop table if exists #cat
select ID_CATEGORY_5_ext = cast(ID_CATEGORY_5_ext as nvarchar(50))
into #cat
from I_MISSION (nolock)
where MISSION in (N'Вино', N'Просекко (игристое)') and MAIN is not null
group by cast(ID_CATEGORY_5_ext as nvarchar(50))

drop table if exists #x

;with t1 as
(
select	b.ID_CONTACT
		, checks = count(distinct b.ID_CHECK)
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		inner join #cat as k
		on c.ID_CATEGORY_5_ext = k.ID_CATEGORY_5_ext
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0                         -- анонимные чеки не агрегируем (ниже всё равно ID_CONTACT<>0)
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT
having count(distinct b.ID_CHECK) >= 2          -- 2+ покупок миссии за 8 недель (порог тематических пиво/вино, решение Елены)
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
), ex as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101402)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101401
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
and c.ID_CONTACT is null
and ex.ID_CONTACT is null
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
set @msg = concat(N'101401 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
-- ЧАСТЬ 4. ВОСКРЕСЕНЬЕ 04.10
-- ==================================================================================

-- 101406 · Скидка 20% на средства для мытья полов и универсальные чистящие · Активные, Новые · вс 04.10
-- 101406_Скидка 20 на средства для мытья полов и универсальные чистящие

-- защита: шапка акции должна быть заведена (CVM_october_2026_I_PROMO.sql); список уже собран — блок пропускается
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101406 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101406 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101406)
begin
	raiserror(N'101406 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
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
select ID_PROMO = 101406
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
set @msg = concat(N'101406 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
select ID_PROMO = 101390, LIST_NAME = N'101390_Активируй 20 на каши и хлопья', DATA_START = cast('2026-09-30' as date), MANZANA_ONLINE = N'да'
union all select 101404, N'101404_Активируй 20 кешбэка на конфеты в коробках и шоколад', '2026-10-01', N'да'
union all select 101428, N'101428_Отток 50 кешбэка по дням', '2026-10-01', N'да'
union all select 101402, N'101402_Тематическая рассылка пиво', '2026-10-02', N'нет'
union all select 101401, N'101401_Тематическая рассылка вино и просекко', '2026-10-02', N'нет'
union all select 101406, N'101406_Скидка 20 на средства для мытья полов и универсальные чистящие', '2026-10-04', N'да'
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
		where ID_PROMO in (101390, 101404, 101428, 101402, 101401, 101406) and CONTROL_GROUP = 0
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
order by p.DATA_START, p.ID_PROMO
GO



-- ==================================================================================
-- ВЫГРУЗКА СПИСКОВ НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================


-- 101390_Активируй 20 на каши и хлопья
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101390 and CONTROL_GROUP = 0
GO

-- 101404_Активируй 20 кешбэка на конфеты в коробках и шоколад
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101404 and CONTROL_GROUP = 0
GO

-- 101428_Отток 50 кешбэка по дням
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101428 and CONTROL_GROUP = 0
GO

-- 101402_Тематическая рассылка пиво
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101402 and CONTROL_GROUP = 0
GO

-- 101401_Тематическая рассылка вино и просекко
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101401 and CONTROL_GROUP = 0
GO

-- 101406_Скидка 20 на средства для мытья полов и универсальные чистящие
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101406 and CONTROL_GROUP = 0
GO
