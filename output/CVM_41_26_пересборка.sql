-- CVM неделя 41 · пересборка одним скриптом (01.10.2026): «Дарим 100 монет на неделю» 101426 и готовый кофе 101403. Запускать целиком (F5).
-- Что делает: показывает, что сейчас лежит по этим двум акциям → удаляет их строки из I_PROMO_OFFER → собирает оба списка заново → стандартная таблица для исполнителей: что грузить и какие объёмы (11 списков пн 05.10 и вт 06.10) → выгрузки двух пересобранных списков.
-- 101426: низкочастотные Активные и Новые (до 3 визитов за 6 недель) + Случайные, МИНУС получатели 101427 «300р. на чек от 3000р.» (дубль убран, решение Елены 01.10.2026).
-- 101403: Активные и Новые, 2+ чеков «Тонус», «Готовый кофе» или молотый кофе за 8 недель и не более 2 чеков с готовым кофе; с пушем и токеном.
-- Не трогаются: слипы 101393–101396 (уже собраны), 101427, серии 101397–101400, шапки в I_PROMO и все остальные акции. Перед запуском список 101427 должен быть собран.
-- Запускать один раз: при повторном запуске оба списка снова удалятся и соберутся с новой контрольной группой.

-- защита: без списка 101427 ничего не удаляем и не собираем
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101427)
begin
	raiserror(N'Нет списка 101427 — скрипт остановлен, ничего не удалено', 16, 1)
	set noexec on
end
GO


-- ==================================================================================
-- ШАГ 1. ЧТО ЛЕЖИТ СЕЙЧАС по акциям 101426, 101403
-- ==================================================================================

select ID_PROMO, CONTROL_GROUP, [Клиентов] = count(distinct ID_CONTACT), [Строк] = count(*)
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101426, 101403) and ID_COMPANY = 1
group by ID_PROMO, CONTROL_GROUP
order by ID_PROMO, CONTROL_GROUP
GO


-- ==================================================================================
-- ШАГ 2. УДАЛЕНИЕ СТАРЫХ СТРОК этих двух акций (и на отправку, и контрольные группы); слипы и остальные акции не трогаются
-- ==================================================================================

set xact_abort on
begin tran

delete from I_PROMO_OFFER
where ID_PROMO in (101426, 101403) and ID_COMPANY = 1

select [Удалено строк] = @@rowcount

commit
GO


-- ==================================================================================
-- ШАГ 3. КОНТРОЛЬ УДАЛЕНИЯ — ожидание: 0 строк
-- ==================================================================================

select [Осталось строк] = count(*)
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101426, 101403) and ID_COMPANY = 1
GO


-- ==================================================================================
-- ШАГ 4. «ДАРИМ 100 МОНЕТ НА НЕДЕЛЮ» 101426 — обе части в один список, минус 101427
-- ==================================================================================

drop table if exists #done
create table #done (ID_PROMO int, PART tinyint)
GO

-- 101426 · часть 1 (низкочастотные Активные и Новые, КГ 5%) · Дарим 100 монет на неделю · кроме списка 101427 · 05.10–31.10, список на месяц
-- 101426_Дарим 100 монет на неделю

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101426 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101426 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101426)
begin
	raiserror(N'101426 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

-- каскад: из списка исключаются получатели 101427 «300р. на чек от 3000р.» — иначе две коммуникации спорят друг с другом (решение Елены 01.10.2026)
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101427)
begin
	raiserror(N'Нет списка 101427 — 101426 не строим', 16, 1)
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
	and a.VISITS between 1 and 3		-- 1–2 визита в месяц: VISITS считается за 6 недель (42 дня); берём всех низкочастотных, включая Новых (решение Елены 01.10.2026)
group by a.ID_CONTACT
)
, ex as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101427)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101426
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
set @msg = concat(N'101426 часть 1 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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


-- маркер ставим, только если первая часть действительно записана (при ошибке INSERT вторая часть не пойдёт)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101426)
	insert into #done values (101426, 1)

GO

-- 101426 · часть 2 (Случайные, КГ 10%) · Дарим 100 монет на неделю · кроме списка 101427 · 05.10–31.10, в тот же список
-- 101426_Дарим 100 монет на неделю

-- защита: вторая часть идёт только следом за первой частью этого же прогона
if not exists (select 1 from #done where ID_PROMO = 101426 and PART = 1)
begin
	raiserror(N'101426: первая часть в этом прогоне не строилась — вторая часть пропущена', 10, 1)
	return
end

-- каскад: из списка исключаются получатели 101427 «300р. на чек от 3000р.» — иначе две коммуникации спорят друг с другом (решение Елены 01.10.2026)
if not exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101427)
begin
	raiserror(N'Нет списка 101427 — 101426 не строим', 16, 1)
	return
end

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (4)
group by a.ID_CONTACT
)
, ex as(
select ID_CONTACT
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101427)
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
group by ID_CONTACT
)
select ID_PROMO = 101426
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
set @msg = concat(N'101426 часть 2 (сегмент 4): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
-- ШАГ 5. ГОТОВЫЙ КОФЕ 101403
-- ==================================================================================

-- 101403 · 10% скидка на готовый кофе до 12 утра · Активные и Новые — 2+ покупок «Тонус», «Готовый кофе» или молотого кофе за 8 недель и не более 2 покупок готового кофе (решение Елены 01.10.2026) · 06.10–31.10
-- 101403_10 скидка на готовый кофе до 12 утра

-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101403 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101403 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101403)
begin
	raiserror(N'101403 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

-- опорная дата — последний офлайн-чек (ID_ORGANIZATION = 1)
declare @maxd datetime = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1)

-- категории один раз во временную таблицу: миссии «Тонус» и «Готовый кофе» + молотый кофе
-- (молотый кофе — категории 62030202 «Кофе молотый» и 62030207 «Кофе молотый / дрип-пакеты» миссии «Кофе и чай дома»;
--  SKU со словом «свежемолотый» в ассортименте нет, взяты все SKU молотого кофе — решение Елены 01.10.2026)
-- имя #cat5_kofe, а не #cat5: в блоках 101397 и 101400 таблица #cat5 создаётся с одной колонкой, и если она осталась в окне SSMS,
--  батч со ссылкой на IS_K не компилируется (Invalid column name 'IS_K') — ревью 01.10.2026; на состав списка не влияет
drop table if exists #cat5_kofe
select ID_CATEGORY_5
		, IS_K = max(iif(MISSION = N'Готовый кофе', 1, 0))		-- 1 — категория готового кофе
into #cat5_kofe
from I_MISSION (nolock)
where MAIN is not null
	and (MISSION in (N'Тонус (энергетики, кола)', N'Готовый кофе')
		or (MISSION = N'Кофе и чай дома' and cast(ID_CATEGORY_5_ext as nvarchar(50)) in (N'62030202', N'62030207')))
group by ID_CATEGORY_5

-- чеки с покупками этих категорий за 8 недель — один проход по I_CHECK, одна строка на чек
-- (вместо двух count(distinct) по строкам чеков: «Тонус» — миллионы строк, на них запрос зависал 01.10.2026)
drop table if exists #chk
select	b.ID_CONTACT
		, b.ID_CHECK
		, IS_K = max(k.IS_K)		-- 1 — в чеке есть готовый кофе
into #chk
from I_CHECK as b (nolock)
		inner join #cat5_kofe as k
		on b.ID_CATEGORY_5 = k.ID_CATEGORY_5
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0
	and b.DATA between dateadd(week,-8, @maxd) and @maxd
group by b.ID_CONTACT, b.ID_CHECK

drop table if exists #x

;with t1 as
(
select	ID_CONTACT
		, checks = count(*)
		, coffee_checks = sum(IS_K)
from #chk
group by ID_CONTACT
having count(*) >= 2          -- 2+ чеков с «Тонус», «Готовый кофе» или молотым кофе за 8 недель
	and sum(IS_K) <= 2        -- и не более 2 чеков с готовым кофе за те же 8 недель (решение Елены 01.10.2026)
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
select ID_PROMO = 101403
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
set @msg = concat(N'101403 (сегменты 1, 2, 3): итерация подбора КГ ', @iter, N', ошибка ', @error)
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
-- СВОДКА ДЛЯ ИСПОЛНИТЕЛЕЙ — что грузить и какие объёмы: все списки пн 05.10 и вт 06.10 (11 акций)
-- ==================================================================================

;with p as (
select ID_PROMO = 101427, LIST_NAME = N'101427_Активируй 300р на чек от 3000р', DATA_START = cast('2026-10-05' as date), MANZANA_ONLINE = N'да'
union all select 101426, N'101426_Дарим 100 монет на неделю', '2026-10-05', N'да'
union all select 101398, N'101398_Коммуникация по ПП', '2026-10-06', N'нет'
union all select 101399, N'101399_Коммуникация по готовой еде', '2026-10-06', N'нет'
union all select 101397, N'101397_Коммуникация детские категории', '2026-10-06', N'нет'
union all select 101400, N'101400_Коммуникация товары для животных', '2026-10-06', N'нет'
union all select 101403, N'101403_10 скидка на готовый кофе до 12 утра', '2026-10-06', N'да'
union all select 101396, N'101396_Пиво купон на 100р на 7 дней', '2026-10-06', N'да'
union all select 101395, N'101395_Крепкий алкоголь купон на 100р на 7 дней', '2026-10-06', N'да'
union all select 101394, N'101394_Вина купон на 100р на 7 дней', '2026-10-06', N'да'
union all select 101393, N'101393_Купон 50р на любую покупку', '2026-10-06', N'да'
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
		where ID_PROMO in (101427, 101426, 101398, 101399, 101397, 101400, 101403, 101396, 101395, 101394, 101393) and CONTROL_GROUP = 0
		group by ID_PROMO
		) o
		on p.ID_PROMO = o.ID_PROMO
order by p.DATA_START, p.ID_PROMO
GO


-- контроль пересечения — ожидание: 0 (получателей 101427 в списке 101426 нет)
select [101426 пересекается с 101427] = (select count(*) from I_PROMO_OFFER a (nolock) join I_PROMO_OFFER b (nolock) on a.ID_CONTACT = b.ID_CONTACT and b.ID_PROMO = 101427 where a.ID_PROMO = 101426)
GO


-- ==================================================================================
-- ВЫГРУЗКА СПИСКОВ НА ОТПРАВКУ (CONTROL_GROUP = 0) — два пересобранных списка
-- ==================================================================================

-- 101426_Дарим 100 монет на неделю
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101426 and CONTROL_GROUP = 0
GO

-- 101403_10 скидка на готовый кофе до 12 утра
select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101403 and CONTROL_GROUP = 0
GO


set noexec off
GO
