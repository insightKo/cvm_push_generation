-- CVM · неделя 42 · кластерные кешбэки чт 15.10 – вс 18.10.2026 · 10 акций
-- Стандартная сборка как CVM_101412.sql: I_CVM_CONTACT + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG,
-- КГ 5% (RN/20) по BUDGET для SEGMENT 1–3, 10% (RN/10) по LTV для Случайных — отдельно по каждой акции.
-- Тип покупателя — I_CLUSTER_CONTACT (последнее окно, модель how6-2026-10-09), CLUSTER_DETAIL:
--   1 Продукты по мелочи → 101436 · 2 Редкие → 101435 · 3 Частые с большой корзиной → 101437 · 4 Регулярные за продуктами → 101438
--   5 Большая закупка продуктов → 101413 · 8 За хлебом и водой → 101439 · 9 Ультра-частые → 101444 · 10 Вино, крепкое, кофе → 101443
--   6 Много нон-фуда + 7 Большая закупка с нон-фудом → 101415 (покупали женскую гигиену за 52 недели) / 101441 (остальные)
-- Сегменты: 101437, 101438, 101444 — 1, 2, 3 (Случайных там единицы); остальные — 1, 2, 3, 4.
-- LOAD_TO_ML = 1 (решение Елены 13.10.2026), LOAD_TO_MC = 1.

USE mci_model
GO


-- ==================================================================================
-- 0. УДАЛЕНИЕ ПРЕЖНИХ СТРОК ЭТИХ 10 АКЦИЙ
-- ==================================================================================

delete from I_PROMO_OFFER
where ID_PROMO in (101413, 101415, 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444)
GO


-- ==================================================================================
-- 1. СБОРКА СПИСКОВ
-- ==================================================================================

declare @ref date = (select max(REFERENCE_DATE) from I_CLUSTER_CONTACT (nolock)
					 where ID_ORGANIZATION = 1 and MODEL_VERSION = 'how6-2026-10-09')
declare @maxd date = '2026-10-12'	-- последняя дата чеков (окно женской гигиены — 52 недели)

-- клиенты с типом покупателя
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
		and k.REFERENCE_DATE = @ref
where a.ID_COMPANY = 1 and a.ID_ORGANIZATION = 1
	and a.ID_CONTACT <> 0
	and a.SEGMENT in (1, 2, 3, 4)
	and k.CLUSTER_DETAIL between 1 and 10
group by a.ID_CONTACT

-- покупали женскую гигиену за 52 недели (только нон-фуд типы 6 и 7)
drop table if exists #hyg

select	b.ID_CONTACT
into #hyg
from I_CHECK as b (nolock)
		inner join I_PRODUCT as c (nolock)
		on b.ID_PRODUCT = c.ID_PRODUCT
		and b.ID_COMPANY = c.ID_COMPANY
		inner join #cl as n
		on n.ID_CONTACT = b.ID_CONTACT
		and n.CLUSTER_DETAIL in (6, 7)
where b.ID_COMPANY = 1 and b.ID_ORGANIZATION = 1
	and b.ID_CONTACT <> 0
	and b.DATA between dateadd(week, -52, @maxd) and @maxd
	and cast(c.ID_CATEGORY_5_ext as nvarchar(50)) in (N'63020203', N'63020204', N'63020205')	-- прокладки ежедневные, тампоны, критические
group by b.ID_CONTACT

-- акция клиента
drop table if exists #pr

select	n.ID_CONTACT
		, n.SEGMENT
		, ID_PROMO = case n.CLUSTER_DETAIL
						when 1 then 101436
						when 2 then 101435
						when 3 then 101437
						when 4 then 101438
						when 5 then 101413
						when 8 then 101439
						when 9 then 101444
						when 10 then 101443
						else iif(h.ID_CONTACT is not null, 101415, 101441)
					 end
into #pr
from #cl as n
		left join #hyg as h
		on h.ID_CONTACT = n.ID_CONTACT

delete from #pr where SEGMENT = 4 and ID_PROMO in (101437, 101438, 101444)

-- список: пуш, минус I_GLOBAL_CG
drop table if exists #x

select	ID_PROMO = t.ID_PROMO
		, t.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 1
		, ID_COMPANY = 1
		, CRM_GUID
		, LOAD_TO_MC = 1
		, PART = iif(t.SEGMENT = 4, 4, 1)
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


-- КГ по каждой акции и части (1 — SEGMENT 1–3, RN/20; 4 — Случайные, RN/10)
drop table if exists #cg
create table #cg (ID_PROMO int, ID_CONTACT int)

declare @p int, @part int, @div int, @error float, @iter int, @msg nvarchar(200)

declare parts cursor local fast_forward for
	select distinct ID_PROMO, PART from #t
open parts
fetch next from parts into @p, @part

while @@fetch_status = 0
begin
	set @div = iif(@part = 4, 10, 20)
	set @error = 1.0
	set @iter = 0

	while @error >= 0.0005
	begin

	drop table if exists #local_cg

	;with x as
	(
	select ID_CONTACT
			, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
	from #t
	where ID_PROMO = @p and PART = @part
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
	from	#t as c
			left join #local_cg as b
			on c.ID_CONTACT=b.ID_CONTACT
	where c.ID_PROMO = @p and c.PART = @part
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
	set @msg = concat(@p, N' часть ', @part, N': итерация подбора КГ ', @iter, N', ошибка ', @error)
	raiserror(@msg, 0, 1) with nowait
	end

	select ID_PROMO = @p, PART = @part, * from #stat

	insert into #cg (ID_PROMO, ID_CONTACT)
	select @p, ID_CONTACT from #local_cg

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


INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
from #x

GO


-- ==================================================================================
-- СВОДКА ПО КЛИЕНТАМ
-- ==================================================================================

select o.ID_PROMO
		, [Название списка] = p.PROMO_NAME_ML
		, [Клиентов на отправку] = count(distinct iif(o.CONTROL_GROUP = 0, o.ID_CONTACT, null))
		, [КГ] = count(distinct iif(o.CONTROL_GROUP = 1, o.ID_CONTACT, null))
		, [Дата старта] = cast('2026-10-15' as date)
		, [Загрузка в Манзана Онлайн] = N'да'
from I_PROMO_OFFER as o (nolock)
		inner join I_PROMO as p (nolock)
		on p.ID_PROMO = o.ID_PROMO
		and p.ID_COMPANY = 1
where o.ID_PROMO in (101413, 101415, 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444)
	and o.ID_COMPANY = 1 and o.ID_ORGANIZATION = 1
group by o.ID_PROMO, p.PROMO_NAME_ML
order by o.ID_PROMO
GO


-- ==================================================================================
-- ПИСЬМО О ФОРМИРОВАНИИ СПИСКОВ (anda@manzanagroup.ru)
-- ==================================================================================

select [Строка письма] = concat(o.ID_PROMO, N' · ', p.PROMO_NAME_ML, N' · клиентов ', count(distinct o.ID_CONTACT), N' · старт 15.10.2026 · Манзана Онлайн: да')
from I_PROMO_OFFER as o (nolock)
		inner join I_PROMO as p (nolock)
		on p.ID_PROMO = o.ID_PROMO
		and p.ID_COMPANY = 1
where o.ID_PROMO in (101413, 101415, 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444)
	and o.CONTROL_GROUP = 0
	and o.ID_COMPANY = 1 and o.ID_ORGANIZATION = 1
group by o.ID_PROMO, p.PROMO_NAME_ML
order by o.ID_PROMO
GO


-- ==================================================================================
-- ВЫГРУЗКА СПИСКОВ НА ОТПРАВКУ (CONTROL_GROUP = 0)
-- ==================================================================================

select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101413 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101415 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101435 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101436 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101437 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101438 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101439 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101441 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101443 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
select CRM_GUID from I_PROMO_OFFER (nolock) where ID_PROMO = 101444 and CONTROL_GROUP = 0 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
GO


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #cl
drop table if exists #hyg
drop table if exists #pr
drop table if exists #x
drop table if exists #t
drop table if exists #cg
drop table if exists #local_cg
drop table if exists #stat
GO
