-- =============================================================================
-- Выгрузки для модели массового промо (promo_product_model.py)
-- =============================================================================
-- Запускать перед волной (после I_PRODUCT_SCORE_refresh.sql), результаты
-- сохранять в CSV (имена файлов — в комментарии каждого блока).
-- На этих выгрузках модель ОЦЕНИВАЕТ параметры из данных:
--   Э1. дневные агрегаты SKU (12 недель)  -> ценовая эластичность β по SKU
--   Э2. межпокупочные циклы SKU           -> конверсия прироста объёма в визиты φ
--   Э3. сэмпл клиент × товар (2% базы)    -> честное пересечение охвата набора
--   Э4. недельные скидки SKU (12 недель)  -> флаг круглогодичной скидки
--   Э5. базовые показатели организаций за 2 недели -> знаменатели долей,
--       влияние на средний чек сети
-- История промо-кампаний НЕ используется: эластичность оценивается на
-- естественной вариации фактических цен в чеках день ото дня.
-- =============================================================================

set nocount on;

declare @idc int = 1;
declare @weeks int = 12;            -- глубина истории для оценки
declare @min_contacts int = 1000;   -- кандидаты: товары с охватом за 2 недели от N покупателей
declare @sample_mod int = 50;       -- сэмпл клиентов 1/50 = 2%

declare @maxd date = (select max(DATA) from I_CHECK (nolock) where ID_COMPANY = @idc);
declare @d_end   date = dateadd(day, -((datediff(day, '19000101', @maxd) + 1) % 7), @maxd);
declare @d_start date = dateadd(week, -@weeks, dateadd(day, 1, @d_end));

select [Окно оценки: с] = @d_start, [по] = @d_end;


-- Кандидаты: товары из свежего I_PRODUCT_SCORE с достаточным охватом
drop table if exists #cand;

select ID_ORGANIZATION, ID_PRODUCT
into #cand
from dbo.I_PRODUCT_SCORE (nolock)
where ID_COMPANY = @idc and CONTACTS >= @min_contacts
group by ID_ORGANIZATION, ID_PRODUCT;


-- Не-фродовые клиенты (SEGMENT=0 — фрод, исключается из всех выгрузок)
drop table if exists #ok;

select ID_CONTACT, ID_ORGANIZATION
into #ok
from I_CVM_CONTACT (nolock)
where ID_COMPANY = @idc and ID_CONTACT <> 0 and SEGMENT <> 0
group by ID_CONTACT, ID_ORGANIZATION;


-- Строки чеков окна оценки по кандидатам (организация — из заголовка чека)
drop table if exists #l;

select	h.ID_ORGANIZATION
		, c.ID_PRODUCT
		, c.ID_CHECK
		, c.ID_CONTACT
		, c.DATA
		, AMT = cast(c.COST_DISCOUNT as float)   -- сумма строки = цена x кол-во − скидка (НЕ цена!)
		, QTY = cast(c.QUANTITY as float)        -- количество; при другом имени колонки поправить
		, DISC = cast(c.REAL_DISCOUNT as float)  -- сумма скидки строки
into #l
from I_CHECK as c (nolock)
		join I_CHECKHEADER as h (nolock)
		on c.ID_CHECK = h.ID_CHECK
		and c.ID_CONTACT = h.ID_CONTACT
		and c.ID_COMPANY = h.ID_COMPANY
		join #cand as k
		on h.ID_ORGANIZATION = k.ID_ORGANIZATION
		and c.ID_PRODUCT = k.ID_PRODUCT
		join #ok as o
		on c.ID_CONTACT = o.ID_CONTACT
		and h.ID_ORGANIZATION = o.ID_ORGANIZATION
where c.ID_COMPANY = @idc and c.ID_CONTACT <> 0
	and c.DATA between @d_start and @d_end;


-- =============================================================================
-- Э1. Дневные агрегаты SKU -> model_data.csv
--     Q — объём в ШТУКАХ за день, PRICE — фактическая цена единицы
--     (= сумма строк / количество; сумма строки сама по себе не цена)
-- =============================================================================
select	ID_ORGANIZATION
		, ID_PRODUCT
		, DATA
		, Q = sum(QTY)
		, PRICE = sum(AMT) / nullif(sum(QTY), 0)
from #l
group by ID_ORGANIZATION, ID_PRODUCT, DATA
order by ID_ORGANIZATION, ID_PRODUCT, DATA;


-- =============================================================================
-- Э2. Межпокупочный цикл SKU -> product_cycle.csv
--     Медианный интервал (дни) между последовательными покупками товара
--     одним клиентом; REPEAT_BUYERS — сколько клиентов покупали повторно
-- =============================================================================
;with p as (
	select ID_ORGANIZATION, ID_PRODUCT, ID_CONTACT, DATA
	from #l
	group by ID_ORGANIZATION, ID_PRODUCT, ID_CONTACT, DATA
), g as (
	select	ID_ORGANIZATION
			, ID_PRODUCT
			, ID_CONTACT
			, GAP = datediff(day,
					lag(DATA) over (partition by ID_ORGANIZATION, ID_PRODUCT, ID_CONTACT order by DATA),
					DATA)
	from p
), gg as (
	select ID_ORGANIZATION, ID_PRODUCT, ID_CONTACT, GAP
	from g
	where GAP is not null and GAP > 0
), med as (
	-- count(distinct) внутри OVER запрещён (Msg 10759) — медиана и покупатели раздельно
	select distinct
			ID_ORGANIZATION
			, ID_PRODUCT
			, T_MEDIAN = percentile_cont(0.5) within group (order by GAP)
						over (partition by ID_ORGANIZATION, ID_PRODUCT)
	from gg
), rb as (
	select	ID_ORGANIZATION
			, ID_PRODUCT
			, REPEAT_BUYERS = count(distinct ID_CONTACT)
	from gg
	group by ID_ORGANIZATION, ID_PRODUCT
)
select	m.ID_ORGANIZATION
		, m.ID_PRODUCT
		, m.T_MEDIAN
		, r.REPEAT_BUYERS
from med as m
		join rb as r
		on m.ID_ORGANIZATION = r.ID_ORGANIZATION
		and m.ID_PRODUCT = r.ID_PRODUCT
order by m.ID_ORGANIZATION, m.ID_PRODUCT;


-- =============================================================================
-- Э4. Товары с постоянной (круглогодичной) скидкой -> perm_discount.csv
--     По неделям 12-недельного окна: скидка недели = REAL_DISCOUNT / (COST_DISCOUNT
--     + REAL_DISCOUNT). Неделя «со скидкой» — технический порог 3%.
--     Порог «круглогодичности» (доля недель, мин. размер) задаётся в модели.
-- =============================================================================
;with w as (
	select	ID_ORGANIZATION
			, ID_PRODUCT
			, W = datediff(week, '19000101', DATA)
			, DISC_PCT = sum(DISC) / nullif(sum(AMT) + sum(DISC), 0)
	from #l
	group by ID_ORGANIZATION, ID_PRODUCT, datediff(week, '19000101', DATA)
)
select	ID_ORGANIZATION
		, ID_PRODUCT
		, WEEKS = count(*)
		, DISC_WEEKS = sum(iif(DISC_PCT >= 0.03, 1, 0))
		, AVG_DISC = avg(DISC_PCT)
from w
group by ID_ORGANIZATION, ID_PRODUCT
order by ID_ORGANIZATION, ID_PRODUCT;


-- =============================================================================
-- Э5. Базовые показатели организаций за ПОСЛЕДНИЕ 2 НЕДЕЛИ -> base_totals.csv
--     Все чеки сети (не только кандидаты): чеки, клиенты, ТО — знаменатели
--     для долей покрытия и расчёта влияния на средний чек сети
-- =============================================================================
declare @d_end5   date = @d_end;
declare @d_start5 date = dateadd(day, -13, @d_end5);

;with c as (
	select	h.ID_ORGANIZATION
			, h.ID_CHECK
			, h.ID_CONTACT
			, S = sum(h.COST_DISCOUNT * 1.0)
	from I_CHECKHEADER as h (nolock)
			join #ok as o
			on h.ID_CONTACT = o.ID_CONTACT
			and h.ID_ORGANIZATION = o.ID_ORGANIZATION
	where h.ID_COMPANY = @idc and h.ID_CONTACT <> 0
		and h.DATA between @d_start5 and @d_end5
	group by h.ID_ORGANIZATION, h.ID_CHECK, h.ID_CONTACT
	having sum(h.COST_DISCOUNT * 1.0) > 0
)
select	ID_ORGANIZATION
		, TOTAL_CHECKS  = count(*)
		, TOTAL_CLIENTS = count(distinct ID_CONTACT)
		, TOTAL_TO      = sum(S)
from c
group by ID_ORGANIZATION
order by ID_ORGANIZATION;


-- =============================================================================
-- Э3. Сэмпл клиент × товар за 6-недельное окно скоров -> client_sample.csv
--     Окно совпадает с окном CONTACTS в I_PRODUCT_SCORE — вероятности покрытия
--     согласованы. Детерминированный сэмпл 2% клиентов (ID_CONTACT % 50 = 0);
--     доля сэмпла передаётся в модель параметром --sample-share 0.02
-- =============================================================================
declare @d_end2   date = @d_end;
declare @d_start2 date = dateadd(day, -41, @d_end2);

select	h.ID_ORGANIZATION
		, c.ID_CONTACT
		, c.ID_PRODUCT
from I_CHECK as c (nolock)
		join I_CHECKHEADER as h (nolock)
		on c.ID_CHECK = h.ID_CHECK
		and c.ID_CONTACT = h.ID_CONTACT
		and c.ID_COMPANY = h.ID_COMPANY
		join #cand as k
		on h.ID_ORGANIZATION = k.ID_ORGANIZATION
		and c.ID_PRODUCT = k.ID_PRODUCT
		join #ok as o
		on c.ID_CONTACT = o.ID_CONTACT
		and h.ID_ORGANIZATION = o.ID_ORGANIZATION
where c.ID_COMPANY = @idc and c.ID_CONTACT <> 0
	and c.ID_CONTACT % @sample_mod = 0
	and c.DATA between @d_start2 and @d_end2
group by h.ID_ORGANIZATION, c.ID_CONTACT, c.ID_PRODUCT
order by h.ID_ORGANIZATION, c.ID_CONTACT, c.ID_PRODUCT;
