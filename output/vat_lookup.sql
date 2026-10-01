-- =============================================================================
-- Э6. НДС по товару -> product_vat.csv
-- =============================================================================
-- Зачем: PL считается от Доп ТО БЕЗ НДС. До сих пор НДС в модели не было
-- (promo_product_model.py: PL = Доп ТО x 0.30 - Скидка), в файлах «расчёт для КД»
-- он зашит константой (деление итога на 1.15). Нужна ставка по каждому товару:
-- для набора из продуктов (10%) и алкоголя/непрода (22% с 01.01.2026) смешанная
-- ставка сильно отличается от любой средней.
--
-- Ставку НИ В КОЕМ СЛУЧАЕ не проставлять вручную по названию категории:
-- сначала блок 1 (найти реальную колонку в БД), и только если её нет —
-- блок 3 (вывод из фактической выручки) или согласование справочника.
--
-- Запускать вместе с model_extracts.sql, на том же окне.
-- =============================================================================

set nocount on;

declare @idc int = 1;


-- =============================================================================
-- БЛОК 1. РАЗВЕДКА: есть ли ставка НДС в схеме
--         Прогнать ПЕРВЫМ и прислать результат — дальше блок 2 или блок 3
-- =============================================================================
select	[таблица] = s.name + '.' + t.name
		, [колонка] = c.name
		, [тип] = ty.name
		, [длина] = c.max_length
from sys.columns as c
		join sys.tables as t on c.object_id = t.object_id
		join sys.schemas as s on t.schema_id = s.schema_id
		join sys.types as ty on c.user_type_id = ty.user_type_id
where c.name like '%NDS%'
	or c.name like '%VAT%'
	or c.name like '%TAX%'
	or c.name like '%NALOG%'
order by t.name, c.name;

-- Заодно — полный состав I_PRODUCT: ставка может лежать под неочевидным именем
select	[колонка] = c.name
		, [тип] = ty.name
from sys.columns as c
		join sys.types as ty on c.user_type_id = ty.user_type_id
where c.object_id = object_id('dbo.I_PRODUCT')
order by c.column_id;


-- =============================================================================
-- БЛОК 2. ЕСЛИ колонка нашлась: ставка по каждому товару набора
--         Подставить реальное имя вместо <VAT_COLUMN> и раскомментировать.
--         VAT_RATE отдаём долей (0.10 / 0.22), а не процентом.
-- =============================================================================
/*
select	p.ID_PRODUCT
		, p.ID_PRODUCT_EXTERNAL
		, p.I_CATEGORY_5
		, VAT_RATE = case
				when cast(p.<VAT_COLUMN> as float) > 1 then cast(p.<VAT_COLUMN> as float) / 100.0
				else cast(p.<VAT_COLUMN> as float)
			end
from I_PRODUCT as p (nolock)
		join dbo.I_PRODUCT_SCORE as s (nolock)
		on p.ID_PRODUCT = s.ID_PRODUCT
		and s.ID_COMPANY = @idc
group by p.ID_PRODUCT, p.ID_PRODUCT_EXTERNAL, p.I_CATEGORY_5, p.<VAT_COLUMN>
order by p.ID_PRODUCT;
*/


-- =============================================================================
-- БЛОК 3. ЕСЛИ колонки в схеме НЕТ: вывести ставку из фактических данных чека
--         Работает только при наличии в I_CHECK суммы налога по строке
--         (типовые имена: NDS_SUM / SUM_NDS / TAX_AMOUNT / VAT_SUM).
--         Ставка = сумма налога / (сумма строки - сумма налога), округляется
--         к ближайшей законной ступени; RATE_SHARE показывает, насколько
--         однородна ставка внутри товара (должна быть близка к 1).
--         Имя колонки подставить вместо <TAX_COLUMN>.
-- =============================================================================
/*
declare @maxd date = (select max(DATA) from I_CHECK (nolock) where ID_COMPANY = @idc);
declare @d_end   date = dateadd(day, -((datediff(day, '19000101', @maxd) + 1) % 7), @maxd);
declare @d_start date = dateadd(day, -13, @d_end);

;with l as (
	select	c.ID_PRODUCT
			, AMT = cast(c.COST_DISCOUNT as float)
			, TAX = cast(c.<TAX_COLUMN> as float)
	from I_CHECK as c (nolock)
	where c.ID_COMPANY = @idc
		and c.DATA between @d_start and @d_end
		and c.COST_DISCOUNT > 0
		and c.<TAX_COLUMN> > 0
), r as (
	select	ID_PRODUCT
			, RAW_RATE = TAX / nullif(AMT - TAX, 0)
			, AMT
	from l
), b as (
	select	ID_PRODUCT
			, AMT
			, RATE = case
					when RAW_RATE < 0.05 then 0.00
					when RAW_RATE < 0.16 then 0.10
					when RAW_RATE < 0.21 then 0.20
					else 0.22
				end
	from r
	where RAW_RATE between 0 and 0.30
), agg as (
	select	ID_PRODUCT
			, RATE
			, W = sum(AMT)
			, RN = row_number() over (partition by ID_PRODUCT order by sum(AMT) desc)
			, TOT = sum(sum(AMT)) over (partition by ID_PRODUCT)
	from b
	group by ID_PRODUCT, RATE
)
select	ID_PRODUCT
		, VAT_RATE = RATE
		, RATE_SHARE = W / nullif(TOT, 0)   -- доля выручки товара на этой ставке
from agg
where RN = 1
order by ID_PRODUCT;
*/


-- =============================================================================
-- БЛОК 4. Смешанная ставка набора (после того как появился product_vat.csv)
--         Взвешивание по фактической выручке за 2 недели — та же база,
--         на которой модель считает Доп ТО. Это число заменяет «средние 16%».
--         Выполнять только когда таблица #vat заполнена результатом блока 2/3.
-- =============================================================================
/*
select	[смешанная ставка НДС] = sum(v.VAT_RATE * s.PRICE_2W * s.CHECKS_2W)
								 / nullif(sum(s.PRICE_2W * s.CHECKS_2W), 0)
from dbo.I_PRODUCT_SCORE as s (nolock)
		join #vat as v on v.ID_PRODUCT = s.ID_PRODUCT
where s.ID_COMPANY = @idc and s.CHECKS_2W > 0;
*/
