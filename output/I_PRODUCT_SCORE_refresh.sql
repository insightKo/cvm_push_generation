-- =============================================================================
-- I_PRODUCT_SCORE (с колонкой PRICE_INDEX) — регулярный пересчёт (для SQL Agent)
-- =============================================================================
-- Согласовано 17.08.2026. Скрипт самодостаточный: даты определяет сам, таблицы
-- создаёт при первом запуске, пересчитывает целиком. Ставить в расписание
-- раз в 2 недели (пн, перед стартом волны) или чаще — расчёт идемпотентный.
--
-- Логика:
--   Окно: последние 6 ПОЛНЫХ недель пн–вс от max(DATA) I_CHECK — тот же горизонт,
--   что у клиентских показателей I_CVM_CONTACT (VISITS/BUDGET за 6 недель):
--   все скоры согласованы по окну. CONTACTS/CHECKS — за 6 недель; базу «за волну»
--   модель получает масштабированием CHECKS x 14/42.
--   АКТИВНЫЙ АССОРТИМЕНТ: в публикацию попадают только товары с продажами за
--   последние 2 недели — защита от выведенного из ассортимента (блок 3а).
--   Фрод (SEGMENT=0 в I_CVM_CONTACT) исключён из ВСЕХ расчётов: клиентские
--   скоры строятся без него, чеки фродовых клиентов не попадают в базу.
--
--   Клиентские скоры 0–1 — «текущий к максимуму без выбросов» внутри организации:
--       FREQ_P   = VISITS / P99(VISITS), cap 1   (частота визитов)
--       BUDGET_P = BUDGET / P99(BUDGET), cap 1   (бюджет клиента)
--       P99 вместо максимума — защита от выбросов (перекупы, аномалии).
--   Чековый скор: чеки с суммой выше P99.9 организации — ВЫБРОСЫ, исключаются
--   из расчёта целиком; CHECK_P = сумма чека / P99.9 (максимум чистой базы).
--
--   Скоры товара (I_PRODUCT_SCORE, одна таблица; поле PRICE_INDEX добавляется
--   на старте, старый скор полностью очищается перед вставкой):
--       FREQ_SCORE    = средний FREQ_P   уникальных покупателей товара
--       LOYALTY_SCORE = средний BUDGET_P уникальных покупателей товара
--       CHECK_SCORE   = средний CHECK_P чеков, в которых товар встречается
--       CONTACTS/CHECKS — покупатели и чеки товара за окно
--
--   Ценовой индекс товара «по тому, какие клиенты покупают» (колонка PRICE_INDEX):
--       1) цена ЕДИНИЦЫ товара = сумма строк чека / суммарное количество
--          (COST_DISCOUNT строки = цена x кол-во − скидка, само по себе НЕ цена);
--       2) PRICE_RANK_P = цена единицы к P95 цен своей категории (орг. × кат.-5), cap 1;
--       3) ценовой профиль клиента PRICE_P = средний PRICE_RANK_P купленных им
--          товаров за окно (по строкам покупок — частые покупки весят больше);
--       4) PRICE_INDEX товара = средний PRICE_P его уникальных покупателей.
--       Читается так: 0.80 — товар покупают клиенты, выбирающие дорогое в своих
--       категориях; 0.20 — покупатели чувствительны к цене.
--
-- ПРОВЕРИТЬ ПРИ ПЕРВОМ ЗАПУСКЕ:
--   * сумма строки в I_CHECK взята как COST_DISCOUNT — если колонка называется
--     иначе (SUM/PRICE и т.п.), поправить в блоке 5;
--   * external-колонка в I_PRODUCT взята как ID_PRODUCT_EXTERNAL (блок 9).
-- =============================================================================

-- 0. DDL ОТДЕЛЬНЫМ БАТЧЕМ (GO обязателен): батч компилируется целиком до
--    выполнения, и без GO расчёт падал бы с Msg 207 (Invalid column name) —
--    новые колонки ещё не существуют на момент компиляции батча расчёта.
--    Блок переменных не использует. Таблица результата — ОДНА: I_PRODUCT_SCORE.
if object_id('dbo.I_PRODUCT_SCORE') is null
	create table dbo.I_PRODUCT_SCORE (
		ID_COMPANY int not null
		, ID_ORGANIZATION int not null
		, ID_CATEGORY_5 int null
		, ID_PRODUCT bigint not null
		, FREQ_SCORE float null
		, LOYALTY_SCORE float null
		, CHECK_SCORE float null
		, CONTACTS int null
		, CHECKS int null
		, PRICE_INDEX float null
		, SHOPS_2W int null			-- магазинов с продажами товара за последние 2 недели
		, CONTACTS_2W int null		-- покупателей товара за последние 2 недели
		, CHECKS_2W int null		-- чеков с товаром за последние 2 недели
		, PRICE_2W float null		-- средняя цена единицы за последние 2 недели
		, DISCOUNT_2W float null	-- средняя реальная скидка за последние 2 недели
	);

if col_length('dbo.I_PRODUCT_SCORE', 'PRICE_INDEX') is null
	alter table dbo.I_PRODUCT_SCORE add PRICE_INDEX float null;
if col_length('dbo.I_PRODUCT_SCORE', 'SHOPS_2W') is null
	alter table dbo.I_PRODUCT_SCORE add SHOPS_2W int null;
if col_length('dbo.I_PRODUCT_SCORE', 'CONTACTS_2W') is null
	alter table dbo.I_PRODUCT_SCORE add CONTACTS_2W int null;
if col_length('dbo.I_PRODUCT_SCORE', 'CHECKS_2W') is null
	alter table dbo.I_PRODUCT_SCORE add CHECKS_2W int null;
if col_length('dbo.I_PRODUCT_SCORE', 'PRICE_2W') is null
	alter table dbo.I_PRODUCT_SCORE add PRICE_2W float null;
if col_length('dbo.I_PRODUCT_SCORE', 'DISCOUNT_2W') is null
	alter table dbo.I_PRODUCT_SCORE add DISCOUNT_2W float null;
GO


set nocount on;

declare @idc int = 1;

declare @maxd date = (select max(DATA) from I_CHECK (nolock) where ID_COMPANY = @idc);
-- последнее завершившееся воскресенье ('19000101' — понедельник);
-- если max(DATA) само воскресенье — неделя считается полной
declare @d_end   date = dateadd(day, -((datediff(day, '19000101', @maxd) + 1) % 7), @maxd);
declare @d_start date = dateadd(day, -41, @d_end);          -- 6 полных недель = горизонт I_CVM_CONTACT
declare @d_active date = dateadd(day, -13, @d_end);         -- активный ассортимент: продажи за 2 недели

select [Окно расчёта: пн] = @d_start, [вс] = @d_end, [Активный ассортимент с] = @d_active;


-- 1. Скоры клиентов: значение к P99 базы организации, cap 1 (не ранг —
--    сохраняем масштаб поведения; P99 вместо максимума отсекает выбросы).
--    Фрод (SEGMENT=0) исключается здесь и через джойны — из ВСЕХ расчётов ниже
drop table if exists #client_score;

;with c as
(
select	ID_CONTACT
		, ID_ORGANIZATION
		, VISITS = max(isnull(VISITS, 0))
		, BUDGET = iif(max(isnull(BUDGET, 0)) < 0, 0, max(isnull(BUDGET, 0)))	-- возвраты: бюджет не бывает < 0
from I_CVM_CONTACT (nolock)
where ID_COMPANY = @idc and ID_CONTACT <> 0 and SEGMENT <> 0
group by ID_CONTACT, ID_ORGANIZATION
), t as
(
select distinct
		ID_ORGANIZATION
		, V99 = percentile_cont(0.99) within group (order by VISITS * 1.0) over (partition by ID_ORGANIZATION)
		, B99 = percentile_cont(0.99) within group (order by BUDGET * 1.0) over (partition by ID_ORGANIZATION)
from c
)
select	c.ID_CONTACT
		, c.ID_ORGANIZATION
		, FREQ_P   = iif(t.V99 > 0, iif(c.VISITS * 1.0 / t.V99 > 1, 1, c.VISITS * 1.0 / t.V99), 0)
		, BUDGET_P = iif(t.B99 > 0, iif(c.BUDGET * 1.0 / t.B99 > 1, 1, c.BUDGET * 1.0 / t.B99), 0)
into #client_score
from c
		join t
		on c.ID_ORGANIZATION = t.ID_ORGANIZATION;


-- 2. Чеки окна: выбросы по сумме (выше P99.9 организации) исключаются целиком,
--    CHECK_P = сумма чека к максимуму чистой базы (P99.9), cap 1.
--    Только чеки не-фродовых клиентов — джойн на #client_score.
drop table if exists #cheq0;

select	h.ID_CHECK
		, h.ID_CONTACT
		, h.ID_ORGANIZATION
		, ID_SHOP = max(h.ID_SHOP)		-- магазин чека; ПРОВЕРИТЬ имя колонки в I_CHECKHEADER
		, S = sum(h.COST_DISCOUNT * 1.0)
into #cheq0
from I_CHECKHEADER as h (nolock)
		join #client_score as s
		on h.ID_CONTACT = s.ID_CONTACT
		and h.ID_ORGANIZATION = s.ID_ORGANIZATION
where h.ID_COMPANY = @idc and h.ID_CONTACT <> 0
	and h.DATA between @d_start and @d_end
group by h.ID_CHECK, h.ID_CONTACT, h.ID_ORGANIZATION
having sum(h.COST_DISCOUNT * 1.0) > 0;	-- возвраты и нулевые чеки — не покупки

drop table if exists #cheq;

;with t as
(
select distinct
		ID_ORGANIZATION
		, S999 = percentile_cont(0.999) within group (order by S) over (partition by ID_ORGANIZATION)
from #cheq0
)
select	c.ID_CHECK
		, c.ID_CONTACT
		, c.ID_ORGANIZATION
		, c.ID_SHOP
		, CHECK_P = c.S / t.S999
into #cheq
from #cheq0 as c
		join t
		on c.ID_ORGANIZATION = t.ID_ORGANIZATION
where c.S <= t.S999;		-- чеки-выбросы по сумме удалены из расчёта


-- 3. Строки чеков окна: товар × чек × клиент (организация и перцентиль чека из #cheq)
drop table if exists #line;

select distinct
		c.ID_PRODUCT
		, c.ID_CATEGORY_5
		, c.ID_CHECK
		, c.ID_CONTACT
		, c.DATA
		, q.ID_ORGANIZATION
		, q.ID_SHOP
		, q.CHECK_P
into #line
from I_CHECK as c (nolock)
		join #cheq as q
		on c.ID_CHECK = q.ID_CHECK
		and c.ID_CONTACT = q.ID_CONTACT
where c.ID_COMPANY = @idc and c.ID_CONTACT <> 0
	and c.DATA between @d_start and @d_end;


-- 3а. АКТИВНЫЙ АССОРТИМЕНТ + свежие метрики за последние 2 недели (@d_active..@d_end),
--     все из БД, а не из статичного файла ассортимента:
--     SHOPS_2W    — в скольких магазинах товар продавался,
--     CONTACTS_2W — сколько клиентов его покупали,
--     CHECKS_2W   — чеков с товаром,
--     PRICE_2W    — средняя цена единицы (сумма строк / количество),
--     DISCOUNT_2W — средняя реальная скидка = REAL_DISCOUNT / (COST_DISCOUNT + REAL_DISCOUNT).
--     Товар без продаж за 2 недели в публикацию не попадает (вывод из ассортимента).
--     Чеки — только чистые (без фрода и выбросов, через #cheq).
drop table if exists #active;

;with l as
(
select	q.ID_ORGANIZATION
		, c.ID_PRODUCT
		, c.ID_CHECK
		, c.ID_CONTACT
		, q.ID_SHOP
		, AMT  = cast(c.COST_DISCOUNT as float)		-- сумма строки ПОСЛЕ скидки
		, QTY  = cast(c.QUANTITY as float)
		, DISC = cast(c.REAL_DISCOUNT as float)		-- сумма скидки строки
from I_CHECK as c (nolock)
		join #cheq as q
		on c.ID_CHECK = q.ID_CHECK
		and c.ID_CONTACT = q.ID_CONTACT
where c.ID_COMPANY = @idc and c.ID_CONTACT <> 0
	and c.DATA between @d_active and @d_end
)
select	ID_ORGANIZATION
		, ID_PRODUCT
		, SHOPS_2W    = count(distinct ID_SHOP)
		, CONTACTS_2W = count(distinct ID_CONTACT)
		, CHECKS_2W   = count(distinct ID_CHECK)
		, PRICE_2W    = sum(AMT) / nullif(sum(QTY), 0)
		-- процент скидки = REAL_DISCOUNT / (COST_DISCOUNT + REAL_DISCOUNT), в границах [0, 1]
		, DISCOUNT_2W = case when sum(AMT) + sum(DISC) > 0
				then iif(sum(DISC) / (sum(AMT) + sum(DISC)) < 0, 0,
					 iif(sum(DISC) / (sum(AMT) + sum(DISC)) > 1, 1,
						 sum(DISC) / (sum(AMT) + sum(DISC))))
				else null end
into #active
from l
group by ID_ORGANIZATION, ID_PRODUCT;


-- 4а. Чековая часть: CHECK_SCORE по чекам, в которых товар встречается
drop table if exists #by_check;

select	ID_ORGANIZATION
		, ID_CATEGORY_5
		, ID_PRODUCT
		, CHECK_SCORE = avg(CHECK_P)
		, CHECKS = count(distinct ID_CHECK)
into #by_check
from #line
group by ID_ORGANIZATION, ID_CATEGORY_5, ID_PRODUCT;


-- 4б. Клиентская часть: FREQ_SCORE и LOYALTY_SCORE по уникальным покупателям товара
--     (покупатели без профиля в I_CVM_CONTACT своей организации в средние не входят)
drop table if exists #by_client;

;with bc as
(
select	ID_ORGANIZATION
		, ID_CATEGORY_5
		, ID_PRODUCT
		, ID_CONTACT
from #line
group by ID_ORGANIZATION, ID_CATEGORY_5, ID_PRODUCT, ID_CONTACT
)
select	bc.ID_ORGANIZATION
		, bc.ID_CATEGORY_5
		, bc.ID_PRODUCT
		, FREQ_SCORE    = avg(s.FREQ_P)
		, LOYALTY_SCORE = avg(s.BUDGET_P)
		, CONTACTS = count(distinct bc.ID_CONTACT)
into #by_client
from bc
		join #client_score as s
		on bc.ID_CONTACT = s.ID_CONTACT
		and bc.ID_ORGANIZATION = s.ID_ORGANIZATION
group by bc.ID_ORGANIZATION, bc.ID_CATEGORY_5, bc.ID_PRODUCT;


-- =============================================================================
-- ЦЕНОВОЙ ИНДЕКС ТОВАРА — по тому, какие клиенты покупают
-- =============================================================================

-- 5. Фактическая цена ЕДИНИЦЫ товара за окно.
--    COST_DISCOUNT строки = цена x количество − скидка (это сумма строки, НЕ цена!),
--    поэтому цена единицы = сумма строк / суммарное количество.
--    Ценовое позиционирование СОЗНАТЕЛЬНО по цене С УЧЁТОМ скидки (решение Елены
--    18.08.2026): на части товаров скидка круглогодичная, полная цена фиктивна.
--    Имена колонок COST_DISCOUNT/QUANTITY в I_CHECK — проверить, при других поправить.
drop table if exists #prod_price;

select	q.ID_ORGANIZATION
		, c.ID_CATEGORY_5
		, c.ID_PRODUCT
		, UNIT_PRICE = sum(cast(c.COST_DISCOUNT as float)) / nullif(sum(cast(c.QUANTITY as float)), 0)
into #prod_price
from I_CHECK as c (nolock)
		join #cheq as q
		on c.ID_CHECK = q.ID_CHECK
		and c.ID_CONTACT = q.ID_CONTACT
where c.ID_COMPANY = @idc and c.ID_CONTACT <> 0
	and c.DATA between @d_start and @d_end
group by q.ID_ORGANIZATION, c.ID_CATEGORY_5, c.ID_PRODUCT;


-- 6. Ценовой скор товара: цена единицы к P95 цен своей категории (организация × категория-5),
--    cap 1 — та же нормировка «текущий к максимуму без выбросов»
drop table if exists #prod_price_rank;

;with t as
(
select distinct
		ID_ORGANIZATION
		, ID_CATEGORY_5
		, P95 = percentile_cont(0.95) within group (order by UNIT_PRICE) over (partition by ID_ORGANIZATION, ID_CATEGORY_5)
from #prod_price
where UNIT_PRICE is not null
)
select	p.ID_ORGANIZATION
		, p.ID_CATEGORY_5
		, p.ID_PRODUCT
		, PRICE_RANK_P = iif(t.P95 > 0, iif(p.UNIT_PRICE / t.P95 > 1, 1, p.UNIT_PRICE / t.P95), 0)
into #prod_price_rank
from #prod_price as p
		join t
		on p.ID_ORGANIZATION = t.ID_ORGANIZATION
		and p.ID_CATEGORY_5 = t.ID_CATEGORY_5
where p.UNIT_PRICE is not null;


-- 7. Ценовой профиль клиента = средний ценовой перцентиль купленных товаров
--    (по строкам покупок за окно — частые покупки весят больше)
drop table if exists #client_price;

select	l.ID_ORGANIZATION
		, l.ID_CONTACT
		, PRICE_P = avg(r.PRICE_RANK_P)
into #client_price
from #line as l
		join #prod_price_rank as r
		on l.ID_PRODUCT = r.ID_PRODUCT
		and l.ID_CATEGORY_5 = r.ID_CATEGORY_5
		and l.ID_ORGANIZATION = r.ID_ORGANIZATION
group by l.ID_ORGANIZATION, l.ID_CONTACT;


-- 8. PRICE_INDEX товара = средний ценовой профиль его уникальных покупателей
drop table if exists #price_index;

;with bc as
(
select	ID_ORGANIZATION
		, ID_CATEGORY_5
		, ID_PRODUCT
		, ID_CONTACT
from #line
group by ID_ORGANIZATION, ID_CATEGORY_5, ID_PRODUCT, ID_CONTACT
)
select	ID_COMPANY = @idc
		, bc.ID_ORGANIZATION
		, bc.ID_CATEGORY_5
		, bc.ID_PRODUCT
		, PRICE_INDEX = avg(p.PRICE_P)
		, CONTACTS = count(distinct bc.ID_CONTACT)
into #price_index
from bc
		join #client_price as p
		on bc.ID_CONTACT = p.ID_CONTACT
		and bc.ID_ORGANIZATION = p.ID_ORGANIZATION
group by bc.ID_ORGANIZATION, bc.ID_CATEGORY_5, bc.ID_PRODUCT;


-- =============================================================================
-- ПУБЛИКАЦИЯ (создание таблиц при первом запуске + полный пересчёт)
-- =============================================================================

-- 9. Публикация: полная очистка старого скора + вставка нового (одна таблица,
--    PRICE_INDEX — колонкой, left join: товар без ценового индекса не теряется)
begin tran;
	truncate table dbo.I_PRODUCT_SCORE;		-- очистка текущего скора (старая методология)

	insert into dbo.I_PRODUCT_SCORE
		(ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5, ID_PRODUCT,
		 FREQ_SCORE, LOYALTY_SCORE, CHECK_SCORE, CONTACTS, CHECKS, PRICE_INDEX,
		 SHOPS_2W, CONTACTS_2W, CHECKS_2W, PRICE_2W, DISCOUNT_2W)
	select	@idc
			, a.ID_ORGANIZATION
			, a.ID_CATEGORY_5
			, a.ID_PRODUCT
			, b.FREQ_SCORE
			, b.LOYALTY_SCORE
			, a.CHECK_SCORE
			, b.CONTACTS
			, a.CHECKS
			, x.PRICE_INDEX
			, f.SHOPS_2W
			, f.CONTACTS_2W
			, f.CHECKS_2W
			, f.PRICE_2W
			, f.DISCOUNT_2W
	from #by_check as a
			join #by_client as b
			on a.ID_ORGANIZATION = b.ID_ORGANIZATION
			and a.ID_PRODUCT = b.ID_PRODUCT
			and a.ID_CATEGORY_5 = b.ID_CATEGORY_5
			join #active as f
			on a.ID_ORGANIZATION = f.ID_ORGANIZATION
			and a.ID_PRODUCT = f.ID_PRODUCT
			left join #price_index as x
			on a.ID_ORGANIZATION = x.ID_ORGANIZATION
			and a.ID_PRODUCT = x.ID_PRODUCT
			and a.ID_CATEGORY_5 = x.ID_CATEGORY_5;
commit;


-- 10. Контроль прогона (в лог джоба)
select	[Таблица] = 'I_PRODUCT_SCORE'
		, [Строк] = count(*)
		, [С ценовым индексом] = sum(iif(PRICE_INDEX is not null, 1, 0))
		, [Окно с] = @d_start, [по] = @d_end
from dbo.I_PRODUCT_SCORE where ID_COMPANY = @idc;


-- 11. Выгрузки для модели подбора — сохранить в data/I_PRODUCT_SCORE_NEW.xlsx:
--     результат 11.1 -> лист «I_PRODUCT_SCORE», результат 11.2 -> лист «выгрузка»

-- 11.1 Скоры целиком
select *
from dbo.I_PRODUCT_SCORE (nolock)
where ID_COMPANY = @idc
order by ID_ORGANIZATION, FREQ_SCORE desc;

-- 11.2 Маппинг external + названия (категория и товар)
select distinct
		p.ID_PRODUCT
		, p.ID_PRODUCT_EXTERNAL
		, p.ID_CATEGORY_5
		, p.ID_CATEGORY_5_ext
		, p.I_CATEGORY_5
		, PRODUCT_NAME
from I_PRODUCT as p (nolock)
		join dbo.I_PRODUCT_SCORE as s (nolock)
		on p.ID_PRODUCT = s.ID_PRODUCT
where p.ID_COMPANY = @idc;
