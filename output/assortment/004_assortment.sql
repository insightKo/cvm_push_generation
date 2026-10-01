-- Справочник ассортимента ДИКСИ в базе портала: SKU, иерархия категорий и метрики по городам.
--
-- Источник — выгрузка «ассортимент» из МСИ (data/ассортимент.xlsx, 14 113 SKU × 3 города).
-- Пока у портала нет прямого доступа к mci_model, справочник грузится файлом; структура повторяет
-- выгрузку один в один, ничего не досчитываем сверх агрегатов по городам.
--
-- ПОМЕТКА НА ДАЛЬШЕ (решение Елены 09.09.2026): файловая загрузка — временная.
-- Постоянный источник ассортимента — таблицы МСИ I_PRODUCT и I_ASSORTMANF: оттуда берём и SKU,
-- и родные названия категорий всех уровней. Как появится доступ — переводим наполнение
-- этих таблиц на них и файловую сборку выключаем.
--
-- Уровни категорий лежат одной таблицей. Коды всех трёх уровней есть в выгрузке, а имена собраны
-- по кусочкам: cat5 — 1011 из 1011, cat4 — 284 из 336 (I_PRODUCT.I_CATEGORY_4), cat3 — 47 из 77
-- (имя cat3 стоит в отчётах рядом с именем cat4, код cat3 получает имя голосованием своих детей).
-- Остальное остаётся NULL — догадками не заполняем, эти имена придут из I_PRODUCT / I_ASSORTMANF.
-- Откуда взято имя — в name_source, спорные случаи — в has_name_conflict.

create schema if not exists cvm;

-- ---------- категории: три уровня одной таблицей ----------
create table if not exists cvm.category (
  code               text primary key,              -- внешний ext-код категории (стабильный ключ МСИ)
  level              smallint not null check (level in (3, 4, 5)),
  parent_code        text references cvm.category(code) deferrable initially deferred,
  name               text,                          -- NULL = имени в источниках нет (весь cat3, часть cat4)
  name_source        text,                          -- откуда взято имя
  has_name_conflict  boolean not null default false, -- у кода встречались разные названия — взято частотное
  sku_cnt            integer not null default 0,    -- товаров в выгрузке под этим кодом
  loaded_at          timestamptz not null default now()
);

create index if not exists category_parent_idx on cvm.category (parent_code);
create index if not exists category_level_idx on cvm.category (level);

-- ---------- товары ----------
create table if not exists cvm.product (
  sku            text primary key,                  -- ID_PRODUCT_EXTERNAL как в выгрузке, с суффиксом _0
  sku_code       text not null,                     -- он же без _0 — в этом виде код ходит по акциям
  id_product     integer,                           -- внутренний ID МСИ, где известен (12 134 из 14 113)
  product_name   text not null,
  cat5_code      text references cvm.category(code) deferrable initially deferred,
  cat4_code      text references cvm.category(code) deferrable initially deferred,
  cat3_code      text references cvm.category(code) deferrable initially deferred,
  shops_total    integer,                           -- сумма COUNT_SHOPS по городам — представленность
  checks_total   bigint,                            -- сумма COUNT_CHECK по городам
  cities         smallint,                          -- в скольких городах выгрузки встречается SKU
  avg_price      numeric(14, 4),                    -- AVG_PRICE, взвешенная по COUNT_CHECK
  loaded_at      timestamptz not null default now()
);

create index if not exists product_cat5_idx on cvm.product (cat5_code);
create index if not exists product_cat4_idx on cvm.product (cat4_code);
create index if not exists product_id_product_idx on cvm.product (id_product);
create index if not exists product_sku_code_idx on cvm.product (sku_code);

-- ---------- метрики SKU × город ----------
-- Города выгрузки: «Moscow + MO», «Spb+LO», «OTHER». Колонки — как в выгрузке, без переименований.
-- PAIRED_SKU в исходном файле частично испорчен Excel-ом в дату («01,10,5382» вместо 1.105382):
-- разобранное число в paired_sku, исходный текст рядом в paired_sku_raw — оригинал всегда виден.
create table if not exists cvm.product_city (
  sku                   text not null references cvm.product(sku) on delete cascade,
  city                  text not null,
  count_shops           integer,
  count_check           bigint,
  real_sale             numeric,
  purchase_time         numeric,
  avg_price             numeric(14, 4),
  penetration_check     numeric,
  penetration_budget    numeric,
  count_check_weekend   numeric,
  count_check_workday   numeric,
  count_check_festaday  numeric,
  count_check_mon       numeric,
  count_check_tue       numeric,
  count_check_wed       numeric,
  count_check_thu       numeric,
  count_check_fri       numeric,
  count_check_st        numeric,
  count_check_su        numeric,
  paired_budget         numeric,
  paired_sku            numeric,
  paired_sku_raw        text,                        -- исходный текст, если значение восстановлено из даты
  loaded_at             timestamptz not null default now(),
  primary key (sku, city)
);

create index if not exists product_city_city_idx on cvm.product_city (city);

-- ---------- витрина: товар с именами категорий ----------
create or replace view cvm.v_assortment as
select p.sku,
       p.sku_code,
       p.id_product,
       p.product_name,
       p.cat5_code, c5.name as cat5_name,
       p.cat4_code, c4.name as cat4_name,
       p.cat3_code,
       p.shops_total,
       p.checks_total,
       p.avg_price
  from cvm.product p
  left join cvm.category c5 on c5.code = p.cat5_code
  left join cvm.category c4 on c4.code = p.cat4_code;

comment on table cvm.category is 'Категории cat3/cat4/cat5 по ext-кодам; дальше имена берём из I_PRODUCT / I_ASSORTMANF';
comment on table cvm.product is 'Ассортимент ДИКСИ из файловой выгрузки; постоянный источник — I_PRODUCT и I_ASSORTMANF';
comment on table cvm.product_city is 'Метрики SKU в разрезе городов выгрузки';
