-- ============================================================================================
-- CVM_PRICE_INDEX_MARTS — новые товарные и клиентские витрины на нашем справочнике категорий:
--   продажи товара, знаменатели магазина, полка (место товара), ценовой индекс клиента, ценовой сегмент,
--   индекс лояльности, уникальность корзины, цена SKU, сопокупки. Методика — output/ТЗ_показатели_на_справочнике_
--   категорий_CVM_2026-10-19.txt (разделы 0–6, номера формул в комментариях — оттуда).
-- Запуск: обменник (CVM_API_nightly) или SSMS целиком (F5). Создаёт таблицы и процедуры и СРАЗУ запускает пересчёт
--   всей истории разовым заданием SQL Agent «CVM_PRICE_MARTS_HISTORY» (неделя за неделей от первой недели чеков, потом
--   месяцы). Прерванный пересчёт — запустить файл ещё раз: посчитанные недели и месяцы пропускаются (журнал I_CVM_PM_LOG).
-- Старые витрины (I_ASSORT_*, I_SHOP_*, I_CATEGORY_*_FOR, I_CONT_ASSORT_*, I_CONTACT_PRICE_SEGMENT, I_CH_INDEXES)
--   НЕ трогаются; потребители переключаются отдельно, по ОК.
--
-- Действия по объектам:
--   dbo.I_CVM_PM_LOG             — создаётся; журнал расчёта: период, время, строки, ошибка.
--   dbo.I_CVM_FORMAT_COEF        — создаётся ПУСТОЙ; коэффициент формата упаковки k(p) (ТЗ 2.1 п. 2). Пока пусто,
--                                  k = 1: поправки на формат нет, её добавит проверка 3 → пересчёт истории.
--   dbo.I_CVM_SEGMENT_BOUNDS     — создаётся; границы трёх ценовых сегментов по годам (терцили, ТЗ 2.2).
--   dbo.I_CVM_PRODUCT_WEEK       — создаётся; товар × магазин × неделя × канал (ТЗ 3.1–3.11, 3.13).
--   dbo.I_CVM_SHOP_WEEK          — создаётся; магазин × неделя × канал (ТЗ 4).
--   dbo.I_CVM_SHELF_WEEK         — создаётся; полка: товар × сити-репорт × неделя × канал + сеть (ТЗ 3.12, 2.1).
--   dbo.I_CVM_CLIENT_DAY         — создаётся; рабочая: клиент × день, хранятся последние 12 недель (окно 56 дней).
--   dbo.I_CVM_CLIENT_WEEK        — создаётся; клиент × неделя покупки × канал: индексы, сегмент, цена SKU (ТЗ 2).
--   dbo.I_CVM_PRODUCT_MONTH      — создаётся; товар × магазин × месяц × канал (узкая месячная, ТЗ 3).
--   dbo.I_CVM_SHOP_MONTH         — создаётся; магазин × месяц × канал.
--   dbo.I_CVM_PRODUCT_CITY_MONTH — создаётся; уникальные покупатели товара × сити-репорт × месяц × канал + сеть.
--   dbo.I_CVM_BASKET_PAIR_WEEK, dbo.I_CVM_BASKET_CAT_WEEK — создаются; рабочие для сопокупок, последние 8 недель.
--   dbo.I_CVM_BASKET_LINK        — создаётся; сопокупки «товар × категория-сосед» за последние 8 недель (ТЗ 6).
--   dbo.cvm_pm_period            — процедура: расчёт одной недели или одного месяца (обе организации).
--   dbo.cvm_pm_segment_bounds    — процедура: границы сегментов за год и проставление сегментов.
--   dbo.cvm_pm_basket_window     — процедура: сопокупки за последние 8 закрытых недель.
--   dbo.cvm_price_marts          — процедура-оркестратор: @mode = 'history' (вся история) / 'nightly' (окно 10 дней).
--   Задание SQL Agent CVM_PRICE_MARTS_HISTORY — создаётся и запускается разово (вся история), после успеха удаляет себя.
--   I_CHECK, I_CHECKHEADER, I_SHOP, I_LFL_SHOPS, справочник категорий — только чтение (nolock).
--   Шаг в «MCI Regular update» НЕ ставится — после истории, отдельным скриптом.
--
-- 31.10.2026 — пересчёт с разметкой единиц (решение Елены «полный пересчёт»):
--   стоп в начале файла, если разметка единиц (CVM_PRODUCT_UNIT_LOAD.sql) ещё не залита — обменник повторит позже;
--   раздел 6 — ОДИН РАЗ очищает таблицы витрин, посчитанные 24–28.10 «за упаковку» (разметки не было), и помечает
--   записи журнала того расчёта STATUS = 'old' (остаются для истории, посчитанными не считаются) — задание раздела 7
--   считает всю историю заново. Читателей у витрин нет (только процедуры этого файла). Повтор файла ничего не очищает.
--
-- Решения, по которым написан скрипт (рекомендации ТЗ; поменять — пересчитать историю):
--   место на полке приведено к ширине m = (r − 1) / (n − 1) (вопрос 1); сегменты — терцили прошлого года (13);
--   SKU чека для цены SKU — I_CHECKHEADER.SKU, ТО до скидки = COST_DISCOUNT + DISCOUNT шапки, как в отчёте эффекта (14);
--   сопокупки — товар × категория-сосед по сети, окно 8 недель (4); месячные уникальные — грань сити-репорт (5).
-- Минимумов голосов нет: товар встаёт на полку с 1 голоса, индекс клиента — с 1 голоса; числа голосов хранятся
--   (VOTES, VOTES_INDEX), минимум из данных накладывает читатель — порогов «от руки» в расчёте нет.
-- Товар без разметки единиц (I_CVM_PRODUCT_UNIT пуст или нет строки) сравнивается «за единицу продажи» с такими же
--   товарами своей категории (UNIT_BASIS = 2; решение 11.10 «без единицы — за штуку, сравнение с такими же»);
--   с разметкой — за кг / л / шт полки (UNIT_BASIS = 1). После заполнения разметки — пересчитать историю.
--   Объём из разметки берётся, только если единица названия = единице полки (UNIT_NAME = UNIT); товары в другой
--   единице (молоко «900Г» на полке «л» и т.п.) — за единицу продажи, пока модель не оценит множитель кг↔л (24.10).
--   Колонка UNIT_NAME появляется скриптом CVM_PRODUCT_UNIT_LOAD.sql — он должен выполниться раньше этого.
-- Весовой товар недели: дробное количество хотя бы в одной покупке недели или пометка «весовой» в разметке;
--   у него цена = оплата / кг (на полке «кг»).
-- Год к году на полке — только офлайн (LFL — офлайн-магазины): места среди товаров, продававшихся на LFL-магазинах
--   в обе недели (w и w − 52) с одной основой цены в обе недели, список LFL — месяц недели w.
-- Ожидание «MCI Regular update» — только для недель внутри окна перезаливки (последние 10 дней), не дольше 2 ч:
--   иначе неделя может прочитаться посреди перезаливки чеков. Будущий шаг самого задания — с @wait_job = 0.
-- Время (замер 24–28.10): неделя 10–26 мин (недавние дольше), месяц ≈ 24 мин; вся история ≈ 3,8 суток (недели 58 ч, месяцы 32 ч).
-- ============================================================================================
use mci_model;
go
set noexec off;
set nocount on;
go

-- 0. Разметка единиц сборки 31.10.2026 должна быть залита раньше (в обменнике 1_CVM_PRODUCT_UNIT_LOAD.sql, этот файл —
--    2_…: раннер идёт по алфавиту); нет — ничего не делать
if col_length(N'dbo.I_CVM_PRODUCT_UNIT', N'UNIT_NAME') is null
   or not exists (select 1 from dbo.I_CVM_PRODUCT_UNIT (nolock) where SOURCE = 1 and DATE_UPDATE >= '20261031')
begin
    raiserror(N'СТОП: разметка единиц (CVM_PRODUCT_UNIT_LOAD.sql) ещё не залита — ничего не делаю, обменник повторит.', 16, 1);
    set noexec on;
end
go

-- ============================== 1. ТАБЛИЦЫ ==============================
if object_id(N'dbo.I_CVM_PM_LOG') is null
create table dbo.I_CVM_PM_LOG (
      ID            int identity(1, 1) not null constraint PK_I_CVM_PM_LOG primary key clustered
    , KIND          char(1)        not null      -- W неделя, M месяц, B границы сегментов, L сопокупки
    , PERIOD_START  date           not null
    , START_TIME    datetime2(0)   not null
    , FINISH_TIME   datetime2(0)   null
    , STATUS        varchar(10)    not null      -- run / ok / error
    , ROWS_INFO     nvarchar(400)  null
    , ERROR_TEXT    nvarchar(2000) null
);
go
if object_id(N'dbo.I_CVM_FORMAT_COEF') is null
create table dbo.I_CVM_FORMAT_COEF (
      ID_PRODUCT      bigint  not null
    , ID_ORGANIZATION int     not null
    , BASE_YEAR       int     not null
    , K               float   not null   -- во сколько раз упаковка дороже/дешевле того же товара в обычной для полки
    , constraint PK_I_CVM_FORMAT_COEF primary key clustered (ID_PRODUCT, ID_ORGANIZATION, BASE_YEAR)
);
go
if object_id(N'dbo.I_CVM_SEGMENT_BOUNDS') is null
create table dbo.I_CVM_SEGMENT_BOUNDS (
      [YEAR]          int          not null
    , ID_ORGANIZATION int          not null
    , B1              float        not null   -- индекс ≤ B1 → сегмент 3 (низкий)
    , B2              float        not null   -- индекс ≥ B2 → сегмент 1 (высокий)
    , CLIENTS         int          not null
    , LOAD_DATE       datetime2(0) not null
    , constraint PK_I_CVM_SEGMENT_BOUNDS primary key clustered ([YEAR], ID_ORGANIZATION)
);
go

-- товар × магазин × неделя × канал (ТЗ 3); месячная — тот же состав
if object_id(N'dbo.I_CVM_PRODUCT_WEEK') is null
begin
    create table dbo.I_CVM_PRODUCT_WEEK (
          ID_COMPANY         int            not null
        , ID_ORGANIZATION    int            not null
        , ID_SHOP            int            not null
        , ID_PRODUCT         bigint         not null
        , WEEK_START         date           not null   -- понедельник
        , WEEKNUMBER         int            null       -- номер недели I_CALENDAR
        , UNIT_BASIS         tinyint        not null   -- 1 за кг/л/шт полки, 2 за единицу продажи (нет разметки)
        , IS_WEIGHT          bit            not null   -- весовой в этой неделе
        , TURNOVER           decimal(19, 2) null       -- 3.1 Σ COST_DISCOUNT нетто (с возвратами)
        , TURNOVER_BUY       decimal(19, 2) null       -- 3.2 Σ S(c,p) по голосам
        , CHECKS_BUY         int            not null   -- 3.3 |A(p)| — чеки покупки = голоса
        , CHECKS_RETURN      int            not null   -- 3.4 чеки с q(c,p) < 0
        , BUYERS             int            not null   -- 3.5 клиенты с голосом (уникальны в строке магазина)
        , QTY_NET            decimal(19, 3) null       -- 3.6 Σ QUANTITY нетто
        , QTY_BUY            decimal(19, 3) null       --     Σ q(c,p) по голосам
        , QTY_UNIT           decimal(19, 3) null       --     Σ q(c,p) × V(p) по голосам (только UNIT_BASIS = 1)
        , SUM_UNIT_PRICE     float          null       -- 3.9 Σ u(v) по голосам (до k); цена = сумма / голоса
        , SUM_UNIT_PRICE_SQ  float          null       --     Σ u(v)² — разброс цены
        , VOTES_FRAC         int            not null   --     голоса с дробным количеством
        , VOTES_DISCOUNT     int            not null   --     голоса с кассовой скидкой
        , DISCOUNT           decimal(19, 2) null       -- 3.7 Σ (COST − COST_DISCOUNT)
        , REAL_DISCOUNT      decimal(19, 2) null       -- 3.8 Σ REAL_DISCOUNT (от регулярной цены)
        , SUM_HOURS          float          null       -- 3.10 Σ часа чека по чекам покупки
        , CHECKS_MON int not null, CHECKS_TUE int not null, CHECKS_WED int not null, CHECKS_THU int not null
        , CHECKS_FRI int not null, CHECKS_SAT int not null, CHECKS_SUN int not null      -- 3.11
        , CHECKS_WORKDAY int not null, CHECKS_WEEKEND int not null, CHECKS_HOLIDAY int not null
        , BASKET_REST_SUM    decimal(19, 2) null       -- 3.13 Σ (S(c) − S(c,p)) по чекам покупки
        , BASKET_SKU_REST    int            null       --      Σ (SKU(c) − 1)
        , LOAD_DATE          datetime2(0)   not null
    );
    create clustered columnstore index cci_I_CVM_PRODUCT_WEEK on dbo.I_CVM_PRODUCT_WEEK;
end
go
if object_id(N'dbo.I_CVM_PRODUCT_MONTH') is null
begin
    create table dbo.I_CVM_PRODUCT_MONTH (
          ID_COMPANY int not null, ID_ORGANIZATION int not null, ID_SHOP int not null, ID_PRODUCT bigint not null
        , YEAR_MONTH int not null
        , UNIT_BASIS tinyint not null, IS_WEIGHT bit not null
        , TURNOVER decimal(19, 2) null, TURNOVER_BUY decimal(19, 2) null
        , CHECKS_BUY int not null, CHECKS_RETURN int not null, BUYERS int not null
        , QTY_NET decimal(19, 3) null, QTY_BUY decimal(19, 3) null, QTY_UNIT decimal(19, 3) null
        , SUM_UNIT_PRICE float null, SUM_UNIT_PRICE_SQ float null, VOTES_FRAC int not null, VOTES_DISCOUNT int not null
        , DISCOUNT decimal(19, 2) null, REAL_DISCOUNT decimal(19, 2) null, SUM_HOURS float null
        , CHECKS_MON int not null, CHECKS_TUE int not null, CHECKS_WED int not null, CHECKS_THU int not null
        , CHECKS_FRI int not null, CHECKS_SAT int not null, CHECKS_SUN int not null
        , CHECKS_WORKDAY int not null, CHECKS_WEEKEND int not null, CHECKS_HOLIDAY int not null
        , BASKET_REST_SUM decimal(19, 2) null, BASKET_SKU_REST int null
        , LOAD_DATE datetime2(0) not null
    );
    create clustered columnstore index cci_I_CVM_PRODUCT_MONTH on dbo.I_CVM_PRODUCT_MONTH;
end
go
-- магазин × период × канал (ТЗ 4)
if object_id(N'dbo.I_CVM_SHOP_WEEK') is null
begin
    create table dbo.I_CVM_SHOP_WEEK (
          ID_COMPANY      int            not null
        , ID_ORGANIZATION int            not null
        , ID_SHOP         int            not null
        , WEEK_START      date           not null
        , WEEKNUMBER      int            null
        , CHECKS_BUY      int            not null   -- 4.1 C⁺ — чеки покупки
        , CHECKS_RETURN   int            not null   -- 4.2 чеки без голосов, с возвратом
        , CLIENTS_BUY     int            not null   -- 4.3 клиенты с чеком покупки
        , TURNOVER        decimal(19, 2) null       -- 4.4 Σ COST_DISCOUNT нетто всех карточных чеков
        , TURNOVER_BUY    decimal(19, 2) null       --     Σ S(c) чеков покупки
        , SKU_SUM         int            null       -- 4.5 Σ SKU(c) чеков покупки (разные товары с голосом)
        , SKU_HDR_SUM     int            null       --     Σ I_CHECKHEADER.SKU чеков покупки
        , SUM_HOURS       float          null       --     Σ часа чека по чекам покупки
        , CHECKS_WORKDAY int not null, CHECKS_WEEKEND int not null, CHECKS_HOLIDAY int not null
        , LOAD_DATE       datetime2(0)   not null
    );
    create clustered columnstore index cci_I_CVM_SHOP_WEEK on dbo.I_CVM_SHOP_WEEK;
end
go
if object_id(N'dbo.I_CVM_SHOP_MONTH') is null
begin
    create table dbo.I_CVM_SHOP_MONTH (
          ID_COMPANY int not null, ID_ORGANIZATION int not null, ID_SHOP int not null, YEAR_MONTH int not null
        , CHECKS_BUY int not null, CHECKS_RETURN int not null, CLIENTS_BUY int not null
        , TURNOVER decimal(19, 2) null, TURNOVER_BUY decimal(19, 2) null
        , SKU_SUM int null, SKU_HDR_SUM int null, SUM_HOURS float null
        , CHECKS_WORKDAY int not null, CHECKS_WEEKEND int not null, CHECKS_HOLIDAY int not null
        , LOAD_DATE datetime2(0) not null
    );
    create clustered columnstore index cci_I_CVM_SHOP_MONTH on dbo.I_CVM_SHOP_MONTH;
end
go
-- полка (ТЗ 3.12, 2.1): ID_CITY_REPORT = 0 — вся сеть канала, −1 — не определён
if object_id(N'dbo.I_CVM_SHELF_WEEK') is null
begin
    create table dbo.I_CVM_SHELF_WEEK (
          ID_ORGANIZATION  int          not null
        , ID_CITY_REPORT   int          not null
        , WEEK_START       date         not null
        , ID_PRODUCT       bigint       not null
        , ID_CVM_CATEGORY  int          null       -- категория на момент расчёта
        , UNIT_BASIS       tinyint      not null
        , IN_SCALE         bit          not null   -- категория со шкалой (не табак, не «без шкалы», сопоставлена)
        , VOTES            int          not null
        , BUYERS           int          not null   -- уникальные покупатели в сити-репорте / сети
        , PRICE            float        null       -- P = Σ u / k / голоса (2.1 п. 3)
        , PRICE_SQ_SUM     float        null       -- Σ u² (до k) — разброс цены товара
        , SHELF_RANK       float        null       -- r: 1 = самая низкая цена, равные цены — средний ранг
        , SHELF_WIDTH      int          null       -- n: товаров на полке (категория × основа цены)
        , SHELF_PLACE      float        null       -- m = (r − 1) / (n − 1); пусто при n = 1
        , CAT_WIDTH        int          not null   -- товаров категории в ячейке (для уникальности корзины)
        , PRICE_LFL        float        null       -- цена на LFL-магазинах (офлайн)
        , RANK_YOY         float        null       -- место среди общих с w − 52 товаров, неделя w
        , RANK_YOY_PREV    float        null       -- место тех же товаров в w − 52
        , WIDTH_YOY        int          null       -- число общих товаров
        , LOAD_DATE        datetime2(0) not null
    );
    create clustered columnstore index cci_I_CVM_SHELF_WEEK on dbo.I_CVM_SHELF_WEEK;
end
go
-- клиент × день (рабочая, последние 12 недель): суммы для окна 56 дней
if object_id(N'dbo.I_CVM_CLIENT_DAY') is null
begin
    create table dbo.I_CVM_CLIENT_DAY (
          ID_CONTACT      int            not null
        , ID_ORGANIZATION int            not null
        , DATA            date           not null
        , VOTES_ALL       int            not null   -- все голоса
        , M_SUM           float          null       -- Σ m(v) по голосам с местом
        , M_N             int            not null   -- голоса с местом (в индекс)
        , SC_N            int            not null   -- голоса категорий со шкалой
        , PEN_SUM         float          null       -- Σ pen(v)
        , SING_SUM        float          null       -- Σ 1 / ширина категории
        , CHECKS_BUY      int            not null
        , TURNOVER        decimal(19, 2) null       -- Σ S(c) чеков покупки (нетто)
        , TURNOVER_FULL   decimal(19, 2) null       -- Σ (COST_DISCOUNT + DISCOUNT) шапки — ТО до скидки
        , SKU_HDR         int            null       -- Σ I_CHECKHEADER.SKU
        , SKU_VOTES       int            null       -- Σ SKU(c)
        , DISCOUNT        decimal(19, 2) null
        , REAL_DISCOUNT   decimal(19, 2) null
    );
    create clustered columnstore index cci_I_CVM_CLIENT_DAY on dbo.I_CVM_CLIENT_DAY;
end
go
-- клиент × неделя покупки × канал (ТЗ 2): строка — за неделю, где у клиента была покупка
if object_id(N'dbo.I_CVM_CLIENT_WEEK') is null
begin
    create table dbo.I_CVM_CLIENT_WEEK (
          ID_CONTACT        int            not null
        , ID_ORGANIZATION   int            not null
        , WEEK_START        date           not null
        , WEEKNUMBER        int            null
        , LAST_DATE         date           not null   -- последняя покупка клиента в неделе
        , WIN_FROM          date           not null   -- LAST_DATE − 55 дней
        -- неделя
        , CHECKS_BUY        int            not null
        , TURNOVER          decimal(19, 2) null
        , TURNOVER_FULL     decimal(19, 2) null
        , SKU_HDR           int            null
        , SKU_VOTES         int            null
        , DISCOUNT          decimal(19, 2) null
        , REAL_DISCOUNT     decimal(19, 2) null
        -- окно 56 дней
        , W_CHECKS_BUY      int            not null
        , W_TURNOVER        decimal(19, 2) null
        , W_TURNOVER_FULL   decimal(19, 2) null
        , W_SKU_HDR         int            null
        , W_SKU_VOTES       int            null
        , VOTES_WIN         int            not null   -- все голоса окна
        , VOTES_SCALE       int            not null   -- голоса категорий со шкалой
        , VOTES_INDEX       int            not null   -- голоса с местом на полке
        , PRICE_INDEX       float          null       -- 2.1 PI = Σ m / голоса с местом
        , PRICE_SEGMENT     tinyint        null       -- 2.2: 1 высокий, 2 средний, 3 низкий
        , LOYALTY_INDEX     float          null       -- 2.3 LI = Σ pen / голоса со шкалой
        , UNIQUE_BASKET     float          null       -- 2.4 UB = Σ 1/n / голоса со шкалой
        , PRICE_SKU         decimal(19, 4) null       -- 2.6 неделя: ТО до скидки / SKU шапки
        , W_PRICE_SKU       decimal(19, 4) null       --     окно
        , LOAD_DATE         datetime2(0)   not null
    );
    create clustered columnstore index cci_I_CVM_CLIENT_WEEK on dbo.I_CVM_CLIENT_WEEK;
end
go
if object_id(N'dbo.I_CVM_PRODUCT_CITY_MONTH') is null
begin
    create table dbo.I_CVM_PRODUCT_CITY_MONTH (
          ID_ORGANIZATION int not null, ID_CITY_REPORT int not null   -- 0 = вся сеть
        , YEAR_MONTH int not null, ID_PRODUCT bigint not null
        , VOTES int not null, BUYERS int not null
        , LOAD_DATE datetime2(0) not null
    );
    create clustered columnstore index cci_I_CVM_PRODUCT_CITY_MONTH on dbo.I_CVM_PRODUCT_CITY_MONTH;
end
go
-- сопокупки (ТЗ 6)
if object_id(N'dbo.I_CVM_BASKET_PAIR_WEEK') is null
begin
    create table dbo.I_CVM_BASKET_PAIR_WEEK (
          ID_ORGANIZATION int not null, WEEK_START date not null
        , ID_PRODUCT bigint not null, ID_CVM_CATEGORY int not null   -- якорь × категория-сосед
        , JOINT_CHECKS int not null                                  -- чеки якоря с соседом
    );
    create clustered columnstore index cci_I_CVM_BASKET_PAIR_WEEK on dbo.I_CVM_BASKET_PAIR_WEEK;
end
go
if object_id(N'dbo.I_CVM_BASKET_CAT_WEEK') is null
create table dbo.I_CVM_BASKET_CAT_WEEK (
      ID_ORGANIZATION int not null, WEEK_START date not null
    , ID_CVM_CATEGORY int not null       -- 0 = все чеки покупки
    , CHECKS int not null
    , constraint PK_I_CVM_BASKET_CAT_WEEK primary key clustered (ID_ORGANIZATION, WEEK_START, ID_CVM_CATEGORY)
);
go
if object_id(N'dbo.I_CVM_BASKET_LINK') is null
begin
    create table dbo.I_CVM_BASKET_LINK (
          ID_ORGANIZATION  int          not null
        , WIN_FROM         date         not null
        , WIN_TO           date         not null
        , ID_PRODUCT       bigint       not null
        , ID_CVM_CATEGORY  int          not null
        , ANCHOR_CHECKS    int          not null   -- |A(p)|
        , JOINT_CHECKS     int          not null   -- Σ has(c,g) по A(p)
        , J                float        not null   -- совместная частота
        , ALL_CHECKS       int          not null   -- |C(T)|
        , NEIGHBOR_CHECKS  int          not null   -- Σ has(c,g) по C(T)
        , B                float        not null   -- базовая частота
        , LIFT             float        not null   -- подъём J / B
        , LOAD_DATE        datetime2(0) not null
    );
    create clustered columnstore index cci_I_CVM_BASKET_LINK on dbo.I_CVM_BASKET_LINK;
end
go

-- ============================== 2. РАСЧЁТ ОДНОЙ НЕДЕЛИ / ОДНОГО МЕСЯЦА ==============================
-- Временные таблицы — #pm_*: процедура видит временные таблицы вызвавшей сессии, общие имена задели бы рабочие.
-- Чеки читаются кусками по 7 дней (чек живёт в одном дне) прямо в колоночные временные таблицы.
create or alter procedure dbo.cvm_pm_period
      @kind        char(1)        -- 'W' неделя (понедельник @d1), 'M' месяц (первое число @d1)
    , @d1          date
    , @with_basket bit = 0        -- неделя: считать пары сопокупок (только последние 8 недель)
as
begin
    set nocount on;
    set xact_abort on;

    declare @d2 date = case when @kind = 'W' then dateadd(day, 6, @d1) else eomonth(@d1) end;
    declare @ym int = year(@d1) * 100 + month(@d1);
    declare @wn int = (select max(WEEKNUMBER) from dbo.I_CALENDAR (nolock) where [DATE] = @d1);
    declare @now datetime2(0) = sysdatetime();
    declare @log int, @c1 date, @c2 date;
    declare @w_prev date = dateadd(week, -52, @d1);
    declare @lfl_ym int;

    insert into dbo.I_CVM_PM_LOG (KIND, PERIOD_START, START_TIME, STATUS) values (@kind, @d1, @now, 'run');
    set @log = scope_identity();

    begin try
        -- магазины, сити-репорт; e-com — сити-репорт города заказа по офлайн-магазинам (город с двумя — не сопоставлен)
        drop table if exists #pm_shop;
        select ID_SHOP, ID_ORGANIZATION = max(ID_ORGANIZATION), ID_CITY_REPORT = max(ID_CITY_REPORT)
        into #pm_shop
        from dbo.I_SHOP (nolock)
        where ID_COMPANY = 1
        group by ID_SHOP;
        create unique clustered index ix_pm_shop on #pm_shop (ID_SHOP);

        drop table if exists #pm_city;
        select ID_CITY, ID_CITY_REPORT = min(ID_CITY_REPORT)
        into #pm_city
        from dbo.I_SHOP (nolock)
        where ID_COMPANY = 1 and ID_ORGANIZATION = 1 and ID_CITY is not null and ID_CITY_REPORT is not null
        group by ID_CITY
        having min(ID_CITY_REPORT) = max(ID_CITY_REPORT);
        create unique clustered index ix_pm_city on #pm_city (ID_CITY);

        -- ---------- голоса: чек × товар (ТЗ 0) — единственное чтение I_CHECK ----------
        drop table if exists #pm_cp;
        create table #pm_cp (
              ID_CHECK bigint not null, ID_PRODUCT bigint not null, ID_ORGANIZATION int not null
            , ID_SHOP int null, ID_CITY_REPORT int not null, ID_CONTACT int null, DATA date null
            , DAYOFWEEK int null, DAYTYPE int null, HOURS decimal(19, 6) null
            , Q decimal(38, 6) null, S decimal(38, 4) null, DISC decimal(38, 4) null, REAL_DISC decimal(38, 4) null
            , IS_FRAC int not null
            , index cci_pm_cp clustered columnstore);

        set @c1 = @d1;
        while @c1 <= @d2
        begin
            set @c2 = case when dateadd(day, 6, @c1) < @d2 then dateadd(day, 6, @c1) else @d2 end;
            insert into #pm_cp with (tablock) (ID_CHECK, ID_PRODUCT, ID_ORGANIZATION, ID_SHOP, ID_CITY_REPORT, ID_CONTACT, DATA
                , DAYOFWEEK, DAYTYPE, HOURS, Q, S, DISC, REAL_DISC, IS_FRAC)
            select  a.ID_CHECK
                  , a.ID_PRODUCT
                  , a.ID_ORGANIZATION
                  , max(a.ID_SHOP)
                  , max(isnull(case when a.ID_ORGANIZATION = 1 then s.ID_CITY_REPORT else c.ID_CITY_REPORT end, -1))
                  , max(a.ID_CONTACT)
                  , max(a.DATA)
                  , max(a.DAYOFWEEK)
                  , max(a.DAYTYPE)
                  , max(a.CLOCK + a.[TIME] / 60.0)
                  , sum(a.QUANTITY)
                  , sum(a.COST_DISCOUNT)
                  , sum(a.DISCOUNT)
                  , sum(a.REAL_DISCOUNT)
                  , max(case when a.QUANTITY > 0 and a.QUANTITY <> round(a.QUANTITY, 0) then 1 else 0 end)
            from dbo.I_CHECK as a (nolock)
                 left join #pm_shop as s on s.ID_SHOP = a.ID_SHOP
                 left join #pm_city as c on c.ID_CITY = a.ID_CITY
            where a.ID_COMPANY = 1 and a.ID_CONTACT <> 0 and a.ID_ORGANIZATION in (1, 3)
              and a.DATA >= @c1 and a.DATA <= @c2
              and a.ID_PRODUCT > 0
            group by a.ID_CHECK, a.ID_PRODUCT, a.ID_ORGANIZATION
            option (recompile);
            set @c1 = dateadd(day, 1, @c2);
        end

        -- ---------- чеки: S(c), SKU(c), чек покупки / возвратный; шапка — ТО до скидки и SKU ----------
        drop table if exists #pm_c;
        create table #pm_c (
              ID_CHECK bigint not null, ID_ORGANIZATION int not null, ID_SHOP int null, ID_CITY_REPORT int not null
            , ID_CONTACT int null, DATA date null, DAYOFWEEK int null, DAYTYPE int null, HOURS decimal(19, 6) null
            , S_CHECK decimal(38, 4) null, VOTES int not null, HAS_RET int not null
            , DISC decimal(38, 4) null, REAL_DISC decimal(38, 4) null
            , IS_BUY bit not null, IS_RET_CHECK bit not null
            , TO_FULL decimal(38, 4) null, SKU_HDR int null
            , index cci_pm_c clustered columnstore);

        set @c1 = @d1;
        while @c1 <= @d2
        begin
            set @c2 = case when dateadd(day, 6, @c1) < @d2 then dateadd(day, 6, @c1) else @d2 end;
            insert into #pm_c with (tablock) (ID_CHECK, ID_ORGANIZATION, ID_SHOP, ID_CITY_REPORT, ID_CONTACT, DATA, DAYOFWEEK, DAYTYPE
                , HOURS, S_CHECK, VOTES, HAS_RET, DISC, REAL_DISC, IS_BUY, IS_RET_CHECK, TO_FULL, SKU_HDR)
            select  g.ID_CHECK, g.ID_ORGANIZATION, g.ID_SHOP, g.ID_CITY_REPORT, g.ID_CONTACT, g.DATA, g.DAYOFWEEK, g.DAYTYPE
                  , g.HOURS, g.S_CHECK, g.VOTES, g.HAS_RET, g.DISC, g.REAL_DISC
                  , case when g.VOTES > 0 then 1 else 0 end
                  , case when g.VOTES = 0 and g.HAS_RET = 1 then 1 else 0 end
                  , h.COST_DISCOUNT + h.DISCOUNT
                  , h.SKU
            from (
                select  ID_CHECK
                      , ID_ORGANIZATION = max(ID_ORGANIZATION), ID_SHOP = max(ID_SHOP), ID_CITY_REPORT = max(ID_CITY_REPORT)
                      , ID_CONTACT = max(ID_CONTACT), DATA = max(DATA), DAYOFWEEK = max(DAYOFWEEK), DAYTYPE = max(DAYTYPE)
                      , HOURS = max(HOURS)
                      , S_CHECK = sum(S)
                      , VOTES   = sum(case when Q > 0 then 1 else 0 end)
                      , HAS_RET = max(case when Q < 0 then 1 else 0 end)
                      , DISC = sum(DISC), REAL_DISC = sum(REAL_DISC)
                from #pm_cp
                where DATA >= @c1 and DATA <= @c2
                group by ID_CHECK
            ) as g
            left join (
                -- предагрегат шапки: защита от дублей ID_CHECK
                select  ID_CHECK, ID_ORGANIZATION
                      , COST_DISCOUNT = sum(COST_DISCOUNT), DISCOUNT = sum(DISCOUNT), SKU = sum(SKU)
                from dbo.I_CHECKHEADER (nolock)
                where ID_COMPANY = 1 and ID_ORGANIZATION in (1, 3) and DATA >= @c1 and DATA <= @c2
                group by ID_CHECK, ID_ORGANIZATION
            ) as h on h.ID_CHECK = g.ID_CHECK and h.ID_ORGANIZATION = g.ID_ORGANIZATION
            option (recompile);
            set @c1 = dateadd(day, 1, @c2);
        end

        -- ---------- товары периода: категория, шкала, единица, весовой, основа цены ----------
        drop table if exists #pm_p0;
        select  p.ID_PRODUCT
              , ID_CVM_CATEGORY = pc.ID_CVM_CATEGORY
              , IN_SCALE  = cast(case when pc.HOW > 0 and k.STATUS = 1 and k.NO_SCALE = 0 and pc.ID_CVM_GROUP <> 7
                                      then 1 else 0 end as bit)                   -- группа 7 «Табак» — вне шкалы всегда
              , IS_WEIGHT = cast(case when p.ANY_FRAC = 1 or isnull(u.IS_WEIGHT_NAME, 0) = 1 then 1 else 0 end as bit)
              , V         = case when u.QTY_TOTAL > 0 and u.UNIT_NAME = k.UNIT then u.QTY_TOTAL end   -- объём в единице названия; другая единица — пока за единицу продажи (множитель кг↔л — модель, 23.10)
              , k.UNIT
        into #pm_p0
        from (select ID_PRODUCT, ANY_FRAC = max(IS_FRAC) from #pm_cp group by ID_PRODUCT) as p
             left join dbo.I_CVM_PRODUCT_CATEGORY as pc (nolock) on pc.ID_PRODUCT = p.ID_PRODUCT
             left join dbo.I_CVM_CATEGORY as k (nolock) on k.ID_CVM_CATEGORY = pc.ID_CVM_CATEGORY
             left join dbo.I_CVM_PRODUCT_UNIT as u (nolock) on u.ID_PRODUCT = p.ID_PRODUCT;

        drop table if exists #pm_p;
        select  ID_PRODUCT, ID_CVM_CATEGORY, IN_SCALE, IS_WEIGHT
              -- основа цены: 1 — за единицу полки (весовой на полке «кг» — за кг; фасованный с разметкой — за объём),
              --              2 — за единицу продажи (нет разметки)
              , UNIT_BASIS = cast(case when IS_WEIGHT = 1 and UNIT = N'кг' then 1
                                       when IS_WEIGHT = 0 and V is not null then 1
                                       else 2 end as tinyint)
              , UNIT_MULT  = cast(case when IS_WEIGHT = 0 and V is not null then V else 1 end as float)
        into #pm_p
        from #pm_p0;
        create unique clustered index ix_pm_p on #pm_p (ID_PRODUCT);

        -- ---------- товар × магазин (ТЗ 3) ----------
        drop table if exists #pm_pw;
        select  v.ID_ORGANIZATION, v.ID_SHOP, v.ID_PRODUCT
              , p.UNIT_BASIS, p.IS_WEIGHT
              , TURNOVER          = sum(v.S)
              , TURNOVER_BUY      = sum(case when v.Q > 0 then v.S end)
              , CHECKS_BUY        = sum(case when v.Q > 0 then 1 else 0 end)
              , CHECKS_RETURN     = sum(case when v.Q < 0 then 1 else 0 end)
              , BUYERS            = count(distinct case when v.Q > 0 then v.ID_CONTACT end)
              , QTY_NET           = sum(v.Q)
              , QTY_BUY           = sum(case when v.Q > 0 then v.Q end)
              , QTY_UNIT          = sum(case when v.Q > 0 and p.UNIT_BASIS = 1 then v.Q * p.UNIT_MULT end)
              , SUM_UNIT_PRICE    = sum(case when v.Q > 0 then v.S / nullif(v.Q * p.UNIT_MULT, 0) end)
              , SUM_UNIT_PRICE_SQ = sum(case when v.Q > 0 then square(v.S / nullif(v.Q * p.UNIT_MULT, 0)) end)
              , VOTES_FRAC        = sum(case when v.Q > 0 and v.IS_FRAC = 1 then 1 else 0 end)
              , VOTES_DISCOUNT    = sum(case when v.Q > 0 and v.DISC > 0 then 1 else 0 end)
              , DISCOUNT          = sum(v.DISC)
              , REAL_DISCOUNT     = sum(v.REAL_DISC)
              , SUM_HOURS         = sum(case when v.Q > 0 then v.HOURS end)
              , CHECKS_MON = sum(case when v.Q > 0 and v.DAYOFWEEK = 1 then 1 else 0 end)
              , CHECKS_TUE = sum(case when v.Q > 0 and v.DAYOFWEEK = 2 then 1 else 0 end)
              , CHECKS_WED = sum(case when v.Q > 0 and v.DAYOFWEEK = 3 then 1 else 0 end)
              , CHECKS_THU = sum(case when v.Q > 0 and v.DAYOFWEEK = 4 then 1 else 0 end)
              , CHECKS_FRI = sum(case when v.Q > 0 and v.DAYOFWEEK = 5 then 1 else 0 end)
              , CHECKS_SAT = sum(case when v.Q > 0 and v.DAYOFWEEK = 6 then 1 else 0 end)
              , CHECKS_SUN = sum(case when v.Q > 0 and v.DAYOFWEEK = 7 then 1 else 0 end)
              , CHECKS_WORKDAY = sum(case when v.Q > 0 and v.DAYTYPE = 1 then 1 else 0 end)
              , CHECKS_WEEKEND = sum(case when v.Q > 0 and v.DAYTYPE = 2 then 1 else 0 end)
              , CHECKS_HOLIDAY = sum(case when v.Q > 0 and v.DAYTYPE = 3 then 1 else 0 end)
              , BASKET_REST_SUM = sum(case when v.Q > 0 then c.S_CHECK - v.S end)
              , BASKET_SKU_REST = sum(case when v.Q > 0 then c.VOTES - 1 end)
        into #pm_pw
        from #pm_cp as v
             join #pm_c as c on c.ID_CHECK = v.ID_CHECK
             join #pm_p as p on p.ID_PRODUCT = v.ID_PRODUCT
        group by v.ID_ORGANIZATION, v.ID_SHOP, v.ID_PRODUCT, p.UNIT_BASIS, p.IS_WEIGHT;
        create clustered columnstore index cci_pm_pw on #pm_pw;

        -- ---------- магазин (ТЗ 4) ----------
        drop table if exists #pm_sw;
        select  ID_ORGANIZATION, ID_SHOP
              , CHECKS_BUY    = sum(case when IS_BUY = 1 then 1 else 0 end)
              , CHECKS_RETURN = sum(case when IS_RET_CHECK = 1 then 1 else 0 end)
              , CLIENTS_BUY   = count(distinct case when IS_BUY = 1 then ID_CONTACT end)
              , TURNOVER      = sum(S_CHECK)
              , TURNOVER_BUY  = sum(case when IS_BUY = 1 then S_CHECK end)
              , SKU_SUM       = sum(case when IS_BUY = 1 then VOTES end)
              , SKU_HDR_SUM   = sum(case when IS_BUY = 1 then SKU_HDR end)
              , SUM_HOURS     = sum(case when IS_BUY = 1 then HOURS end)
              , CHECKS_WORKDAY = sum(case when IS_BUY = 1 and DAYTYPE = 1 then 1 else 0 end)
              , CHECKS_WEEKEND = sum(case when IS_BUY = 1 and DAYTYPE = 2 then 1 else 0 end)
              , CHECKS_HOLIDAY = sum(case when IS_BUY = 1 and DAYTYPE = 3 then 1 else 0 end)
        into #pm_sw
        from #pm_c
        group by ID_ORGANIZATION, ID_SHOP;
        create unique clustered index ix_pm_sw on #pm_sw (ID_ORGANIZATION, ID_SHOP);

        if @kind = 'M'
        begin
            -- уникальные покупатели товара: сити-репорт и вся сеть канала (0) (вопрос 5 ТЗ)
            drop table if exists #pm_cm;
            select  ID_ORGANIZATION
                  , ID_CITY_REPORT = case when grouping(ID_CITY_REPORT) = 1 then 0 else ID_CITY_REPORT end
                  , ID_PRODUCT
                  , VOTES = count(*), BUYERS = count(distinct ID_CONTACT)
            into #pm_cm
            from #pm_cp
            where Q > 0
            group by grouping sets ((ID_ORGANIZATION, ID_CITY_REPORT, ID_PRODUCT), (ID_ORGANIZATION, ID_PRODUCT));
            create clustered columnstore index cci_pm_cm on #pm_cm;

            begin tran;
                delete from dbo.I_CVM_PRODUCT_MONTH      where YEAR_MONTH = @ym;
                delete from dbo.I_CVM_SHOP_MONTH         where YEAR_MONTH = @ym;
                delete from dbo.I_CVM_PRODUCT_CITY_MONTH where YEAR_MONTH = @ym;

                insert into dbo.I_CVM_PRODUCT_MONTH with (tablock) (ID_COMPANY, ID_ORGANIZATION, ID_SHOP, ID_PRODUCT, YEAR_MONTH, UNIT_BASIS, IS_WEIGHT
                    , TURNOVER, TURNOVER_BUY, CHECKS_BUY, CHECKS_RETURN, BUYERS, QTY_NET, QTY_BUY, QTY_UNIT
                    , SUM_UNIT_PRICE, SUM_UNIT_PRICE_SQ, VOTES_FRAC, VOTES_DISCOUNT, DISCOUNT, REAL_DISCOUNT, SUM_HOURS
                    , CHECKS_MON, CHECKS_TUE, CHECKS_WED, CHECKS_THU, CHECKS_FRI, CHECKS_SAT, CHECKS_SUN
                    , CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, BASKET_REST_SUM, BASKET_SKU_REST, LOAD_DATE)
                select 1, ID_ORGANIZATION, ID_SHOP, ID_PRODUCT, @ym, UNIT_BASIS, IS_WEIGHT
                    , TURNOVER, TURNOVER_BUY, CHECKS_BUY, CHECKS_RETURN, BUYERS, QTY_NET, QTY_BUY, QTY_UNIT
                    , SUM_UNIT_PRICE, SUM_UNIT_PRICE_SQ, VOTES_FRAC, VOTES_DISCOUNT, DISCOUNT, REAL_DISCOUNT, SUM_HOURS
                    , CHECKS_MON, CHECKS_TUE, CHECKS_WED, CHECKS_THU, CHECKS_FRI, CHECKS_SAT, CHECKS_SUN
                    , CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, BASKET_REST_SUM, BASKET_SKU_REST, @now
                from #pm_pw;

                insert into dbo.I_CVM_SHOP_MONTH with (tablock) (ID_COMPANY, ID_ORGANIZATION, ID_SHOP, YEAR_MONTH, CHECKS_BUY, CHECKS_RETURN, CLIENTS_BUY
                    , TURNOVER, TURNOVER_BUY, SKU_SUM, SKU_HDR_SUM, SUM_HOURS, CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, LOAD_DATE)
                select 1, ID_ORGANIZATION, ID_SHOP, @ym, CHECKS_BUY, CHECKS_RETURN, CLIENTS_BUY
                    , TURNOVER, TURNOVER_BUY, SKU_SUM, SKU_HDR_SUM, SUM_HOURS, CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, @now
                from #pm_sw;

                insert into dbo.I_CVM_PRODUCT_CITY_MONTH with (tablock) (ID_ORGANIZATION, ID_CITY_REPORT, YEAR_MONTH, ID_PRODUCT, VOTES, BUYERS, LOAD_DATE)
                select ID_ORGANIZATION, ID_CITY_REPORT, @ym, ID_PRODUCT, VOTES, BUYERS, @now
                from #pm_cm;
            commit;

            update dbo.I_CVM_PM_LOG
            set FINISH_TIME = sysdatetime(), STATUS = 'ok'
              , ROWS_INFO = concat(N'товар×магазин ', (select count(*) from #pm_pw), N'; магазинов ', (select count(*) from #pm_sw))
            where ID = @log;
            return;
        end

        -- ================= дальше — только неделя =================

        -- ---------- коэффициент формата k(p): базовый год < года недели, иначе ближайший; нет — 1.
        --            K — для недели w, K_PREV — для недели w − 52 (год к году) ----------
        drop table if exists #pm_k;
        select  p.ID_PRODUCT, o.ID_ORGANIZATION
              , K = isnull((select top (1) f.K
                            from dbo.I_CVM_FORMAT_COEF as f (nolock)
                            where f.ID_PRODUCT = p.ID_PRODUCT and f.ID_ORGANIZATION = o.ID_ORGANIZATION
                            order by case when f.BASE_YEAR < year(@d1) then 0 else 1 end, f.BASE_YEAR desc), 1.0)
              , K_PREV = isnull((select top (1) f.K
                            from dbo.I_CVM_FORMAT_COEF as f (nolock)
                            where f.ID_PRODUCT = p.ID_PRODUCT and f.ID_ORGANIZATION = o.ID_ORGANIZATION
                            order by case when f.BASE_YEAR < year(@w_prev) then 0 else 1 end, f.BASE_YEAR desc), 1.0)
        into #pm_k
        from #pm_p as p
             cross join (values (1), (3)) as o (ID_ORGANIZATION);
        create unique clustered index ix_pm_k on #pm_k (ID_PRODUCT, ID_ORGANIZATION);

        -- ---------- полка (ТЗ 2.1 п. 3–4, 3.12): сити-репорт и вся сеть (0) ----------
        drop table if exists #pm_sb;
        select  v.ID_ORGANIZATION
              , ID_CITY_REPORT = case when grouping(v.ID_CITY_REPORT) = 1 then 0 else v.ID_CITY_REPORT end
              , v.ID_PRODUCT
              , VOTES  = count(*)
              , BUYERS = count(distinct v.ID_CONTACT)
              , SUMU   = sum(v.S / nullif(v.Q * p.UNIT_MULT, 0))
              , SUMU2  = sum(square(v.S / nullif(v.Q * p.UNIT_MULT, 0)))
        into #pm_sb
        from #pm_cp as v
             join #pm_p as p on p.ID_PRODUCT = v.ID_PRODUCT
        where v.Q > 0
        group by grouping sets ((v.ID_ORGANIZATION, v.ID_CITY_REPORT, v.ID_PRODUCT), (v.ID_ORGANIZATION, v.ID_PRODUCT));
        create clustered columnstore index cci_pm_sb on #pm_sb;

        drop table if exists #pm_shelf;
        with s as (
            select  b.ID_ORGANIZATION, b.ID_CITY_REPORT, b.ID_PRODUCT
                  , p.ID_CVM_CATEGORY, p.UNIT_BASIS, p.IN_SCALE
                  , b.VOTES, b.BUYERS
                  , PRICE = b.SUMU / nullif(b.VOTES * k.K, 0)
                  , PRICE_SQ_SUM = b.SUMU2
            from #pm_sb as b
                 join #pm_p as p on p.ID_PRODUCT = b.ID_PRODUCT
                 join #pm_k as k on k.ID_PRODUCT = b.ID_PRODUCT and k.ID_ORGANIZATION = b.ID_ORGANIZATION
        )
        select  s.*
              , RANK_ASC  = rank() over (partition by ID_ORGANIZATION, ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by PRICE)
              , RANK_DESC = rank() over (partition by ID_ORGANIZATION, ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by PRICE desc)
              , N         = count(*) over (partition by ID_ORGANIZATION, ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS)
              , CAT_WIDTH = count(*) over (partition by ID_ORGANIZATION, ID_CITY_REPORT, ID_CVM_CATEGORY)
        into #pm_shelf
        from s;
        create clustered columnstore index cci_pm_shelf on #pm_shelf;

        -- ---------- год к году (офлайн, LFL-магазины): места среди общих товаров w и w − 52
        --            с одинаковой основой цены в обе недели ----------
        set @lfl_ym = (select max(YEAR_month) from dbo.I_LFL_SHOPS (nolock)
                       where ID_COMPANY = 1 and YEAR_month <= year(@d2) * 100 + month(@d2));
        drop table if exists #pm_lfl;
        select distinct l.ID_SHOP, ID_CITY_REPORT = isnull(s.ID_CITY_REPORT, -1)
        into #pm_lfl
        from dbo.I_LFL_SHOPS as l (nolock)
             join #pm_shop as s on s.ID_SHOP = l.ID_SHOP and s.ID_ORGANIZATION = 1
        where l.ID_COMPANY = 1 and l.YEAR_month = @lfl_ym;
        create unique clustered index ix_pm_lfl on #pm_lfl (ID_SHOP);

        drop table if exists #pm_yoy;
        with cur as (
            select  ID_CITY_REPORT = case when grouping(l.ID_CITY_REPORT) = 1 then 0 else l.ID_CITY_REPORT end
                  , w.ID_PRODUCT, w.UNIT_BASIS
                  , VOTES = sum(w.CHECKS_BUY), SUMU = sum(w.SUM_UNIT_PRICE)
            from #pm_pw as w join #pm_lfl as l on l.ID_SHOP = w.ID_SHOP
            where w.ID_ORGANIZATION = 1 and w.CHECKS_BUY > 0
            group by grouping sets ((l.ID_CITY_REPORT, w.ID_PRODUCT, w.UNIT_BASIS), (w.ID_PRODUCT, w.UNIT_BASIS))
        ), prev as (
            select  ID_CITY_REPORT = case when grouping(l.ID_CITY_REPORT) = 1 then 0 else l.ID_CITY_REPORT end
                  , w.ID_PRODUCT, w.UNIT_BASIS
                  , VOTES = sum(w.CHECKS_BUY), SUMU = sum(w.SUM_UNIT_PRICE)
            from dbo.I_CVM_PRODUCT_WEEK as w (nolock) join #pm_lfl as l on l.ID_SHOP = w.ID_SHOP
            where w.WEEK_START = @w_prev and w.ID_ORGANIZATION = 1 and w.CHECKS_BUY > 0
            group by grouping sets ((l.ID_CITY_REPORT, w.ID_PRODUCT, w.UNIT_BASIS), (w.ID_PRODUCT, w.UNIT_BASIS))
        ), both_ as (
            select  c.ID_CITY_REPORT, c.ID_PRODUCT, p.ID_CVM_CATEGORY, c.UNIT_BASIS
                  , P_CUR  = c.SUMU / nullif(c.VOTES * k.K, 0)
                  , P_PREV = r.SUMU / nullif(r.VOTES * k.K_PREV, 0)
            from cur as c
                 join prev as r on r.ID_CITY_REPORT = c.ID_CITY_REPORT and r.ID_PRODUCT = c.ID_PRODUCT
                               and r.UNIT_BASIS = c.UNIT_BASIS                      -- одна основа цены в обе недели
                 join #pm_p as p on p.ID_PRODUCT = c.ID_PRODUCT and p.IN_SCALE = 1
                 join #pm_k as k on k.ID_PRODUCT = c.ID_PRODUCT and k.ID_ORGANIZATION = 1
        )
        select  ID_CITY_REPORT, ID_PRODUCT, PRICE_LFL = P_CUR
              , WIDTH_YOY = count(*) over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS)
              , RANK_YOY = (rank() over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by P_CUR)
                            + count(*) over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS) + 1
                            - rank() over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by P_CUR desc)) / 2.0
              , RANK_YOY_PREV = (rank() over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by P_PREV)
                            + count(*) over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS) + 1
                            - rank() over (partition by ID_CITY_REPORT, ID_CVM_CATEGORY, UNIT_BASIS order by P_PREV desc)) / 2.0
        into #pm_yoy
        from both_;
        create clustered columnstore index cci_pm_yoy on #pm_yoy;

        -- ---------- вклад голосов клиента (ТЗ 2.1 п. 5, 2.3, 2.4) → клиент × день ----------
        drop table if exists #pm_cd;
        with votes as (
            select  v.ID_CONTACT, v.ID_ORGANIZATION, v.DATA
                  , VOTES_ALL = count(*)
                  , M_SUM     = sum(case when p.IN_SCALE = 1 and sh.N > 1
                                         then ((sh.RANK_ASC + sh.N + 1 - sh.RANK_DESC) / 2.0 - 1) / (sh.N - 1) end)
                  , M_N       = sum(case when p.IN_SCALE = 1 and sh.N > 1 then 1 else 0 end)
                  , SC_N      = sum(case when p.IN_SCALE = 1 then 1 else 0 end)
                  , PEN_SUM   = sum(case when p.IN_SCALE = 1 then pw.CHECKS_BUY * 1.0 / nullif(sw.CHECKS_BUY, 0) end)
                  , SING_SUM  = sum(case when p.IN_SCALE = 1 then 1.0 / sh.CAT_WIDTH end)
            from #pm_cp as v
                 join #pm_p as p on p.ID_PRODUCT = v.ID_PRODUCT
                 join #pm_shelf as sh on sh.ID_ORGANIZATION = v.ID_ORGANIZATION and sh.ID_CITY_REPORT = v.ID_CITY_REPORT
                                     and sh.ID_PRODUCT = v.ID_PRODUCT
                 join #pm_pw as pw on pw.ID_ORGANIZATION = v.ID_ORGANIZATION and pw.ID_SHOP = v.ID_SHOP and pw.ID_PRODUCT = v.ID_PRODUCT
                 join #pm_sw as sw on sw.ID_ORGANIZATION = v.ID_ORGANIZATION and sw.ID_SHOP = v.ID_SHOP
            where v.Q > 0
            group by v.ID_CONTACT, v.ID_ORGANIZATION, v.DATA
        ), checks as (
            select  ID_CONTACT, ID_ORGANIZATION, DATA
                  , CHECKS_BUY = count(*), TURNOVER = sum(S_CHECK), TURNOVER_FULL = sum(TO_FULL)
                  , SKU_HDR = sum(SKU_HDR), SKU_VOTES = sum(VOTES), DISCOUNT = sum(DISC), REAL_DISCOUNT = sum(REAL_DISC)
            from #pm_c
            where IS_BUY = 1
            group by ID_CONTACT, ID_ORGANIZATION, DATA
        )
        select  c.ID_CONTACT, c.ID_ORGANIZATION, c.DATA
              , VOTES_ALL = isnull(v.VOTES_ALL, 0), v.M_SUM, M_N = isnull(v.M_N, 0), SC_N = isnull(v.SC_N, 0)
              , v.PEN_SUM, v.SING_SUM
              , c.CHECKS_BUY, c.TURNOVER, c.TURNOVER_FULL, c.SKU_HDR, c.SKU_VOTES, c.DISCOUNT, c.REAL_DISCOUNT
        into #pm_cd
        from checks as c
             left join votes as v on v.ID_CONTACT = c.ID_CONTACT and v.ID_ORGANIZATION = c.ID_ORGANIZATION and v.DATA = c.DATA;
        create clustered columnstore index cci_pm_cd on #pm_cd;

        -- ---------- сопокупки за неделю (ТЗ 6): пары «чек якоря × категория-сосед», только категории со шкалой ----------
        if @with_basket = 1
        begin
            drop table if exists #pm_cc;
            select  v.ID_CHECK, v.ID_ORGANIZATION, p.ID_CVM_CATEGORY, CNT = count(*)
            into #pm_cc
            from #pm_cp as v join #pm_p as p on p.ID_PRODUCT = v.ID_PRODUCT
            where v.Q > 0 and p.IN_SCALE = 1
            group by v.ID_CHECK, v.ID_ORGANIZATION, p.ID_CVM_CATEGORY;
            create clustered columnstore index cci_pm_cc on #pm_cc;

            drop table if exists #pm_pair;
            select  v.ID_ORGANIZATION, v.ID_PRODUCT, cc.ID_CVM_CATEGORY, JOINT_CHECKS = count(*)
            into #pm_pair
            from #pm_cp as v
                 join #pm_p as p on p.ID_PRODUCT = v.ID_PRODUCT and p.IN_SCALE = 1
                 join #pm_cc as cc on cc.ID_CHECK = v.ID_CHECK
            where v.Q > 0
              and (cc.ID_CVM_CATEGORY <> p.ID_CVM_CATEGORY or cc.CNT > 1)      -- сам якорь соседом не считается
            group by v.ID_ORGANIZATION, v.ID_PRODUCT, cc.ID_CVM_CATEGORY;
            create clustered columnstore index cci_pm_pair on #pm_pair;

            drop table if exists #pm_bcat;
            select ID_ORGANIZATION, ID_CVM_CATEGORY, CHECKS = count(*)
            into #pm_bcat
            from #pm_cc group by ID_ORGANIZATION, ID_CVM_CATEGORY
            union all
            select ID_ORGANIZATION, 0, sum(case when IS_BUY = 1 then 1 else 0 end)
            from #pm_c group by ID_ORGANIZATION;
        end

        -- ---------- запись недели: одна транзакция «удалил — вставил» ----------
        begin tran;
            delete from dbo.I_CVM_PRODUCT_WEEK where WEEK_START = @d1;
            delete from dbo.I_CVM_SHOP_WEEK    where WEEK_START = @d1;
            delete from dbo.I_CVM_SHELF_WEEK   where WEEK_START = @d1;
            delete from dbo.I_CVM_CLIENT_WEEK  where WEEK_START = @d1;
            delete from dbo.I_CVM_CLIENT_DAY   where DATA >= @d1 and DATA <= @d2;

            insert into dbo.I_CVM_PRODUCT_WEEK with (tablock) (ID_COMPANY, ID_ORGANIZATION, ID_SHOP, ID_PRODUCT, WEEK_START, WEEKNUMBER, UNIT_BASIS, IS_WEIGHT
                , TURNOVER, TURNOVER_BUY, CHECKS_BUY, CHECKS_RETURN, BUYERS, QTY_NET, QTY_BUY, QTY_UNIT
                , SUM_UNIT_PRICE, SUM_UNIT_PRICE_SQ, VOTES_FRAC, VOTES_DISCOUNT, DISCOUNT, REAL_DISCOUNT, SUM_HOURS
                , CHECKS_MON, CHECKS_TUE, CHECKS_WED, CHECKS_THU, CHECKS_FRI, CHECKS_SAT, CHECKS_SUN
                , CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, BASKET_REST_SUM, BASKET_SKU_REST, LOAD_DATE)
            select 1, ID_ORGANIZATION, ID_SHOP, ID_PRODUCT, @d1, @wn, UNIT_BASIS, IS_WEIGHT
                , TURNOVER, TURNOVER_BUY, CHECKS_BUY, CHECKS_RETURN, BUYERS, QTY_NET, QTY_BUY, QTY_UNIT
                , SUM_UNIT_PRICE, SUM_UNIT_PRICE_SQ, VOTES_FRAC, VOTES_DISCOUNT, DISCOUNT, REAL_DISCOUNT, SUM_HOURS
                , CHECKS_MON, CHECKS_TUE, CHECKS_WED, CHECKS_THU, CHECKS_FRI, CHECKS_SAT, CHECKS_SUN
                , CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, BASKET_REST_SUM, BASKET_SKU_REST, @now
            from #pm_pw;

            insert into dbo.I_CVM_SHOP_WEEK with (tablock) (ID_COMPANY, ID_ORGANIZATION, ID_SHOP, WEEK_START, WEEKNUMBER, CHECKS_BUY, CHECKS_RETURN, CLIENTS_BUY
                , TURNOVER, TURNOVER_BUY, SKU_SUM, SKU_HDR_SUM, SUM_HOURS, CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, LOAD_DATE)
            select 1, ID_ORGANIZATION, ID_SHOP, @d1, @wn, CHECKS_BUY, CHECKS_RETURN, CLIENTS_BUY
                , TURNOVER, TURNOVER_BUY, SKU_SUM, SKU_HDR_SUM, SUM_HOURS, CHECKS_WORKDAY, CHECKS_WEEKEND, CHECKS_HOLIDAY, @now
            from #pm_sw;

            insert into dbo.I_CVM_SHELF_WEEK with (tablock) (ID_ORGANIZATION, ID_CITY_REPORT, WEEK_START, ID_PRODUCT, ID_CVM_CATEGORY, UNIT_BASIS, IN_SCALE
                , VOTES, BUYERS, PRICE, PRICE_SQ_SUM, SHELF_RANK, SHELF_WIDTH, SHELF_PLACE, CAT_WIDTH
                , PRICE_LFL, RANK_YOY, RANK_YOY_PREV, WIDTH_YOY, LOAD_DATE)
            select  s.ID_ORGANIZATION, s.ID_CITY_REPORT, @d1, s.ID_PRODUCT, s.ID_CVM_CATEGORY, s.UNIT_BASIS, s.IN_SCALE
                  , s.VOTES, s.BUYERS, s.PRICE, s.PRICE_SQ_SUM
                  , SHELF_RANK  = case when s.IN_SCALE = 1 then (s.RANK_ASC + s.N + 1 - s.RANK_DESC) / 2.0 end
                  , SHELF_WIDTH = case when s.IN_SCALE = 1 then s.N end
                  , SHELF_PLACE = case when s.IN_SCALE = 1 and s.N > 1
                                       then ((s.RANK_ASC + s.N + 1 - s.RANK_DESC) / 2.0 - 1) / (s.N - 1) end
                  , s.CAT_WIDTH
                  , y.PRICE_LFL, y.RANK_YOY, y.RANK_YOY_PREV, y.WIDTH_YOY, @now
            from #pm_shelf as s
                 left join #pm_yoy as y on s.ID_ORGANIZATION = 1 and y.ID_CITY_REPORT = s.ID_CITY_REPORT and y.ID_PRODUCT = s.ID_PRODUCT;

            insert into dbo.I_CVM_CLIENT_DAY with (tablock) (ID_CONTACT, ID_ORGANIZATION, DATA, VOTES_ALL, M_SUM, M_N, SC_N, PEN_SUM, SING_SUM
                , CHECKS_BUY, TURNOVER, TURNOVER_FULL, SKU_HDR, SKU_VOTES, DISCOUNT, REAL_DISCOUNT)
            select ID_CONTACT, ID_ORGANIZATION, DATA, VOTES_ALL, M_SUM, M_N, SC_N, PEN_SUM, SING_SUM
                , CHECKS_BUY, TURNOVER, TURNOVER_FULL, SKU_HDR, SKU_VOTES, DISCOUNT, REAL_DISCOUNT
            from #pm_cd;

            -- клиент × неделя: окно 56 дней до последней покупки недели (ТЗ 1, 2)
            with cl as (
                select  ID_CONTACT, ID_ORGANIZATION, LAST_DATE = max(DATA)
                      , CHECKS_BUY = sum(CHECKS_BUY), TURNOVER = sum(TURNOVER), TURNOVER_FULL = sum(TURNOVER_FULL)
                      , SKU_HDR = sum(SKU_HDR), SKU_VOTES = sum(SKU_VOTES), DISCOUNT = sum(DISCOUNT), REAL_DISCOUNT = sum(REAL_DISCOUNT)
                from #pm_cd
                group by ID_CONTACT, ID_ORGANIZATION
            ), win as (
                select  cl.ID_CONTACT, cl.ID_ORGANIZATION
                      , W_CHECKS_BUY = sum(d.CHECKS_BUY), W_TURNOVER = sum(d.TURNOVER), W_TURNOVER_FULL = sum(d.TURNOVER_FULL)
                      , W_SKU_HDR = sum(d.SKU_HDR), W_SKU_VOTES = sum(d.SKU_VOTES)
                      , VOTES_WIN = sum(d.VOTES_ALL), VOTES_SCALE = sum(d.SC_N), VOTES_INDEX = sum(d.M_N)
                      , M_SUM = sum(d.M_SUM), PEN_SUM = sum(d.PEN_SUM), SING_SUM = sum(d.SING_SUM)
                from cl
                     join dbo.I_CVM_CLIENT_DAY as d (nolock) on d.ID_CONTACT = cl.ID_CONTACT and d.ID_ORGANIZATION = cl.ID_ORGANIZATION
                                                   and d.DATA >= dateadd(day, -55, cl.LAST_DATE) and d.DATA <= cl.LAST_DATE
                                                   and d.DATA >= dateadd(day, -55, @d1) and d.DATA <= @d2   -- отсечение групп строк
                group by cl.ID_CONTACT, cl.ID_ORGANIZATION
            )
            insert into dbo.I_CVM_CLIENT_WEEK with (tablock) (ID_CONTACT, ID_ORGANIZATION, WEEK_START, WEEKNUMBER, LAST_DATE, WIN_FROM
                , CHECKS_BUY, TURNOVER, TURNOVER_FULL, SKU_HDR, SKU_VOTES, DISCOUNT, REAL_DISCOUNT
                , W_CHECKS_BUY, W_TURNOVER, W_TURNOVER_FULL, W_SKU_HDR, W_SKU_VOTES, VOTES_WIN, VOTES_SCALE, VOTES_INDEX
                , PRICE_INDEX, PRICE_SEGMENT, LOYALTY_INDEX, UNIQUE_BASKET, PRICE_SKU, W_PRICE_SKU, LOAD_DATE)
            select  cl.ID_CONTACT, cl.ID_ORGANIZATION, @d1, @wn, cl.LAST_DATE, dateadd(day, -55, cl.LAST_DATE)
                  , cl.CHECKS_BUY, cl.TURNOVER, cl.TURNOVER_FULL, cl.SKU_HDR, cl.SKU_VOTES, cl.DISCOUNT, cl.REAL_DISCOUNT
                  , w.W_CHECKS_BUY, w.W_TURNOVER, w.W_TURNOVER_FULL, w.W_SKU_HDR, w.W_SKU_VOTES, w.VOTES_WIN, w.VOTES_SCALE, w.VOTES_INDEX
                  , PRICE_INDEX   = case when w.VOTES_INDEX > 0 then w.M_SUM / w.VOTES_INDEX end
                  , PRICE_SEGMENT = case when w.VOTES_INDEX = 0 or b.B1 is null then null
                                         when w.M_SUM / w.VOTES_INDEX >= b.B2 then 1
                                         when w.M_SUM / w.VOTES_INDEX <= b.B1 then 3
                                         else 2 end
                  , LOYALTY_INDEX = case when w.VOTES_SCALE > 0 then w.PEN_SUM / w.VOTES_SCALE end
                  , UNIQUE_BASKET = case when w.VOTES_SCALE > 0 then w.SING_SUM / w.VOTES_SCALE end
                  , PRICE_SKU     = cl.TURNOVER_FULL / nullif(cl.SKU_HDR, 0)
                  , W_PRICE_SKU   = w.W_TURNOVER_FULL / nullif(w.W_SKU_HDR, 0)
                  , @now
            from cl
                 join win as w on w.ID_CONTACT = cl.ID_CONTACT and w.ID_ORGANIZATION = cl.ID_ORGANIZATION
                 left join dbo.I_CVM_SEGMENT_BOUNDS as b (nolock) on b.[YEAR] = year(cl.LAST_DATE) - 1
                                                                 and b.ID_ORGANIZATION = cl.ID_ORGANIZATION;

            if @with_basket = 1
            begin
                delete from dbo.I_CVM_BASKET_PAIR_WEEK where WEEK_START = @d1;
                delete from dbo.I_CVM_BASKET_CAT_WEEK  where WEEK_START = @d1;
                insert into dbo.I_CVM_BASKET_PAIR_WEEK with (tablock) (ID_ORGANIZATION, WEEK_START, ID_PRODUCT, ID_CVM_CATEGORY, JOINT_CHECKS)
                select ID_ORGANIZATION, @d1, ID_PRODUCT, ID_CVM_CATEGORY, JOINT_CHECKS from #pm_pair;
                insert into dbo.I_CVM_BASKET_CAT_WEEK (ID_ORGANIZATION, WEEK_START, ID_CVM_CATEGORY, CHECKS)
                select ID_ORGANIZATION, @d1, ID_CVM_CATEGORY, CHECKS from #pm_bcat;
            end
        commit;

        update dbo.I_CVM_PM_LOG
        set FINISH_TIME = sysdatetime(), STATUS = 'ok'
          , ROWS_INFO = concat(N'чек×товар ', (select count(*) from #pm_cp), N'; товар×магазин ', (select count(*) from #pm_pw)
                             , N'; полка ', (select count(*) from #pm_shelf), N'; клиентов ', (select count(*) from #pm_cd))
        where ID = @log;
    end try
    begin catch
        if @@trancount > 0 rollback;
        update dbo.I_CVM_PM_LOG set FINISH_TIME = sysdatetime(), STATUS = 'error', ERROR_TEXT = left(error_message(), 2000)
        where ID = @log;
        throw;
    end catch
end
go

-- ============================== 3. ГРАНИЦЫ СЕГМЕНТОВ ЗА ГОД ==============================
-- Терцили PRICE_INDEX по клиентам года (у клиента — последняя строка года), по каналу. Ставит сегменты строкам
-- следующего года; если у года нет границ прошлого года (первый год истории) — и строкам самого года.
-- Строки переписываются по неделям (удалил — вставил, своя транзакция на неделю), только если сегмент меняется.
create or alter procedure dbo.cvm_pm_segment_bounds @year int
as
begin
    set nocount on;
    set xact_abort on;
    declare @y1 date = datefromparts(@year, 1, 1), @y2 date = datefromparts(@year, 12, 31);
    declare @first_year bit, @wk date, @wk_last date, @n int;

    -- последняя строка года у клиента: хеш-агрегат и соединение, без сортировки всех строк года
    drop table if exists #pm_last;
    select w.ID_ORGANIZATION, w.PRICE_INDEX
    into #pm_last
    from (select ID_CONTACT, ID_ORGANIZATION, LD = max(LAST_DATE)
          from dbo.I_CVM_CLIENT_WEEK (nolock)
          where LAST_DATE >= @y1 and LAST_DATE <= @y2 and PRICE_INDEX is not null
          group by ID_CONTACT, ID_ORGANIZATION) as m
         join dbo.I_CVM_CLIENT_WEEK as w (nolock)
           on w.ID_CONTACT = m.ID_CONTACT and w.ID_ORGANIZATION = m.ID_ORGANIZATION and w.LAST_DATE = m.LD
    where w.LAST_DATE >= @y1 and w.LAST_DATE <= @y2 and w.PRICE_INDEX is not null;
    create clustered columnstore index cci_pm_last on #pm_last;

    drop table if exists #pm_b;
    select distinct ID_ORGANIZATION
         , B1 = percentile_cont(1.0 / 3) within group (order by PRICE_INDEX) over (partition by ID_ORGANIZATION)
         , B2 = percentile_cont(2.0 / 3) within group (order by PRICE_INDEX) over (partition by ID_ORGANIZATION)
         , CLIENTS = count(*) over (partition by ID_ORGANIZATION)
    into #pm_b
    from #pm_last;

    begin tran;
        delete from dbo.I_CVM_SEGMENT_BOUNDS where [YEAR] = @year;
        insert into dbo.I_CVM_SEGMENT_BOUNDS ([YEAR], ID_ORGANIZATION, B1, B2, CLIENTS, LOAD_DATE)
        select @year, ID_ORGANIZATION, B1, B2, CLIENTS, sysdatetime() from #pm_b;
    commit;

    set @first_year = case when exists (select 1 from dbo.I_CVM_SEGMENT_BOUNDS (nolock) where [YEAR] = @year - 1) then 0 else 1 end;

    select @wk = min(WEEK_START), @wk_last = max(WEEK_START)
    from dbo.I_CVM_CLIENT_WEEK (nolock)
    where PRICE_INDEX is not null
      and LAST_DATE >= case when @first_year = 1 then @y1 else dateadd(year, 1, @y1) end
      and LAST_DATE <= dateadd(year, 1, @y2);

    while @wk <= @wk_last
    begin
        drop table if exists #pm_rw;
        select  w.ID_CONTACT, w.ID_ORGANIZATION, w.WEEK_START, w.WEEKNUMBER, w.LAST_DATE, w.WIN_FROM
              , w.CHECKS_BUY, w.TURNOVER, w.TURNOVER_FULL, w.SKU_HDR, w.SKU_VOTES, w.DISCOUNT, w.REAL_DISCOUNT
              , w.W_CHECKS_BUY, w.W_TURNOVER, w.W_TURNOVER_FULL, w.W_SKU_HDR, w.W_SKU_VOTES, w.VOTES_WIN, w.VOTES_SCALE, w.VOTES_INDEX
              , w.PRICE_INDEX, w.PRICE_SEGMENT, w.LOYALTY_INDEX, w.UNIQUE_BASKET, w.PRICE_SKU, w.W_PRICE_SKU, w.LOAD_DATE
              , NEW_SEG = cast(case when w.PRICE_INDEX >= b.B2 then 1 when w.PRICE_INDEX <= b.B1 then 3 else 2 end as tinyint)
        into #pm_rw
        from dbo.I_CVM_CLIENT_WEEK as w (nolock)
             join dbo.I_CVM_SEGMENT_BOUNDS as b (nolock) on b.ID_ORGANIZATION = w.ID_ORGANIZATION and b.[YEAR] = @year
        where w.WEEK_START = @wk and w.PRICE_INDEX is not null
          and (year(w.LAST_DATE) = @year + 1 or (@first_year = 1 and year(w.LAST_DATE) = @year))
          and isnull(w.PRICE_SEGMENT, 0) <> case when w.PRICE_INDEX >= b.B2 then 1 when w.PRICE_INDEX <= b.B1 then 3 else 2 end;
        create clustered columnstore index cci_pm_rw on #pm_rw;

        if exists (select 1 from #pm_rw)
        begin
            begin tran;
                delete w
                from dbo.I_CVM_CLIENT_WEEK as w
                where w.WEEK_START = @wk
                  and exists (select 1 from #pm_rw as r where r.ID_CONTACT = w.ID_CONTACT and r.ID_ORGANIZATION = w.ID_ORGANIZATION);

                insert into dbo.I_CVM_CLIENT_WEEK with (tablock) (ID_CONTACT, ID_ORGANIZATION, WEEK_START, WEEKNUMBER, LAST_DATE, WIN_FROM
                    , CHECKS_BUY, TURNOVER, TURNOVER_FULL, SKU_HDR, SKU_VOTES, DISCOUNT, REAL_DISCOUNT
                    , W_CHECKS_BUY, W_TURNOVER, W_TURNOVER_FULL, W_SKU_HDR, W_SKU_VOTES, VOTES_WIN, VOTES_SCALE, VOTES_INDEX
                    , PRICE_INDEX, PRICE_SEGMENT, LOYALTY_INDEX, UNIQUE_BASKET, PRICE_SKU, W_PRICE_SKU, LOAD_DATE)
                select  ID_CONTACT, ID_ORGANIZATION, WEEK_START, WEEKNUMBER, LAST_DATE, WIN_FROM
                    , CHECKS_BUY, TURNOVER, TURNOVER_FULL, SKU_HDR, SKU_VOTES, DISCOUNT, REAL_DISCOUNT
                    , W_CHECKS_BUY, W_TURNOVER, W_TURNOVER_FULL, W_SKU_HDR, W_SKU_VOTES, VOTES_WIN, VOTES_SCALE, VOTES_INDEX
                    , PRICE_INDEX, NEW_SEG, LOYALTY_INDEX, UNIQUE_BASKET, PRICE_SKU, W_PRICE_SKU, LOAD_DATE
                from #pm_rw;
            commit;
        end
        set @wk = dateadd(week, 1, @wk);
    end

    select @n = count(*) from #pm_b;
    insert into dbo.I_CVM_PM_LOG (KIND, PERIOD_START, START_TIME, FINISH_TIME, STATUS, ROWS_INFO)
    values ('B', @y1, sysdatetime(), sysdatetime(), 'ok', concat(N'границы за ', @year, N', каналов ', @n));
end
go

-- ============================== 4. СОПОКУПКИ ЗА 8 НЕДЕЛЬ ==============================
create or alter procedure dbo.cvm_pm_basket_window
as
begin
    set nocount on;
    set xact_abort on;
    declare @to date = (select max(WEEK_START) from dbo.I_CVM_BASKET_CAT_WEEK (nolock));
    if @to is null return;
    declare @from date = dateadd(week, -7, @to);
    declare @now datetime2(0) = sysdatetime();

    drop table if exists #pm_anchor;
    select  w.ID_ORGANIZATION, w.ID_PRODUCT, ANCHOR_CHECKS = sum(w.CHECKS_BUY)
    into #pm_anchor
    from dbo.I_CVM_PRODUCT_WEEK as w (nolock)
    where w.WEEK_START >= @from and w.WEEK_START <= @to and w.CHECKS_BUY > 0
    group by w.ID_ORGANIZATION, w.ID_PRODUCT;

    drop table if exists #pm_cat;
    select ID_ORGANIZATION, ID_CVM_CATEGORY, CHECKS = sum(CHECKS)
    into #pm_cat
    from dbo.I_CVM_BASKET_CAT_WEEK (nolock)
    where WEEK_START >= @from and WEEK_START <= @to
    group by ID_ORGANIZATION, ID_CVM_CATEGORY;

    -- собрать заранее: в транзакции — только очистка и вставка готового
    drop table if exists #pm_link;
    select  p.ID_ORGANIZATION, WIN_FROM = @from, WIN_TO = dateadd(day, 6, @to), p.ID_PRODUCT, p.ID_CVM_CATEGORY
          , a.ANCHOR_CHECKS, JOINT_CHECKS = p.JOINT
          , J = p.JOINT * 1.0 / a.ANCHOR_CHECKS
          , ALL_CHECKS = t.CHECKS, NEIGHBOR_CHECKS = c.CHECKS
          , B = c.CHECKS * 1.0 / t.CHECKS
          , LIFT = (p.JOINT * 1.0 / a.ANCHOR_CHECKS) / (c.CHECKS * 1.0 / t.CHECKS)
    into #pm_link
    from (select ID_ORGANIZATION, ID_PRODUCT, ID_CVM_CATEGORY, JOINT = sum(JOINT_CHECKS)
          from dbo.I_CVM_BASKET_PAIR_WEEK (nolock)
          where WEEK_START >= @from and WEEK_START <= @to
          group by ID_ORGANIZATION, ID_PRODUCT, ID_CVM_CATEGORY) as p
         join #pm_anchor as a on a.ID_ORGANIZATION = p.ID_ORGANIZATION and a.ID_PRODUCT = p.ID_PRODUCT
         join #pm_cat as c on c.ID_ORGANIZATION = p.ID_ORGANIZATION and c.ID_CVM_CATEGORY = p.ID_CVM_CATEGORY and c.CHECKS > 0
         join #pm_cat as t on t.ID_ORGANIZATION = p.ID_ORGANIZATION and t.ID_CVM_CATEGORY = 0 and t.CHECKS > 0;
    create clustered columnstore index cci_pm_link on #pm_link;

    begin tran;
        truncate table dbo.I_CVM_BASKET_LINK;
        insert into dbo.I_CVM_BASKET_LINK with (tablock) (ID_ORGANIZATION, WIN_FROM, WIN_TO, ID_PRODUCT, ID_CVM_CATEGORY, ANCHOR_CHECKS
            , JOINT_CHECKS, J, ALL_CHECKS, NEIGHBOR_CHECKS, B, LIFT, LOAD_DATE)
        select ID_ORGANIZATION, WIN_FROM, WIN_TO, ID_PRODUCT, ID_CVM_CATEGORY, ANCHOR_CHECKS
            , JOINT_CHECKS, J, ALL_CHECKS, NEIGHBOR_CHECKS, B, LIFT, @now
        from #pm_link;
    commit;

    -- рабочие пары старше окна не нужны
    delete from dbo.I_CVM_BASKET_PAIR_WEEK where WEEK_START < @from;
    delete from dbo.I_CVM_BASKET_CAT_WEEK  where WEEK_START < @from;

    insert into dbo.I_CVM_PM_LOG (KIND, PERIOD_START, START_TIME, FINISH_TIME, STATUS, ROWS_INFO)
    values ('L', @from, @now, sysdatetime(), 'ok', concat(N'сопокупки ', @from, N' — ', dateadd(day, 6, @to)));
end
go

-- ============================== 5. ОРКЕСТРАТОР ==============================
-- @mode = 'history' — все закрытые недели и месяцы, которых ещё нет в журнале (или все с @force = 1), от @from;
-- @mode = 'nightly' — недели, пересекающие последние 10 дней загрузки, и недостающие; прошлый месяц в дни 1–10.
-- Неделя / месяц закрыты, когда есть чеки офлайна за последний день и e-com тоже (если e-com к этой дате уже был).
-- Ожидание «MCI Regular update» (не дольше 2 ч) — только для недель внутри окна перезаливки (последние 10 дней):
-- иначе можно прочитать неделю посреди перезаливки чеков. Шагом самого задания вызывать с @wait_job = 0.
create or alter procedure dbo.cvm_price_marts
      @mode     varchar(10) = 'nightly'
    , @from     date = null
    , @force    bit = 0
    , @wait_job bit = 1
as
begin
    set nocount on;

    declare @max1 date, @max3 date, @min3 date, @first date;
    select  @max1  = max(case when ID_ORGANIZATION = 1 then DATA end)
          , @max3  = max(case when ID_ORGANIZATION = 3 then DATA end)
          , @min3  = min(case when ID_ORGANIZATION = 3 then DATA end)
          , @first = min(DATA)
    from dbo.I_CHECKHEADER (nolock)
    where ID_COMPANY = 1;

    -- последний закрытый день и окно перезаливки MCI
    declare @closed date = case when @min3 is null or @max3 >= @max1 then @max1 else @max3 end;
    declare @reload_from date = dateadd(day, -9, @max1);

    -- понедельник любой даты: 01.01.1900 — понедельник
    declare @start date = case when @mode = 'history' then isnull(@from, @first) else dateadd(day, -9, @max1) end;
    set @start = dateadd(day, -(datediff(day, '19000101', @start) % 7), @start);
    if @mode = 'history' and @start < @first set @start = dateadd(week, 1, @start);   -- первая полная неделя

    -- последняя закрытая неделя: понедельник недели, чьё воскресенье ≤ @closed
    declare @last_week date = dateadd(day, -6, dateadd(day, -((datediff(day, '19000101', @closed) + 1) % 7), @closed));
    declare @basket_from date = dateadd(week, -7, @last_week);

    -- в ночном режиме — ещё и недостающие недели после последней посчитанной
    declare @done date, @mdone date;
    if @mode = 'nightly'
    begin
        set @done = (select max(PERIOD_START) from dbo.I_CVM_PM_LOG (nolock) where KIND = 'W' and STATUS = 'ok');
        if @done is not null and dateadd(week, 1, @done) < @start set @start = dateadd(week, 1, @done);
    end

    declare @w date = @start, @did_week bit = 0, @wb bit, @yr int, @this bit
          , @wait_until datetime2(0), @js1 datetime, @js2 datetime, @try int;

    while @w <= @last_week
    begin
        set @this = 0;
        if @force = 1 or @mode = 'nightly'
           or not exists (select 1 from dbo.I_CVM_PM_LOG (nolock) where KIND = 'W' and PERIOD_START = @w and STATUS = 'ok')
        begin
            set @wb = case when @w >= @basket_from then 1 else 0 end;
            set @try = 0;
            while @try < 2
            begin
                set @try += 1;
                if @wait_job = 1 and dateadd(day, 6, @w) >= @reload_from
                begin
                    set @wait_until = dateadd(hour, 2, sysdatetime());
                    select @js1 = max(a.start_execution_date)
                    from msdb.dbo.sysjobactivity as a (nolock) join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
                    where j.name = N'MCI Regular update';
                    while exists (select 1 from msdb.dbo.sysjobactivity as a (nolock)
                                  join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
                                  where j.name = N'MCI Regular update' and a.start_execution_date is not null
                                    and a.stop_execution_date is null
                                    and a.session_id = (select max(session_id) from msdb.dbo.syssessions (nolock)))
                    begin
                        if sysdatetime() >= @wait_until
                        begin
                            ;throw 50001, N'СТОП: «MCI Regular update» идёт дольше 2 ч — неделя не посчитана, запустить блок ещё раз.', 1;
                        end
                        waitfor delay '00:05:00';
                    end
                end

                exec dbo.cvm_pm_period @kind = 'W', @d1 = @w, @with_basket = @wb;

                -- задание стартовало во время расчёта недели внутри окна перезаливки — посчитать её ещё раз
                if @wait_job = 1 and dateadd(day, 6, @w) >= @reload_from
                begin
                    select @js2 = max(a.start_execution_date)
                    from msdb.dbo.sysjobactivity as a (nolock) join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
                    where j.name = N'MCI Regular update';
                    if isnull(@js2, '19000101') = isnull(@js1, '19000101') break;
                end
                else break;
            end
            set @did_week = 1;
            set @this = 1;

            -- рабочая клиент × день: хранить 12 недель
            delete from dbo.I_CVM_CLIENT_DAY where DATA < dateadd(week, -12, @w);
            alter index cci_I_CVM_CLIENT_DAY on dbo.I_CVM_CLIENT_DAY reorganize;
        end

        -- неделя закрыла год → границы сегментов этого года (и если их нет после прерванного прогона)
        if year(dateadd(day, 6, @w)) > year(@w)
           or (month(dateadd(day, 6, @w)) = 12 and day(dateadd(day, 6, @w)) = 31)
        begin
            set @yr = year(@w);
            if @this = 1 or not exists (select 1 from dbo.I_CVM_PM_LOG (nolock)
                                        where KIND = 'B' and PERIOD_START = datefromparts(@yr, 1, 1) and STATUS = 'ok')
                exec dbo.cvm_pm_segment_bounds @year = @yr;
        end
        set @w = dateadd(week, 1, @w);
    end

    if @did_week = 1 or not exists (select 1 from dbo.I_CVM_PM_LOG (nolock) where KIND = 'L' and STATUS = 'ok')
        exec dbo.cvm_pm_basket_window;

    -- месяцы: закрытые; история — все от первого полного; ночью — недостающие и прошлый в дни 1–10
    declare @m date =
        case when @mode = 'history' then datefromparts(year(@start), month(@start), 1)
             else dateadd(month, -1, datefromparts(year(@closed), month(@closed), 1)) end;
    if @mode = 'history' and @m < @first set @m = dateadd(month, 1, @m);
    if @mode = 'nightly'
    begin
        set @mdone = (select max(PERIOD_START) from dbo.I_CVM_PM_LOG (nolock) where KIND = 'M' and STATUS = 'ok');
        if @mdone is not null and dateadd(month, 1, @mdone) < @m set @m = dateadd(month, 1, @mdone);
    end
    while eomonth(@m) <= @closed
    begin
        if @force = 1
           or not exists (select 1 from dbo.I_CVM_PM_LOG (nolock) where KIND = 'M' and PERIOD_START = @m and STATUS = 'ok')
           or (@mode = 'nightly' and day(@closed) <= 10 and @m = dateadd(month, -1, datefromparts(year(@closed), month(@closed), 1)))
            exec dbo.cvm_pm_period @kind = 'M', @d1 = @m;
        set @m = dateadd(month, 1, @m);
    end
end
go

-- ============================== 6. СБРОС РАСЧЁТА «ЗА УПАКОВКУ» (24–28.10.2026) — ОДИН РАЗ ==============================
-- Признак: в журнале есть записи 'ok', начатые до 29.10.2026 (тот расчёт). После сброса их нет — повтор файла ничего не очищает.
-- Идущее задание истории — не трогать.
set xact_abort on;
if exists (select 1 from dbo.I_CVM_PM_LOG (nolock) where STATUS = 'ok' and START_TIME < '20261029')
   and not exists (select 1 from msdb.dbo.sysjobactivity as a with (nolock)
                   join msdb.dbo.sysjobs as j with (nolock) on j.job_id = a.job_id
                   where j.name = N'CVM_PRICE_MARTS_HISTORY'
                     and a.session_id = (select max(session_id) from msdb.dbo.syssessions with (nolock))
                     and a.start_execution_date is not null and a.stop_execution_date is null)
begin
    begin transaction;
    truncate table dbo.I_CVM_PRODUCT_WEEK;
    truncate table dbo.I_CVM_PRODUCT_MONTH;
    truncate table dbo.I_CVM_SHOP_WEEK;
    truncate table dbo.I_CVM_SHOP_MONTH;
    truncate table dbo.I_CVM_SHELF_WEEK;
    truncate table dbo.I_CVM_CLIENT_DAY;
    truncate table dbo.I_CVM_CLIENT_WEEK;
    truncate table dbo.I_CVM_PRODUCT_CITY_MONTH;
    truncate table dbo.I_CVM_BASKET_PAIR_WEEK;
    truncate table dbo.I_CVM_BASKET_CAT_WEEK;
    truncate table dbo.I_CVM_BASKET_LINK;
    truncate table dbo.I_CVM_SEGMENT_BOUNDS;
    update dbo.I_CVM_PM_LOG set STATUS = 'old' where STATUS = 'ok';   -- таблицы пусты целиком — посчитанным не считается ничего
    commit transaction;
    raiserror(N'Расчёт «за упаковку» сброшен: таблицы витрин очищены, журнал 24–28.10 помечен old.', 10, 1) with nowait;
end
set xact_abort off;
go

-- ============================== 7. ВСЯ ИСТОРИЯ — СРАЗУ: РАЗОВОЕ ЗАДАНИЕ SQL AGENT ==============================
-- История идёт 1–3 суток, поэтому считает её разовое задание «CVM_PRICE_MARTS_HISTORY», а не эта сессия (и не обменник):
-- задание запускается сразу, после успеха удаляет себя; при ошибке остаётся — причина в dbo.I_CVM_PM_LOG и истории задания.
-- Повторный запуск этого файла: идущее задание не трогается; остановленное — пересоздаётся и продолжает с места остановки.
use msdb;
go
set nocount on;
declare @job sysname = N'CVM_PRICE_MARTS_HISTORY', @rc int = 0;

if exists (select 1 from msdb.dbo.sysjobactivity as a with (nolock)
           join msdb.dbo.sysjobs as j with (nolock) on j.job_id = a.job_id
           where j.name = @job
             and a.session_id = (select max(session_id) from msdb.dbo.syssessions with (nolock))
             and a.start_execution_date is not null and a.stop_execution_date is null)
begin
    raiserror(N'Задание CVM_PRICE_MARTS_HISTORY уже идёт — не трогаю.', 10, 1);
    return;
end

if exists (select 1 from msdb.dbo.sysjobs with (nolock) where name = @job)
begin
    exec @rc = msdb.dbo.sp_delete_job @job_name = @job;
    if @rc <> 0 begin raiserror(N'СТОП: старое задание CVM_PRICE_MARTS_HISTORY не удалено — новое не создано.', 16, 1); return; end
end

exec @rc = msdb.dbo.sp_add_job @job_name = @job, @enabled = 1, @delete_level = 1,
     @description = N'Разовый пересчёт всей истории новых витрин ценового индекса (CVM_PRICE_INDEX_MARTS.sql); после успеха удаляется.';
if @rc <> 0 begin raiserror(N'СТОП: задание не создано (sp_add_job).', 16, 1); return; end
exec @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_name = N'history', @subsystem = N'TSQL',
     @database_name = N'mci_model', @command = N'exec dbo.cvm_price_marts @mode = ''history'';',
     @on_success_action = 1, @on_fail_action = 2;
if @rc <> 0 begin exec msdb.dbo.sp_delete_job @job_name = @job; raiserror(N'СТОП: шаг задания не создан — задание удалено.', 16, 1); return; end
exec @rc = msdb.dbo.sp_add_jobserver @job_name = @job;
if @rc <> 0 begin exec msdb.dbo.sp_delete_job @job_name = @job; raiserror(N'СТОП: задание не привязано к серверу — удалено.', 16, 1); return; end

exec @rc = msdb.dbo.sp_start_job @job_name = @job;
if @rc <> 0 raiserror(N'СТОП: задание создано, но не запустилось — запустить вручную: EXEC msdb.dbo.sp_start_job @job_name = N''CVM_PRICE_MARTS_HISTORY'';', 16, 1);
go

set noexec off;
go

-- Контроль хода (можно запускать в любой момент): последние записи журнала
-- select top (20) KIND, PERIOD_START, START_TIME, FINISH_TIME, STATUS, ROWS_INFO, ERROR_TEXT
-- from mci_model.dbo.I_CVM_PM_LOG (nolock) order by ID desc;
