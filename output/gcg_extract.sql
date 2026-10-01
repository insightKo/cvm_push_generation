/*==============================================================================
  ВЫДЕЛЕНИЕ ГЛОБАЛЬНОЙ КОНТРОЛЬНОЙ ГРУППЫ (ГКГ) — ПОМЕСЯЧНО.

  Логика:
    - когорты по «последнему активному месяцу» (MAX_YM): каждый месяц из #ym
      обрабатывается отдельно;
    - внутри месяца контакты сортируются по LTV / бюджету / первой-последней
      покупке -> режутся на сегменты по 35 -> из каждого 1 случайный;
    - REJECTION-SAMPLING: пере-выбор случайного набора, пока средние контроля и
      таргета не совпадут по бюджету И по LTV с точностью 0.05% (или пока не
      исчерпан лимит попыток — тогда берём ЛУЧШИЙ из виденных наборов).
    - результат по всем месяцам копится в #cg_new -> I_GLOBAL_CG_2026.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;

DECLARE @act_date date = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY = @idc);

-- Границы жизненного цикла (для отчётов), отсчёт от @act_date. Лимиты Дикси:
DECLARE @new_days      int = 6  * 7;   -- НОВЫЕ:    первая покупка в последние 6 недель
DECLARE @active_days   int = 6  * 7;   -- АКТИВНЫЕ: последняя покупка в последние 6 недель
DECLARE @sleeping_days int = 15 * 7;   -- СПЯЩИЕ:   последняя покупка 6–15 недель назад; дальше — ОТТОК

-- Настройки отбора
DECLARE @seg_size int   = 35;          -- размер сегмента (доля ГКГ ~ 1/35 ≈ 2.86%)
DECLARE @tol      float = 0.0005;      -- целевая точность совпадения (0.05%)
DECLARE @max_try  int   = 300;         -- максимум попыток rejection-sampling на месяц (защита от зацикливания)

-- Граница цикла по месяцам: для теста поставь конкретный YYYYMM (напр. 202605),
-- для полного прогона — минимальный месяц из #ym.
DECLARE @stop_ym  int;                 -- задаётся ниже, после #ym


-----------------------------------------------------------------------
-- Список месяцев (от свежего к старому)
-----------------------------------------------------------------------
drop table if exists #ym;
select YEAR_MONTH
     , RN = ROW_NUMBER() over (order by YEAR_MONTH desc)
into #ym
from I_CALENDAR (nolock)
where YEAR_MONTH between (select min(YEAR_MONTH) from I_CHECKHEADER (nolock) where ID_COMPANY = @idc)
                    and (select max(YEAR_MONTH) from I_CHECKHEADER (nolock) where ID_COMPANY = @idc)
group by YEAR_MONTH;

-- ТЕСТ: обрабатывать до 202605 включительно. Для полного прогона раскомментируй min():
SET @stop_ym = 202605;
-- SET @stop_ym = (select min(YEAR_MONTH) from #ym);


-----------------------------------------------------------------------
-- Профиль контакта за ВСЮ историю (одна строка на контакт)
-----------------------------------------------------------------------
drop table if exists #first_purch;
select a.ID_CONTACT
     , a.ID_COMPANY
     , MIN_DATA           = min(DATA)
     , MAX_DATA           = max(DATA)
     , MAX_YM             = max(YEAR_MONTH)                       -- последний активный месяц (когорта)
     , LTV                = sum(COST_DISCOUNT)
     , LTV_100            = round(sum(COST_DISCOUNT) / 100, 0) * 100
     , COUNT_CHECK        = count(a.ID_CHECK)
     , MIN_WEEKNUMBER     = min(WEEKNUMBER)
     , MAX_WEEKNUMBER     = max(WEEKNUMBER)
     , COST_DISCOUNT_OFF  = sum(IIF(a.ID_ORGANIZATION = 1, a.COST_DISCOUNT, 0))
     , COST_DISCOUNT_ECOM = sum(IIF(a.ID_ORGANIZATION = 3, a.COST_DISCOUNT, 0))
     , CHANNEL            = IIF(count(distinct a.ID_ORGANIZATION) = 2, 2, max(a.ID_ORGANIZATION)) -- 1-off,2-omni,3-online
     , HAS_PUSH
     , TOKENS
into #first_purch
from I_CHECKHEADER a (nolock)
     left join I_CONTACT as f (nolock)
        on  a.ID_CONTACT = f.ID_CONTACT
        and a.ID_COMPANY = f.ID_COMPANY
where a.ID_COMPANY = @idc
group by a.ID_CONTACT, a.ID_COMPANY, HAS_PUSH, TOKENS;


-----------------------------------------------------------------------
-- Накопитель результата
-----------------------------------------------------------------------
drop table if exists #cg_new;
create table #cg_new (ID_CONTACT bigint, CONTROL_GROUP int);


-----------------------------------------------------------------------
-- Переменные цикла (объявляем ОДИН раз — не внутри while!)
-----------------------------------------------------------------------
DECLARE @error  float, @error1 float, @try int, @best float, @maxerr float;
DECLARE @i  int = 1;
DECLARE @ym int = (select YEAR_MONTH from #ym where RN = @i);


-----------------------------------------------------------------------
-- ГЛАВНЫЙ ЦИКЛ ПО МЕСЯЦАМ
-----------------------------------------------------------------------
while @ym >= @stop_ym
BEGIN

    ------------------------------------------------------------------
    -- Когорта месяца: контакты с MAX_YM = @ym, агрегаты за всю историю
    ------------------------------------------------------------------
    drop table if exists #purchases;
    select a.ID_COMPANY
         , a.ID_CONTACT
         , CHANNEL
         , HAS_PUSH
         , TOKENS
         , e.LTV
         , e.LTV_100
         , e.COUNT_CHECK
         , MIN_WEEKNUMBER
         , MAX_WEEKNUMBER
         , COST_DISCOUNT_OFF_10  = round(sum(IIF(a.ID_ORGANIZATION = 1, a.COST_DISCOUNT, 0)) / 10, 0) * 10
         , COST_DISCOUNT_ECOM_10 = round(sum(IIF(a.ID_ORGANIZATION = 3, a.COST_DISCOUNT, 0)) / 10, 0) * 10
         , COST_DISCOUNT_OFF     = sum(IIF(a.ID_ORGANIZATION = 1, a.COST_DISCOUNT, 0))
         , COST_DISCOUNT_ECOM    = sum(IIF(a.ID_ORGANIZATION = 3, a.COST_DISCOUNT, 0))
         , BUDGET                = sum(COST_DISCOUNT)
         , BUDGET_10             = round(sum(COST_DISCOUNT) / 10, 0) * 10
         , CHECKS                = count(ID_CHECK)
    into #purchases
    from I_CHECKHEADER (nolock) as a
         inner join #first_purch as e
            on a.ID_COMPANY = e.ID_COMPANY
           and a.ID_CONTACT = e.ID_CONTACT
    where a.ID_CONTACT <> 0
      and e.MAX_YM = @ym
    group by a.ID_COMPANY, a.ID_CONTACT, CHANNEL, HAS_PUSH, TOKENS
           , e.LTV, e.LTV_100, e.COUNT_CHECK, MIN_WEEKNUMBER, MAX_WEEKNUMBER;

    ------------------------------------------------------------------
    -- Фиксированный порядок -> сегменты (внутри сегмента отбор случайный)
    ------------------------------------------------------------------
    drop table if exists #interval;
    select ID_COMPANY
         , b.ID_CONTACT
         , CHANNEL
         , HAS_PUSH
         , RN = ROW_NUMBER() over (order by LTV_100, BUDGET_10, MIN_WEEKNUMBER desc, MAX_WEEKNUMBER)
    into #interval
    from #purchases as b
    group by ID_COMPANY, b.ID_CONTACT, CHANNEL, HAS_PUSH
           , LTV_100, BUDGET_10, MIN_WEEKNUMBER, MAX_WEEKNUMBER;

    ------------------------------------------------------------------
    -- REJECTION-SAMPLING: крутим, пока не сойдётся ИЛИ не кончатся попытки.
    -- Держим лучший из виденных наборов в #cg_best.
    ------------------------------------------------------------------
    set @error  = 1.0;
    set @error1 = 1.0;
    set @try    = 0;
    set @best   = 999;
    drop table if exists #cg_best;

    while (@error >= @tol or @error1 >= @tol) and @try < @max_try
    BEGIN
        set @try += 1;

        -- один случайный набор: 1 контакт из каждого сегмента
        drop table if exists #cg;
        ;with t as (
            select ID_CONTACT, ID_COMPANY, CHANNEL, HAS_PUSH, SEGMENT = RN / @seg_size
            from #interval
        )
        , a as (
            select ID_CONTACT, ID_COMPANY, CHANNEL, HAS_PUSH, t.SEGMENT
                 , NI = ROW_NUMBER() over (partition by t.SEGMENT order by newid())
            from t
        )
        select ID_CONTACT
        into #cg
        from a
        where NI <= 1;

        -- средние по контролю (CG=1) и таргету (CG=0)
        drop table if exists #stat;
        select CG = IIF(b.ID_CONTACT is not null, 1, 0)
             , COST_DISCOUNT = sum(a.BUDGET) * 1.0 / count(a.ID_CONTACT)
             , LTV           = sum(a.LTV)    * 1.0 / count(a.ID_CONTACT)
             , COUNT_CLIENT  = count(distinct a.ID_CONTACT)
        into #stat
        from #purchases as a
             left join #cg as b on a.ID_CONTACT = b.ID_CONTACT
        group by IIF(b.ID_CONTACT is not null, 1, 0);

        -- относительные отклонения (NULL при вырожденной когорте -> считаем «не сошлось»)
        set @error  = isnull((select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
                              from #stat a join #stat b on a.CG = 0 and b.CG = 1), 1);
        set @error1 = isnull((select abs(a.LTV - b.LTV) / nullif(a.LTV, 0)
                              from #stat a join #stat b on a.CG = 0 and b.CG = 1), 1);

        -- запоминаем лучший набор (по худшей из двух ошибок)
        set @maxerr = IIF(@error > @error1, @error, @error1);
        if @maxerr < @best
        BEGIN
            set @best = @maxerr;
            drop table if exists #cg_best;
            select ID_CONTACT into #cg_best from #cg;
        END
    END

    -- добавляем лучший набор месяца в общий результат
    insert into #cg_new (ID_CONTACT, CONTROL_GROUP)
    select ID_CONTACT, CONTROL_GROUP = 1
    from #cg_best
    group by ID_CONTACT;

    -- следующий месяц
    set @i  = @i + 1;
    set @ym = (select YEAR_MONTH from #ym where RN = @i);
END


/*=============================================================================
  ПОВЕДЕНИЕ ЗА ПОСЛЕДНИЕ 6 НЕДЕЛЬ (для отчётов баланса)
=============================================================================*/
drop table if exists #recent;
select a.ID_CONTACT
     , a.ID_COMPANY
     , R6_COUNT      = count(a.ID_CONTACT_DATA)
     , R6_SPEND      = sum(a.COST_DISCOUNT)
     , R6_SPEND_OFF  = sum(IIF(a.ID_ORGANIZATION = 1, a.COST_DISCOUNT, 0))
     , R6_SPEND_ECOM = sum(IIF(a.ID_ORGANIZATION = 3, a.COST_DISCOUNT, 0))
     , R6_AVG_CHECK  = sum(a.COST_DISCOUNT) * 1.0 / count(a.ID_CONTACT_DATA)
into #recent
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= dateadd(day, -@active_days, @act_date)   -- последние 6 недель
group by a.ID_CONTACT, a.ID_COMPANY;


/*=============================================================================
  ПРИЗНАКИ ЖИЗНЕННОГО ЦИКЛА (для отчётов) — по датам, а не по YYYYWW.
=============================================================================*/
drop table if exists #strata;
select p.ID_COMPANY
     , p.ID_CONTACT
     , p.CHANNEL
     , CHANNEL_MIX = case
           when p.COST_DISCOUNT_ECOM = 0 then 0
           when p.COST_DISCOUNT_OFF  = 0 then 3
           when p.COST_DISCOUNT_ECOM * 1.0 / nullif(p.COST_DISCOUNT_ECOM + p.COST_DISCOUNT_OFF, 0) < 0.5 then 1
           else 2 end
     , HAS_PUSH_FLAG = IIF(p.HAS_PUSH = 1 OR p.TOKENS > 0, 1, 0)
     , LIFECYCLE = case
           when datediff(day, p.MIN_DATA, @act_date) <= @new_days      then 'NEW'
           when datediff(day, p.MAX_DATA, @act_date) <= @active_days    then 'ACTIVE'
           when datediff(day, p.MAX_DATA, @act_date) <= @sleeping_days  then 'SLEEPING'
           else 'CHURN' end
into #strata
from #first_purch as p;


/*=============================================================================
  ОТЧЁТЫ О БАЛАНСЕ. CONTROL и TARGET должны совпадать по средним.
  ВНИМАНИЕ: пока обработаны не все месяцы (@stop_ym=202605), в контроле только
  когорты этих месяцев — общий отчёт станет валидным после полного прогона.
=============================================================================*/

-- 7.1 Общий баланс (вся история + последние 6 недель)
select GROUP_NAME      = IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET')
     , CONTACTS        = count(*)
     , SHARE_PCT       = cast(100.0 * count(*) / sum(count(*)) over () as decimal(5,2))
     , AVG_CHECK       = sum(p.LTV * 1.0) / nullif(sum(p.COUNT_CHECK), 0)
     , AVG_COUNT_CHECK = avg(p.COUNT_CHECK * 1.0)
     , AVG_LTV         = avg(p.LTV * 1.0)
     , R6_ACTIVE_PCT   = cast(100.0 * sum(IIF(r.ID_CONTACT is not null, 1, 0)) / count(*) as decimal(5,2))
     , R6_AVG_SPEND    = avg(isnull(r.R6_SPEND, 0) * 1.0)
     , R6_AVG_COUNT    = avg(isnull(r.R6_COUNT, 0) * 1.0)
     , SHARE_OFFLINE_PCT = cast(100.0 * sum(IIF(p.CHANNEL = 1, 1, 0)) / count(*) as decimal(5,2))
     , SHARE_OMNI_PCT    = cast(100.0 * sum(IIF(p.CHANNEL = 2, 1, 0)) / count(*) as decimal(5,2))
     , SHARE_ONLINE_PCT  = cast(100.0 * sum(IIF(p.CHANNEL = 3, 1, 0)) / count(*) as decimal(5,2))
     , SHARE_PUSH_PCT    = cast(100.0 * sum(IIF(p.HAS_PUSH = 1 OR p.TOKENS > 0, 1, 0)) / count(*) as decimal(5,2))
from #first_purch as p
     left join #cg_new as c on p.ID_CONTACT = c.ID_CONTACT
     left join #recent as r on p.ID_COMPANY = r.ID_COMPANY and p.ID_CONTACT = r.ID_CONTACT
group by IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET');


-- 7.2 Баланс по жизненному циклу
select s.LIFECYCLE
     , GRP             = IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET')
     , CONTACTS        = count(*)
     , CG_SHARE_PCT    = cast(100.0 * sum(IIF(c.ID_CONTACT is not null, 1, 0))
                              / nullif(sum(count(*)) over (partition by s.LIFECYCLE), 0) as decimal(5,2))
     , AVG_LTV         = avg(p.LTV * 1.0)
     , AVG_CHECK       = sum(p.LTV * 1.0) / nullif(sum(p.COUNT_CHECK), 0)
     , R6_AVG_SPEND    = avg(isnull(r.R6_SPEND, 0) * 1.0)
     , R6_AVG_COUNT    = avg(isnull(r.R6_COUNT, 0) * 1.0)
from #strata as s
     join      #first_purch as p on s.ID_COMPANY = p.ID_COMPANY and s.ID_CONTACT = p.ID_CONTACT
     left join #cg_new      as c on s.ID_CONTACT = c.ID_CONTACT
     left join #recent      as r on s.ID_COMPANY = r.ID_COMPANY and s.ID_CONTACT = r.ID_CONTACT
group by s.LIFECYCLE, IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET')
order by s.LIFECYCLE, GRP;


-- 7.3 Баланс по каналам (траты оффлайн/онлайн, вся история + 6 недель)
select CHANNEL_NAME   = case p.CHANNEL when 1 then '1-OFFLINE' when 2 then '2-OMNI' when 3 then '3-ONLINE' end
     , GRP            = IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET')
     , CONTACTS       = count(*)
     , CG_SHARE_PCT   = cast(100.0 * sum(IIF(c.ID_CONTACT is not null, 1, 0))
                             / nullif(sum(count(*)) over (partition by p.CHANNEL), 0) as decimal(5,2))
     , AVG_SPEND_OFF     = avg(p.COST_DISCOUNT_OFF  * 1.0)
     , AVG_SPEND_ECOM    = avg(p.COST_DISCOUNT_ECOM * 1.0)
     , R6_AVG_SPEND_OFF  = avg(isnull(r.R6_SPEND_OFF,  0) * 1.0)
     , R6_AVG_SPEND_ECOM = avg(isnull(r.R6_SPEND_ECOM, 0) * 1.0)
from #first_purch as p
     left join #cg_new as c on p.ID_CONTACT = c.ID_CONTACT
     left join #recent as r on p.ID_COMPANY = r.ID_COMPANY and p.ID_CONTACT = r.ID_CONTACT
group by p.CHANNEL, IIF(c.ID_CONTACT is not null, 'CONTROL', 'TARGET')
order by p.CHANNEL, GRP;


/*=============================================================================
  ФИНАЛЬНАЯ ЗАПИСЬ -> I_GLOBAL_CG_2026 (staging). Раскомментируй, когда прогонишь
  все месяцы и отчёты устроят. Перелив в I_GLOBAL_CG — отдельным шагом.
=============================================================================*/
-- if object_id('I_GLOBAL_CG_2026') is null
--     create table I_GLOBAL_CG_2026 (ID_CONTACT bigint, CONTROL_GROUP int, ID_COMPANY int);
-- truncate table I_GLOBAL_CG_2026;
-- insert into I_GLOBAL_CG_2026 (ID_CONTACT, CONTROL_GROUP, ID_COMPANY)
-- select ID_CONTACT, CONTROL_GROUP = 1, ID_COMPANY = @idc
-- from #cg_new
-- group by ID_CONTACT;

-- truncate table I_GLOBAL_CG;
-- insert into I_GLOBAL_CG (ID_CONTACT, CONTROL_GROUP, ID_COMPANY)
-- select ID_CONTACT, CONTROL_GROUP, ID_COMPANY from I_GLOBAL_CG_2026;
