/*==============================================================================
  ПРОВЕРКА ПОКРЫТИЯ I_GLOBAL_CG. Диагностика, ничего не считает и не меняет.

  Цель: понять, почему доп ТО вышел отрицательным во всех сегментах.
  Гипотеза: ГКГ нарезана по когортам MAX_YM (месяц ПОСЛЕДНЕЙ покупки), в
  gcg_extract.sql тестовый прогон ограничен @stop_ym = 202605, а финальная
  заливка закомментирована. Тогда КГ покрывает только свежие когорты, и
  внутри оттока/спящих контроль состоит из более «свежих» клиентов, чем
  масса, — доп ТО механически уходит в минус.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;
DECLARE @dt_from date = '2026-07-01';


-----------------------------------------------------------------------
-- 1. Что вообще лежит в I_GLOBAL_CG
-----------------------------------------------------------------------
select ROWS_TOTAL       = count(*)
     , CONTACTS_UNIQUE  = count(distinct ID_CONTACT)
     , DUPLICATES       = count(*) - count(distinct ID_CONTACT)
     , COMPANIES        = count(distinct ID_COMPANY)
from I_GLOBAL_CG (nolock);

-- какие значения CONTROL_GROUP встречаются
select CONTROL_GROUP
     , ROWS_CNT        = count(*)
     , CONTACTS_UNIQUE = count(distinct ID_CONTACT)
from I_GLOBAL_CG (nolock)
group by CONTROL_GROUP
order by CONTROL_GROUP;


-----------------------------------------------------------------------
-- 2. ГЛАВНОЕ: покрытие ГКГ по месяцу ПОСЛЕДНЕЙ покупки до июля (MAX_YM)
--    Норма ~2.9% (нарезка 1/35) в каждом месяце. Месяцы с 0 — не покрыты:
--    оттуда клиенты попадают только в массу, и сравнение становится кривым.
-----------------------------------------------------------------------
drop table if exists #hist;
select a.ID_CONTACT
     , MAX_DATA = max(a.DATA)
     , MAX_YM   = max(year(a.DATA) * 100 + month(a.DATA))
into #hist
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT;

drop table if exists #cg;
select ID_CONTACT
into #cg
from I_GLOBAL_CG (nolock)
where ID_COMPANY = @idc
  and CONTROL_GROUP = 1
group by ID_CONTACT;

select h.MAX_YM
     , CLIENTS      = count(*)
     , CG_CLIENTS   = sum(IIF(c.ID_CONTACT is not null, 1, 0))
     , CG_SHARE_PCT = cast(100.0 * sum(IIF(c.ID_CONTACT is not null, 1, 0)) / count(*) as decimal(6,3))
from #hist as h
     left join #cg as c on h.ID_CONTACT = c.ID_CONTACT
group by h.MAX_YM
order by h.MAX_YM desc;


-----------------------------------------------------------------------
-- 2б. КТО ДЕЛАЕТ МИНУС: топ клиентов КГ по июльскому ТО.
--     КГ примерно в 35 раз меньше массы, поэтому каждый рубль в контроле
--     входит в ТО_ожидаемый умноженным на K = N_масса / N_КГ. Один клиент
--     с чеком 3 млн даёт около -100 млн в доп ТО. Здесь видно, сколько
--     минуса делают первые 20 клиентов контроля.
-----------------------------------------------------------------------
drop table if exists #july_cg;
select a.ID_CONTACT
     , SPEND = sum(a.COST_DISCOUNT)
into #july_cg
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @dt_from
  and a.DATA <  dateadd(month, 1, @dt_from)
  and exists (select 1 from #cg as c where c.ID_CONTACT = a.ID_CONTACT)
group by a.ID_CONTACT;

DECLARE @k float = (select count(*) * 1.0 / nullif((select count(*) from #cg), 0) from #hist);

select TOP 20
       j.ID_CONTACT
     , JULY_SPEND        = j.SPEND
     , K_SCALE           = @k
     , WEIGHT_IN_EXPECTED = j.SPEND * @k        -- вклад клиента в ТО_ожидаемый
from #july_cg as j
order by j.SPEND desc;

-- сколько всего «весят» первые 20 клиентов КГ
select TOP20_SPEND          = sum(t.SPEND)
     , TOP20_WEIGHT         = sum(t.SPEND) * @k
     , CG_TOTAL_JULY_SPEND  = (select sum(SPEND) from #july_cg)
     , TOP20_SHARE_OF_CG_PCT = cast(100.0 * sum(t.SPEND)
                                    / nullif((select sum(SPEND) from #july_cg), 0) as decimal(6,2))
from (select TOP 20 SPEND from #july_cg order by SPEND desc) as t;


-----------------------------------------------------------------------
-- 3. То же в разрезе сегментов жизненного цикла — где именно провал
-----------------------------------------------------------------------
DECLARE @pre_date date = dateadd(day, -1, @dt_from);

select LIFECYCLE = case
           when datediff(day, h.MAX_DATA, @pre_date) <= 42  then '1-ACTIVE'
           when datediff(day, h.MAX_DATA, @pre_date) <= 105 then '2-OTTOK_6_15w'
           else '3-SLEEPING_15w+' end
     , CLIENTS      = count(*)
     , CG_CLIENTS   = sum(IIF(c.ID_CONTACT is not null, 1, 0))
     , CG_SHARE_PCT = cast(100.0 * sum(IIF(c.ID_CONTACT is not null, 1, 0)) / count(*) as decimal(6,3))
     -- средняя давность последней покупки: если у КГ она заметно МЕНЬШЕ,
     -- контроль «свежее» массы — это и есть источник минуса
     , DAYS_SINCE_LAST_MASS = avg(IIF(c.ID_CONTACT is null,     datediff(day, h.MAX_DATA, @pre_date) * 1.0, NULL))
     , DAYS_SINCE_LAST_CG   = avg(IIF(c.ID_CONTACT is not null, datediff(day, h.MAX_DATA, @pre_date) * 1.0, NULL))
from #hist as h
     left join #cg as c on h.ID_CONTACT = c.ID_CONTACT
group by case
           when datediff(day, h.MAX_DATA, @pre_date) <= 42  then '1-ACTIVE'
           when datediff(day, h.MAX_DATA, @pre_date) <= 105 then '2-OTTOK_6_15w'
           else '3-SLEEPING_15w+' end
order by 1;
