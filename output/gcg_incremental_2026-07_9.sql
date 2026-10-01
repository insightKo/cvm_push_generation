/*==============================================================================
  ОТЧЁТ ПО ГКГ ЗА ИЮЛЬ 2026.
  ВЕРСИЯ 9: то же, что _8, но с предварительным отсечением выбросов
            в обоих сегментах — и в активных, и в оттоке со спящими.

  ПРАВИЛА РАСЧЁТА (не менялись):

  Активные (SEGMENT = 2) — с поправочным коэффициентом на период «ДО»:
      KOEF   = ТО КГ / ТО КГ ДО                  (чистый прирост контроля)
      Доп ТО = ТО ЦГ - ТО ЦГ ДО * KOEF
      Период «ДО» = 6 недель до начала месяца (20.05-30.06.2026).

  Отток (SEGMENT = 5) — приведение контроля к численности ЦГ,
  период «ДО» не используется:
      Доп ТО = ТО ЦГ - ТО КГ / Клиентов КГ * Клиентов ЦГ

  ОТСЕЧЕНИЕ ВЫБРОСОВ:
    - порог Тьюки P75 + 3*IQR считается ВНУТРИ КАЖДОГО СЕГМЕНТА и только по
      ненулевым тратам: у оттока медиана июльского ТО нулевая, поэтому общий
      порог на смеси сегментов был бы некорректен для обоих;
    - активные проверяются и по июлю, и по периоду «ДО» (он входит в формулу);
      отток — только по июлю, «ДО» у него не используется;
    - клиент-выброс удаляется ЦЕЛИКОМ: и из оборота, и из численности,
      одинаково из ЦГ и из КГ. Порог для группы общий.
    - что именно отсечено, показывает блок 5.

  СЕГМЕНТЫ (по дате последней покупки до июля, отсчёт от 30.06.2026):
      SEGMENT = 2  Активные — покупка в последние 6 недель
      SEGMENT = 5  Отток    — всё остальное, СПЯЩИЕ ВКЛЮЧЕНЫ В ОТТОК

  ЦГ = целевая группа (масса, получала коммуникации), КГ = глобальный контроль.
  Новые исключены из обеих групп — это отдельный SEGMENT = 1.
  Фрод в расчёте ГКГ не используется.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;

DECLARE @dt_from date = '2026-07-01';
DECLARE @dt_to   date = '2026-08-01';                     -- правая граница НЕ включается
DECLARE @pre_date date = dateadd(day, -1, @dt_from);      -- 30.06.2026

DECLARE @active_days int = 6 * 7;    -- граница активных: 6 недель
DECLARE @new_days    int = 6 * 7;    -- новые: первая покупка в последние 6 недель

DECLARE @pre_weeks int = 6;                                      -- «ДО» = 6 недель
DECLARE @pre_from  date = dateadd(week, -@pre_weeks, @dt_from);  -- 20.05.2026

DECLARE @tukey_k float = 3.0;        -- жёсткость отсечения: 3 — дальние выбросы, 1.5 — строже

DECLARE @ym  int = year(@dt_from) * 100 + month(@dt_from);
DECLARE @yy  int = year(@dt_from);
DECLARE @mm  int = month(@dt_from);


-----------------------------------------------------------------------
-- 1. Список КГ (дедуплицированный)
-----------------------------------------------------------------------
drop table if exists #cg;
select ID_CONTACT
into #cg
from I_GLOBAL_CG (nolock)
where ID_COMPANY = @idc
  and CONTROL_GROUP = 1
group by ID_CONTACT;


-----------------------------------------------------------------------
-- 2. История до июля: первая и последняя покупка
-----------------------------------------------------------------------
drop table if exists #hist;
select a.ID_CONTACT
     , MIN_DATA = min(a.DATA)
     , MAX_DATA = max(a.DATA)
into #hist
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 3. База: группа + сегмент (спящие внутри оттока), без новых
-----------------------------------------------------------------------
drop table if exists #base;
select h.ID_CONTACT
     , GRP = IIF(c.ID_CONTACT is not null, 'CG', 'TG')     -- TG = ЦГ, CG = КГ
     , SEGMENT = IIF(datediff(day, h.MAX_DATA, @pre_date) <= @active_days, 2, 5)
into #base
from #hist as h
     left join #cg as c on h.ID_CONTACT = c.ID_CONTACT
where datediff(day, h.MIN_DATA, @pre_date) > @new_days;    -- новые исключены


-----------------------------------------------------------------------
-- 4. Покупки июля и периода «ДО» (6 недель, только активные)
-----------------------------------------------------------------------
drop table if exists #july;
select a.ID_CONTACT
     , SPEND  = sum(a.COST_DISCOUNT)
     , CHECKS = count(a.ID_CHECK)
into #july
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @dt_from
  and a.DATA <  @dt_to
  and exists (select 1 from #base as b where b.ID_CONTACT = a.ID_CONTACT)
group by a.ID_CONTACT;

drop table if exists #before;
select a.ID_CONTACT
     , SPEND = sum(a.COST_DISCOUNT)
into #before
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @pre_from
  and a.DATA <  @dt_from
  and exists (select 1 from #base as b where b.ID_CONTACT = a.ID_CONTACT and b.SEGMENT = 2)
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 5. ВЫБРОСЫ: пороги внутри сегмента, отчёт и чистка
-----------------------------------------------------------------------
-- порог по июлю — свой для каждого сегмента, по ненулевым тратам
drop table if exists #q_july;
select distinct
       SEGMENT
     , P25 = PERCENTILE_CONT(0.25) within group (order by SPEND * 1.0) over (partition by SEGMENT)
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND * 1.0) over (partition by SEGMENT)
into #q_july
from (select b.SEGMENT, j.SPEND
      from #base as b join #july as j on b.ID_CONTACT = j.ID_CONTACT
      where j.SPEND > 0) as t;

-- порог по периоду «ДО» — только активные
drop table if exists #q_bef;
select distinct
       P25 = PERCENTILE_CONT(0.25) within group (order by SPEND * 1.0) over ()
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND * 1.0) over ()
into #q_bef
from (select f.SPEND
      from #before as f join #base as b on b.ID_CONTACT = f.ID_CONTACT
      where b.SEGMENT = 2 and f.SPEND > 0) as t;

DECLARE @cut_bef float = (select P75 + @tukey_k * (P75 - P25) from #q_bef);

drop table if exists #cuts;
select s.SEGMENT
     , CUT_JULY = isnull(q.P75 + @tukey_k * (q.P75 - q.P25), 1e18)
     , CUT_BEF  = IIF(s.SEGMENT = 2, isnull(@cut_bef, 1e18), 1e18)   -- у оттока «ДО» не проверяется
into #cuts
from (select distinct SEGMENT from #base) as s
     left join #q_july as q on s.SEGMENT = q.SEGMENT;

drop table if exists #outliers;
select b.ID_CONTACT
     , b.SEGMENT
     , b.GRP
     , JULY_SPEND = isnull(j.SPEND, 0)
     , BEF_SPEND  = isnull(f.SPEND, 0)
into #outliers
from #base as b
     join      #cuts   as k on b.SEGMENT = k.SEGMENT
     left join #july   as j on b.ID_CONTACT = j.ID_CONTACT
     left join #before as f on b.ID_CONTACT = f.ID_CONTACT
where isnull(j.SPEND, 0) > k.CUT_JULY
   or isnull(f.SPEND, 0) > k.CUT_BEF;

-- что отсечено
select [Сегмент]       = IIF(o.SEGMENT = 2, 'Активные', 'Отток')
     , [Группа]        = IIF(o.GRP = 'TG', 'ЦГ', 'КГ')
     , [Порог июль]    = k.CUT_JULY
     , [Порог ДО]      = IIF(o.SEGMENT = 2, k.CUT_BEF, null)
     , [Клиентов]      = count(*)
     , [ТО июль]       = sum(o.JULY_SPEND)
     , [Макс ТО июль]  = max(o.JULY_SPEND)
from #outliers as o
     join #cuts as k on o.SEGMENT = k.SEGMENT
group by o.SEGMENT, o.GRP, k.CUT_JULY, k.CUT_BEF
order by o.SEGMENT, o.GRP;

-- чистка: клиент-выброс убирается целиком
delete b from #base   as b where exists (select 1 from #outliers as o where o.ID_CONTACT = b.ID_CONTACT);
delete j from #july   as j where exists (select 1 from #outliers as o where o.ID_CONTACT = j.ID_CONTACT);
delete f from #before as f where exists (select 1 from #outliers as o where o.ID_CONTACT = f.ID_CONTACT);


-----------------------------------------------------------------------
-- 6. Агрегаты по сегменту и группе (уже без выбросов)
-----------------------------------------------------------------------
drop table if exists #stat;
select b.SEGMENT
     , b.GRP
     , CLIENTS   = count(*)
     , BUYERS    = sum(IIF(j.ID_CONTACT is not null, 1, 0))
     , TO_PERIOD = sum(isnull(j.SPEND, 0))
     , CHECKS    = sum(isnull(j.CHECKS, 0))
     , TO_BEFORE = sum(isnull(f.SPEND, 0))        -- 0 у оттока: #before только по активным
into #stat
from #base as b
     left join #july   as j on b.ID_CONTACT = j.ID_CONTACT
     left join #before as f on b.ID_CONTACT = f.ID_CONTACT
group by b.SEGMENT, b.GRP;


-----------------------------------------------------------------------
-- 7. ОТЧЁТ
-----------------------------------------------------------------------
drop table if exists #report;
select [ID_ORGANIZATION] = cast(null as int)
     , [Канал продаж]    = cast(null as varchar(30))
     , [YEAR_MONTH]      = @ym
     , [Год]             = @yy
     , [Месяц]           = @mm
     , [Сегментканала]   = cast(null as varchar(30))
     , [SEGMENT]         = t.SEGMENT
     , [Сегмент]         = IIF(t.SEGMENT = 2, 'Активные', 'Отток')
     , [Доп ТО р] =
           IIF(t.SEGMENT = 2
             -- активные: поправка на «ДО»
             , t.TO_PERIOD - t.TO_BEFORE * (c.TO_PERIOD * 1.0 / nullif(c.TO_BEFORE, 0))
             -- отток: приведение контроля к численности ЦГ
             , t.TO_PERIOD - c.TO_PERIOD * 1.0 / nullif(c.CLIENTS, 0) * t.CLIENTS)
     , [Клиентов ЦГ]     = t.CLIENTS
     , [Клиентов КГ]     = c.CLIENTS
     , [ТО ЦГ]           = t.TO_PERIOD
     , [ТО КГ]           = c.TO_PERIOD
     , [ТО ЦГ ДО]        = t.TO_BEFORE
     , [ТО КГ ДО]        = c.TO_BEFORE
     , [Бюджет ЦГ]       = t.TO_PERIOD * 1.0 / nullif(t.BUYERS, 0)
     , [Бюджет КГ]       = c.TO_PERIOD * 1.0 / nullif(c.BUYERS, 0)
     , [Ср чек ЦГ]       = t.TO_PERIOD * 1.0 / nullif(t.CHECKS, 0)
     , [Ср чек КГ]       = c.TO_PERIOD * 1.0 / nullif(c.CHECKS, 0)
     , [BUDGET]          = t.TO_PERIOD * 1.0 / nullif(t.CLIENTS, 0)
     , [BUDGET_CG]       = c.TO_PERIOD * 1.0 / nullif(c.CLIENTS, 0)
     , [BUDGET_bef]      = t.TO_BEFORE * 1.0 / nullif(t.CLIENTS, 0)
     , [BUDGET_CG_bef]   = c.TO_BEFORE * 1.0 / nullif(c.CLIENTS, 0)
into #report
from #stat as t
     join #stat as c on t.SEGMENT = c.SEGMENT and t.GRP = 'TG' and c.GRP = 'CG';

select * from #report order by [SEGMENT];

-- итоговая строка
select [Сегмент]     = 'ИТОГО'
     , [Доп ТО р]    = sum([Доп ТО р])
     , [Клиентов ЦГ] = sum([Клиентов ЦГ])
     , [Клиентов КГ] = sum([Клиентов КГ])
     , [ТО ЦГ]       = sum([ТО ЦГ])
     , [ТО КГ]       = sum([ТО КГ])
from #report;
