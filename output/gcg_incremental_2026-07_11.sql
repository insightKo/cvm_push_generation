/*==============================================================================
  ОТЧЁТ ПО ГКГ ЗА ИЮЛЬ 2026.
  ВЕРСИЯ 11: поправочный коэффициент на период «ДО» применяется к ОБОИМ
             сегментам. Формула единая, различается только окно «ДО».

  ФОРМУЛА (оба сегмента):
      KOEF   = ТО КГ / ТО КГ ДО                  (чистый прирост контроля)
      Доп ТО = ТО ЦГ - ТО ЦГ ДО * KOEF

  ОКНО «ДО»:
      Активные (SEGMENT = 2) — 6 недель до начала месяца (20.05-30.06.2026).
      Отток    (SEGMENT = 5) — ВЕСЬ ПЕРИОД НАБЛЮДЕНИЙ, вся история покупок
                               до 01.07.2026. Шестинедельное окно для оттока
                               не годится: там покупок нет по определению,
                               иначе клиент был бы активным, и коэффициент
                               выродился бы в деление на ноль.

  Приведение по численности больше не нужно ни одному сегменту: базой служит
  собственный оборот группы за «ДО», он уже в её масштабе, а контроль даёт
  только безразмерный индекс изменения.

  СЕГМЕНТЫ (по дате последней покупки до июля, отсчёт от 30.06.2026):
      SEGMENT = 2  Активные — покупка в последние 6 недель
      SEGMENT = 5  Отток    — всё остальное, СПЯЩИЕ ВКЛЮЧЕНЫ В ОТТОК

  ОТСЕЧЕНИЕ ВЫБРОСОВ (границы Тьюки, k = 3):
      верхняя граница = P75 + k*IQR,  нижняя = P25 - k*IQR
    - квартили считаются ВНУТРИ КАЖДОГО СЕГМЕНТА, по ненулевым суммам,
      отдельно для июля и отдельно для «ДО» этого сегмента;
    - нижняя граница ловит крупные отрицательные итоги (возвраты, сторно);
    - оба сегмента проверяются и по июлю, и по «ДО» — теперь «ДО» входит
      в формулу у обоих;
    - клиент-выброс удаляется ЦЕЛИКОМ: и из оборота, и из численности,
      одинаково из ЦГ и из КГ. Отсечение по итогу клиента за период,
      а не по отдельному чеку.

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

DECLARE @pre_weeks int = 6;                                      -- «ДО» активных = 6 недель
DECLARE @pre_from  date = dateadd(week, -@pre_weeks, @dt_from);  -- 20.05.2026
-- «ДО» оттока — вся история до @dt_from, отдельной границы не требуется

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
-- 4. Покупки июля и периода «ДО»
--    Окно «ДО» зависит от сегмента: активные — 6 недель, отток — вся история
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
     join #base as b on b.ID_CONTACT = a.ID_CONTACT
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
  and (b.SEGMENT = 5                    -- отток: вся история до июля
       or a.DATA >= @pre_from)          -- активные: последние 6 недель
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 5. ВЫБРОСЫ: границы сверху и снизу внутри сегмента, отчёт и чистка
-----------------------------------------------------------------------
-- квартили по июлю — свои для каждого сегмента, по ненулевым суммам
drop table if exists #q_july;
select distinct
       SEGMENT
     , P25 = PERCENTILE_CONT(0.25) within group (order by SPEND * 1.0) over (partition by SEGMENT)
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND * 1.0) over (partition by SEGMENT)
into #q_july
from (select b.SEGMENT, j.SPEND
      from #base as b join #july as j on b.ID_CONTACT = j.ID_CONTACT
      where j.SPEND <> 0) as t;

-- квартили по «ДО» — тоже свои для каждого сегмента (окна разные)
drop table if exists #q_bef;
select distinct
       SEGMENT
     , P25 = PERCENTILE_CONT(0.25) within group (order by SPEND * 1.0) over (partition by SEGMENT)
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND * 1.0) over (partition by SEGMENT)
into #q_bef
from (select b.SEGMENT, f.SPEND
      from #base as b join #before as f on b.ID_CONTACT = f.ID_CONTACT
      where f.SPEND <> 0) as t;

drop table if exists #cuts;
select s.SEGMENT
     , CUT_JULY_HI  = isnull(qj.P75 + @tukey_k * (qj.P75 - qj.P25),  1e18)
     , CUT_JULY_LOW = isnull(qj.P25 - @tukey_k * (qj.P75 - qj.P25), -1e18)
     , CUT_BEF_HI   = isnull(qb.P75 + @tukey_k * (qb.P75 - qb.P25),  1e18)
     , CUT_BEF_LOW  = isnull(qb.P25 - @tukey_k * (qb.P75 - qb.P25), -1e18)
into #cuts
from (select distinct SEGMENT from #base) as s
     left join #q_july as qj on s.SEGMENT = qj.SEGMENT
     left join #q_bef  as qb on s.SEGMENT = qb.SEGMENT;

drop table if exists #outliers;
select b.ID_CONTACT
     , b.SEGMENT
     , b.GRP
     , JULY_SPEND = isnull(j.SPEND, 0)
     , BEF_SPEND  = isnull(f.SPEND, 0)
     , REASON = case when isnull(j.SPEND, 0) > k.CUT_JULY_HI  then 'июль — верх'
                     when isnull(j.SPEND, 0) < k.CUT_JULY_LOW then 'июль — низ (минус)'
                     when isnull(f.SPEND, 0) > k.CUT_BEF_HI   then 'ДО — верх'
                     else 'ДО — низ (минус)' end
into #outliers
from #base as b
     join      #cuts   as k on b.SEGMENT = k.SEGMENT
     left join #july   as j on b.ID_CONTACT = j.ID_CONTACT
     left join #before as f on b.ID_CONTACT = f.ID_CONTACT
where isnull(j.SPEND, 0) > k.CUT_JULY_HI
   or isnull(j.SPEND, 0) < k.CUT_JULY_LOW
   or isnull(f.SPEND, 0) > k.CUT_BEF_HI
   or isnull(f.SPEND, 0) < k.CUT_BEF_LOW;

-- что отсечено и почему
select [Сегмент]         = IIF(o.SEGMENT = 2, 'Активные', 'Отток')
     , [Группа]          = IIF(o.GRP = 'TG', 'ЦГ', 'КГ')
     , o.REASON
     , [Порог июль верх] = k.CUT_JULY_HI
     , [Порог июль низ]  = k.CUT_JULY_LOW
     , [Порог ДО верх]   = k.CUT_BEF_HI
     , [Порог ДО низ]    = k.CUT_BEF_LOW
     , [Клиентов]        = count(*)
     , [ТО июль]         = sum(o.JULY_SPEND)
     , [Макс ТО июль]    = max(o.JULY_SPEND)
     , [Мин ТО июль]     = min(o.JULY_SPEND)
from #outliers as o
     join #cuts as k on o.SEGMENT = k.SEGMENT
group by o.SEGMENT, o.GRP, o.REASON
       , k.CUT_JULY_HI, k.CUT_JULY_LOW, k.CUT_BEF_HI, k.CUT_BEF_LOW
order by o.SEGMENT, o.GRP, o.REASON;

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
     , TO_BEFORE = sum(isnull(f.SPEND, 0))
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
       -- единая формула: поправка на «ДО» для обоих сегментов
     , [Доп ТО р]        = t.TO_PERIOD - t.TO_BEFORE * (c.TO_PERIOD * 1.0 / nullif(c.TO_BEFORE, 0))
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
