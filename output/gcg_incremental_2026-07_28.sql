/*==============================================================================
  ОТЧЁТ ПО ГКГ ЗА ИЮЛЬ 2026. ВЕРСИЯ 28 = _27 с ИСПРАВЛЕНИЕМ ОШИБКИ.

  В _27 блок квартилей был написан как "select * ... group by" — SQL Server
  такую конструкцию не принимает, а подзапрос с оконной функцией без
  агрегации возвращал строку на каждого клиента. Возвращён рабочий приём
  из _11: два отдельных "select distinct".

  ФОРМА ОТЧЁТА — как в отчёте по ГКГ: две строки, 23 колонки, строка ИТОГО.
      SEGMENT = 2  Активные              - покупка в последние 6 недель
      SEGMENT = 5  Отток (со спящими)    - всё остальное
  Новые исключены: их нет в ГКГ, пары для сравнения у них не существует.

  ФОРМУЛА - единая для обоих сегментов, различается только окно «ДО»:
      KOEF   = ТО КГ / ТО КГ ДО                (безразмерный индекс контроля)
      Доп ТО = ТО ЦГ - ТО ЦГ ДО * KOEF
  Приведение по численности не нужно: базой служит собственный оборот группы
  за «ДО», он уже в её масштабе, а контроль даёт только индекс изменения.

  ОКНО «ДО»:
      Активные - 6 недель до начала месяца (20.05-30.06.2026)
      Отток    - ВЕСЬ ПЕРИОД НАБЛЮДЕНИЙ, вся история покупок до 01.07.2026.
                 Шестинедельное окно им не годится: они там не покупали по
                 определению, иначе были бы активными, и KOEF выродился бы.

  ВЫБРОСЫ - удаляются, клиент выбывает целиком из обеих групп:
      границы Тьюки: сверху P75 + k*IQR, снизу P25 - k*IQR;
      квартили считаются ВНУТРИ СЕГМЕНТА и только по ненулевым суммам —
      у оттока медиана июльского оборота нулевая, общий порог на смеси
      сегментов был бы неверен сразу для обоих;
      проверяются ОБА периода, июль и «ДО», у каждого свои границы;
      порог общий для ЦГ и КГ, отдельных порогов по группам нет.
      Нижняя граница ловит крупные возвраты и сторно.

  ЧТО ДОБАВЛЕНО ПРОТИВ _11:
      - ошибка, доверительный интервал и вердикт значимости рядом с доп ТО:
        без них точечная цифра по ГКГ не интерпретируется;
      - диагностика удалённых (сколько клиентов и оборота, по группам) —
        под @debug, чтобы не засорять отчёт.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc   int = 1;
DECLARE @debug bit = 0;              -- 1 = показать пороги и что удалено

DECLARE @dt_from  date = '2026-07-01';
DECLARE @dt_to    date = '2026-08-01';                  -- граница НЕ включается
DECLARE @pre_date date = dateadd(day, -1, @dt_from);    -- 30.06.2026

DECLARE @active_days int = 6 * 7;    -- граница активных
DECLARE @new_days    int = 6 * 7;    -- новые: первая покупка в последние 6 недель

DECLARE @pre_weeks int = 6;                                      -- окно «ДО» активных
DECLARE @pre_from  date = dateadd(week, -@pre_weeks, @dt_from);  -- 20.05.2026

DECLARE @tukey_k float = 3.0;        -- 3 = только дальние выбросы, 1.5 = строже

DECLARE @ym int = year(@dt_from) * 100 + month(@dt_from);
DECLARE @yy int = year(@dt_from);
DECLARE @mm int = month(@dt_from);


-----------------------------------------------------------------------
-- 1. Контрольная группа (дедуплицированная)
-----------------------------------------------------------------------
drop table if exists #cg;
select ID_CONTACT
into #cg
from I_GLOBAL_CG (nolock)
where ID_COMPANY = @idc and CONTROL_GROUP = 1
group by ID_CONTACT;


-----------------------------------------------------------------------
-- 2. Популяция и сегмент. Новые отсечены сразу, having по первой покупке.
-----------------------------------------------------------------------
drop table if exists #hist;
select a.ID_CONTACT
     , SEGMENT = IIF(datediff(day, max(a.DATA), @pre_date) <= @active_days, 2, 5)
into #hist
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT
having datediff(day, min(a.DATA), @pre_date) > @new_days;


-----------------------------------------------------------------------
-- 3. Обороты: июль и «ДО» (окно зависит от сегмента)
-----------------------------------------------------------------------
drop table if exists #july;
select a.ID_CONTACT
     , SPEND  = sum(cast(a.COST_DISCOUNT as float))
     , CHECKS = count(a.ID_CHECK)
into #july
from I_CHECKHEADER as a (nolock)
     join #hist as h on h.ID_CONTACT = a.ID_CONTACT
where a.ID_COMPANY = @idc
  and a.DATA >= @dt_from and a.DATA < @dt_to
group by a.ID_CONTACT;

drop table if exists #before;
select a.ID_CONTACT
     , SPEND = sum(cast(a.COST_DISCOUNT as float))
into #before
from I_CHECKHEADER as a (nolock)
     join #hist as h on h.ID_CONTACT = a.ID_CONTACT
where a.ID_COMPANY = @idc
  and a.DATA < @dt_from
  and (h.SEGMENT = 5                                  -- отток: вся история
       or a.DATA >= @pre_from)                        -- активные: 6 недель
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 4. Клиентская таблица
-----------------------------------------------------------------------
drop table if exists #cl;
select h.ID_CONTACT
     , h.SEGMENT
     , GRP    = IIF(c.ID_CONTACT is not null, 'CG', 'TG')   -- TG = ЦГ
     , Y      = isnull(j.SPEND, 0)
     , CHECKS = isnull(j.CHECKS, 0)
     , BUYER  = IIF(j.ID_CONTACT is not null, 1, 0)
     , X      = isnull(f.SPEND, 0)
into #cl
from #hist as h
     left join #cg     as c on h.ID_CONTACT = c.ID_CONTACT
     left join #july   as j on h.ID_CONTACT = j.ID_CONTACT
     left join #before as f on h.ID_CONTACT = f.ID_CONTACT;


-----------------------------------------------------------------------
-- 5. Границы выбросов: внутри сегмента, по ненулевым, для обоих периодов
-----------------------------------------------------------------------
-- квартили июля: внутри сегмента, только по ненулевым суммам
drop table if exists #q_y;
select distinct
       SEGMENT
     , P25 = PERCENTILE_CONT(0.25) within group (order by SPEND) over (partition by SEGMENT)
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND) over (partition by SEGMENT)
into #q_y
from (select SEGMENT, SPEND = Y from #cl where Y <> 0) as t;

-- квартили периода «ДО»: так же, окна у сегментов разные, поэтому по сегменту
drop table if exists #q_x;
select distinct
       SEGMENT
     , P25 = PERCENTILE_CONT(0.25) within group (order by SPEND) over (partition by SEGMENT)
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND) over (partition by SEGMENT)
into #q_x
from (select SEGMENT, SPEND = X from #cl where X <> 0) as t;

drop table if exists #cuts;
select s.SEGMENT
     , Y_HI  = isnull(qy.P75 + @tukey_k * (qy.P75 - qy.P25),  1e18)
     , Y_LOW = isnull(qy.P25 - @tukey_k * (qy.P75 - qy.P25), -1e18)
     , X_HI  = isnull(qx.P75 + @tukey_k * (qx.P75 - qx.P25),  1e18)
     , X_LOW = isnull(qx.P25 - @tukey_k * (qx.P75 - qx.P25), -1e18)
into #cuts
from (select distinct SEGMENT from #cl) as s
     left join #q_y as qy on qy.SEGMENT = s.SEGMENT
     left join #q_x as qx on qx.SEGMENT = s.SEGMENT;

if @debug = 1
begin
select [Сегмент] = IIF(b.SEGMENT = 2, 'Активные', 'Отток (со спящими)')
     , [Группа]  = IIF(b.GRP = 'TG', 'ЦГ', 'КГ')
     , [Клиентов до чистки] = count(*)
     , [Удаляется]          = sum(IIF(b.Y > k.Y_HI or b.Y < k.Y_LOW or b.X > k.X_HI or b.X < k.X_LOW, 1, 0))
     , [Доля удалённых, %]  = cast(100.0 * sum(IIF(b.Y > k.Y_HI or b.Y < k.Y_LOW or b.X > k.X_HI or b.X < k.X_LOW, 1, 0)) / count(*) as decimal(8,4))
     , [Их ТО июля, руб]    = sum(IIF(b.Y > k.Y_HI or b.Y < k.Y_LOW or b.X > k.X_HI or b.X < k.X_LOW, b.Y, 0))
     , [Порог июль верх]    = max(k.Y_HI)
     , [Порог июль низ]     = max(k.Y_LOW)
     , [Порог ДО верх]      = max(k.X_HI)
from #cl as b join #cuts as k on b.SEGMENT = k.SEGMENT
group by b.SEGMENT, b.GRP
order by b.SEGMENT, b.GRP;
end;

delete b
from #cl as b join #cuts as k on b.SEGMENT = k.SEGMENT
where b.Y > k.Y_HI or b.Y < k.Y_LOW
   or b.X > k.X_HI or b.X < k.X_LOW;


-----------------------------------------------------------------------
-- 6. Агрегаты
-----------------------------------------------------------------------
drop table if exists #stat;
select SEGMENT, GRP
     , CLIENTS   = count(*) * 1.0
     , BUYERS    = sum(BUYER * 1.0)
     , TO_PERIOD = sum(Y)
     , TO_BEFORE = sum(X)
     , CHK       = sum(CHECKS * 1.0)
     , MX        = avg(X)
into #stat
from #cl
group by SEGMENT, GRP;


-----------------------------------------------------------------------
-- 7. ОТЧЁТ
-----------------------------------------------------------------------
drop table if exists #rep;
select [ID_ORGANIZATION] = cast(null as int)
     , [Канал продаж]    = cast(null as varchar(30))
     , [YEAR_MONTH]      = @ym
     , [Год]             = @yy
     , [Месяц]           = @mm
     , [Сегмент канала]  = cast(null as varchar(30))
     , [SEGMENT]         = t.SEGMENT
     , [Сегмент]         = IIF(t.SEGMENT = 2, 'Активные', 'Отток')
     , [Доп ТО р]        = t.TO_PERIOD - t.TO_BEFORE * (c.TO_PERIOD / nullif(c.TO_BEFORE, 0))
     , [KOEF]            = c.TO_PERIOD / nullif(c.TO_BEFORE, 0)
     , [Клиентов ЦГ]     = t.CLIENTS
     , [Клиентов КГ]     = c.CLIENTS
     , [ТО ЦГ]           = t.TO_PERIOD
     , [ТО КГ]           = c.TO_PERIOD
     , [ТО ЦГ ДО]        = t.TO_BEFORE
     , [ТО КГ ДО]        = c.TO_BEFORE
     , [Бюджет ЦГ]       = t.TO_PERIOD / nullif(t.BUYERS, 0)
     , [Бюджет КГ]       = c.TO_PERIOD / nullif(c.BUYERS, 0)
     , [Ср чек ЦГ]       = t.TO_PERIOD / nullif(t.CHK, 0)
     , [Ср чек КГ]       = c.TO_PERIOD / nullif(c.CHK, 0)
     , [BUDGET]          = t.TO_PERIOD / nullif(t.CLIENTS, 0)
     , [BUDGET_CG]       = c.TO_PERIOD / nullif(c.CLIENTS, 0)
     , [BUDGET_bef]      = t.TO_BEFORE / nullif(t.CLIENTS, 0)
     , [BUDGET_CG_bef]   = c.TO_BEFORE / nullif(c.CLIENTS, 0)
into #rep
from #stat as t
     join #stat as c on t.SEGMENT = c.SEGMENT and t.GRP = 'TG' and c.GRP = 'CG';

-- ошибка оценки: дельта-метод для отношения, D = Y - KOEF*X в каждой группе
drop table if exists #se;
select b.SEGMENT
     , SE = r.NT * sqrt(  var(IIF(b.GRP = 'TG', b.Y - r.KOEF * b.X, null)) / r.NT
                        + square(r.MXT / nullif(r.MXC, 0))
                          * var(IIF(b.GRP = 'CG', b.Y - r.KOEF * b.X, null)) / r.NC)
into #se
from #cl as b
     join (select t.SEGMENT
                , KOEF = c.TO_PERIOD / nullif(c.TO_BEFORE, 0)
                , NT = t.CLIENTS, NC = c.CLIENTS, MXT = t.MX, MXC = c.MX
           from #stat as t join #stat as c
             on t.SEGMENT = c.SEGMENT and t.GRP = 'TG' and c.GRP = 'CG') as r
       on r.SEGMENT = b.SEGMENT
group by b.SEGMENT, r.NT, r.NC, r.KOEF, r.MXT, r.MXC;

select r.*
     , [SE]           = e.SE
     , [ДИ 95% низ]   = r.[Доп ТО р] - 1.96 * e.SE
     , [ДИ 95% верх]  = r.[Доп ТО р] + 1.96 * e.SE
     , [t]            = r.[Доп ТО р] / nullif(e.SE, 0)
     , [Значимо]      = IIF(abs(r.[Доп ТО р] / nullif(e.SE, 0)) >= 1.96, 'ДА', 'НЕТ')
from #rep as r join #se as e on e.SEGMENT = r.[SEGMENT]
order by r.[SEGMENT];

-- ИТОГО
select [Сегмент]     = 'ИТОГО'
     , [Доп ТО р]    = sum(r.[Доп ТО р])
     , [Клиентов ЦГ] = sum(r.[Клиентов ЦГ])
     , [Клиентов КГ] = sum(r.[Клиентов КГ])
     , [ТО ЦГ]       = sum(r.[ТО ЦГ])
     , [ТО КГ]       = sum(r.[ТО КГ])
     , [SE]          = sqrt(sum(square(e.SE)))
     , [ДИ 95% низ]  = sum(r.[Доп ТО р]) - 1.96 * sqrt(sum(square(e.SE)))
     , [ДИ 95% верх] = sum(r.[Доп ТО р]) + 1.96 * sqrt(sum(square(e.SE)))
from #rep as r join #se as e on e.SEGMENT = r.[SEGMENT];
