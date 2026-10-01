/*==============================================================================
  ДОП ТО ЗА ИЮЛЬ 2026 ПО ГЛОБАЛЬНОЙ КГ — В РАЗРЕЗЕ ЖИЗНЕННОГО ЦИКЛА.
  ВЕРСИЯ 4: БЕЗ ОТСЕЧЕНИЯ ВЫБРОСОВ — в расчёт входят все клиенты.

  Формула (считается ОТДЕЛЬНО внутри каждого сегмента):
    K            = N_масса / N_КГ                (во сколько раз масса больше КГ)
    ТО_ожидаемый = ТО_КГ * K                     (сколько бы масса сделала без CVM)
    ДОП_ТО       = ТО_масса - ТО_ожидаемый

  Сегменты — по дате последней покупки ДО июля (отсчёт от 30.06.2026):
    1-ACTIVE        покупка в последние 6 недель      (20.05-30.06)
    2-OTTOK_6_15w   последняя покупка 6-15 недель назад (18.03-19.05)
    3-SLEEPING_15w+ последняя покупка больше 15 недель назад
  ВНИМАНИЕ: в gcg_extract.sql эти два диапазона названы наоборот (6-15 нед. =
  SLEEPING, >15 = CHURN). Здесь канон Елены, в названиях указаны недели.

  Считать сегменты вместе НЕЛЬЗЯ: у активных ТО на клиента на порядок выше,
  общий итог был бы средним по ним, а эффект на оттоке — не виден.

  НОВЫЕ ИСКЛЮЧЕНЫ (блок 6). Клиент с первой покупкой в последние 6 недель не
  мог попасть в ГКГ: она нарезалась по когортам MAX_YM на момент прогона
  gcg_extract.sql. При left join такие клиенты ВСЕГДА получают GRP = 'MASS' —
  падают в массу односторонне, и весь их ТО уезжает в «эффект». Там же
  отсекаются когорты привлечения с нулевым покрытием ГКГ.

  ВЫБРОСЫ НЕ ОТСЕКАЮТСЯ. Учитывать при чтении результата: КГ примерно в 35 раз
  меньше массы, поэтому один аномальный клиент в контроле двигает ТО_КГ, а
  через K_SCALE — и весь доп ТО сегмента. Смотреть T_STAT и SIGNIFICANT
  (блок 9) и максимальные чеки, прежде чем выносить цифру в отчёт.

  БАЛАНС: блок 8 сверяет группы до воздействия — по тратам пред-периода
  (26 недель) и по LTV. Если расхождение > 1-2%, смотреть DOP_TO_ADJ.

  КГ дедуплицируется: контакт в I_GLOBAL_CG может лежать несколько раз
  (когорты/перезаливки) — иначе join размножает строки и раздувает группы.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;

DECLARE @dt_from date = '2026-07-01';
DECLARE @dt_to   date = '2026-08-01';                     -- правая граница НЕ включается
DECLARE @pre_date date = dateadd(day, -1, @dt_from);      -- 30.06.2026, точка отсчёта цикла

DECLARE @active_days int = 6  * 7;   -- АКТИВНЫЕ: покупка в последние 6 недель
DECLARE @ottok_days  int = 15 * 7;   -- ОТТОК: 6-15 недель; дальше — СПЯЩИЕ
DECLARE @new_days    int = 6  * 7;   -- НОВЫЕ: первая покупка в последние 6 недель

DECLARE @pre_weeks int = 26;                                     -- окно пред-периода для баланса
DECLARE @pre_from  date = dateadd(week, -@pre_weeks, @dt_from);  -- 31.12.2025


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
-- 2. История до июля: первая/последняя покупка, LTV
-----------------------------------------------------------------------
drop table if exists #hist;
select a.ID_CONTACT
     , MIN_DATA    = min(a.DATA)
     , MAX_DATA    = max(a.DATA)
     , LTV         = sum(a.COST_DISCOUNT)
     , COUNT_CHECK = count(a.ID_CHECK)
into #hist
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 3. Пред-период: траты за 26 недель до июля (для проверки баланса)
--    Окно намеренно длиннее 6 недель: иначе у оттока оно было бы нулевым
--    по построению и сверять группы было бы не по чему.
-----------------------------------------------------------------------
drop table if exists #pre;
select a.ID_CONTACT
     , PRE_SPEND  = sum(a.COST_DISCOUNT)
     , PRE_CHECKS = count(a.ID_CHECK)
into #pre
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @pre_from
  and a.DATA <  @dt_from
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 4. База сравнения: все клиенты до июля + группа + сегмент цикла
-----------------------------------------------------------------------
drop table if exists #base;
select h.ID_CONTACT
     , GRP    = IIF(c.ID_CONTACT is not null, 'CONTROL', 'MASS')
     , IS_NEW = IIF(datediff(day, h.MIN_DATA, @pre_date) <= @new_days, 1, 0)
     , ACQ_YM = year(h.MIN_DATA) * 100 + month(h.MIN_DATA)
     , LIFECYCLE = case
           when datediff(day, h.MAX_DATA, @pre_date) <= @active_days then '1-ACTIVE'
           when datediff(day, h.MAX_DATA, @pre_date) <= @ottok_days  then '2-OTTOK_6_15w'
           else '3-SLEEPING_15w+' end
     , h.LTV
     , h.COUNT_CHECK
     , PRE_SPEND  = isnull(p.PRE_SPEND, 0)
     , PRE_CHECKS = isnull(p.PRE_CHECKS, 0)
into #base
from #hist as h
     left join #cg  as c on h.ID_CONTACT = c.ID_CONTACT
     left join #pre as p on h.ID_CONTACT = p.ID_CONTACT;


-----------------------------------------------------------------------
-- 5. Покупки июля (только по базе сравнения)
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


-----------------------------------------------------------------------
-- 6. НОВЫЕ И НЕПОКРЫТЫЕ КОГОРТЫ: диагностика и исключение
--    У месяцев, покрытых нарезкой ГКГ, CG_SHARE_PCT ~ 2.9% (1/35). Там, где
--    доля падает до 0, ГКГ клиентов не покрывает — они дают только массу.
-----------------------------------------------------------------------
select b.ACQ_YM
     , IS_NEW_IN_WINDOW = max(b.IS_NEW)
     , CLIENTS          = count(*)
     , CG_CLIENTS       = sum(IIF(b.GRP = 'CONTROL', 1, 0))
     , CG_SHARE_PCT     = cast(100.0 * sum(IIF(b.GRP = 'CONTROL', 1, 0)) / count(*) as decimal(6,3))
from #base as b
where b.ACQ_YM >= 202601         -- последние месяцы привлечения; убери фильтр для всей истории
group by b.ACQ_YM
order by b.ACQ_YM;

-- итог по новым перед исключением
select GRP               = b.GRP
     , NEW_CLIENTS       = count(*)
     , NEW_TO_JULY       = sum(isnull(j.SPEND, 0))
     , NEW_TO_PER_CLIENT = avg(isnull(j.SPEND, 0) * 1.0)
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
where b.IS_NEW = 1
group by b.GRP;

-- исключаем новых ИЗ ОБЕИХ ГРУПП (не только помечаем)
delete b from #base as b where b.IS_NEW = 1;

-- ...и когорты привлечения с нулевым покрытием ГКГ
drop table if exists #uncovered;
select ACQ_YM
     , CLIENTS = count(*)
into #uncovered
from #base
group by ACQ_YM
having sum(IIF(GRP = 'CONTROL', 1, 0)) = 0;

select UNCOVERED_ACQ_YM     = u.ACQ_YM
     , MASS_CLIENTS_DROPPED = u.CLIENTS
from #uncovered as u
order by u.ACQ_YM;

delete b from #base as b where exists (select 1 from #uncovered as u where u.ACQ_YM = b.ACQ_YM);

-- синхронизируем июльские покупки с очищенной базой
delete j from #july as j where not exists (select 1 from #base as b where b.ID_CONTACT = j.ID_CONTACT);


-----------------------------------------------------------------------
-- 7. Метрики по сегментам и группам (все клиенты, выбросы НЕ отсекались)
--    MAX_JULY_SPEND — контроль аномалий вручную: если максимум в КГ на
--    порядок выше среднего, доп ТО сегмента держится на одном клиенте.
-----------------------------------------------------------------------
drop table if exists #stat;
select b.LIFECYCLE
     , b.GRP
     , CLIENTS           = count(*)
     , BUYERS            = sum(IIF(j.ID_CONTACT is not null, 1, 0))
     , RESPONSE_PCT      = cast(100.0 * sum(IIF(j.ID_CONTACT is not null, 1, 0)) / count(*) as decimal(6,3))
     , TO_JULY           = sum(isnull(j.SPEND, 0))
     , TO_PER_CLIENT     = avg(isnull(j.SPEND, 0) * 1.0)
     , CHECKS_PER_CLIENT = avg(isnull(j.CHECKS, 0) * 1.0)
     , AVG_CHECK         = sum(isnull(j.SPEND, 0)) * 1.0 / nullif(sum(isnull(j.CHECKS, 0)), 0)
     , TO_STDEV          = stdev(isnull(j.SPEND, 0) * 1.0)
     , MAX_JULY_SPEND    = max(isnull(j.SPEND, 0))
     , PRE_PER_CLIENT    = avg(b.PRE_SPEND * 1.0)
     , LTV_PER_CLIENT    = avg(b.LTV * 1.0)
into #stat
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
group by b.LIFECYCLE, b.GRP;

select * from #stat order by LIFECYCLE, GRP;


-----------------------------------------------------------------------
-- 8. БАЛАНС ГРУПП ДО ВОЗДЕЙСТВИЯ (должно совпадать внутри сегмента)
-----------------------------------------------------------------------
select m.LIFECYCLE
     , N_MASS        = m.CLIENTS
     , N_CG          = c.CLIENTS
     , PRE_MASS      = m.PRE_PER_CLIENT
     , PRE_CG        = c.PRE_PER_CLIENT
     , PRE_DIFF_PCT  = cast(100.0 * (m.PRE_PER_CLIENT - c.PRE_PER_CLIENT)
                            / nullif(c.PRE_PER_CLIENT, 0) as decimal(8,3))
     , LTV_MASS      = m.LTV_PER_CLIENT
     , LTV_CG        = c.LTV_PER_CLIENT
     , LTV_DIFF_PCT  = cast(100.0 * (m.LTV_PER_CLIENT - c.LTV_PER_CLIENT)
                            / nullif(c.LTV_PER_CLIENT, 0) as decimal(8,3))
from #stat as m
     join #stat as c on m.LIFECYCLE = c.LIFECYCLE and m.GRP = 'MASS' and c.GRP = 'CONTROL'
order by m.LIFECYCLE;


-----------------------------------------------------------------------
-- 9. ДОП ТО ПО СЕГМЕНТАМ
-----------------------------------------------------------------------
drop table if exists #dop;
select m.LIFECYCLE
     , N_MASS      = m.CLIENTS
     , N_CG        = c.CLIENTS
     , K_SCALE     = m.CLIENTS * 1.0 / nullif(c.CLIENTS, 0)
     , TO_MASS     = m.TO_JULY
     , TO_CG       = c.TO_JULY
     , TO_EXPECTED = c.TO_JULY * (m.CLIENTS * 1.0 / nullif(c.CLIENTS, 0))
     , DOP_TO      = m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / nullif(c.CLIENTS, 0))
     -- с поправкой на расхождение групп до воздействия (по LTV — определён везде):
     , DOP_TO_ADJ  = m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / nullif(c.CLIENTS, 0))
                                * (m.LTV_PER_CLIENT / nullif(c.LTV_PER_CLIENT, 0))
     , RESPONSE_UPLIFT_PP = m.RESPONSE_PCT - c.RESPONSE_PCT
     , AVG_CHECK_UPLIFT   = m.AVG_CHECK - c.AVG_CHECK
     -- значимость: КГ мала, выбросы не отсекались, у оттока и спящих отклик
     -- низкий — эффект легко оказывается шумом. |T_STAT| >= 1.96 -> 95%.
     , T_STAT = (m.TO_PER_CLIENT - c.TO_PER_CLIENT)
                / nullif(sqrt(  square(m.TO_STDEV) / nullif(m.CLIENTS, 0)
                              + square(c.TO_STDEV) / nullif(c.CLIENTS, 0)), 0)
into #dop
from #stat as m
     join #stat as c on m.LIFECYCLE = c.LIFECYCLE and m.GRP = 'MASS' and c.GRP = 'CONTROL';

select d.*
     , DOP_TO_PCT  = cast(100.0 * d.DOP_TO / nullif(d.TO_EXPECTED, 0) as decimal(8,3))
     , SIGNIFICANT = IIF(abs(d.T_STAT) >= 1.96, 'ДА', 'НЕТ — в пределах шума')
from #dop as d
order by d.LIFECYCLE;


-----------------------------------------------------------------------
-- 10. ИТОГ ОДНОЙ СТРОКОЙ: только доп ТО, в рублях.
--     Значимость, T_STAT и версия с поправкой на баланс — в блоке 9.
-----------------------------------------------------------------------
select ACTIVE   = cast(sum(IIF(d.LIFECYCLE = '1-ACTIVE',        d.DOP_TO, 0)) as decimal(18,0))
     , OTTOK    = cast(sum(IIF(d.LIFECYCLE = '2-OTTOK_6_15w',   d.DOP_TO, 0)) as decimal(18,0))
     , SLEEPING = cast(sum(IIF(d.LIFECYCLE = '3-SLEEPING_15w+', d.DOP_TO, 0)) as decimal(18,0))
     , TOTAL    = cast(sum(d.DOP_TO) as decimal(18,0))
from #dop as d;
