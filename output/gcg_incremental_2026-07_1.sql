/*==============================================================================
  ДОП ТО ЗА ИЮЛЬ 2026 ПО ГЛОБАЛЬНОЙ КГ.
  База: АКТИВНЫЕ клиенты — покупка в 6 недель до июля (20.05-30.06.2026),
        привлечённые ДО этого окна.

  Формула:
    K            = N_масса / N_КГ                (во сколько раз масса больше КГ)
    ТО_ожидаемый = ТО_КГ * K                     (сколько бы масса сделала без CVM)
    ДОП_ТО       = ТО_масса - ТО_ожидаемый
    ДОП_ТО_%     = ДОП_ТО / ТО_ожидаемый

  БАЗА: клиенты с покупкой в 6 недель до 01.07.2026. Спящие и отток исключены —
  у них почти нули с редкими всплесками, вся лишняя дисперсия сидела в них.

  НОВЫЕ ИСКЛЮЧЕНЫ (блок 6). Клиент с первой покупкой внутри окна не мог попасть
  в ГКГ: она нарезалась по когортам MAX_YM на момент прогона gcg_extract.sql.
  При left join такие клиенты ВСЕГДА получают GRP = 'MASS' — попадают в массу
  односторонне, без пары в контроле, и весь их ТО уезжает в «эффект».

  ВЫБРОСЫ: клиент исключается ЦЕЛИКОМ (из обеих групп, по одному порогу),
  если его ТО выше порога Тьюки (P75 + 3*IQR) хотя бы в одном из периодов —
  в июле ИЛИ в пред-периоде. Отчёт об исключённых — блок 7.

  БАЛАНС: блок 10 сверяет группы на пред-периоде (до всякого CVM-воздействия).
  Если расхождение > 1-2%, смотреть DOP_TO_ADJ — доп ТО с поправкой на это
  расхождение. Если группы сбалансированы, DOP_TO и DOP_TO_ADJ почти равны.

  КГ дедуплицируется: контакт в I_GLOBAL_CG может лежать несколько раз
  (когорты/перезаливки) — иначе join размножает строки и раздувает группы.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;

DECLARE @dt_from date = '2026-07-01';
DECLARE @dt_to   date = '2026-08-01';   -- правая граница НЕ включается

DECLARE @active_weeks int = 6;                                      -- окно активности
DECLARE @pre_from date = dateadd(week, -@active_weeks, @dt_from);   -- 20.05.2026

DECLARE @tukey_k float = 3.0;           -- коэффициент Тьюки (3 = только «дальние» выбросы)


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
-- 2. Пред-период: покупки за 6 недель до июля (окно активности)
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
-- 3. Первая покупка за всю историю (для метки «новый»)
-----------------------------------------------------------------------
drop table if exists #first;
select a.ID_CONTACT
     , MIN_DATA = min(a.DATA)
into #first
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 4. База сравнения: активные за 6 недель + группа + метка «новый»
-----------------------------------------------------------------------
drop table if exists #base;
select p.ID_CONTACT
     , GRP    = IIF(c.ID_CONTACT is not null, 'CONTROL', 'MASS')
     , IS_NEW = IIF(f.MIN_DATA >= @pre_from, 1, 0)
     , ACQ_YM = year(f.MIN_DATA) * 100 + month(f.MIN_DATA)
     , p.PRE_SPEND
     , p.PRE_CHECKS
into #base
from #pre as p
     join      #first as f on p.ID_CONTACT = f.ID_CONTACT
     left join #cg    as c on p.ID_CONTACT = c.ID_CONTACT;


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
-- 6. НОВЫЕ КЛИЕНТЫ ПЕРИОДА: диагностика и исключение
--    Проверка «свежести» ГКГ: доля КГ по месяцам привлечения. У месяцев,
--    покрытых нарезкой ГКГ, CG_SHARE_PCT ~ 2.9% (1/35). Там, где доля падает
--    до 0, ГКГ клиентов уже не покрывает — их тоже нельзя брать в массу.
-----------------------------------------------------------------------
select b.ACQ_YM
     , IS_NEW_IN_WINDOW = max(b.IS_NEW)
     , CLIENTS          = count(*)
     , CG_CLIENTS       = sum(IIF(b.GRP = 'CONTROL', 1, 0))
     , CG_SHARE_PCT     = cast(100.0 * sum(IIF(b.GRP = 'CONTROL', 1, 0)) / count(*) as decimal(6,3))
     , TO_JULY          = sum(isnull(j.SPEND, 0))
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
where b.ACQ_YM >= 202601         -- последние месяцы привлечения; убери фильтр для всей истории
group by b.ACQ_YM
order by b.ACQ_YM;

-- итог по новым перед исключением
select GRP              = b.GRP
     , NEW_CLIENTS      = count(*)
     , NEW_TO_JULY      = sum(isnull(j.SPEND, 0))
     , NEW_TO_PER_CLIENT = avg(isnull(j.SPEND, 0) * 1.0)
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
where b.IS_NEW = 1
group by b.GRP;

-- исключаем новых ИЗ ОБЕИХ ГРУПП (не только помечаем)
delete b from #base as b where b.IS_NEW = 1;

-- ...и когорты привлечения, которые ГКГ вообще не покрывает (0 клиентов в КГ).
-- Такие месяцы дают клиентов ТОЛЬКО в массу: сравнение превратилось бы в
-- «масса с новыми» против «контроль без новых». Ловит перекос и на месяцах
-- старше 6-недельного окна, если ГКГ давно не перенарезалась.
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
-- 7. Выбросы: пороги Тьюки по обоим периодам, отчёт об исключаемых
-----------------------------------------------------------------------
drop table if exists #q_july;
select distinct
       P25 = PERCENTILE_CONT(0.25) within group (order by SPEND * 1.0) over ()
     , P75 = PERCENTILE_CONT(0.75) within group (order by SPEND * 1.0) over ()
into #q_july
from #july;

drop table if exists #q_pre;
select distinct
       P25 = PERCENTILE_CONT(0.25) within group (order by PRE_SPEND * 1.0) over ()
     , P75 = PERCENTILE_CONT(0.75) within group (order by PRE_SPEND * 1.0) over ()
into #q_pre
from #base;

DECLARE @cut_july float = (select P75 + @tukey_k * (P75 - P25) from #q_july);
DECLARE @cut_pre  float = (select P75 + @tukey_k * (P75 - P25) from #q_pre);

drop table if exists #outliers;
select b.ID_CONTACT
     , b.GRP
     , PRE_SPEND  = b.PRE_SPEND
     , JULY_SPEND = isnull(j.SPEND, 0)
     , REASON     = case when isnull(j.SPEND, 0) > @cut_july and b.PRE_SPEND > @cut_pre then 'BOTH'
                         when isnull(j.SPEND, 0) > @cut_july then 'JULY'
                         else 'PRE' end
into #outliers
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
where isnull(j.SPEND, 0) > @cut_july
   or b.PRE_SPEND > @cut_pre;

select CUT_JULY        = @cut_july
     , CUT_PRE         = @cut_pre
     , GRP             = o.GRP
     , o.REASON
     , OUTLIER_CLIENTS = count(*)
     , OUTLIER_TO_JULY = sum(o.JULY_SPEND)
     , MAX_JULY_SPEND  = max(o.JULY_SPEND)
from #outliers as o
group by o.GRP, o.REASON
order by o.GRP, o.REASON;


-----------------------------------------------------------------------
-- 8. Чистка: выбросы убираются целиком отовсюду
-----------------------------------------------------------------------
delete b from #base as b where exists (select 1 from #outliers as o where o.ID_CONTACT = b.ID_CONTACT);
delete j from #july as j where exists (select 1 from #outliers as o where o.ID_CONTACT = j.ID_CONTACT);


-----------------------------------------------------------------------
-- 9. Метрики по группам (активные, без новых, без выбросов)
-----------------------------------------------------------------------
drop table if exists #stat;
select b.GRP
     , CLIENTS           = count(*)
     , BUYERS            = sum(IIF(j.ID_CONTACT is not null, 1, 0))
     , RESPONSE_PCT      = cast(100.0 * sum(IIF(j.ID_CONTACT is not null, 1, 0)) / count(*) as decimal(6,3))
     , TO_JULY           = sum(isnull(j.SPEND, 0))
     , TO_PER_CLIENT     = avg(isnull(j.SPEND, 0) * 1.0)
     , CHECKS_PER_CLIENT = avg(isnull(j.CHECKS, 0) * 1.0)
     , AVG_CHECK         = sum(isnull(j.SPEND, 0)) * 1.0 / nullif(sum(isnull(j.CHECKS, 0)), 0)
     , PRE_TO            = sum(b.PRE_SPEND * 1.0)
     , PRE_PER_CLIENT    = avg(b.PRE_SPEND * 1.0)
     , PRE_CHECKS_PER_CLIENT = avg(b.PRE_CHECKS * 1.0)
into #stat
from #base as b
     left join #july as j on b.ID_CONTACT = j.ID_CONTACT
group by b.GRP;

select * from #stat order by GRP;


-----------------------------------------------------------------------
-- 10. БАЛАНС ГРУПП НА ПРЕД-ПЕРИОДЕ (до воздействия — должно совпадать)
-----------------------------------------------------------------------
select PRE_PER_CLIENT_MASS = m.PRE_PER_CLIENT
     , PRE_PER_CLIENT_CG   = c.PRE_PER_CLIENT
     , PRE_DIFF_PCT        = cast(100.0 * (m.PRE_PER_CLIENT - c.PRE_PER_CLIENT)
                                  / nullif(c.PRE_PER_CLIENT, 0) as decimal(8,3))
     , PRE_CHECKS_MASS     = m.PRE_CHECKS_PER_CLIENT
     , PRE_CHECKS_CG       = c.PRE_CHECKS_PER_CLIENT
     , PRE_CHECKS_DIFF_PCT = cast(100.0 * (m.PRE_CHECKS_PER_CLIENT - c.PRE_CHECKS_PER_CLIENT)
                                  / nullif(c.PRE_CHECKS_PER_CLIENT, 0) as decimal(8,3))
from #stat as m
     join #stat as c on m.GRP = 'MASS' and c.GRP = 'CONTROL';


-----------------------------------------------------------------------
-- 11. ИТОГ: доп ТО за июль (активные, без новых, без выбросов)
-----------------------------------------------------------------------
select N_MASS       = m.CLIENTS
     , N_CG         = c.CLIENTS
     , K_SCALE      = m.CLIENTS * 1.0 / c.CLIENTS
     , TO_MASS      = m.TO_JULY
     , TO_CG        = c.TO_JULY
     , TO_EXPECTED  = c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS)      -- ТО массы без CVM
     , DOP_TO       = m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS)
     , DOP_TO_PCT   = cast(100.0 * (m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS))
                           / nullif(c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS), 0) as decimal(8,3))
     -- с поправкой на расхождение групп в пред-периоде (см. блок 10):
     , DOP_TO_ADJ   = m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS)
                                * (m.PRE_PER_CLIENT / nullif(c.PRE_PER_CLIENT, 0))
     , DOP_TO_ADJ_PCT = cast(100.0 * (m.TO_JULY - c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS)
                                                 * (m.PRE_PER_CLIENT / nullif(c.PRE_PER_CLIENT, 0)))
                             / nullif(c.TO_JULY * (m.CLIENTS * 1.0 / c.CLIENTS)
                                      * (m.PRE_PER_CLIENT / nullif(c.PRE_PER_CLIENT, 0)), 0) as decimal(8,3))
     , RESPONSE_UPLIFT_PP = m.RESPONSE_PCT - c.RESPONSE_PCT
     , AVG_CHECK_UPLIFT   = m.AVG_CHECK - c.AVG_CHECK
from #stat as m
     join #stat as c on m.GRP = 'MASS' and c.GRP = 'CONTROL';
