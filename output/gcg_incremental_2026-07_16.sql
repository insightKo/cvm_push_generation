/*==============================================================================
  ЭФФЕКТ CVM ПО ГЛОБАЛЬНОЙ КГ ЗА ИЮЛЬ 2026.
  ВЕРСИЯ 16 = _15, но НОВЫЕ ПОЛНОСТЬЮ ИСКЛЮЧЕНЫ ИЗ АНАЛИЗА.

  ПРИЧИНА: новых нет в ГКГ. Нарезка контроля делалась раньше, чем они
  пришли, поэтому у новичка нет и не может быть пары в контрольной группе:
  при left join он всегда попадает в ЦГ. Сравнивать его не с чем, а его
  оборот односторонне уезжает в "эффект".

  Новый = первая покупка в последние 6 недель до даты сегментации.
  Такие клиенты убираются ИЗ ОБЕИХ ГРУПП и из основной оценки тоже,
  а не только из разбивки. Фильтр стоит в блоке 2 (having по первой покупке),
  поэтому дальше они не появляются нигде.

  Сегменты после исключения:
      2  Активные - покупка в последние 6 недель
      5  Отток    - последняя покупка 6-15 недель назад
      6  Спящие   - последняя покупка больше 15 недель назад

  ВЕРСИЯ 15 = _14 + ЧЕТЫРЕ СЕГМЕНТА В ДЕКОМПОЗИЦИИ.

  Коды соответствуют справочнику I_CVM_CONTACT.SEGMENT, границы - канон:
      1  Новые    - первая покупка в последние 6 недель
      2  Активные - покупка в последние 6 недель (кроме новых)
      5  Отток    - последняя покупка 6-15 недель назад
      6  Спящие   - последняя покупка больше 15 недель назад
  Раньше новые не выделялись, а спящие сидели внутри оттока.
  НИКТО НЕ ИСКЛЮЧАЕТСЯ: все четыре сегмента входят и в основную оценку,
  и в декомпозицию. Не входят только клиенты без покупок до июля - у них
  нет препериода, сегмент им присвоить не из чего.

  ОСНОВНАЯ ЦИФРА (блок 7) считается по всей популяции без сегментации и от
  этой правки не меняется. Разбивка остаётся ДЕКОМПОЗИЦИЕЙ.

  ВЕРСИЯ 14 = _13 + ИСКЛЮЧЕНА ПЕРВАЯ НЕДЕЛЯ ИЮЛЯ + ПРИВЕДЕНИЕ К МЕСЯЦУ.

  Окно измерения: 08.07-31.07.2026. Первая неделя исключена - группа в это
  время ещё не была запущена, и включать её значит разбавлять эффект
  периодом, когда воздействия не было.

  ПРИВЕДЕНИЕ К ПОЛНОМУ МЕСЯЦУ считается не по числу дней, а по фактической
  доле оборота, приходящейся на окно измерения внутри июля. Календарное
  деление 31/24 дало бы перекос: у первой недели своя структура дней недели,
  а оборот по дням недели неравномерен.

  ВНИМАНИЕ при трактовке приведённой цифры: это RUN-RATE, то есть "сколько
  было бы за месяц, если бы коммуникации шли с той же интенсивностью весь
  июль". Это НЕ фактический эффект июля: в первую неделю воздействия не было,
  и эффекта там тоже не было. Для отчёта о факте брать цифру за окно.

  ВЕРСИЯ 13 = _12 + ИСПРАВЛЕН БАГ ТИПОВ в блоке 8.
  Было avg(BUYER * 1.0) -> numeric; p*(1-p)/N при N ~ 12 млн округлялось
  в ноль, SE выходила нулевой и вердикт значимости был ложно-положительным.
  Стало avg(cast(... as float)).

  Отличия от _11:

  1. ОСНОВНАЯ ЦИФРА СЧИТАЕТСЯ БЕЗ СЕГМЕНТАЦИИ, по всей популяции.
     Сегмент по дате последней покупки — переменная, на которую влияют сами
     коммуникации. Кондиционирование на неё даёт минус ОДНОВРЕМЕННО в обоих
     сегментах при реально положительном общем эффекте. Разбивка осталась,
     но помечена как ДЕКОМПОЗИЦИЯ (блок 10), не как причинная оценка.

  2. ВМЕСТО ОТНОШЕНИЯ СРЕДНИХ — РЕГРЕССИОННЫЙ НАКЛОН (CUPED / ANCOVA).
     Было: KOEF = ТО_КГ / ТО_КГ_ДО. Это верно только если зависимость
     проходит через ноль; в рознице человек покупает и при малом «ДО»,
     поэтому отношение средних систематически пере-корректирует.
     Стало: theta = Cov(Y,X)/Var(X), оценённый НА КОНТРОЛЕ.
         D_i    = Y_i - theta*(X_i - X_pooled)
         tau    = mean(D в ЦГ) - mean(D в КГ)
         Доп ТО = tau * N_ЦГ

  3. КОВАРИАТА X — ФИКСИРОВАННОЕ ОКНО, ДИЗЪЮНКТНОЕ ОКНУ СЕГМЕНТАЦИИ:
     недели -19..-7 до старта периода. В _11 для оттока бралась вся история,
     из-за чего знаменатель был раздут в десятки раз и ошибка коэффициента
     в 1% превращалась в ошибку ответа в 100%.

  4. ВИНЗОРИЗАЦИЯ ВМЕСТО УДАЛЕНИЯ ВЫБРОСОВ.
     В _9.._11 клиент удалялся по величине ИЮЛЬСКОГО оборота — это отбор по
     исходу: промо сдвигает распределение вправо, поэтому из ЦГ вырезалось
     больше, чем из КГ, и оценка смещалась вниз. Здесь ни один клиент не
     выпадает, оборот подрезается сверху по порогу, посчитанному на данных
     ГОДИЧНОЙ ДАВНОСТИ (до периода воздействия).

  5. ПЕЧАТАЕТСЯ НЕОПРЕДЕЛЁННОСТЬ: SE, доверительный интервал, MDE.
     Без интервала точечная оценка по ГКГ не интерпретируется.

  ПАРАМЕТРЫ, КОТОРЫЕ НАДО ПОДТВЕРДИТЬ (помечены ТРЕБУЕТ ПОДТВЕРЖДЕНИЯ):
    @seg_date  - дата формирования ГКГ. Пока стоит конец июня, и тогда
                 сегментация в блоке 10 остаётся post-treatment.
    @cap_pct   - квантиль винзоризации.
    KPI        - COST_DISCOUNT это оборот ПОСЛЕ скидки. Скидочные механики
                 механически уменьшают его у ЦГ. Если KPI должен быть валовым
                 оборотом или маржой - менять источник, а не формулу.
==============================================================================*/

SET NOCOUNT ON;

DECLARE @idc int = 1;

DECLARE @dt_from date = '2026-07-01';
DECLARE @dt_to   date = '2026-08-01';                 -- правая граница НЕ включается

-- Начало окна измерения: первая неделя июля исключена (группа не запущена)
DECLARE @dt_meas date = '2026-07-08';

-- Ковариатное окно X: недели -19..-7 до старта. Дизъюнктно 6-недельному окну,
-- по которому определяется активность, поэтому не вырождается у оттока.
DECLARE @x_from date = dateadd(week, -19, @dt_from);  -- 18.02.2026
DECLARE @x_to   date = dateadd(week,  -7, @dt_from);  -- 13.05.2026

-- Окно для порога винзоризации: тот же месяц год назад (заведомо до периода).
DECLARE @cap_from date = dateadd(year, -1, @dt_meas);   -- окно той же длины год назад
DECLARE @cap_to   date = dateadd(year, -1, @dt_to);
DECLARE @cap_pct  float = 0.999;                      -- ТРЕБУЕТ ПОДТВЕРЖДЕНИЯ

-- Дата, на которую фиксируется сегмент для ДЕКОМПОЗИЦИИ (блок 10).
-- ТРЕБУЕТ ПОДТВЕРЖДЕНИЯ: подставить дату формирования ГКГ. Пока стоит конец
-- июня - значит сегмент post-treatment и посегментные числа смещены.
DECLARE @seg_date date = dateadd(day, -1, @dt_from);
DECLARE @active_days  int = 6  * 7;   -- граница активных и новых
DECLARE @ottok_days   int = 15 * 7;   -- граница оттока; дальше спящие


-----------------------------------------------------------------------
-- 1. Контрольная группа (дедуплицированная)
-----------------------------------------------------------------------
drop table if exists #cg;
select ID_CONTACT
into #cg
from I_GLOBAL_CG (nolock)
where ID_COMPANY = @idc
  and CONTROL_GROUP = 1
group by ID_CONTACT;


-----------------------------------------------------------------------
-- 2. Популяция: клиенты с покупками ДО начала периода
-----------------------------------------------------------------------
drop table if exists #hist;
select a.ID_CONTACT
     , MAX_DATA = max(a.DATA)
     , MIN_DATA = min(a.DATA)
into #hist
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA < @dt_from
group by a.ID_CONTACT
having datediff(day, min(a.DATA), @seg_date) > @active_days;   -- НОВЫЕ ИСКЛЮЧЕНЫ


-----------------------------------------------------------------------
-- 3. Порог винзоризации по данным годичной давности
-----------------------------------------------------------------------
drop table if exists #cap_base;
select a.ID_CONTACT
     , SPEND = sum(cast(a.COST_DISCOUNT as float))
into #cap_base
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @cap_from
  and a.DATA <  @cap_to
group by a.ID_CONTACT
having sum(cast(a.COST_DISCOUNT as float)) > 0;

drop table if exists #cap;
select distinct CAP = PERCENTILE_CONT(@cap_pct) within group (order by SPEND) over ()
into #cap
from #cap_base;

DECLARE @cap float = (select CAP from #cap);
select [Порог винзоризации, руб] = @cap
     , [Квантиль]                = @cap_pct
     , [Окно порога]             = concat(convert(varchar(10), @cap_from, 104), ' - ', convert(varchar(10), @cap_to, 104));


-----------------------------------------------------------------------
-- 4. Исход Y (июль) и ковариата X (недели -19..-7)
-----------------------------------------------------------------------
drop table if exists #y;
select a.ID_CONTACT
     , SPEND  = sum(cast(a.COST_DISCOUNT as float))
     , CHECKS = count(a.ID_CHECK)
into #y
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @dt_meas
  and a.DATA <  @dt_to
group by a.ID_CONTACT;

-- Коэффициент приведения к полному месяцу: доля оборота окна в обороте июля
DECLARE @to_full float, @to_meas float;
select @to_full = sum(cast(a.COST_DISCOUNT as float))
     , @to_meas = sum(IIF(a.DATA >= @dt_meas, cast(a.COST_DISCOUNT as float), 0))
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @dt_from
  and a.DATA <  @dt_to;

DECLARE @k_scale float = @to_full / nullif(@to_meas, 0);

select [Окно измерения]        = concat(convert(varchar(10), @dt_meas, 104), ' - ', convert(varchar(10), dateadd(day, -1, @dt_to), 104))
     , [Оборот июля, руб]      = @to_full
     , [Оборот окна, руб]      = @to_meas
     , [Доля окна, %]          = cast(100.0 * @to_meas / nullif(@to_full, 0) as decimal(6,3))
     , [Коэффициент приведения]= @k_scale;

drop table if exists #x;
select a.ID_CONTACT
     , SPEND = sum(cast(a.COST_DISCOUNT as float))
into #x
from I_CHECKHEADER as a (nolock)
where a.ID_COMPANY = @idc
  and a.ID_CONTACT <> 0
  and a.DATA >= @x_from
  and a.DATA <  @x_to
group by a.ID_CONTACT;


-----------------------------------------------------------------------
-- 5. Клиентская таблица анализа
--    Y винзоризован сверху и снизу: ни один клиент не выпадает.
-----------------------------------------------------------------------
drop table if exists #cl;
select h.ID_CONTACT
     , GRP    = IIF(c.ID_CONTACT is not null, 'CG', 'TG')          -- TG = ЦГ
     , Y      = case when isnull(y.SPEND, 0) >  @cap then @cap
                     when isnull(y.SPEND, 0) < -@cap then -@cap
                     else isnull(y.SPEND, 0) end
     , Y_RAW  = isnull(y.SPEND, 0)
     , BUYER  = IIF(y.ID_CONTACT is not null, 1, 0)
     , CHECKS = isnull(y.CHECKS, 0)
     , X      = case when isnull(x.SPEND, 0) >  @cap then @cap
                     when isnull(x.SPEND, 0) < -@cap then -@cap
                     else isnull(x.SPEND, 0) end
     , SEGMENT = case
           when datediff(day, h.MAX_DATA, @seg_date) <= @active_days then 2   -- Активные
           when datediff(day, h.MAX_DATA, @seg_date) <= @ottok_days  then 5   -- Отток
           else 6 end                                                          -- Спящие
into #cl
from #hist as h
     left join #cg as c on h.ID_CONTACT = c.ID_CONTACT
     left join #y  as y on h.ID_CONTACT = y.ID_CONTACT
     left join #x  as x on h.ID_CONTACT = x.ID_CONTACT;

create index ix_cl_grp on #cl (GRP);


-----------------------------------------------------------------------
-- 6. Наклон theta = Cov(Y,X)/Var(X), оценённый НА КОНТРОЛЕ
-----------------------------------------------------------------------
DECLARE @n_c float, @sx float, @sy float, @sxy float, @sxx float;
select @n_c = count(*) * 1.0
     , @sx  = sum(X), @sy = sum(Y), @sxy = sum(X * Y), @sxx = sum(X * X)
from #cl where GRP = 'CG';

DECLARE @theta float = (@sxy - @sx * @sy / @n_c) / nullif(@sxx - @sx * @sx / @n_c, 0);
DECLARE @xbar  float = (select avg(X) from #cl);

-- корреляция Y и X на контроле: показывает, стоила ли поправка того
DECLARE @rho float =
    (@sxy - @sx * @sy / @n_c)
    / nullif(sqrt(nullif(@sxx - @sx * @sx / @n_c, 0))
           * sqrt(nullif((select sum(Y * Y) from #cl where GRP = 'CG') - @sy * @sy / @n_c, 0)), 0);

select [theta (наклон)] = @theta
     , [rho (корр. Y,X на КГ)] = @rho
     , [Снижение дисперсии, %] = cast(100.0 * (@rho * @rho) as decimal(5,2))
     , [X_pooled] = @xbar;


-----------------------------------------------------------------------
-- 7. ОСНОВНАЯ ОЦЕНКА — по всей популяции, без сегментации
-----------------------------------------------------------------------
drop table if exists #d;
select GRP
     , N     = count(*) * 1.0
     , MEAN  = avg(Y - @theta * (X - @xbar))
     , VARD  = var(Y - @theta * (X - @xbar))
     , MEAN_Y = avg(Y)
     , VAR_Y  = var(Y)
     , P_BUY  = avg(cast(BUYER as float))
     , MEAN_CH = avg(cast(CHECKS as float))
     , VAR_CH  = var(cast(CHECKS as float))
into #d
from #cl
group by GRP;

select [Оценка]            = 'CUPED (осн.)'
     , [Доп ТО, руб]       = (t.MEAN - c.MEAN) * t.N
     , [SE, руб]           = t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [ДИ 95% низ]        = (t.MEAN - c.MEAN) * t.N - 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [ДИ 95% верх]       = (t.MEAN - c.MEAN) * t.N + 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [t-стат]            = (t.MEAN - c.MEAN) / nullif(sqrt(t.VARD / t.N + c.VARD / c.N), 0)
     , [Значимо на 95%]    = IIF(abs((t.MEAN - c.MEAN) / nullif(sqrt(t.VARD / t.N + c.VARD / c.N), 0)) >= 1.96, 'ДА', 'НЕТ')
     , [MDE при 80%, руб]  = 2.80 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [Uplift на клиента] = t.MEAN - c.MEAN
     , [N ЦГ]              = t.N
     , [N КГ]              = c.N
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG'
union all
-- для сравнения: простая разность средних, без ковариатной поправки
select [Оценка]            = 'Разность средних'
     , (t.MEAN_Y - c.MEAN_Y) * t.N
     , t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)
     , (t.MEAN_Y - c.MEAN_Y) * t.N - 1.96 * t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)
     , (t.MEAN_Y - c.MEAN_Y) * t.N + 1.96 * t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)
     , (t.MEAN_Y - c.MEAN_Y) / nullif(sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N), 0)
     , IIF(abs((t.MEAN_Y - c.MEAN_Y) / nullif(sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N), 0)) >= 1.96, 'ДА', 'НЕТ')
     , 2.80 * t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)
     , t.MEAN_Y - c.MEAN_Y
     , t.N, c.N
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG';


-- 7б. ПРИВЕДЕНИЕ К ПОЛНОМУ МЕСЯЦУ (run-rate, не факт июля - см. шапку)
select [Оценка]                  = 'CUPED, приведено к месяцу'
     , [Коэффициент]             = @k_scale
     , [Доп ТО за окно, руб]     = (t.MEAN - c.MEAN) * t.N
     , [Доп ТО за месяц, руб]    = (t.MEAN - c.MEAN) * t.N * @k_scale
     , [SE за месяц, руб]        = t.N * sqrt(t.VARD / t.N + c.VARD / c.N) * @k_scale
     , [ДИ 95% низ, месяц]       = ((t.MEAN - c.MEAN) * t.N - 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)) * @k_scale
     , [ДИ 95% верх, месяц]      = ((t.MEAN - c.MEAN) * t.N + 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)) * @k_scale
     , [Значимость не меняется]  = IIF(abs((t.MEAN - c.MEAN) / nullif(sqrt(t.VARD / t.N + c.VARD / c.N), 0)) >= 1.96, 'ДА', 'НЕТ')
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG'
union all
select 'Разность средних, приведено к месяцу'
     , @k_scale
     , (t.MEAN_Y - c.MEAN_Y) * t.N
     , (t.MEAN_Y - c.MEAN_Y) * t.N * @k_scale
     , t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N) * @k_scale
     , ((t.MEAN_Y - c.MEAN_Y) * t.N - 1.96 * t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)) * @k_scale
     , ((t.MEAN_Y - c.MEAN_Y) * t.N + 1.96 * t.N * sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N)) * @k_scale
     , IIF(abs((t.MEAN_Y - c.MEAN_Y) / nullif(sqrt(t.VAR_Y / t.N + c.VAR_Y / c.N), 0)) >= 1.96, 'ДА', 'НЕТ')
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG';


-----------------------------------------------------------------------
-- 8. Дополнительные исходы: отклик и частота.
--    У них нет тяжёлого хвоста, поэтому они значимы там, где оборот - нет.
-----------------------------------------------------------------------
select [Показатель]     = 'Доля покупателей'
     , [ЦГ]             = t.P_BUY
     , [КГ]             = c.P_BUY
     , [Разница]        = t.P_BUY - c.P_BUY
     , [SE]             = sqrt(t.P_BUY * (1 - t.P_BUY) / t.N + c.P_BUY * (1 - c.P_BUY) / c.N)
     , [Значимо на 95%] = IIF(abs(t.P_BUY - c.P_BUY) >= 1.96 * sqrt(t.P_BUY * (1 - t.P_BUY) / t.N + c.P_BUY * (1 - c.P_BUY) / c.N), 'ДА', 'НЕТ')
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG'
union all
select [Показатель]     = 'Чеков на клиента'
     , t.MEAN_CH, c.MEAN_CH, t.MEAN_CH - c.MEAN_CH
     , sqrt(t.VAR_CH / t.N + c.VAR_CH / c.N)
     , IIF(abs(t.MEAN_CH - c.MEAN_CH) >= 1.96 * sqrt(t.VAR_CH / t.N + c.VAR_CH / c.N), 'ДА', 'НЕТ')
from #d as t join #d as c on t.GRP = 'TG' and c.GRP = 'CG';


-----------------------------------------------------------------------
-- 9. ДИАГНОСТИКА
-----------------------------------------------------------------------
-- 9.1 Баланс на препериоде. |SMD| < 0.02 - норма. Большой SMD означает,
--     что группы несопоставимы ещё до воздействия и оценке верить нельзя.
select [Проверка] = 'Баланс X (препериод)'
     , [X на клиента ЦГ] = t.MX
     , [X на клиента КГ] = c.MX
     , [Расхождение, %]  = cast(100.0 * (t.MX - c.MX) / nullif(c.MX, 0) as decimal(8,3))
     , [SMD]             = (t.MX - c.MX) / nullif(sqrt((t.VX + c.VX) / 2), 0)
     , [Норма]           = 'модуль SMD < 0.02'
from (select GRP, MX = avg(X), VX = var(X) from #cl group by GRP) as t
     join (select GRP, MX = avg(X), VX = var(X) from #cl group by GRP) as c
       on t.GRP = 'TG' and c.GRP = 'CG';

-- 9.2 Состав сегментов в группах. Если доли различаются - это прямое
--     доказательство того, что сегмент post-treatment, и посегментные
--     числа (блок 10) причинно интерпретировать нельзя.
select [Проверка] = 'Состав сегментов'
     , [Сегмент]  = case SEGMENT when 2 then '2 Активные'
                     when 5 then '5 Отток 6-15 нед' else '6 Спящие 15+ нед' end
     , [Доля в ЦГ, %] = cast(100.0 * sum(IIF(GRP = 'TG', 1, 0)) / nullif(sum(sum(IIF(GRP = 'TG', 1, 0))) over (), 0) as decimal(6,3))
     , [Доля в КГ, %] = cast(100.0 * sum(IIF(GRP = 'CG', 1, 0)) / nullif(sum(sum(IIF(GRP = 'CG', 1, 0))) over (), 0) as decimal(6,3))
from #cl
group by SEGMENT;

-- 9.3 Влияние винзоризации: сколько оборота подрезано в каждой группе.
select [Проверка] = 'Винзоризация'
     , GRP
     , [Клиентов подрезано]   = sum(IIF(Y_RAW > @cap or Y_RAW < -@cap, 1, 0))
     , [Доля подрезанных, %]  = cast(100.0 * sum(IIF(Y_RAW > @cap or Y_RAW < -@cap, 1, 0)) / count(*) as decimal(8,4))
     , [Срезано оборота, руб] = sum(Y_RAW) - sum(Y)
from #cl
group by GRP;


-----------------------------------------------------------------------
-- 10. ДЕКОМПОЗИЦИЯ ПО СЕГМЕНТАМ — НЕ ПРИЧИННАЯ ОЦЕНКА.
--     Сегмент зависит от поведения клиента, на которое влияют сами
--     коммуникации, поэтому посегментные числа смещены вниз в ОБОИХ
--     сегментах, и их сумма не обязана сходиться с блоком 7.
--     Строки нужны для управленческой раскладки, не для вывода об эффекте.
-----------------------------------------------------------------------
drop table if exists #ds;
select SEGMENT, GRP
     , N    = count(*) * 1.0
     , MEAN = avg(Y - @theta * (X - @xbar))
     , VARD = var(Y - @theta * (X - @xbar))
into #ds
from #cl
group by SEGMENT, GRP;

select [Сегмент]      = case t.SEGMENT when 2 then '2 Активные'
                            when 5 then '5 Отток 6-15 нед' else '6 Спящие 15+ нед' end
     , [Доп ТО, руб]  = (t.MEAN - c.MEAN) * t.N
     , [SE, руб]      = t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [ДИ 95% низ]   = (t.MEAN - c.MEAN) * t.N - 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [ДИ 95% верх]  = (t.MEAN - c.MEAN) * t.N + 1.96 * t.N * sqrt(t.VARD / t.N + c.VARD / c.N)
     , [N ЦГ]         = t.N
     , [N КГ]         = c.N
     , [Статус]       = 'ДЕКОМПОЗИЦИЯ, не причинная оценка'
from #ds as t join #ds as c on t.SEGMENT = c.SEGMENT and t.GRP = 'TG' and c.GRP = 'CG'
order by t.SEGMENT;
