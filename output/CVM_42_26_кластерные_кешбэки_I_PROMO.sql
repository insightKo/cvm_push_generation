-- CVM · 42 неделя (12.10.–18.10.2026) · кластерные кешбэки · шапки 10 акций в I_PROMO · 13.10.2026
-- Все 10: старт чт 15.10.2026, окончание вс 18.10.2026, канал PUSH, механика «Кэшбек активируемый» (16), сгорание монет 25.10.2026 23:59
--   (срок сгорания — в условиях «CVM offline»: колонки под него в I_PROMO нет).
--   ОБНОВЛЕНИЕ существующих шапок (заведены CVM_october_2026_I_PROMO.sql):
--     101413 «Активируй 20% кешбэка на колбасы, сосиски и ветчину» → «Активируй 30% кешбэка на колбасы, сосиски и ветчину» (30%, решение Елены 13.10.2026, уже в «CVM offline»);
--       даты прежние 15.10–18.10, меняется окно истории (в аудитории теперь Случайные). Процент в I_PROMO живёт только в названии и PROMO_NAME_ML — отдельной колонки нет;
--     101415 «Скидка 20% на женскую гигиену» 18.10 (Активируемая скидка) → «Активируй 50% кешбэка на товары для женской гигиены» 15.10–18.10 (Кэшбек активируемый).
--   ЗАВЕДЕНИЕ новых: 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444.
-- Аудитория (I_CVM_CONTACT, SEGMENT 1–4 по полю CLUSTER) — скрипт выборки CVM_42_26_кластерные_кешбэки_выгрузка.sql; этот файл только шапки.
-- Значения шапки — как у 101413/101404 (PUSH, 'Активные', ID_SEGMENT 4, 'Кэшбек активируемый', 16, ID_CHANNEL 1, 'CVM', ID_COMPANY 1), кроме окна истории:
--   в аудитории есть Случайные (SEGMENT 4) → START_DATE_FROM = старт − 52*3 недели (правило заведения акций); FINISH_DATE_FROM = старт − 1 день;
--   окно «после» — 4 дня всем: финиш + 1 … финиш + 4 (решение Елены 21.09.2026).
-- PROMO_NAME_ML — ID_PROMO + '_' + название до 20 символов: без спецсимволов и служебных слов (активируй, скидка, на, и, в, с, по, от, для, к, за, из, до),
--   без повторов, слова целиком, пока помещаются (правило 13.10.2026, коммит a4d6a5f). Функции dbo.CVM_PROMO_NAME_ML в базе на 13.10 нет (проверено чтением),
--   поэтому значения посчитаны по правилу и записаны ниже явно; поправить можно UPDATE-ом. LOAD_TO_ML в I_PROMO нет — он ставится в списке (I_PROMO_OFFER, = 1 — решение Елены 13.10.2026).
-- Повторный запуск безопасен: номер уже заведён этим скриптом — значения перезаписываются теми же.
-- Скрипт — один пакет: проверки, запись и контроль идут вместе. Любая остановка — до изменения, I_PROMO не меняется.
-- Откат — блок в конце файла (закомментирован): прежние значения 101413/101415 (сняты чтением 13.10.2026) и удаление 8 новых шапок.

USE mci_model
GO

set xact_abort on
set nocount on

declare @d date = '2026-10-15'		-- старт (чт)
declare @fd date = '2026-10-18'		-- финиш (вс)
declare @rows int, @msg nvarchar(2048)

-- открытая транзакция в этой вкладке: COMMIT ниже её не закрыл бы (запись осталась бы незафиксированной), а THROW при xact_abort откатил бы и её — стоп до всего
if @@trancount > 0
begin
	raiserror(N'42 нед.: в этой вкладке открыта транзакция — завершить её (COMMIT/ROLLBACK) или запустить скрипт в новой вкладке. Ничего не изменено', 16, 1);
	return;
end

-- колонка PROMO_NAME_ML (CVM_I_PROMO_PROMO_NAME_ML.sql, 13.10.2026) — до блока «До»: он её читает и без колонки упал бы с Msg 207 вместо понятного стопа
if col_length('dbo.I_PROMO', 'PROMO_NAME_ML') is null
begin
	;throw 50001, N'42 нед.: в dbo.I_PROMO нет колонки PROMO_NAME_ML — сначала CVM_I_PROMO_PROMO_NAME_ML.sql. Ничего не изменено', 1;
end


-- ==================================================================================
-- ПАРАМЕТРЫ: 10 акций
-- ==================================================================================

drop table if exists #w42_p

create table #w42_p (
	  ID_PROMO int not null primary key
	, PROMO_NAME nvarchar(250) collate database_default not null
	, PROMO_NAME_ML nvarchar(32) collate database_default not null
	, IS_NEW bit not null					-- 1 — новая шапка (заводится), 0 — существующая (обновляется)
	, OLD_NAME nvarchar(250) collate database_default null			-- название до изменения у существующих
	, CLUSTER_NOTE nvarchar(200) not null	-- для чтения: кому
)

insert #w42_p (ID_PROMO, PROMO_NAME, PROMO_NAME_ML, IS_NEW, OLD_NAME, CLUSTER_NOTE)
values
	  (101413, N'Активируй 30% кешбэка на колбасы, сосиски и ветчину',          N'101413_30 кешбэка колбасы',   0, N'Активируй 20% кешбэка на колбасы, сосиски и ветчину', N'Большая закупка продуктов')
	, (101415, N'Активируй 50% кешбэка на товары для женской гигиены',          N'101415_50 кешбэка товары',    0, N'Скидка 20% на женскую гигиену',                      N'Много нон-фуда + Большая закупка с нон-фудом, покупали женскую гигиену за 52 недели')
	, (101435, N'Активируй 50% кешбэка на кока кола',                           N'101435_50 кешбэка кока кола', 1, null, N'Редкие')
	, (101436, N'Активируй 50% кешбэка на онигири',                             N'101436_50 кешбэка онигири',   1, null, N'Продукты по мелочи')
	, (101437, N'Активируй 30% кешбэка на яйца',                                N'101437_30 кешбэка яйца',      1, null, N'Частые с большой корзиной')
	, (101438, N'Активируй 50% кешбэка на творожки',                            N'101438_50 кешбэка творожки',  1, null, N'Регулярные за продуктами')
	, (101439, N'Активируй 50% кешбэка на выпечку собственного производства',   N'101439_50 кешбэка выпечку',   1, null, N'За хлебом и водой')
	, (101441, N'Активируй 50% кешбэка на домашние тапочки',                    N'101441_50 кешбэка домашние',  1, null, N'Много нон-фуда + Большая закупка с нон-фудом, без покупок женской гигиены')
	, (101443, N'Активируй 50% кешбэка на конфеты в коробках',                  N'101443_50 кешбэка конфеты',   1, null, N'Вино, крепкое, кофе')
	, (101444, N'Активируй 50% кешбэка на кофе',                                N'101444_50 кешбэка кофе',      1, null, N'Ультра-частые')


-- ==================================================================================
-- БЛОК 1. ДО — текущие значения всех 10 номеров в I_PROMO (ожидание: 101413 и 101415 — по одной строке, 8 новых — нет строк;
--         при повторном запуске — все 10 уже с новыми значениями)
-- ==================================================================================

drop table if exists #w42_before

select p.*
into #w42_before
from dbo.I_PROMO as p with (nolock)
where p.ID_COMPANY = 1
	and p.ID_PROMO in (select ID_PROMO from #w42_p)

select [Этап] = N'До', x.ID_PROMO, [Ожидание] = iif(x.IS_NEW = 1, N'новая — строки нет', N'существующая — одна строка')
	 , b.PROMO_NAME, b.PROMO_NAME_ML, b.START_DATE_FROM, b.FINISH_DATE_FROM, b.START_DATE, b.FINISH_DATE, b.START_DATE_AFTER, b.FINISH_DATE_AFTER
	 , b.CHANNEL, b.SEGMENT, b.ID_SEGMENT, b.CORE_MESSAGE, b.MECHANICS, b.ID_MECHANICS, b.ID_CHANNEL, b.CATEGORY_FOR_CLIENT, b.ID_CAMPAIGN, b.TYPE_PROMO, b.ID_COMPANY
from #w42_p as x
	left join #w42_before as b on b.ID_PROMO = x.ID_PROMO
order by x.ID_PROMO

-- списки в I_PROMO_OFFER по этим номерам (для сведения: пересобирает их скрипт выгрузки) — CCI, только равенство/список по ID_PROMO
select [Списки до] = o.ID_PROMO, [Строк] = count(*), [На отправку] = sum(iif(o.CONTROL_GROUP = 0, 1, 0))
from dbo.I_PROMO_OFFER as o with (nolock)
where o.ID_PROMO in (101413, 101415, 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444)
	and o.ID_COMPANY = 1 and o.ID_ORGANIZATION = 1
group by o.ID_PROMO
order by o.ID_PROMO


-- ==================================================================================
-- БЛОК 2. ПРОВЕРКИ — при любой из них скрипт останавливается до изменения, I_PROMO не меняется
-- ==================================================================================

-- (колонка PROMO_NAME_ML проверена в начале скрипта)

-- дубли номера
set @msg = null
select @msg = concat(@msg, iif(@msg is null, N'', N', '), ID_PROMO, N' (', n, N' строк)')
from (select ID_PROMO, n = count(*) from #w42_before group by ID_PROMO having count(*) > 1) as d
if @msg is not null
begin
	set @msg = concat(N'42 нед.: в I_PROMO больше одной строки на номер — ', @msg, N'. Ничего не изменено, смотри «До»');
	throw 50002, @msg, 1;
end

-- существующие 101413 и 101415 должны быть на месте
set @msg = null
select @msg = concat(@msg, iif(@msg is null, N'', N', '), x.ID_PROMO)
from #w42_p as x
where x.IS_NEW = 0 and not exists (select 1 from #w42_before as b where b.ID_PROMO = x.ID_PROMO)
if @msg is not null
begin
	set @msg = concat(N'42 нед.: нет шапки ', @msg, N' в I_PROMO (ожидалась из CVM_october_2026_I_PROMO.sql) — обновлять нечего. Ничего не изменено');
	throw 50003, @msg, 1;
end

-- под номером — та же акция: у существующих — прежнее или уже новое название, у новых — строки нет или она уже заведена этим скриптом; везде TYPE_PROMO = CVM
set @msg = null
select @msg = concat(@msg, iif(@msg is null, N'', N'; '), b.ID_PROMO, N' «', b.PROMO_NAME, N'», TYPE_PROMO ', isnull(b.TYPE_PROMO, N'NULL'))
from #w42_before as b
	inner join #w42_p as x on x.ID_PROMO = b.ID_PROMO
where isnull(b.TYPE_PROMO, N'') <> N'CVM'
	or isnull(b.PROMO_NAME, N'') not in (x.PROMO_NAME, isnull(x.OLD_NAME, x.PROMO_NAME))
if @msg is not null
begin
	set @msg = concat(N'42 нед.: номер занят другой акцией — ', @msg, N'. Ничего не изменено, смотри «До»');
	throw 50004, @msg, 1;
end

-- PROMO_NAME_ML не длиннее колонки и без повторов среди всех акций (имя списка для ML должно быть уникальным)
set @msg = null
select @msg = concat(@msg, iif(@msg is null, N'', N', '), x.PROMO_NAME_ML)
from #w42_p as x
where len(x.PROMO_NAME_ML) > 32
	or exists (select 1 from dbo.I_PROMO as p with (nolock)
			   where p.ID_COMPANY = 1 and p.PROMO_NAME_ML = x.PROMO_NAME_ML and p.ID_PROMO <> x.ID_PROMO)
if @msg is not null
begin
	set @msg = concat(N'42 нед.: имя списка для ML длиннее 32 знаков или уже занято другой акцией — ', @msg, N'. Ничего не изменено');
	throw 50005, @msg, 1;
end


-- ==================================================================================
-- БЛОК 3. ЗАПИСЬ — одна транзакция: UPDATE существующих (и повторно заведённых), INSERT отсутствующих
-- ==================================================================================

begin tran;

update p
set START_DATE_FROM   = dateadd(week, -52*3, @d)
  , FINISH_DATE_FROM  = dateadd(day, -1, @d)
  , START_DATE        = @d
  , FINISH_DATE       = @fd
  , PROMO_NAME        = x.PROMO_NAME
  , START_DATE_AFTER  = dateadd(day, +1, @fd)
  , FINISH_DATE_AFTER = dateadd(day, +4, @fd)
  , CHANNEL           = N'PUSH'
  , SEGMENT           = N'Активные'
  , ID_SEGMENT        = 4
  , CORE_MESSAGE      = null
  , MECHANICS         = N'Кэшбек активируемый'
  , ID_MECHANICS      = 16
  , ID_CHANNEL        = 1
  , CATEGORY_FOR_CLIENT = null
  , ID_CAMPAIGN       = null
  , TYPE_PROMO        = N'CVM'
  , PROMO_NAME_ML     = x.PROMO_NAME_ML
from dbo.I_PROMO as p
	inner join #w42_p as x on x.ID_PROMO = p.ID_PROMO
where p.ID_COMPANY = 1
	and p.TYPE_PROMO = N'CVM'
	and p.PROMO_NAME in (x.PROMO_NAME, isnull(x.OLD_NAME, x.PROMO_NAME));	-- та же строка, что в проверке

set @rows = @@rowcount
if @rows <> (select count(*) from #w42_before)
begin
	rollback;
	set @msg = concat(N'42 нед.: обновлено ', @rows, N' строк вместо ', (select count(*) from #w42_before), N' — I_PROMO изменилась между проверкой и записью. Ничего не изменено, запусти скрипт ещё раз');
	throw 50006, @msg, 1;
end

insert into dbo.I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, @d), dateadd(day, -1, @d), @d, @fd, x.ID_PROMO, x.PROMO_NAME, dateadd(day, +1, @fd), dateadd(day, +4, @fd)
	 , N'PUSH', N'Активные', 4, null, N'Кэшбек активируемый', 16, 1, null, null, N'CVM', 1, x.PROMO_NAME_ML
from #w42_p as x
where not exists (select 1 from #w42_before as b where b.ID_PROMO = x.ID_PROMO);

set @rows = @@rowcount

commit;

declare @n_before int
select @n_before = count(*) from #w42_before	-- в PRINT подзапрос нельзя (Msg 1046) — считаем заранее
print concat(N'42 нед.: обновлено шапок ', @n_before, N', заведено новых ', @rows)


-- ==================================================================================
-- БЛОК 4. ПОСЛЕ — ожидание у всех 10: 2023-10-19 · 2026-10-14 · 2026-10-15 · 2026-10-18 · 2026-10-19 · 2026-10-22, дней после = 4, «OK»
-- ==================================================================================

select [Этап] = N'После', p.ID_PROMO, p.PROMO_NAME, p.PROMO_NAME_ML
	 , p.START_DATE_FROM, p.FINISH_DATE_FROM, p.START_DATE, p.FINISH_DATE, p.START_DATE_AFTER, p.FINISH_DATE_AFTER
	 , [Дней после] = datediff(day, p.START_DATE_AFTER, p.FINISH_DATE_AFTER) + 1
	 , p.CHANNEL, p.SEGMENT, p.ID_SEGMENT, p.MECHANICS, p.ID_MECHANICS, p.ID_CHANNEL, p.TYPE_PROMO
	 , [Было] = iif(b.ID_PROMO is null, N'заведена', N'обновлена')
	 , [Проверка] = iif(p.START_DATE_FROM = dateadd(week, -52*3, @d) and p.FINISH_DATE_FROM = dateadd(day, -1, @d)
						and p.START_DATE = @d and p.FINISH_DATE = @fd
						and p.START_DATE_AFTER = dateadd(day, +1, @fd) and p.FINISH_DATE_AFTER = dateadd(day, +4, @fd)
						and p.PROMO_NAME = x.PROMO_NAME and p.PROMO_NAME_ML = x.PROMO_NAME_ML
						and p.ID_MECHANICS = 16 and p.ID_CHANNEL = 1 and p.TYPE_PROMO = N'CVM'
					  , N'OK', N'НЕ СОВПАДАЕТ')
from dbo.I_PROMO as p with (nolock)
	inner join #w42_p as x on x.ID_PROMO = p.ID_PROMO
	left join (select distinct ID_PROMO from #w42_before) as b on b.ID_PROMO = p.ID_PROMO
where p.ID_COMPANY = 1
order by p.ID_PROMO

drop table if exists #w42_p
drop table if exists #w42_before
GO


-- ==================================================================================
-- ОТКАТ (закомментирован) — выделить блок, снять комментарий (Ctrl+Shift+U) и выполнить выделенное.
-- Возвращает 101413 и 101415 к значениям на 13.10.2026 и удаляет 8 новых шапок. Списки этих номеров в I_PROMO_OFFER откат не трогает:
-- если скрипт выгрузки уже прогнан, сначала решить судьбу списков (у новых номеров без шапки списки повиснут).
-- ==================================================================================

/*
set xact_abort on
begin tran;

update p
set START_DATE_FROM = '2026-09-03', FINISH_DATE_FROM = '2026-10-14', START_DATE = '2026-10-15', FINISH_DATE = '2026-10-18'
  , PROMO_NAME = N'Активируй 20% кешбэка на колбасы, сосиски и ветчину'
  , START_DATE_AFTER = '2026-10-19', FINISH_DATE_AFTER = '2026-10-22'
  , CHANNEL = N'PUSH', SEGMENT = N'Активные', ID_SEGMENT = 4, CORE_MESSAGE = null
  , MECHANICS = N'Кэшбек активируемый', ID_MECHANICS = 16, ID_CHANNEL = 1, CATEGORY_FOR_CLIENT = null, ID_CAMPAIGN = null
  , TYPE_PROMO = N'CVM', PROMO_NAME_ML = N'101413_20 кешбэка колбасы'
from dbo.I_PROMO as p
where p.ID_PROMO = 101413 and p.ID_COMPANY = 1 and p.TYPE_PROMO = N'CVM';

update p
set START_DATE_FROM = '2026-09-06', FINISH_DATE_FROM = '2026-10-17', START_DATE = '2026-10-18', FINISH_DATE = '2026-10-18'
  , PROMO_NAME = N'Скидка 20% на женскую гигиену'
  , START_DATE_AFTER = '2026-10-19', FINISH_DATE_AFTER = '2026-10-22'
  , CHANNEL = N'PUSH', SEGMENT = N'Активные', ID_SEGMENT = 4, CORE_MESSAGE = null
  , MECHANICS = N'Активируемая скидка', ID_MECHANICS = 4, ID_CHANNEL = 1, CATEGORY_FOR_CLIENT = null, ID_CAMPAIGN = null
  , TYPE_PROMO = N'CVM', PROMO_NAME_ML = N'101415_20 женскую гигиену'
from dbo.I_PROMO as p
where p.ID_PROMO = 101415 and p.ID_COMPANY = 1 and p.TYPE_PROMO = N'CVM';

delete from dbo.I_PROMO
where ID_COMPANY = 1 and TYPE_PROMO = N'CVM'
	and ID_PROMO in (101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444);

commit;

select ID_PROMO, PROMO_NAME, PROMO_NAME_ML, START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, START_DATE_AFTER, FINISH_DATE_AFTER, MECHANICS, ID_MECHANICS
from dbo.I_PROMO with (nolock)
where ID_COMPANY = 1
	and ID_PROMO in (101413, 101415, 101435, 101436, 101437, 101438, 101439, 101441, 101443, 101444)
order by ID_PROMO
*/
