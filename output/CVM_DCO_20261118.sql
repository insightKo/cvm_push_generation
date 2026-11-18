-- CVM · ДЦО на день: пуш ср 18.11.2026 · 65 акций 101579–101643 · Активные, Новые · механика «Коммуникация»
-- Решения Елены 17.11.2026 (сессия ДЦО по кластерам): ДЦО на 18.11 разложены по кластерам офлайна (присвоение 15.11) — каждому
--   кластеру до 6 товаров еды и до 6 нон-фуда, которые кластер берёт чаще среднего (× к среднему с нормировкой на число категорий
--   на клиента), товар — самый покупаемый кластером в округе; у кластеров 2–9 без категорий их кешбэков 19–27.11; кластеру 6 — без
--   кошек, 9 — без прокладок, 7 — только еда. Подбор: output/ДЦО_2026-11-18/ДЦО_2026-11-18_по_кластерам.xlsx (версия 3).
-- Акция = товар со скидкой (одинаковый товар у нескольких кластеров — одна акция); разная скидка или товар в округах — две акции.
-- Пуш получают Активные и Новые (сегменты 1–3 с пушем и приложением, ЦФО и СЗФО) с кластером; каждому один пуш — раздача
--   внутри кластера по правилам 11.11: покупал за 6 недель категорию товара кластера — каскадом от редкой в кластере к частой;
--   нет — товар из группы справочника CVM, где больше всего купленных за 6 недель категорий; нет — категория за 52 недели;
--   остаток — товар кластера по умолчанию (еда, место 1). Без кластера (присвоение 15.11) — не входят.
-- Раздачу считал портал на российском сервере по снимку покупок на 16.11 и положил в базу списков портала
--   (lists.dco_20261118: id_promo, id_contact; 2 928 387 строк, клиент один раз). Скрипт читает её через связанный сервер CVM_PORTAL.
-- Стандартная сборка: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG; других пушей на 18.11 нет (кешбэки кластеров
--   101567–101578 — с 19.11), вычитать некого; КГ 5% (RN/20) по BUDGET — подбирается по каждой акции отдельно.
-- LOAD_TO_ML = 2 (PUSH-коммуникация, в Manzana Online не идёт), LOAD_TO_MC = 1 (PUSH) — как ДЦО 30.10 и 11.11.
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после дня акции (позже 18.11.2026) — ничего не делается;
--   · шапки I_PROMO заводятся, только если их нет;
--   · акция, список которой уже есть в I_PROMO_OFFER, пропускается — повторный прогон её не задвоит;
--   · из базы списков прочитано не столько строк по акциям, сколько в раздаче → ошибка, ничего не пишется;
--   · у акции после сборки меньше 80% раздачи → ошибка, ничего не пишется;
--   · КГ акции не подобралась за 1 000 итераций → списки сошедшихся акций записываются, в конце ошибка —
--     через 10 минут повтор только по оставшимся.
-- Выгрузки CRM_GUID здесь нет: списки в Campaign грузит робот из базы списков после согласования в портале.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
GO

USE mci_model
GO


-- ==================================================================================
-- 1. ШАПКИ 101579–101643 В I_PROMO (заводятся, только если их нет)
-- ==================================================================================

if cast(getdate() as date) > '2026-11-18'
begin
	raiserror(N'DCO 18.11: promo day is over - headers skipped, file can be removed', 10, 1)
	return
end

declare @d date = '2026-11-18'		-- старт и финиш (ср)

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -6, @d), dateadd(day, -1, @d), @d, @d, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, @d), dateadd(day, +4, @d)
	 , N'PUSH', N'Активные, Новые', 4, null, (select MECHANICS from dbo.I_MECHANICS (nolock) where ID_MECHANICS = 7), 7, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101579, N'ДЦО Ролл Take it -70% на второй', N'101579_ДЦО Ролл Take it второй 70')
	, (101580, N'ДЦО Батончик Big Picnic -30%', N'101580_ДЦО Батончик Big Picnic 30')
	, (101581, N'ДЦО Булочка с сыром ХБИ -40% на второй', N'101581_ДЦО Булочка с сыром ХБИ второй 40')
	, (101582, N'ДЦО Чипсы Русская картошка -70% на второй', N'101582_ДЦО Русская картошка второй 70')
	, (101583, N'ДЦО Напиток Псыж -10%', N'101583_ДЦО Напиток Псыж 10')
	, (101584, N'ДЦО Хурма -25%', N'101584_ДЦО Хурма 25')
	, (101585, N'ДЦО Печенье Яшкино -11%', N'101585_ДЦО Печенье Яшкино 11')
	, (101586, N'ДЦО Шоколад Kinder -10%', N'101586_ДЦО Шоколад Kinder 10')
	, (101587, N'ДЦО Семечки OPNGO ЦФО -70% на второй', N'101587_ДЦО Семечки OPNGO ЦФО второй 70')
	, (101588, N'ДЦО Семечки Джинн СЗФО -28%', N'101588_ДЦО Семечки Джинн СЗФО 28')
	, (101589, N'ДЦО Конфеты Рот Фронт -10%', N'101589_ДЦО Конфеты Рот Фронт 10')
	, (101590, N'ДЦО Мармелад Французский сад -18%', N'101590_ДЦО Мармелад Французский сад 18')
	, (101591, N'ДЦО Сыр тёртый Стародуб -70% на второй', N'101591_ДЦО Сыр тёртый Стародуб второй 70')
	, (101592, N'ДЦО Маслины Iberica -70% на второй', N'101592_ДЦО Маслины Iberica второй 70')
	, (101593, N'ДЦО Свёкла варёная -8%', N'101593_ДЦО Свёкла варёная 8')
	, (101594, N'ДЦО Креветки Мореслав -8%', N'101594_ДЦО Креветки Мореслав 8')
	, (101595, N'ДЦО Огурцы Дядя Ваня ЦФО -17%', N'101595_ДЦО Огурцы Дядя Ваня ЦФО 17')
	, (101596, N'ДЦО Огурцы Круглое лето СЗФО -11%', N'101596_ДЦО Огурцы Круглое лето СЗФО 11')
	, (101597, N'ДЦО Горошек Дядя Ваня -18%', N'101597_ДЦО Горошек Дядя Ваня 18')
	, (101598, N'ДЦО Полотенца бумажные Papia -25%', N'101598_ДЦО Полотенца бумажные Papia 25')
	, (101599, N'ДЦО Губки Big City -14%', N'101599_ДЦО Губки Big City 14')
	, (101600, N'ДЦО Салфетки Pero -20%', N'101600_ДЦО Салфетки Pero 20')
	, (101601, N'ДЦО Synergetic для посуды -11%', N'101601_ДЦО Synergetic для посуды 11')
	, (101602, N'ДЦО Гель для стирки Persil -21%', N'101602_ДЦО Гель для стирки Persil 21')
	, (101603, N'ДЦО Domestos для унитаза -22%', N'101603_ДЦО Domestos для унитаза 22')
	, (101604, N'ДЦО Свёкла -12%', N'101604_ДЦО Свёкла 12')
	, (101605, N'ДЦО Помело -34%', N'101605_ДЦО Помело 34')
	, (101606, N'ДЦО Шпикачки Бородина -18%', N'101606_ДЦО Шпикачки Бородина 18')
	, (101607, N'ДЦО Мороженое 48 копеек -16%', N'101607_ДЦО Мороженое 48 копеек 16')
	, (101608, N'ДЦО Творожная масса -15% на второй', N'101608_ДЦО Творожная масса второй 15')
	, (101609, N'ДЦО Салат Айсберг -23%', N'101609_ДЦО Салат Айсберг 23')
	, (101610, N'ДЦО Колбаса Брауншвейгская -23%', N'101610_ДЦО Колбаса Брауншвейгская 23')
	, (101611, N'ДЦО Крылышки копчёные -13%', N'101611_ДЦО Крылышки копчёные 13')
	, (101612, N'ДЦО Студень Мясолюбов -13%', N'101612_ДЦО Студень Мясолюбов 13')
	, (101613, N'ДЦО Грудинка Папа может -13%', N'101613_ДЦО Грудинка Папа может 13')
	, (101614, N'ДЦО Колбаса Филейская Вязанка -11%', N'101614_ДЦО Колбаса Филейская Вязанка 11')
	, (101615, N'ДЦО Кофе MacCoffee 3в1 -11%', N'101615_ДЦО Кофе MacCoffee 3в1 11')
	, (101616, N'ДЦО Нектар детский -20%', N'101616_ДЦО Нектар детский 20')
	, (101617, N'ДЦО Булочки для бургера ЦФО -20%', N'101617_ДЦО Булочки для бургера ЦФО 20')
	, (101618, N'ДЦО Сдоба Венская Дарница СЗФО -41%', N'101618_ДЦО Сдоба Венская Дарница СЗФО 41')
	, (101619, N'ДЦО Корм Brit для собак -10%', N'101619_ДЦО Корм Brit для собак 10')
	, (101620, N'ДЦО Порошок Бимакс -15%', N'101620_ДЦО Порошок Бимакс 15')
	, (101621, N'ДЦО Прокладки Bella -10%', N'101621_ДЦО Прокладки Bella 10')
	, (101622, N'ДЦО Domestos универсальный -22%', N'101622_ДЦО Domestos универсальный 22')
	, (101623, N'ДЦО Мыло Absolut ЦФО -10%', N'101623_ДЦО Мыло Absolut ЦФО 10')
	, (101624, N'ДЦО Мыло Absolut СЗФО -17%', N'101624_ДЦО Мыло Absolut СЗФО 17')
	, (101625, N'ДЦО Пюре фруктовое детское -20% на второй', N'101625_ДЦО Пюре детское второй 20')
	, (101626, N'ДЦО Kinder Сюрприз -70% на второй', N'101626_ДЦО Kinder Сюрприз второй 70')
	, (101627, N'ДЦО Макароны Barilla -13%', N'101627_ДЦО Макароны Barilla 13')
	, (101628, N'ДЦО Чай Greenfield -25%', N'101628_ДЦО Чай Greenfield 25')
	, (101629, N'ДЦО Подушечки Любятово -13%', N'101629_ДЦО Подушечки Любятово 13')
	, (101630, N'ДЦО Зелень ассорти -16%', N'101630_ДЦО Зелень ассорти 16')
	, (101631, N'ДЦО Минеральная вода Рычал-Су -16%', N'101631_ДЦО Минеральная вода Рычал Су 16')
	, (101632, N'ДЦО Попкорн Ouch -70% на второй', N'101632_ДЦО Попкорн Ouch второй 70')
	, (101633, N'ДЦО Томатная паста Круглое лето -70% на второй', N'101633_ДЦО Томатная паста второй 70')
	, (101634, N'ДЦО Вареники Сибирская коллекция -70% на второй', N'101634_ДЦО Вареники второй 70')
	, (101635, N'ДЦО Пюре Доширак ЦФО -24%', N'101635_ДЦО Пюре Доширак ЦФО 24')
	, (101636, N'ДЦО Пюре Доширак СЗФО -29%', N'101636_ДЦО Пюре Доширак СЗФО 29')
	, (101637, N'ДЦО Палочки кукурузные Кузя -22%', N'101637_ДЦО Палочки кукурузные Кузя 22')
	, (101638, N'ДЦО Корнишоны Круглое лето ЦФО -22%', N'101638_ДЦО Корнишоны Круглое лето ЦФО 22')
	, (101639, N'ДЦО Лакомство Dreamies -11%', N'101639_ДЦО Лакомство Dreamies 11')
	, (101640, N'ДЦО Торт Невские берега ЦФО -14%', N'101640_ДЦО Торт Невские берега ЦФО 14')
	, (101641, N'ДЦО Торт Фили Бейкер СЗФО -17%', N'101641_ДЦО Торт Фили Бейкер СЗФО 17')
	, (101642, N'ДЦО Мегачипсы -70% на второй', N'101642_ДЦО Мегачипсы второй 70')
	, (101643, N'ДЦО Конфеты Отломи -22%', N'101643_ДЦО Конфеты Отломи 22')
	) v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)
GO


-- ==================================================================================
-- 2. СБОРКА СПИСКОВ 101579–101643
-- ==================================================================================

SET XACT_ABORT ON

-- защита: день акции прошёл — блок пропускается
if cast(getdate() as date) > '2026-11-18'
begin
	raiserror(N'DCO 18.11: promo day is over - lists skipped, file can be removed', 10, 1)
	return
end
-- защита: все 65 шапок в I_PROMO (раздел 1) и номер не занят чужой акцией (уникальности ID_PROMO в I_PROMO нет)
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_PROMO between 101579 and 101643 and ID_COMPANY = 1) < 65
	or exists (select 1 from I_PROMO (nolock) where ID_PROMO between 101579 and 101643 and ID_COMPANY = 1 and isnull(PROMO_NAME, N'') not like N'ДЦО %')
begin
	raiserror(N'DCO 18.11: headers 101579-101643 missing or number taken by another promo - nothing written', 16, 1)
	return
end

-- раздача на 18.11 (ru: cvm-api:/tmp/dco1811/assign18.log) — эталон для сверки чтения и для защиты 80%
drop table if exists #ref
create table #ref (ID_PROMO int not null primary key, N_REF int not null)
insert into #ref (ID_PROMO, N_REF) values
	  (101579,   29472), (101580,   72356), (101581,   86847), (101582,   82153), (101583,   53363), (101584,  324762), (101585,   19040)
	, (101586,   77580), (101587,   53468), (101588,   14988), (101589,   48030), (101590,   36469), (101591,  105230), (101592,   67343)
	, (101593,   81947), (101594,   17922), (101595,   13311), (101596,   11413), (101597,   19547), (101598,   21875), (101599,   40942)
	, (101600,   24425), (101601,   53808), (101602,   38812), (101603,   45043), (101604,   39160), (101605,  111279), (101606,   55366)
	, (101607,   25327), (101608,  150950), (101609,  126226), (101610,   26419), (101611,   14438), (101612,   45668), (101613,   23848)
	, (101614,   29224), (101615,   16861), (101616,   16855), (101617,   27400), (101618,   11431), (101619,   28885), (101620,    9979)
	, (101621,   13032), (101622,   15102), (101623,   46542), (101624,   11267), (101625,   14372), (101626,   34146), (101627,   30482)
	, (101628,   20729), (101629,   62332), (101630,   60665), (101631,   29560), (101632,  147945), (101633,   25526), (101634,   20261)
	, (101635,   15847), (101636,    5335), (101637,   17946), (101638,   16043), (101639,   23099), (101640,   41026), (101641,   12152)
	, (101642,   48297), (101643,   17219)

-- акции, списки которых уже записаны, — пропускаются
drop table if exists #done
select r.ID_PROMO
into #done
from #ref r
where exists (select 1 from I_PROMO_OFFER o (nolock) where o.ID_PROMO = r.ID_PROMO)

if (select count(*) from #done) = 65
begin
	raiserror(N'DCO 18.11: all 65 lists already in I_PROMO_OFFER - skipped', 10, 1)
	return
end

declare @msg nvarchar(400)

-- проверок и ожиданий ночного задания нет (Елена 01.11.2026): список собирается сразу

-- раздача из базы списков портала (2 928 387 строк, клиент один раз)
drop table if exists #o
create table #o (ID_PROMO int not null, ID_CONTACT int not null, index cci_o clustered columnstore)

-- динамическим SQL: текст OPENQUERY — константа, ошибка связи падает здесь же, ничего не записано
exec sys.sp_executesql N'insert into #o (ID_PROMO, ID_CONTACT)
select id_promo, id_contact
from openquery(CVM_PORTAL, ''select id_promo, id_contact from lists.dco_20261118'')'

-- сверка чтения по каждой акции с раздачей: чужой номер из таблицы портала не доходит до записи
declare @n bigint = (select count(*) from #o)
declare @n_ref bigint = (select sum(N_REF) from #ref)
if exists (select 1
		   from #ref r
		   full join (select ID_PROMO, N = count(*) from #o group by ID_PROMO) o
		   on o.ID_PROMO = r.ID_PROMO
		   where r.ID_PROMO is null or o.ID_PROMO is null or o.N <> r.N_REF)
	or (select count(distinct ID_CONTACT) from #o) <> @n
begin
	set @msg = concat(N'DCO 18.11: read ', @n, N' rows from portal lists, expected ', @n_ref, N' distinct by 65 lists as in #ref - nothing written')
	raiserror(@msg, 16, 1)
	return
end

drop table if exists #x

select o.ID_PROMO
		, o.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 2
		, ID_COMPANY = 1
		, b.CRM_GUID
		, LOAD_TO_MC = 1
into #x
from	  #o as o
		inner join I_CONTACT as b (nolock)
		on o.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on o.ID_CONTACT = c.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
and c.ID_CONTACT is null
and o.ID_PROMO not in (select ID_PROMO from #done)
group by o.ID_PROMO
		, o.ID_CONTACT
		, b.CRM_GUID

create clustered columnstore index cci on #x

-- защита: у акции после сборки меньше 80% раздачи — неполное чтение, ничего не пишем
if exists (select 1
		   from #ref r
		   left join (select ID_PROMO, N = count(distinct ID_CONTACT) from #x group by ID_PROMO) x
		   on x.ID_PROMO = r.ID_PROMO
		   where r.ID_PROMO not in (select ID_PROMO from #done)
		   and isnull(x.N, 0) < 0.8 * r.N_REF)
begin
	set @msg = (select top 1 concat(N'DCO 18.11: list ', r.ID_PROMO, N' has ', isnull(x.N, 0), N' clients - less than 80%% of ', r.N_REF, N', nothing written')
				from #ref r
				left join (select ID_PROMO, N = count(distinct ID_CONTACT) from #x group by ID_PROMO) x
				on x.ID_PROMO = r.ID_PROMO
				where r.ID_PROMO not in (select ID_PROMO from #done)
				and isnull(x.N, 0) < 0.8 * r.N_REF
				order by r.ID_PROMO)
	raiserror(@msg, 16, 1)
	return
end


drop table if exists #t

select	a.ID_PROMO
		, a.ID_CONTACT
		, COST_DISCOUNT = round(sum(b.BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(b.BUDGET)
		, MIN_DATA = min(b.FIRST_DATA)
		, LAST_DATA = max(b.LAST_DATA)
into #t
from	(select distinct ID_PROMO, ID_CONTACT, ID_COMPANY from #x) as a
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION = 1
where b.ID_CONTACT <> 0
group by a.ID_PROMO, a.ID_CONTACT

create clustered columnstore index cci on #t


-- КГ 5% по каждой акции: страты RN/20 по MIN_DATA, COST_DISCOUNT, LAST_DATA, из страты одна случайная строка;
-- акция принята, когда средний COST_DISCOUNT1 в КГ отличается от ЦГ меньше чем на 0,05%; дальше перебираются только
-- непринятые акции
drop table if exists #cg
create table #cg (ID_PROMO int not null, ID_CONTACT int not null)
drop table if exists #ok
create table #ok (ID_PROMO int not null primary key, ERR float not null, ITER int not null)

-- потолок 2 ч: сошедшиеся КГ записываются до таймаута sqlcmd -t 10800, остальные — повтором
declare @iter int = 0, @t0 datetime2 = sysdatetime()

while @iter < 1000
	and datediff(second, @t0, sysdatetime()) < 7200
	and exists (select 1 from #ref r where r.ID_PROMO not in (select ID_PROMO from #done) and r.ID_PROMO not in (select ID_PROMO from #ok))
begin

set @iter += 1

drop table if exists #local_cg

;with x as
(
select t.ID_PROMO
		, t.ID_CONTACT
		, RN = ROW_NUMBER() over (partition by t.ID_PROMO order by t.MIN_DATA, t.COST_DISCOUNT, t.LAST_DATA)
from #t t
where t.ID_PROMO not in (select ID_PROMO from #ok)
), a as (
select ID_PROMO
		, ID_CONTACT
		, SEGMENT = RN/20
from x
), n as (
select ID_PROMO
		, ID_CONTACT
		, R = ROW_NUMBER() over (partition by ID_PROMO, SEGMENT order by newid())
from a
)
select ID_PROMO
		, ID_CONTACT
into #local_cg
from n
where R = 1

drop table if exists #stat;
;with t as (
select c.ID_PROMO
		, CG = IIF(b.ID_CONTACT is not NULL, 1, 0)
		, COST_DISCOUNT1 = cast(c.COST_DISCOUNT1 as float)
from	#t as c
		left join #local_cg as b
		on c.ID_PROMO = b.ID_PROMO
		and c.ID_CONTACT = b.ID_CONTACT
where c.ID_PROMO not in (select ID_PROMO from #ok)
)
select	ID_PROMO
		, CG
		, COST_DISCOUNT = sum(COST_DISCOUNT1) / count(*)
into #stat
from t
group by ID_PROMO, CG
;

insert into #ok (ID_PROMO, ERR, ITER)
select a.ID_PROMO
		, abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT
		, @iter
from #stat a
	join #stat b
		on a.ID_PROMO = b.ID_PROMO
		and a.CG = 0
		and b.CG = 1
where a.COST_DISCOUNT <> 0
and abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / a.COST_DISCOUNT < 0.0005

insert into #cg (ID_PROMO, ID_CONTACT)
select l.ID_PROMO, l.ID_CONTACT
from #local_cg l
	join #ok k
		on k.ID_PROMO = l.ID_PROMO
		and k.ITER = @iter

if @iter % 20 = 0
begin
	set @msg = concat(N'DCO 18.11: CG iteration ', @iter, N', accepted ', (select count(*) from #ok), N' of ', 65 - (select count(*) from #done))
	raiserror(@msg, 0, 1) with nowait
end

end

select k.ID_PROMO, k.ERR, k.ITER from #ok k order by k.ID_PROMO


UPDATE d
SET CONTROL_GROUP = 1
from	#cg as a
		inner join #x as d
		on a.ID_PROMO = d.ID_PROMO
		and a.ID_CONTACT = d.ID_CONTACT


-- запись: по акции одной вставкой, только принятые КГ; акцию, записанную другим запуском, пропускаем
declare @id int
declare c cursor local fast_forward for select ID_PROMO from #ok order by ID_PROMO
open c
fetch next from c into @id
while @@FETCH_STATUS = 0
begin
	if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = @id)
	begin
		set @msg = concat(N'DCO 18.11: list ', @id, N' written by another run - skipped')
		raiserror(@msg, 10, 1)
	end
	else
	begin
		INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
		SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
		from #x
		where ID_PROMO = @id
		OPTION (RECOMPILE)
	end
	fetch next from c into @id
end
close c
deallocate c


-- сводка по всем 65 акциям
select p.ID_PROMO
		, [Название списка] = p.PROMO_NAME_ML
		, [Клиентов на отправку] = isnull(o.N0, 0)
		, [КГ] = isnull(o.N1, 0)
		, [Дата старта] = p.START_DATE
		, [Загрузка в Манзана Онлайн] = N'нет'
from I_PROMO p (nolock)
	left join (select ID_PROMO
					, N0 = count(distinct case when CONTROL_GROUP = 0 then ID_CONTACT end)
					, N1 = count(distinct case when CONTROL_GROUP = 1 then ID_CONTACT end)
			   from I_PROMO_OFFER with (nolock)
			   where ID_PROMO between 101579 and 101643 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
			   group by ID_PROMO) o
	on o.ID_PROMO = p.ID_PROMO
where p.ID_PROMO between 101579 and 101643 and p.ID_COMPANY = 1
order by p.ID_PROMO

-- КГ подобралась не у всех — ошибка, через 10 минут повтор только по оставшимся
if exists (select 1 from #ref r where r.ID_PROMO not in (select ID_PROMO from #done) and r.ID_PROMO not in (select ID_PROMO from #ok))
begin
	set @msg = concat(N'DCO 18.11: CG not found in ', @iter, N' iterations for ', (select count(*) from #ref r where r.ID_PROMO not in (select ID_PROMO from #done) and r.ID_PROMO not in (select ID_PROMO from #ok)), N' lists - will retry')
	raiserror(@msg, 16, 1)
	return
end
GO


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #ref
drop table if exists #done
drop table if exists #o
drop table if exists #x
drop table if exists #t
drop table if exists #cg
drop table if exists #ok
drop table if exists #local_cg
drop table if exists #stat
GO
