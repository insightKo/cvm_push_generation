-- CVM · ДЦО на день: пуш ср 11.11.2026, 11:00 · 26 акций 101497–101522 · Активные, Новые · механика «Коммуникация»
-- Решения Елены 10.11.2026:
--   · 15 пушей на трафик (16 акций: хлеб по округам — ЦФО 101498 ржаной, СЗФО 101505 зерновой) + 10 «на второй» на чек;
--     отбор по формам ДЦО и «Тотал Промо» (загружен 10.11), трафик — по числу покупателей категории за 6 недель;
--   · пуш получают все Активные и Новые (сегменты 1–3 с пушем и приложением, ЦФО и СЗФО), списки не пересекаются —
--     каждому один пуш;
--   · раздача (как 30.10): покупал за 6 недель категорию пуша — каскадом от редкой категории к частой; не покупал — пуш,
--     у которого больше всего купленных за 6 недель похожих категорий; нет и похожих — категория пуша за 52 недели;
--     без покупок (1 155) — «Молоко» 101497;
--   · «распределение ок — делай и клади списки» (10.11.2026); тексты пушей согласуются отдельно.
-- Раздачу считал портал на российском сервере по снимку покупок на 04.11 (витрина покупок не обновлялась с 06.11) и
--   положил в базу списков портала (lists.dco_20261111: id_promo, id_contact; 2 953 971 строка, клиент один раз).
--   Скрипт читает её через связанный сервер CVM_PORTAL.
-- Стандартная сборка: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG; других пушей на 11.11 нет (кластер
--   «Перекусы на ходу» — 10.11 и 12.11), вычитать некого; КГ 5% (RN/20) по BUDGET — подбирается по каждой акции отдельно.
-- LOAD_TO_ML = 2 (PUSH-коммуникация, в Manzana Online не идёт), LOAD_TO_MC = 1 (PUSH) — как ДЦО 30.10.
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после дня акции (позже 11.11.2026) — ничего не делается;
--   · шапки I_PROMO заводятся, только если их нет;
--   · акция, список которой уже есть в I_PROMO_OFFER, пропускается — повторный прогон её не задвоит;
--   · из базы списков прочитано не столько строк по акциям, сколько в раздаче → ошибка, ничего не пишется;
--   · у акции после сборки меньше 80% раздачи → ошибка, ничего не пишется;
--   · КГ акции не подобралась за 1 000 итераций → списки сошедшихся акций записываются, в конце ошибка —
--     через 10 минут повтор только по оставшимся.
-- Выгрузки CRM_GUID здесь нет: список на SFTP для Campaign забирает задание cvm-campaign-lists.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
GO

USE mci_model
GO


-- ==================================================================================
-- 1. ШАПКИ 101497–101522 В I_PROMO (заводятся, только если их нет)
-- ==================================================================================

if cast(getdate() as date) > '2026-11-11'
begin
	raiserror(N'DCO 11.11: promo day is over - headers skipped, file can be removed', 10, 1)
	return
end

declare @d date = '2026-11-11'		-- старт и финиш (ср)

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -6, @d), dateadd(day, -1, @d), @d, @d, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, @d), dateadd(day, +4, @d)
	 , N'PUSH', N'Активные, Новые', 4, null, (select MECHANICS from dbo.I_MECHANICS (nolock) where ID_MECHANICS = 7), 7, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101497, N'ДЦО Молоко топлёное -12%',                N'101497_ДЦО Молоко топлёное 12')
	, (101498, N'ДЦО Хлеб ржаной ЦФО -18%',                N'101498_ДЦО Хлеб ржаной ЦФО 18')
	, (101499, N'ДЦО Сметана -23%',                        N'101499_ДЦО Сметана 23')
	, (101500, N'ДЦО Сыр Российский -15%',                 N'101500_ДЦО Сыр Российский 15')
	, (101501, N'ДЦО Творог -20%',                         N'101501_ДЦО Творог 20')
	, (101502, N'ДЦО Йогурт -33%',                         N'101502_ДЦО Йогурт 33')
	, (101503, N'ДЦО Минеральная вода -19%',               N'101503_ДЦО Минеральная вода 19')
	, (101504, N'ДЦО Бедро куриное -17%',                  N'101504_ДЦО Бедро куриное 17')
	, (101505, N'ДЦО Хлеб зерновой СЗФО -25%',             N'101505_ДЦО Хлеб зерновой СЗФО 25')
	, (101506, N'ДЦО Майонез -30%',                        N'101506_ДЦО Майонез 30')
	, (101507, N'ДЦО Картофель -14%',                      N'101507_ДЦО Картофель 14')
	, (101508, N'ДЦО Масло сливочное -28%',                N'101508_ДЦО Масло сливочное 28')
	, (101509, N'ДЦО Виноград -15%',                       N'101509_ДЦО Виноград 15')
	, (101510, N'ДЦО Сырки глазированные -20%',            N'101510_ДЦО Сырки глазированные 20')
	, (101511, N'ДЦО Кола -21%',                           N'101511_ДЦО Кола 21')
	, (101512, N'ДЦО Туалетная бумага -10%',               N'101512_ДЦО Туалетная бумага 10')
	, (101513, N'ДЦО Томаты черри -70% на второй',         N'101513_ДЦО Томаты черри второй 70')
	, (101514, N'ДЦО Кетчуп -70% на второй',               N'101514_ДЦО Кетчуп второй 70')
	, (101515, N'ДЦО Перец сладкий -70% на второй',        N'101515_ДЦО Перец сладкий второй 70')
	, (101516, N'ДЦО Конфеты -70% на второй',              N'101516_ДЦО Конфеты второй 70')
	, (101517, N'ДЦО Кукуруза -70% на второй',             N'101517_ДЦО Кукуруза второй 70')
	, (101518, N'ДЦО Сыр рассольный -70% на второй',       N'101518_ДЦО Сыр рассольный второй 70')
	, (101519, N'ДЦО Кофе растворимый -70% на второй',     N'101519_ДЦО Кофе растворимый второй 70')
	, (101520, N'ДЦО Пельмени -70% на второй',             N'101520_ДЦО Пельмени второй 70')
	, (101521, N'ДЦО Брокколи -70% на второй',             N'101521_ДЦО Брокколи второй 70')
	, (101522, N'ДЦО Кондиционер для белья -70% на второй', N'101522_ДЦО Кондиционер белья второй 70')
	) v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)
GO


-- ==================================================================================
-- 2. СБОРКА СПИСКОВ 101497–101522
-- ==================================================================================

SET XACT_ABORT ON

-- защита: день акции прошёл — блок пропускается
if cast(getdate() as date) > '2026-11-11'
begin
	raiserror(N'DCO 11.11: promo day is over - lists skipped, file can be removed', 10, 1)
	return
end
-- защита: все 26 шапок в I_PROMO (раздел 1) и номер не занят чужой акцией (уникальности ID_PROMO в I_PROMO нет)
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_PROMO between 101497 and 101522 and ID_COMPANY = 1) < 26
	or exists (select 1 from I_PROMO (nolock) where ID_PROMO between 101497 and 101522 and ID_COMPANY = 1 and isnull(PROMO_NAME, N'') not like N'ДЦО %')
begin
	raiserror(N'DCO 11.11: headers 101497-101522 missing or number taken by another promo - nothing written', 16, 1)
	return
end

-- раздача на 10.11 (ru: cvm-api:/tmp/dco1111/assign.log) — эталон для сверки чтения и для защиты 80%
drop table if exists #ref
create table #ref (ID_PROMO int not null primary key, N_REF int not null)
insert into #ref (ID_PROMO, N_REF) values
	  (101497,  49411), (101498,  62451), (101499,  23653), (101500,  34099), (101501,  56835), (101502,  63558), (101503,  98350)
	, (101504, 107600), (101505, 138246), (101506,  38213), (101507,  56227), (101508,  76773), (101509, 132014), (101510, 127612)
	, (101511, 218037), (101512, 147339), (101513,  32582), (101514, 170535), (101515, 144121), (101516, 327475), (101517, 144551)
	, (101518, 171354), (101519, 158612), (101520, 181351), (101521,  98607), (101522,  94365)

-- акции, списки которых уже записаны, — пропускаются
drop table if exists #done
select r.ID_PROMO
into #done
from #ref r
where exists (select 1 from I_PROMO_OFFER o (nolock) where o.ID_PROMO = r.ID_PROMO)

if (select count(*) from #done) = 26
begin
	raiserror(N'DCO 11.11: all 26 lists already in I_PROMO_OFFER - skipped', 10, 1)
	return
end

declare @msg nvarchar(400)

-- проверок и ожиданий ночного задания нет (Елена 01.11.2026): список собирается сразу

-- раздача из базы списков портала (2 953 971 строка, клиент один раз)
drop table if exists #o
create table #o (ID_PROMO int not null, ID_CONTACT int not null, index cci_o clustered columnstore)

-- динамическим SQL: текст OPENQUERY — константа, ошибка связи падает здесь же, ничего не записано
exec sys.sp_executesql N'insert into #o (ID_PROMO, ID_CONTACT)
select id_promo, id_contact
from openquery(CVM_PORTAL, ''select id_promo, id_contact from lists.dco_20261111'')'

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
	set @msg = concat(N'DCO 11.11: read ', @n, N' rows from portal lists, expected ', @n_ref, N' distinct by 26 lists as in #ref - nothing written')
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
	set @msg = (select top 1 concat(N'DCO 11.11: list ', r.ID_PROMO, N' has ', isnull(x.N, 0), N' clients - less than 80%% of ', r.N_REF, N', nothing written')
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
	set @msg = concat(N'DCO 11.11: CG iteration ', @iter, N', accepted ', (select count(*) from #ok), N' of ', 26 - (select count(*) from #done))
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
		set @msg = concat(N'DCO 11.11: list ', @id, N' written by another run - skipped')
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


-- сводка по всем 26 акциям
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
			   where ID_PROMO between 101497 and 101522 and ID_COMPANY = 1 and ID_ORGANIZATION = 1
			   group by ID_PROMO) o
	on o.ID_PROMO = p.ID_PROMO
where p.ID_PROMO between 101497 and 101522 and p.ID_COMPANY = 1
order by p.ID_PROMO

-- КГ подобралась не у всех — ошибка, через 10 минут повтор только по оставшимся
if exists (select 1 from #ref r where r.ID_PROMO not in (select ID_PROMO from #done) and r.ID_PROMO not in (select ID_PROMO from #ok))
begin
	set @msg = concat(N'DCO 11.11: CG not found in ', @iter, N' iterations for ', (select count(*) from #ref r where r.ID_PROMO not in (select ID_PROMO from #done) and r.ID_PROMO not in (select ID_PROMO from #ok)), N' lists - will retry')
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
