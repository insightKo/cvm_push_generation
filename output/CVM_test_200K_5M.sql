-- CVM · две тестовые выборки: 200 000 GUID и 5 000 000 GUID, LOAD_TO_ML = 0
-- Номера акций задаются ОДИН раз ниже — @ID_200K и @ID_5M. Если хоть один занят в I_PROMO или I_PROMO_OFFER,
-- скрипт показывает занятые строки, останавливается и ничего не записывает — поменяй номера и запусти снова.
-- Клиенты произвольные: случайные CRM_GUID из офлайн-базы I_CVM_CONTACT (ID_ORGANIZATION = 1), без фильтра по сегменту и пушам.
-- Глобальная контрольная группа I_GLOBAL_CG исключена. Контрольной группы у тестов нет — все строки CONTROL_GROUP = 0.
-- Выборки независимые: один GUID может попасть в оба списка.
-- Весь скрипт — один пакет без GO, прогонять целиком.

DECLARE @ID_200K int = 100000002   -- первая пара была 100000000 / 100000001
DECLARE @ID_5M   int = 100000003

DECLARE @d date = cast(getdate() as date)
DECLARE @fd date = cast(getdate() as date)


-- ==================================================================================
-- ЧАСТЬ 0. ПРОВЕРКА — номера свободны
-- ==================================================================================

if exists (select 1 from I_PROMO (nolock) where ID_PROMO in (@ID_200K, @ID_5M))
	or exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (@ID_200K, @ID_5M))
begin
	select [Где занят] = N'I_PROMO', ID_PROMO, PROMO_NAME
	from I_PROMO (nolock)
	where ID_PROMO in (@ID_200K, @ID_5M)

	select [Где занят] = N'I_PROMO_OFFER', ID_PROMO, [Строк] = count(*)
	from I_PROMO_OFFER (nolock)
	where ID_PROMO in (@ID_200K, @ID_5M)
	group by ID_PROMO

	raiserror(N'Номер акции уже занят — поменяй @ID_200K / @ID_5M в начале скрипта. Ничего не записано.', 16, 1)
	return
end


-- ==================================================================================
-- ЧАСТЬ 1. База случайных GUID — по одной строке на GUID
-- ==================================================================================

drop table if exists #g

select	b.CRM_GUID
		, ID_CONTACT = min(a.ID_CONTACT)
into #g
from I_CVM_CONTACT as a (nolock)
		inner join I_CONTACT as b (nolock)
		on a.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on a.ID_CONTACT=c.ID_CONTACT
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and b.CRM_GUID is not null
	and c.ID_CONTACT is null
group by b.CRM_GUID



-- ==================================================================================
-- ЧАСТЬ 2. Случайные списки — сортировка заранее, до записи в таблицы
-- ==================================================================================

drop table if exists #s200k
select top (200000) ID_CONTACT, CRM_GUID
into #s200k
from #g
order by newid()

drop table if exists #s5m
select top (5000000) ID_CONTACT, CRM_GUID
into #s5m
from #g
order by newid()


-- ==================================================================================
-- ЧАСТЬ 3. Запись в I_PROMO и I_PROMO_OFFER одной транзакцией — либо всё, либо ничего
-- Не запускать одновременно с недельными выборками.
-- ==================================================================================

set xact_abort on
begin tran

	INSERT INTO [mci_model].[dbo].[I_PROMO]
	values (
		  DATEADD(week,-6, @d)
		  , DATEADD(day,-1, @d)
		  , @d
		  , @fd
		  , @ID_200K
		  , 'Test 200K #2'
		  , dateadd(day,+1,@fd)
		  ,	dateadd(day,+7,@fd)
		  , 'PUSH'
		  , 'ВСЕ оффлайн'
		  , 28
		  , NULL
		  , 'Коммуникация'
		  , 7
		  , 1
		  , NULL
		  , NULL
		  , 'CVM'
		  , 1
		  )

	INSERT INTO [mci_model].[dbo].[I_PROMO]
	values (
		  DATEADD(week,-6, @d)
		  , DATEADD(day,-1, @d)
		  , @d
		  , @fd
		  , @ID_5M
		  , 'Test 5M #2'
		  , dateadd(day,+1,@fd)
		  ,	dateadd(day,+7,@fd)
		  , 'PUSH'
		  , 'ВСЕ оффлайн'
		  , 28
		  , NULL
		  , 'Коммуникация'
		  , 7
		  , 1
		  , NULL
		  , NULL
		  , 'CVM'
		  , 1
		  )

	INSERT INTO I_PROMO_OFFER
	select ID_PROMO = @ID_200K, ID_CONTACT, CONTROL_GROUP = 0, ID_ORGANIZATION = 1, LOAD_TO_ML = 0, ID_COMPANY = 1, CRM_GUID
	from #s200k

	INSERT INTO I_PROMO_OFFER
	select ID_PROMO = @ID_5M, ID_CONTACT, CONTROL_GROUP = 0, ID_ORGANIZATION = 1, LOAD_TO_ML = 0, ID_COMPANY = 1, CRM_GUID
	from #s5m

commit


-- ==================================================================================
-- ЧАСТЬ 4. КОНТРОЛЬ
-- Ожидание: 200 000 и 5 000 000 уникальных GUID, у всех LOAD_TO_ML = 0 и CONTROL_GROUP = 0.
-- ==================================================================================

select	ID_PROMO
		, [Строк] = count(*)
		, [Уникальных GUID] = count(distinct CRM_GUID)
		, [LOAD_TO_ML = 0] = sum(case when LOAD_TO_ML = 0 then 1 else 0 end)
		, [CONTROL_GROUP = 0] = sum(case when CONTROL_GROUP = 0 then 1 else 0 end)
from I_PROMO_OFFER (nolock)
where ID_PROMO in (@ID_200K, @ID_5M)
group by ID_PROMO
order by ID_PROMO


-- ==================================================================================
-- ЧАСТЬ 5. ВЫГРУЗКА — сначала 200 000, потом 5 000 000
-- 5 млн строк в сетку SSMS не выводить: включить «Results to File» без заголовков колонок или выгрузить через bcp.
-- ==================================================================================

select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = @ID_200K and CONTROL_GROUP = 0

select CRM_GUID
from I_PROMO_OFFER (nolock)
where ID_PROMO = @ID_5M and CONTROL_GROUP = 0
GO
