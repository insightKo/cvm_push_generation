-- CVM сентябрь 2026 · обновление I_PROMO от 21.09 — окно «после» у акций недели 39
-- Запускать ДО выборок недели 39 (CVM_39_26.sql).
-- Норма окна «после» — 4 дня: финиш + 1 … финиш + 4. В месячном скрипте стояло 7 дней.
-- Блоки 2–6 перезаводят акции недели 39, блок 7 приводит к норме остальные сентябрьские акции.
-- У 101385 заодно меняется аудитория: только Активные и Новые (решение Елены 21.09) — сегмент «Активные» 4 и окно истории -6 недель.
-- Остальные поля не меняются. Удаление и вставка каждой акции — в одной транзакции.


-- ==================================================================================
-- БЛОК 1. ПРОВЕРКА ПЕРЕД ИЗМЕНЕНИЕМ
-- Ожидание: по каждой акции одна строка в I_PROMO, в I_PROMO_OFFER — 0 клиентов.
-- Если списки уже собраны, шапки не трогать.
-- ==================================================================================

select *
from I_PROMO (nolock)
where ID_PROMO in (101385, 101386, 101387, 101388, 101389)
order by ID_PROMO
GO

select ID_PROMO, [Клиентов в списке] = count(distinct ID_CONTACT)
from I_PROMO_OFFER (nolock)
where ID_PROMO in (101385, 101386, 101387, 101388, 101389)
group by ID_PROMO
GO


-- ==================================================================================
-- БЛОК 2. 101385 · перезаводим: аудитория Активные и Новые, окно «после» 4 дня
-- ==================================================================================

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101385)
begin
	raiserror(N'101385: список уже собран — шапку не трогаем', 16, 1)
	return
end

set xact_abort on
begin tran

DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101385 and ID_COMPANY = 1

-- 101385 · Активируй 20% на грибы и тыкву · Активные, Новые · ср 23.09

DECLARE @d date = '2026-09-23'
DECLARE @fd date = '2026-09-23'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101385
	  , 'Активируй 20% на грибы и тыкву'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Активируемая скидка'
	  , 4
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )

commit
GO


-- ==================================================================================
-- БЛОК 3. 101386 · перезаводим с окном «после» 4 дня
-- ==================================================================================

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101386)
begin
	raiserror(N'101386: список уже собран — шапку не трогаем', 16, 1)
	return
end

set xact_abort on
begin tran

DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101386 and ID_COMPANY = 1

-- 101386 · Активируй 20% кешбэка на сыр и молочные продукты · чт 24.09–27.09

DECLARE @d date = '2026-09-24'
DECLARE @fd date = '2026-09-27'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101386
	  , 'Активируй 20% кешбэка на сыр и молочные продукты'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Кэшбек активируемый'
	  , 16
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )

commit
GO


-- ==================================================================================
-- БЛОК 4. 101387 · перезаводим с окном «после» 4 дня
-- ==================================================================================

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101387)
begin
	raiserror(N'101387: список уже собран — шапку не трогаем', 16, 1)
	return
end

set xact_abort on
begin tran

DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101387 and ID_COMPANY = 1

-- 101387 · 50% кешбэка на сыр и молочные продукты · чт 24.09–27.09

DECLARE @d date = '2026-09-24'
DECLARE @fd date = '2026-09-27'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101387
	  , '50% кешбэка на сыр и молочные продукты'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
	  , NULL
	  , 'Кэшбек активируемый'
	  , 16
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )

commit
GO


-- ==================================================================================
-- БЛОК 5. 101388 · перезаводим с окном «после» 4 дня
-- ==================================================================================

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101388)
begin
	raiserror(N'101388: список уже собран — шапку не трогаем', 16, 1)
	return
end

set xact_abort on
begin tran

DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101388 and ID_COMPANY = 1

-- 101388 · Баланс баллов · сб 26.09

DECLARE @d date = '2026-09-26'
DECLARE @fd date = '2026-09-26'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101388
	  , 'Баланс баллов'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
	  , NULL
	  , 'Коммуникация'
	  , 7
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )

commit
GO


-- ==================================================================================
-- БЛОК 6. 101389 · перезаводим с окном «после» 4 дня
-- ==================================================================================

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101389)
begin
	raiserror(N'101389: список уже собран — шапку не трогаем', 16, 1)
	return
end

set xact_abort on
begin tran

DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101389 and ID_COMPANY = 1

-- 101389 · Скидка 20% на дезодоранты и гели для душа · вс 27.09

DECLARE @d date = '2026-09-27'
DECLARE @fd date = '2026-09-27'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101389
	  , 'Скидка 20% на дезодоранты и гели для душа'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Активируемая скидка'
	  , 4
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )

commit
GO


-- ==================================================================================
-- БЛОК 7. ОСТАЛЬНЫЕ СЕНТЯБРЬСКИЕ АКЦИИ — окно «после» к норме 4 дня
-- Сначала посмотреть, что изменится, потом выполнить UPDATE.
-- ==================================================================================

select ID_PROMO, PROMO_NAME, START_DATE, FINISH_DATE
		, START_DATE_AFTER, FINISH_DATE_AFTER
		, [Дней сейчас] = datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1
		, [Будет с] = dateadd(day, 1, FINISH_DATE)
		, [Будет по] = dateadd(day, 4, FINISH_DATE)
from I_PROMO (nolock)
where ID_COMPANY = 1 and TYPE_PROMO = 'CVM'
	and ID_PROMO between 101360 and 101391
	and (START_DATE_AFTER is null or FINISH_DATE_AFTER is null
		or datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1 <> 4)
order by ID_PROMO
GO

UPDATE I_PROMO
SET START_DATE_AFTER = dateadd(day, 1, FINISH_DATE)
	, FINISH_DATE_AFTER = dateadd(day, 4, FINISH_DATE)
where ID_COMPANY = 1 and TYPE_PROMO = 'CVM'
	and ID_PROMO between 101360 and 101391
	and (START_DATE_AFTER is null or FINISH_DATE_AFTER is null
		or datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1 <> 4)
GO


-- ==================================================================================
-- БЛОК 8. КОНТРОЛЬ
-- Ожидание: запрос пустой — у всех сентябрьских акций окно «после» ровно 4 дня.
-- ==================================================================================

select ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER
		, [Дней] = datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1
from I_PROMO (nolock)
where ID_COMPANY = 1 and TYPE_PROMO = 'CVM'
	and ID_PROMO between 101360 and 101391
	and (START_DATE_AFTER is null or FINISH_DATE_AFTER is null
		or datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1 <> 4)
order by ID_PROMO
GO
