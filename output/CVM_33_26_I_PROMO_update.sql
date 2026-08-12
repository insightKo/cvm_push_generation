-- CVM август 2026 · обновление I_PROMO от 11.08 — изменение плана недели 33
-- Запускать ОДИН раз (месячный скрипт CVM_august_2026_I_PROMO.sql уже прогнан, его повторно не запускать).
-- Изменения: 101348 отменена; у 101347 аудитория сужена до Активные/Новые; добавлены 101358 и 101359.

-- 101348 · Скидка 20% на канцтовары и товары для школы — акция отменена, убираем из I_PROMO
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101348
GO

-- 101347 · было: широкая аудитория (Отток, окно -52*3) — стало: Активные, Новые. Перезаводим строку
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101347
GO

-- 101347 · Активируй 20% кешбэка на сезонные овощи · Активные, Новые · чт 13.08 / сб 15.08

DECLARE @d date = '2026-08-13'
DECLARE @fd date = '2026-08-16'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101347
	  , 'Активируй 20% кешбэка на сезонные овощи'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
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
GO


-- 101358 · Вернем 100 монет за любую покупку в период акции · Активные, Новые · 13.08–16.08, пуша в сетке нет

DECLARE @d date = '2026-08-13'
DECLARE @fd date = '2026-08-16'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101358
	  , 'Вернем 100 монет за любую покупку в период акции'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Кэшбек X баллов'
	  , 8
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO


-- 101359 · Скидка 20% на туалетную бумагу и бумажные полотенца · Активные, Новые · вс 16.08

DECLARE @d date = '2026-08-16'
DECLARE @fd date = '2026-08-16'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101359
	  , 'Скидка 20% на туалетную бумагу и бумажные полотенца'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
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
GO
