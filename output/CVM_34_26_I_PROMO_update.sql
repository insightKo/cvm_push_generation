-- CVM август 2026 · обновление I_PROMO от 11.08 — изменение аудиторий недели 34
-- Запускать ОДИН раз перед выборками недели 34 (CVM_34_26.sql).
-- Изменения по CVM offline: 101349, 101350, 101352, 101353 сужены с широкой аудитории до Активные/Новые
-- (в заведении меняется сегмент «Отток»->«Активные» и окно анализа). 101351 (Спящие/Отток) и остальные — без изменений.

-- 101349 · перезаводим с новым сегментом
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101349
GO

-- 101349 · Активируй 20% на яблоки и груши · Активные, Новые · ср 19.08

DECLARE @d date = '2026-08-19'
DECLARE @fd date = '2026-08-19'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101349
	  , 'Активируй 20% на яблоки и груши'
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


-- 101350 · перезаводим с новым сегментом
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101350
GO

-- 101350 · Активируй 100р. на чек от 1000р. · Активные, Новые · нижняя треть по ср. чеку · чт 20.08 / сб 22.08 / чт 27.08 / пн 31.08

DECLARE @d date = '2026-08-20'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101350
	  , 'Активируй 100р. на чек от 1000р.'
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


-- 101352 · перезаводим с новым сегментом
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101352
GO

-- 101352 · Активируй 200р. на чек от 2000р. · Активные, Новые · средняя треть по ср. чеку · чт 20.08 / сб 22.08 / чт 27.08 / пн 31.08

DECLARE @d date = '2026-08-20'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101352
	  , 'Активируй 200р. на чек от 2000р.'
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


-- 101353 · перезаводим с новым сегментом
DELETE FROM [mci_model].[dbo].[I_PROMO] where ID_PROMO = 101353
GO

-- 101353 · Активируй 300р. на чек от 3000р. · Активные, Новые · верхняя треть по ср. чеку · чт 20.08 / сб 22.08 / чт 27.08 / пн 31.08

DECLARE @d date = '2026-08-20'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101353
	  , 'Активируй 300р. на чек от 3000р.'
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


