-- CVM сентябрь 2026 · заведение ВСЕХ акций месяца в I_PROMO
-- Источник: вкладка «CVM offline» таблицы CVM offline, строки 101360–101390 (заготовка плана от 25.08.2026).
-- Запускается ОДИН раз на старте месяца, после согласования плана. Блоки разделены GO — можно прогнать целиком.
-- Изменения после первого запуска вносить дельта-скриптами, не повторным прогоном.
-- Выборки клиентов — в недельных скриптах (CVM_36_26.sql и далее), без вставок в I_PROMO.
--
-- Правила заполнения (как в CVM_august_2026_I_PROMO.sql):
--   окно истории  -6 недель — если аудитория Активные/Новые или подсегмент активных;
--                 -52*3 недели — если в аудитории есть Отток, Спящие или Случайные;
--   ID_SEGMENT    4 Активные, 2 Отток, 12 Случайные, 6 дети, 9 животные, 11 готовая еда,
--                 30 вино, 31 полуфабрикаты, 32 ПП, 10 Активные (алкоголь);
--   ID_MECHANICS  4 Активируемая скидка, 5 Купон, 7 Коммуникация, 8 Кэшбек X баллов,
--                 16 Кэшбек активируемый, 17 Предначисление бонусов с активацией.


-- ============================== НЕДЕЛЯ 36 ==============================

-- 101369 · 1 сентября — день Kinder: активируй 20% кешбэка на Kinder · Активные. Мамы + Активные, Новые · вт 01.09

DECLARE @d date = '2026-09-01'
DECLARE @fd date = '2026-09-01'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101369
	  , '1 сентября — день Kinder: активируй 20% кешбэка на Kinder'
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

-- 101362 · Пиво купон на 100р. на 7 дней · slip · Активные, Новые без пушей · выдача 03.09–30.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101362
	  , 'Пиво купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'SLIP'
	  , 'Активные (алкоголь)'
	  , 10
	  , NULL
	  , 'Купон'
	  , 5
	  , 4
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101361 · Крепкий алкоголь купон на 100р. на 7 дней · slip · Активные, Новые без пушей · выдача 03.09–30.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101361
	  , 'Крепкий алкоголь купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'SLIP'
	  , 'Активные (алкоголь)'
	  , 10
	  , NULL
	  , 'Купон'
	  , 5
	  , 4
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101360 · Вина купон на 100р. на 7 дней · slip · Активные, Новые без пушей · выдача 03.09–30.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101360
	  , 'Вина купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'SLIP'
	  , 'Активные (алкоголь)'
	  , 10
	  , NULL
	  , 'Купон'
	  , 5
	  , 4
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101370 · Активируй 20% кешбэка на вино и игристое · Активные, Новые · чт 03.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-06'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101370
	  , 'Активируй 20% кешбэка на вино и игристое'
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

-- 101371 · Активируй 20% кешбэка на крепкий алкоголь · Активные, Новые · чт 03.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-06'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101371
	  , 'Активируй 20% кешбэка на крепкий алкоголь'
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

-- 101372 · Активируй 20% кешбэка на пиво · Активные, Новые · чт 03.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-06'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101372
	  , 'Активируй 20% кешбэка на пиво'
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

-- 101373 · 50% кешбэка на алкоголь · Отток, Спящие · чт 03.09

DECLARE @d date = '2026-09-03'
DECLARE @fd date = '2026-09-06'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101373
	  , '50% кешбэка на алкоголь'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
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
GO

-- 101374 · Скидка 20% на средства для мытья посуды · Активные, Новые · вс 06.09

DECLARE @d date = '2026-09-06'
DECLARE @fd date = '2026-09-06'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101374
	  , 'Скидка 20% на средства для мытья посуды'
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


-- ============================== НЕДЕЛЯ 37 ==============================

-- 101363 · Коммуникация детские категории · Активные мамы · вт 08.09, серия сентября

DECLARE @d date = '2026-09-08'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101363
	  , 'Коммуникация детские категории'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (дети)'
	  , 6
	  , NULL
	  , 'Коммуникация'
	  , 7
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101364 · Коммуникация по ПП · Активные ПП · вт 08.09, серия сентября

DECLARE @d date = '2026-09-08'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101364
	  , 'Коммуникация по ПП'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (Правильное питание)'
	  , 32
	  , NULL
	  , 'Коммуникация'
	  , 7
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101365 · Коммуникация по готовой еде · Активные перекус · вт 08.09, серия сентября

DECLARE @d date = '2026-09-08'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101365
	  , 'Коммуникация по готовой еде'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (готовая еда)'
	  , 11
	  , NULL
	  , 'Коммуникация'
	  , 7
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101366 · Коммуникация товары для животных · Активные зоо · вт 08.09, серия сентября

DECLARE @d date = '2026-09-08'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101366
	  , 'Коммуникация товары для животных'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (животные)'
	  , 9
	  , NULL
	  , 'Коммуникация'
	  , 7
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101375 · Активируй 20% на виноград · Активные, Новые, Спящие, Отток · ср 09.09

DECLARE @d date = '2026-09-09'
DECLARE @fd date = '2026-09-09'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101375
	  , 'Активируй 20% на виноград'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
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

-- 101378 · Активируй 50 монет на любые покупки · Активные, Новые · чт 10.09

DECLARE @d date = '2026-09-10'
DECLARE @fd date = '2026-09-13'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101378
	  , 'Активируй 50 монет на любые покупки'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные'
	  , 4
	  , NULL
	  , 'Предначисление бонусов с активацией'
	  , 17
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101376 · Активируй 100 монет на любые покупки · Отток, Спящие · чт 10.09

DECLARE @d date = '2026-09-10'
DECLARE @fd date = '2026-09-13'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101376
	  , 'Активируй 100 монет на любые покупки'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
	  , NULL
	  , 'Предначисление бонусов с активацией'
	  , 17
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101377 · Активируй 100 монет на любые покупки · Случайные · чт 10.09

DECLARE @d date = '2026-09-10'
DECLARE @fd date = '2026-09-13'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101377
	  , 'Активируй 100 монет на любые покупки'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Случайные'
	  , 12
	  , NULL
	  , 'Предначисление бонусов с активацией'
	  , 17
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101368 · Тематическая рассылка пиво · Активные пиво и п/ф · пт 11.09, серия сентября

DECLARE @d date = '2026-09-11'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101368
	  , 'Тематическая рассылка пиво'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (полуфабрикаты)'
	  , 31
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

-- 101367 · Тематическая рассылка вино и просекко · Активные вино и просекко · пт 11.09, серия сентября

DECLARE @d date = '2026-09-11'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101367
	  , 'Тематическая рассылка вино и просекко'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Активные (вино)'
	  , 30
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

-- 101379 · Скидка 20% на носки и колготки · Активные, Новые · вс 13.09

DECLARE @d date = '2026-09-13'
DECLARE @fd date = '2026-09-13'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101379
	  , 'Скидка 20% на носки и колготки'
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


-- ============================== НЕДЕЛЯ 38 ==============================

-- 101380 · Купи 2 раза на неделе от 1000р. и получи 300 монет · Активные, Новые · пн 14.09, частотная

DECLARE @d date = '2026-09-14'
DECLARE @fd date = '2026-09-20'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101380
	  , 'Купи 2 раза на неделе от 1000р. и получи 300 монет'
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

-- 101381 · Активируй 20% на чай, какао и горячий шоколад · Активные, Новые · ср 16.09

DECLARE @d date = '2026-09-16'
DECLARE @fd date = '2026-09-16'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101381
	  , 'Активируй 20% на чай, какао и горячий шоколад'
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

-- 101382 · Активируй 20% кешбэка на мясо и птицу · Активные, Новые · чт 17.09

DECLARE @d date = '2026-09-17'
DECLARE @fd date = '2026-09-20'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101382
	  , 'Активируй 20% кешбэка на мясо и птицу'
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

-- 101383 · 50% кешбэка на мясо и птицу · Отток, Спящие · чт 17.09

DECLARE @d date = '2026-09-17'
DECLARE @fd date = '2026-09-20'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101383
	  , '50% кешбэка на мясо и птицу'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
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
GO

-- 101384 · Скидка 20% на средства для уборки кухни и освежители · Активные, Новые · вс 20.09

DECLARE @d date = '2026-09-20'
DECLARE @fd date = '2026-09-20'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101384
	  , 'Скидка 20% на средства для уборки кухни и освежители'
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


-- ============================== НЕДЕЛЯ 39 ==============================

-- 101385 · Активируй 20% на грибы · Активные, Новые, Спящие, Отток · ср 23.09

DECLARE @d date = '2026-09-23'
DECLARE @fd date = '2026-09-23'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101385
	  , 'Активируй 20% на грибы'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
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

-- 101386 · Активируй 20% кешбэка на сыр и молочные продукты · Активные, Новые · чт 24.09

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

-- 101387 · 50% кешбэка на сыр и молочные продукты · Отток, Спящие · чт 24.09

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
	  ,	dateadd(day,+7,@fd)
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
GO

-- 101388 · Баланс баллов · Активные, Новые, Спящие, Отток · сб 26.09

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
	  ,	dateadd(day,+7,@fd)
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
GO

-- 101389 · Скидка 20% на дезодоранты и гели для душа · Активные, Новые · вс 27.09

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


-- ============================== НЕДЕЛЯ 40 ==============================

-- 101390 · Активируй 20% на каши и хлопья · Активные, Новые, Спящие, Отток · ср 30.09

DECLARE @d date = '2026-09-30'
DECLARE @fd date = '2026-09-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101390
	  , 'Активируй 20% на каши и хлопья'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'PUSH'
	  , 'Отток'
	  , 2
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


-- ==================================================================================
-- ПРОВЕРКА: все 31 акции сентября заведены
-- ==================================================================================

select *
from I_PROMO (nolock)
where ID_PROMO between 101360 and 101390
order by ID_PROMO
GO
