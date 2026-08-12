-- CVM август 2026 · заведение ВСЕХ акций месяца в I_PROMO (кроме 101334 — прошла 02.08)
-- Актуальный план от 11.08: 101348 отменена, 101347 сужена до Активные/Новые, добавлены 101358 и 101359.
-- Запускается ОДИН раз на старте месяца, после согласования плана. Блоки разделены GO — можно прогнать целиком.
-- Изменения после первого запуска вносить дельта-скриптами (CVM_33_26_I_PROMO_update.sql), не повторным прогоном.
-- Выборки клиентов — в недельных скриптах (CVM_32_26.sql, CVM_33_26.sql и далее), без вставок в I_PROMO.


-- ============================== НЕДЕЛЯ 32 ==============================

-- 101335 · Коммуникация детские категории · Активные мамы · вт 04.08, серия августа

DECLARE @d date = '2026-08-04'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101335
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

-- 101336 · Коммуникация товары для животных · Активные зоо · вт 04.08, серия августа

DECLARE @d date = '2026-08-04'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101336
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

-- 101337 · Коммуникация по готовой еде · Активные перекус · вт 04.08, серия августа

DECLARE @d date = '2026-08-04'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101337
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

-- 101338 · Коммуникация по ПП · Активные ПП · вт 04.08, серия августа

DECLARE @d date = '2026-08-04'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101338
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

-- 101332 · Купон 50р. на любую покупку · slip · Активные без пушей · выдача 05.08–24.08

DECLARE @d date = '2026-08-05'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101332
	  , 'Купон 50р. на любую покупку'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'SLIP'
	  , 'Активные'
	  , 4
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

-- 101333 · Купон 100р. на покупку от 900р. · slip · Отток, Спящие без пушей · выдача 05.08–24.08

DECLARE @d date = '2026-08-05'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101333
	  , 'Купон 100р. на покупку от 900р.'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+7,@fd)
	  , 'SLIP'
	  , 'Отток'
	  , 2
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

-- 101339 · Активируй 20% на квас и лимонады · Активные, Новые · ср 05.08

DECLARE @d date = '2026-08-05'
DECLARE @fd date = '2026-08-05'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101339
	  , 'Активируй 20% на квас и лимонады'
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

-- 101340 · Активируй 50 монет на любые покупки · Активные, Новые · чт 06.08 / сб 08.08 / пн 10.08

DECLARE @d date = '2026-08-06'
DECLARE @fd date = '2026-08-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101340
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

-- 101341 · Активируй 100 монет на любые покупки · Отток, Спящие · чт 06.08 / сб 08.08 / пн 10.08

DECLARE @d date = '2026-08-06'
DECLARE @fd date = '2026-08-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101341
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

-- 101342 · Активируй 100 монет на любые покупки · Случайные · чт 06.08 / сб 08.08 / пн 10.08

DECLARE @d date = '2026-08-06'
DECLARE @fd date = '2026-08-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101342
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

-- 101343 · Тематическая рассылка пиво · Активные пиво и п/ф · пт 07.08, серия августа

DECLARE @d date = '2026-08-07'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101343
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

-- 101344 · Тематическая рассылка вино и просекко · Активные вино и просекко · пт 07.08, серия августа

DECLARE @d date = '2026-08-07'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101344
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

-- 101345 · Скидка 20% на средства для стирки · Активные, Новые · вс 09.08

DECLARE @d date = '2026-08-09'
DECLARE @fd date = '2026-08-09'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101345
	  , 'Скидка 20% на средства для стирки'
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


-- ============================== НЕДЕЛЯ 33 ==============================

-- 101346 · Активируй 20% на арбузы и дыни · Активные, Новые, Спящие, Отток · ср 12.08

DECLARE @d date = '2026-08-12'
DECLARE @fd date = '2026-08-12'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101346
	  , 'Активируй 20% на арбузы и дыни'
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


-- ============================== НЕДЕЛЯ 34 ==============================

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

-- 101351 · Активируй 150р. на чек от 1500р. · Спящие, Отток · чт 20.08 / сб 22.08 / чт 27.08 / пн 31.08

DECLARE @d date = '2026-08-20'
DECLARE @fd date = '2026-08-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101351
	  , 'Активируй 150р. на чек от 1500р.'
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

-- 101354 · Скидка 20% на шампуни и уход за волосами · Активные, Новые · вс 23.08

DECLARE @d date = '2026-08-23'
DECLARE @fd date = '2026-08-23'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101354
	  , 'Скидка 20% на шампуни и уход за волосами'
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


-- ============================== НЕДЕЛЯ 35 ==============================

-- 101355 · Активируй 20% на конфеты в коробках · Активные, Новые, Спящие, Отток · ср 26.08

DECLARE @d date = '2026-08-26'
DECLARE @fd date = '2026-08-26'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101355
	  , 'Активируй 20% на конфеты в коробках'
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

-- 101356 · Баланс баллов · Активные, Новые, Спящие, Отток · сб 29.08

DECLARE @d date = '2026-08-29'
DECLARE @fd date = '2026-08-29'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101356
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

-- 101357 · Скидка 20% на средства для ухода за полостью рта · Активные, Новые · вс 30.08

DECLARE @d date = '2026-08-30'
DECLARE @fd date = '2026-08-30'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101357
	  , 'Скидка 20% на средства для ухода за полостью рта'
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
