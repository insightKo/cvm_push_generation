-- CVM октябрь 2026 · заведение ВСЕХ акций месяца в I_PROMO
-- Источник: output/план_октябрь_2026_заготовка.json = строки «CVM offline» 101393–101434 (записаны 29.09.2026).
-- Запускается ОДИН раз, до выборок недели 40. Блоки через GO — можно прогнать целиком; изменения потом — дельта-скриптами CVM_<нед>_26_I_PROMO_update.sql.
-- Потоки заводятся одной строкой на весь период: 101428 «Отток 50% кешбэка по дням» (Отток и Спящие, 01.10–31.10, дни 101428_1…_17 — в CVM offline)
--                                                    и 101426 «Дарим 100 монет на неделю» (низкочастотные Активные + Случайные, 05.10–31.10, недели 101426_1…_4).
-- Правила: окно истории -6 недель для Активных/Новых и подсегментов, -52*3 недели при Оттоке/Спящих/Случайных; окно «после» — 4 дня всем (решение Елены 21.09.2026).
-- ID_SEGMENT и ID_MECHANICS — как у сентябрьских и августовских аналогов (серии пиво/вино — «Активируемая скидка» 4, как 101367/101368).

-- защита: если хоть одна акция месяца уже заведена — скрипт не выполняется (повторный прогон задвоил бы шапки)
-- список номеров — ровно те 32, что заводятся ниже (как в контрольном SELECT); номера-пропуски 101405, 101408… не проверяются
if exists (select 1 from I_PROMO (nolock)
		where ID_PROMO in (101393, 101394, 101395, 101396, 101397, 101398, 101399, 101400, 101401, 101402, 101403, 101404, 101406, 101407, 101410, 101412, 101413, 101415, 101416, 101417, 101419, 101420, 101421, 101422, 101423, 101425, 101426, 101427, 101428, 101432, 101433, 101434)
			and ID_COMPANY = 1)
begin
	raiserror(N'Акции октября уже есть в I_PROMO — скрипт не выполняется', 16, 1)
	set noexec on
end
GO

-- 101393 · Купон 50р. на любую покупку · Активные, Новые — без PUSH/APP, без покупок алкоголя · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101393
	  , 'Купон 50р. на любую покупку'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101394 · Вина купон на 100р. на 7 дней · Активные, Новые — покупатели вина и игристого за 8 недель, без PUSH/APP · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101394
	  , 'Вина купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101395 · Крепкий алкоголь купон на 100р. на 7 дней · Активные, Новые — покупатели крепкого алкоголя за 8 недель, без PUSH/APP · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101395
	  , 'Крепкий алкоголь купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101396 · Пиво купон на 100р. на 7 дней · Активные, Новые — покупатели пива за 8 недель, без PUSH/APP · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101396
	  , 'Пиво купон на 100р. на 7 дней'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101397 · Коммуникация детские категории · Активные. Мамы · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101397
	  , 'Коммуникация детские категории'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101398 · Коммуникация по ПП · Активные. ПП · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101398
	  , 'Коммуникация по ПП'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101399 · Коммуникация по готовой еде · Активные. Перекус · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101399
	  , 'Коммуникация по готовой еде'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101400 · Коммуникация товары для животных · Активные. Зоо · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101400
	  , 'Коммуникация товары для животных'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101401 · Тематическая рассылка вино и просекко · Активные. Вино, Активные. Просекко · 02.10–31.10

DECLARE @d date = '2026-10-02'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101401
	  , 'Тематическая рассылка вино и просекко'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101402 · Тематическая рассылка пиво · Активные. Пиво и П/ф · 02.10–31.10

DECLARE @d date = '2026-10-02'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101402
	  , 'Тематическая рассылка пиво'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
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

-- 101403 · 10% скидка на готовый кофе до 12 утра · Активные. Тонус, Активные. Кофе · 06.10–31.10

DECLARE @d date = '2026-10-06'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101403
	  , '10% скидка на готовый кофе до 12 утра'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные (тонус)'
	  , 29
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

-- 101404 · Активируй 20% кешбэка на конфеты в коробках и шоколад · Активные, Новые · 01.10–04.10

DECLARE @d date = '2026-10-01'
DECLARE @fd date = '2026-10-04'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101404
	  , 'Активируй 20% кешбэка на конфеты в коробках и шоколад'
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
GO

-- 101406 · Скидка 20% на средства для мытья полов и универсальные чистящие · Активные, Новые · 04.10–04.10

DECLARE @d date = '2026-10-04'
DECLARE @fd date = '2026-10-04'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101406
	  , 'Скидка 20% на средства для мытья полов и универсальные чистящие'
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
GO

-- 101407 · Активируй 20% на капусту, свёклу и морковь · Активные, Новые · 07.10–07.10

DECLARE @d date = '2026-10-07'
DECLARE @fd date = '2026-10-07'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101407
	  , 'Активируй 20% на капусту, свёклу и морковь'
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
GO

-- 101410 · Скидка 20% на крем для рук и тела · Активные, Новые · 11.10–11.10

DECLARE @d date = '2026-10-11'
DECLARE @fd date = '2026-10-11'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101410
	  , 'Скидка 20% на крем для рук и тела'
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
GO

-- 101412 · Активируй 20% на мандарины, апельсины и лимоны · Активные, Новые · 14.10–14.10

DECLARE @d date = '2026-10-14'
DECLARE @fd date = '2026-10-14'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101412
	  , 'Активируй 20% на мандарины, апельсины и лимоны'
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
GO

-- 101413 · Активируй 20% кешбэка на колбасы, сосиски и ветчину · Активные, Новые · 15.10–18.10

DECLARE @d date = '2026-10-15'
DECLARE @fd date = '2026-10-18'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101413
	  , 'Активируй 20% кешбэка на колбасы, сосиски и ветчину'
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
GO

-- 101415 · Скидка 20% на женскую гигиену · Активные, Новые · 18.10–18.10

DECLARE @d date = '2026-10-18'
DECLARE @fd date = '2026-10-18'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101415
	  , 'Скидка 20% на женскую гигиену'
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
GO

-- 101416 · Активируй 20% на пельмени и вареники · Активные, Новые · 21.10–21.10

DECLARE @d date = '2026-10-21'
DECLARE @fd date = '2026-10-21'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101416
	  , 'Активируй 20% на пельмени и вареники'
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
GO

-- 101417 · Активируй 20% кешбэка на готовую еду и кулинарию · Активные, Новые · 22.10–25.10

DECLARE @d date = '2026-10-22'
DECLARE @fd date = '2026-10-25'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101417
	  , 'Активируй 20% кешбэка на готовую еду и кулинарию'
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
GO

-- 101419 · Скидка 20% на бумажные и влажные салфетки · Активные, Новые · 25.10–25.10

DECLARE @d date = '2026-10-25'
DECLARE @fd date = '2026-10-25'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101419
	  , 'Скидка 20% на бумажные и влажные салфетки'
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
GO

-- 101420 · Активируй 20% на хурму и гранат · Активные, Новые · 28.10–28.10

DECLARE @d date = '2026-10-28'
DECLARE @fd date = '2026-10-28'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101420
	  , 'Активируй 20% на хурму и гранат'
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
GO

-- 101421 · Активируй 20% кешбэка на вино и игристое · Активные, Новые — покупатели категории за 8 недель · 08.10–10.10

DECLARE @d date = '2026-10-08'
DECLARE @fd date = '2026-10-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101421
	  , 'Активируй 20% кешбэка на вино и игристое'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные (алкоголь)'
	  , 10
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

-- 101422 · Активируй 20% кешбэка на крепкий алкоголь · Активные, Новые — покупатели категории за 8 недель · 08.10–10.10

DECLARE @d date = '2026-10-08'
DECLARE @fd date = '2026-10-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101422
	  , 'Активируй 20% кешбэка на крепкий алкоголь'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные (алкоголь)'
	  , 10
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

-- 101423 · Активируй 20% кешбэка на пиво · Активные, Новые — покупатели категории за 8 недель · 08.10–10.10

DECLARE @d date = '2026-10-08'
DECLARE @fd date = '2026-10-10'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101423
	  , 'Активируй 20% кешбэка на пиво'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Активные (алкоголь)'
	  , 10
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

-- 101425 · Баланс баллов · Активные, Новые, Спящие, Отток · 31.10–31.10

DECLARE @d date = '2026-10-31'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101425
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
GO

-- 101426 · Дарим 100 монет на неделю · Активные низкочастотные (1–2 визита в месяц) + Случайные (выборка 101426, фиксируется 05.10) · 05.10–31.10

DECLARE @d date = '2026-10-05'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101426
	  , 'Дарим 100 монет на неделю'
	  , dateadd(day,+1,@fd)
	  ,	dateadd(day,+4,@fd)
	  , 'PUSH'
	  , 'Случайные'
	  , 12
	  , NULL
	  , 'Предначисленные бонусы'
	  , 15
	  , 1
	  , NULL
	  , NULL
	  , 'CVM'
	  , 1
	  )
GO

-- 101427 · Активируй 300р. на чек от 3000р. · Активные со средним чеком выше 2 500 ₽ (сегмент фиксируется на месяц) · 05.10–31.10

DECLARE @d date = '2026-10-05'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101427
	  , 'Активируй 300р. на чек от 3000р.'
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
GO

-- 101428 · Отток 50% кешбэка по дням · Отток, Спящие (выборка 101428, фиксируется 01.10) · 01.10–31.10

DECLARE @d date = '2026-10-01'
DECLARE @fd date = '2026-10-31'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-52*3, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101428
	  , 'Отток 50% кешбэка по дням'
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
GO

-- 101432 · Активируй 50р. на чек от 500р. · Активные, Новые — ступень 1 по среднему чеку · 29.10–04.11

DECLARE @d date = '2026-10-29'
DECLARE @fd date = '2026-11-04'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101432
	  , 'Активируй 50р. на чек от 500р.'
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
GO

-- 101433 · Активируй 100р. на чек от 1000р. · Активные, Новые — ступень 2 по среднему чеку · 29.10–04.11

DECLARE @d date = '2026-10-29'
DECLARE @fd date = '2026-11-04'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101433
	  , 'Активируй 100р. на чек от 1000р.'
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
GO

-- 101434 · Активируй 300р. на чек от 3000р. · Активные, Новые — ступень 3 по среднему чеку · 29.10–04.11

DECLARE @d date = '2026-10-29'
DECLARE @fd date = '2026-11-04'

INSERT INTO [mci_model].[dbo].[I_PROMO]
values (
	  DATEADD(week,-6, @d)
	  , DATEADD(day,-1, @d)
	  , @d
	  , @fd
	  , 101434
	  , 'Активируй 300р. на чек от 3000р.'
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
GO

set noexec off
GO

-- ==================================================================================
-- КОНТРОЛЬ — все акции октября, окно «после» 4 дня
-- ==================================================================================

select ID_PROMO, PROMO_NAME, START_DATE, FINISH_DATE, START_DATE_AFTER, FINISH_DATE_AFTER
		, [Дней после] = datediff(day, START_DATE_AFTER, FINISH_DATE_AFTER) + 1
from I_PROMO (nolock)
where ID_PROMO in (101393, 101394, 101395, 101396, 101397, 101398, 101399, 101400, 101401, 101402, 101403, 101404, 101406, 101407, 101410, 101412, 101413, 101415, 101416, 101417, 101419, 101420, 101421, 101422, 101423, 101425, 101426, 101427, 101428, 101432, 101433, 101434) and ID_COMPANY = 1
order by START_DATE, ID_PROMO
GO
