-- ============================================================================================
-- CVM_DISC_MODEL — модель эффекта скидок «Доп ТО на 1 р.», ВИТРИНА 1 «ВСЕ СКИДКИ ОТ И ДО» (19–20.10.2026).
--   Каждая скидка, которую получил клиент: клиент × день × магазин × кампания Manzana или скидка на конкретный товар;
--   покупки клиента по дням; цены ценовых зон и регулярная цена зоны по дням; окно каждой скидки по дням, городам и зонам;
--   реестр всех скидок с первым и последним днём (по каналам, зонам и месяцам).
--   Витрины 2 (подбор контрольных групп: ЦГ и look-alike КГ по слоям давности и интервалам суммы «до») и 3 (эффекты)
--   строятся на этой и ставятся следующими файлами.
-- Запуск: целиком в SSMS (F5), когда «MCI Regular update» закончился. Раздел 4 сразу считает всю историю целиком (цены зон
--   за все дни → регулярная цена зон → скидки клиентов → реестр); после неё разделы 5–6 добавляют витрину в обновление
--   (чистка шага 1, ночной шаг); раздел 7 выводит результат. Ночью шаг досчитывает новые дни и пересчитывает перезалитые.
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   Раздел 1 — удаление старой регрессии скидок (решение Елены 16.10.2026 «все текущие таблицы убрать»):
--     dbo.I_FOR_DISC_REGR (процедура) и 14 таблиц регрессии (I_FOR_DISCOUNT_REGR*, I_FOR_TO_REGRESSION, I_REGRESSION_DISCOUNT,
--     REGRESSION_TEST, I_REGR_PARTICIPATION, I_REGR_RUB, I_REGR_PROMO_TRAFFIC) — DROP; объект, который читает другая процедура,
--     представление, функция, синоним или шаг задания, НЕ удаляется (строка в итоге раздела). 19.10 уже удалены 4 из 15 —
--     раздел их пропустит.
--   Раздел 1б — снятие пробного варианта 19.10 этой же витрины (если он стоит): DROP процедуры disc_model_price_ep и таблиц
--     I_DISC_MODEL_PRICE_EP, I_DISC_MODEL_ZONE (старый, по областям), I_DISC_MODEL_CLIENT_DISC (старая, без количества и
--     суммы на полке); TRUNCATE I_DISC_MODEL_CLIENT_DAY, _LINE_DAY, _LINE, _LINE_MONTH, _STATE (строки остановленного запуска);
--     в I_DISC_MODEL_STATE добавляются колонки ZONE_CALC_DT, ZONE_ROWS. I_DISC_MODEL_CAMPAIGN и I_DISC_MODEL_LOG не трогаются.
--   Раздел 2 — таблицы витрины (создаются, если их нет; есть — не трогаются):
--     dbo.I_DISC_MODEL_CLIENT_DAY  — клиент × день × канал: чеки, позиции, ТО на полке (COST), после скидки (COST_DISCOUNT),
--                                    скидка кассы, по карте, ТО по REAL_PRICE — без табака, табак отдельно; основной магазин дня;
--     dbo.I_DISC_MODEL_CLIENT_DISC — клиент × день × магазин × строка скидки: кампания Manzana — списания по её правилам
--                                    (I_CHECK_RULE.VALUE); скидка на товар — регулярная цена зоны × количество − сумма по цене
--                                    продажи; для товаров хранятся количество и сумма по цене продажи (для пересчёта задним числом);
--     dbo.I_DISC_MODEL_LINE_DAY    — день × канал × город × зона × строка скидки: окно скидки по дням;
--     dbo.I_DISC_MODEL_ZONE        — 7 ценовых зон (решение Елены 20.10 по фактическим ценам в чеках): 1 Москва и МО; 2 СПб,
--                                    ЛО, Новгородская, Псковская; 3 Тульская, Рязанская, Владимирская, Калужская, Смоленская,
--                                    Брянская, Тверская; 4 Ярославская, Ивановская, Орловская, Костромская; 5 и 6 — малый формат
--                                    Москвы и МО (два ценовых листа, 38 и 23 магазина); 7 — малый формат СПб (12 магазинов);
--     dbo.I_DISC_MODEL_ZONE_SHOP   — магазин → зона по замеру 19.10 (2 761 магазин; 4 одиночки со своими ценами — вне зон, −2);
--     dbo.I_DISC_MODEL_ZONE_CITY   — запасная зона по области — для магазинов, которых нет в списке (новые);
--     dbo.I_DISC_MODEL_ZONE_DAY    — зона × товар × день: цена зоны (цена большинства её магазинов по чекам), весовой ли;
--     dbo.I_DISC_MODEL_ZONE_REG    — зона × товар × день: регулярная цена зоны, акция в зоне от и до, снижение регулярной цены;
--     dbo.I_DISC_MODEL_LINE, _LINE_MONTH — реестр всех скидок: строка скидки × канал × зона (0 — вся сеть), целиком и по
--                                    месяцам: название, акция CVM, первый и последний день, дней, городов, сумма, чеки;
--     dbo.I_DISC_MODEL_CAMPAIGN    — тип строки отчёта у кампании; пустая, заполняет Елена;
--     dbo.I_DISC_MODEL_STATE       — по дню: посчитаны ли цены зон и скидки клиентов; dbo.I_DISC_MODEL_LOG — журнал запусков.
--   Раздел 3а — процедуры dbo.disc_model_zone_day (цены зон за дни) и dbo.disc_model_zone_reg (регулярная цена зон и пересчёт
--     товарных скидок задним числом); их вызывает disc_model_mart.
--   Раздел 3 — процедура dbo.disc_model_mart (создаётся / заменяется).
--   Раздел 4 — вся история сразу, целиком, без остановки по времени: EXEC dbo.disc_model_mart @hist_days = NULL, @max_days = NULL.
--     Сначала история, потом добавление в обновление (разделы 5–6) — правило Елены 20.10 для всех скриптов новых таблиц.
--   Раздел 5 — dbo.clear_data_update и dbo.reload_clear_data (шаг 1 MCI): одна строка DELETE отметок дней I_DISC_MODEL_STATE
--     с первого перезалитого дня; если строка уже стоит (19.10) — процедуры не трогаются.
--   Раздел 6 — шаг DISC_MODEL_MART в «MCI Regular update» сразу после DISCOUNT_MART (по имени; следующие шаги сдвигаются на 1).
--   Раздел 7 — итог: дни, строки, скидки по месяцам, цены зон; сверка с витриной скидок I_DISCOUNT_DAY.
--   НЕ ТРОГАЕТСЯ: I_CHECK, I_CHECKHEADER, I_CHECK_RULE, I_PRODUCT, I_SHOP, I_CITY, I_CAMPAIGN, I_PROMO_CAMPAIGN_LINK, витрина
--     скидок I_DISCOUNT_* (только чтение, nolock), остальные шаги задания и все прочие объекты mci_model.
--
-- ЧТО В ВИТРИНЕ И ПОЧЕМУ ТАК
--   История — с 19.05.2025: 105 дней до 01.09.2025, чтобы у первых скидок истории была давность Оттока (недели 7–15).
--   Клиенты — ID_CONTACT > 0; каналы — 1 офлайн, 3 онлайн, тестовая организация 2 не берётся. Фрод и ошибочные контакты в
--     витрине есть — исключаются при подборе групп (витрина 2).
--   Табак (610901, 610902, 610904, 610905, 610906, 70030101; зажигалки 610903 — не табак) — вне ТО и вне скидки.
--   Скидка кампании — Σ|VALUE| строк I_CHECK_RULE кампании (VALUE — списания, решение 16.10); плюсы и минусы раздельно.
--   Скидка на товар (решения Елены 20.10). Ценовая зона магазина — по списку магазинов, иначе по области. Цена зоны на день —
--     цена продажи (сумма строки / количество), которая стоит в большинстве магазинов зоны, продававших товар в этот день
--     (цена магазина — самая частая у его строк). Весовой товар (у большинства строк дробное количество) — цены до рубля.
--     Регулярная цена зоны — наибольшая цена зоны с начала истории; если цена зоны ниже неё держится дольше 2 месяцев,
--     с первого такого дня регулярной становится новая цена (задним числом). Покупка по цене ниже регулярной цены зоны своего
--     магазина в этот день — скидка (регулярная − цена продажи) × количество: виноград в зоне 300 ₽, магазин продал по 200 —
--     скидка 100 ₽ в этом магазине, магазин по 300 — скидки нет. Дни снижения регулярной цены — не скидка (строка 3 для
--     сверки). Только офлайн-магазины зон; онлайн и одиночки вне зон — без товарной скидки. REAL_PRICE MCI не используется:
--     это накопленный максимум цены по магазину, он разный даже внутри Москвы (замер 19.10).
--   Акция в зоне «от и до» — первый и последний день, когда товар в магазинах зоны продавался ниже регулярной цены зоны;
--     эпизоды разных магазинов не склеиваются.
--   Строка скидки = (LINE_TYPE, ID_LINE): 1 — кампания Manzana, ID_LINE = ID_CAMPAIGN; 2 — скидка на товар; 3 — снижение
--     регулярной цены товара (не скидка), ID_LINE = ID_PRODUCT; пустой в источнике — −1.
--
-- Процедура dbo.disc_model_mart:
--   EXEC dbo.disc_model_mart;                     — НОЧЬ: новые дни и окно «сегодня − 10» (перезаливка MCI; шаг 1 стирает
--                                                   отметки перезалитых дней — ночь их пересчитывает); не больше 31 дня за ночь
--   EXEC dbo.disc_model_mart @hist_days = NULL;   — то же + вся непосчитанная история с 19.05.2025 (раздел 4)
--   EXEC dbo.disc_model_mart @DATE_FROM = '2026-09-01', @DATE_TO = '2026-09-30';   — пересчитать дни периода
--   @stop_at = '2026-10-20 01:00'                 — закончить к этому времени (только время — сегодня)
--   Каждый запуск: 1) цены зон за дни запуска (I_CHECK: магазин, товар, сумма, количество) → 2) регулярная цена зон по всей
--   истории (без чтения чеков) с пересчётом товарных скидок прошлых дней, если регулярная цена сменилась задним числом →
--   3) скидки клиентов за дни запуска: строки чеков из I_CHECK → списания I_CHECK_RULE диапазонами ID_CHECK → запись DELETE
--   дней + INSERT одной транзакцией → реестр. Ни один запрос не проходит I_CHECK, I_CHECK_RULE целиком. Двойной запуск не
--   идёт (sp_getapplock). Отмена безопасна: повторный запуск продолжит. ПОСЛЕ ОТМЕНЫ в том же окне SSMS выполнить:
--     USE mci_model; IF @@TRANCOUNT > 0 ROLLBACK; EXEC sp_releaseapplock @Resource = N'dbo.disc_model_mart', @LockOwner = 'Session';
--   (или закрыть окно) — иначе ночной шаг, пока окно открыто, будет падать «уже идёт другой запуск».
--   Время и объём (замера нет): цены зон за всю историю — чтение 6 колонок I_CHECK, ориентировочно 2–5 часов; регулярная цена —
--   10–40 минут; скидки клиентов за историю — 8–16 часов: вся история одним запуском — 10–20 часов. Ночь (~10 дней) — ориентировочно
--   40–90 минут. CLIENT_DISC ~2 млрд строк, ZONE_DAY и ZONE_REG — десятки млн. В воскресенье ночь сжимает колоночные индексы.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
IF DB_NAME() <> N'mci_model'
BEGIN
    RAISERROR(N'СТОП: скрипт выполняется не в базе mci_model — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

IF    COL_LENGTH('dbo.I_CHECK', 'ID_CHECK') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_CHEQUEITEM') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_CONTACT') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'DATA') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_SHOP') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_CITY') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_PRODUCT') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'COST') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'COST_DISCOUNT') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'DISCOUNT') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'CARD_DISCOUNT') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_ORGANIZATION') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_COMPANY') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'REAL_COST') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'QUANTITY') IS NULL OR COL_LENGTH('dbo.I_PRODUCT', 'PRODUCT_NAME') IS NULL
   OR COL_LENGTH('dbo.I_PRICE', 'ID_SHOP') IS NULL OR COL_LENGTH('dbo.I_PRICE', 'ID_PRODUCT') IS NULL
   OR COL_LENGTH('dbo.I_PRICE', 'START_DATE') IS NULL OR COL_LENGTH('dbo.I_PRICE', 'FINISH_DATE') IS NULL
   OR COL_LENGTH('dbo.I_PRICE', 'PRICE') IS NULL OR COL_LENGTH('dbo.I_PRICE', 'REAL_PRICE') IS NULL
   OR COL_LENGTH('dbo.I_PRICE', 'ID_ORGANIZATION') IS NULL OR COL_LENGTH('dbo.I_PRICE', 'ID_COMPANY') IS NULL
   OR COL_LENGTH('dbo.I_CHECK_RULE', 'ID_CHECK') IS NULL OR COL_LENGTH('dbo.I_CHECK_RULE', 'ID_CHEQUEITEM') IS NULL
   OR COL_LENGTH('dbo.I_CHECK_RULE', 'ID_CAMPAIGN') IS NULL OR COL_LENGTH('dbo.I_CHECK_RULE', 'VALUE') IS NULL
   OR COL_LENGTH('dbo.I_CHECK_RULE', 'ID_COMPANY') IS NULL
   OR COL_LENGTH('dbo.I_PRODUCT', 'ID_PRODUCT') IS NULL OR COL_LENGTH('dbo.I_PRODUCT', 'ID_CATEGORY_5_ext') IS NULL
   OR COL_LENGTH('dbo.I_CAMPAIGN', 'ID_CAMPAIGN') IS NULL OR COL_LENGTH('dbo.I_CAMPAIGN', 'CAMPAIGN_NAME') IS NULL
   OR COL_LENGTH('dbo.I_PROMO_CAMPAIGN_LINK', 'IS_USED') IS NULL OR COL_LENGTH('dbo.I_PROMO_CAMPAIGN_LINK', 'IS_CURRENT') IS NULL
   OR OBJECT_ID('dbo.I_DISCOUNT_DAY', 'U') IS NULL
   OR COL_LENGTH('dbo.I_SHOP', 'ID_CITY') IS NULL OR COL_LENGTH('dbo.I_SHOP', 'revision_date') IS NULL OR OBJECT_ID('dbo.I_CITY', 'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: в источниках (I_CHECK, I_CHECK_RULE, I_PRODUCT, I_CAMPAIGN, I_PROMO_CAMPAIGN_LINK, I_DISCOUNT_DAY) нет нужных колонок — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. УДАЛЕНИЕ СТАРОЙ РЕГРЕССИИ СКИДОК ==============================
-- Решение Елены 16.10.2026. Шаг PROMO_REGRESSION убран из задания 16.10. Перед DROP каждого объекта — поиск, кто его
-- читает: тексты процедур, представлений, функций и триггеров mci_model (кроме объектов этого же списка) и команды шагов
-- заданий msdb; нашёлся — объект не удаляется, в итоге строка «не удалено» с именами.
SET NOCOUNT ON;

DROP TABLE IF EXISTS #mc_old;
CREATE TABLE #mc_old (RN int NOT NULL PRIMARY KEY, OBJ_NAME sysname NOT NULL, KIND char(1) NOT NULL
                    , ACT nvarchar(30) NULL, WHY nvarchar(1000) NULL);
INSERT INTO #mc_old (RN, OBJ_NAME, KIND) VALUES
      (1,  N'I_FOR_DISC_REGR',              'P')
    , (2,  N'I_FOR_DISCOUNT_REGR',          'U')
    , (3,  N'I_FOR_DISCOUNT_REGR_CAMP',     'U')
    , (4,  N'I_FOR_DISCOUNT_REGR_CATEGORY', 'U')
    , (5,  N'I_FOR_DISCOUNT_REGR_CAT',      'U')
    , (6,  N'I_FOR_DISCOUNT_REGR_PROMO',    'U')
    , (7,  N'I_FOR_DISCOUNT_REGR_PRODUCT',  'U')
    , (8,  N'I_FOR_DISCOUNT_RERG_PROD_TO',  'U')
    , (9,  N'I_FOR_DISCOUNT_REGR_SHOPS',    'U')
    , (10, N'I_FOR_TO_REGRESSION',          'U')
    , (11, N'I_REGRESSION_DISCOUNT',        'U')
    , (12, N'REGRESSION_TEST',              'U')
    , (13, N'I_REGR_PARTICIPATION',         'U')
    , (14, N'I_REGR_RUB',                   'U')
    , (15, N'I_REGR_PROMO_TRAFFIC',         'U');

DECLARE @i int = 1, @name sysname, @kind char(1), @pat nvarchar(400), @refs nvarchar(1000), @jobs nvarchar(1000), @sql nvarchar(400);
WHILE @i <= 15
BEGIN
    SELECT @name = OBJ_NAME, @kind = KIND FROM #mc_old WHERE RN = @i;
    -- имя целиком: до и после — не буква, не цифра и не «_» (иначе I_FOR_DISCOUNT_REGR нашёлся бы внутри _CAMP)
    SET @pat = N'%[^A-Za-z0-9_]' + REPLACE(@name, N'_', N'[_]') + N'[^A-Za-z0-9_]%';
    SELECT @refs = NULL, @jobs = NULL;

    IF OBJECT_ID(N'dbo.' + @name, IIF(@kind = 'P', N'P', N'U')) IS NULL
        UPDATE #mc_old SET ACT = N'нет в базе' WHERE RN = @i;
    ELSE
    BEGIN
        SELECT @refs = left(STRING_AGG(CAST(concat(OBJECT_SCHEMA_NAME(m.object_id), N'.', OBJECT_NAME(m.object_id)) AS nvarchar(max)), N', '), 900)
        FROM sys.sql_modules AS m (nolock)
        WHERE (N' ' + m.definition + N' ') LIKE @pat
          AND m.object_id <> OBJECT_ID(N'dbo.' + @name)
          AND OBJECT_NAME(m.object_id) NOT IN (SELECT OBJ_NAME FROM #mc_old);

        SELECT @jobs = left(STRING_AGG(CAST(concat(j.name, N' / шаг ', s.step_id, N' ', s.step_name) AS nvarchar(max)), N', '), 900)
        FROM msdb.dbo.sysjobsteps AS s (nolock)
             JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = s.job_id
        WHERE (N' ' + s.command + N' ') LIKE @pat;

        SELECT @jobs = concat_ws(N'; ', @jobs, left(STRING_AGG(CAST(concat(N'синоним ', SCHEMA_NAME(y.schema_id), N'.', y.name) AS nvarchar(max)), N', '), 300))
        FROM sys.synonyms AS y (nolock)
        WHERE (N' ' + y.base_object_name + N' ') LIKE @pat;
        SET @jobs = nullif(@jobs, N'');

        IF @refs IS NOT NULL OR @jobs IS NOT NULL
            UPDATE #mc_old SET ACT = N'НЕ удалено'
                             , WHY = left(concat(N'читает: ', concat_ws(N'; ', @refs, @jobs)), 1000) WHERE RN = @i;
        ELSE
        BEGIN
            SET @sql = concat(IIF(@kind = 'P', N'DROP PROCEDURE dbo.', N'DROP TABLE dbo.'), QUOTENAME(@name), N';');
            BEGIN TRY
                EXEC sp_executesql @sql;
                UPDATE #mc_old SET ACT = N'удалено' WHERE RN = @i;
            END TRY
            BEGIN CATCH
                UPDATE #mc_old SET ACT = N'НЕ удалено', WHY = left(concat(N'ошибка ', ERROR_NUMBER(), N': ', ERROR_MESSAGE()), 1000) WHERE RN = @i;
            END CATCH
        END
    END
    SET @i += 1;
END

SELECT [Объект] = concat(N'dbo.', OBJ_NAME), [Вид] = IIF(KIND = 'P', N'процедура', N'таблица'), [Что сделано] = ACT, [Почему] = WHY
FROM #mc_old
ORDER BY RN;
GO

-- ============================== 1б. СНЯТИЕ ПРОБНОГО ВАРИАНТА 19.10 (только объекты этой витрины) ==============================
-- Запуск 19.10 остановлен: товарная скидка в нём шла от REAL_PRICE магазина и склеенных по зоне эпизодов цены, зоны — по
-- областям. Если в базе стоит тот вариант (есть таблица I_DISC_MODEL_PRICE_EP, процедура disc_model_price_ep, справочник
-- зон по городам или таблица скидок клиентов без колонок количества и суммы на полке), он снимается:
--   dbo.disc_model_price_ep (процедура), dbo.I_DISC_MODEL_PRICE_EP, dbo.I_DISC_MODEL_ZONE (старый, по городам),
--   dbo.I_DISC_MODEL_CLIENT_DISC (старая)                 — DROP (создаются заново в разделе 2 в новом виде);
--   dbo.I_DISC_MODEL_CLIENT_DAY, _LINE_DAY, _LINE, _LINE_MONTH, _STATE — TRUNCATE (строки остановленного запуска);
--   dbo.I_DISC_MODEL_STATE — таблица остаётся (её чистит шаг 1 MCI), добавляются колонки ZONE_CALC_DT, ZONE_ROWS.
-- Справочник типов кампаний I_DISC_MODEL_CAMPAIGN и журнал I_DISC_MODEL_LOG не трогаются. Повторный запуск файла ничего
-- из этого не делает: нового варианта не касается.
SET NOCOUNT ON;
DECLARE @old bit = CASE WHEN OBJECT_ID('dbo.I_DISC_MODEL_PRICE_EP', 'U') IS NOT NULL
                          OR OBJECT_ID('dbo.disc_model_price_ep', 'P') IS NOT NULL
                          OR (OBJECT_ID('dbo.I_DISC_MODEL_ZONE', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_ZONE', 'ID_CITY') IS NOT NULL)
                          OR (OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DISC', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_CLIENT_DISC', 'QTY') IS NULL)
                          OR (OBJECT_ID('dbo.I_DISC_MODEL_LINE_DAY', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE_DAY', 'ID_ZONE') IS NULL)
                          OR (OBJECT_ID('dbo.I_DISC_MODEL_LINE', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE', 'ID_ZONE') IS NULL)
                          OR (OBJECT_ID('dbo.I_DISC_MODEL_LINE_MONTH', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE_MONTH', 'ID_ZONE') IS NULL)
                        THEN 1 ELSE 0 END;
IF @old = 1
BEGIN
    DROP PROCEDURE IF EXISTS dbo.disc_model_price_ep;
    DROP TABLE IF EXISTS dbo.I_DISC_MODEL_PRICE_EP;
    IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_ZONE', 'ID_CITY') IS NOT NULL
        DROP TABLE dbo.I_DISC_MODEL_ZONE;
    IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DISC', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_CLIENT_DISC', 'QTY') IS NULL
        DROP TABLE dbo.I_DISC_MODEL_CLIENT_DISC;
    IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DISC', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_CLIENT_DISC;
    -- окно и реестр без колонки зоны (ещё более ранний вариант) — пересоздаются в разделе 2
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_DAY', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE_DAY', 'ID_ZONE') IS NULL
        DROP TABLE dbo.I_DISC_MODEL_LINE_DAY;
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE', 'ID_ZONE') IS NULL
        DROP TABLE dbo.I_DISC_MODEL_LINE;
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_MONTH', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LINE_MONTH', 'ID_ZONE') IS NULL
        DROP TABLE dbo.I_DISC_MODEL_LINE_MONTH;
    IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DAY', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_CLIENT_DAY;
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_DAY', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_LINE_DAY;
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_LINE;
    IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_MONTH', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_LINE_MONTH;
    IF OBJECT_ID('dbo.I_DISC_MODEL_STATE', 'U') IS NOT NULL TRUNCATE TABLE dbo.I_DISC_MODEL_STATE;
    RAISERROR(N'Пробный вариант 19.10 снят: процедура disc_model_price_ep, таблицы эпизодов цены, старого справочника зон и скидок клиентов удалены; строки остановленного запуска очищены.', 10, 1) WITH NOWAIT;
END
ELSE
    RAISERROR(N'Пробного варианта 19.10 в базе нет — ничего не снимаю.', 10, 1) WITH NOWAIT;
IF OBJECT_ID('dbo.I_DISC_MODEL_STATE', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_STATE', 'ZONE_CALC_DT') IS NULL
    ALTER TABLE dbo.I_DISC_MODEL_STATE ADD ZONE_CALC_DT datetime2(0) NULL, ZONE_ROWS int NULL;
IF OBJECT_ID('dbo.I_DISC_MODEL_LOG', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_DISC_MODEL_LOG', 'ZONE_PART_SEC') IS NULL
    ALTER TABLE dbo.I_DISC_MODEL_LOG ADD ZONE_PART_SEC int NULL;
GO

-- ============================== 2. ТАБЛИЦЫ ВИТРИНЫ ==============================
IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DAY', 'U') IS NULL
BEGIN
    -- клиент × день × канал; табак — отдельными колонками (ориентир объёма — порядка 1 млрд строк за историю)
    CREATE TABLE dbo.I_DISC_MODEL_CLIENT_DAY (
          ID_COMPANY        int            NOT NULL
        , DATA              date           NOT NULL   -- день чека
        , ID_ORGANIZATION   int            NOT NULL   -- 1 офлайн, 3 онлайн
        , ID_CONTACT        int            NOT NULL   -- > 0
        , ID_SHOP           int            NULL       -- основной магазин дня: где за день куплено на большую сумму (без табака)
        , ID_CITY           int            NULL       -- город этого магазина (I_CHECK.ID_CITY)
        , SHOPS             int            NOT NULL   -- магазинов за день
        , CHECKS            int            NOT NULL   -- чеков с нетабачными позициями
        , CHECKS_ALL        int            NOT NULL   -- чеков всего (с табаком)
        , LINES             int            NOT NULL   -- позиций без табака
        , COST              decimal(19, 2) NOT NULL   -- без табака: ТО на полке (I_CHECK.COST)
        , COST_DISCOUNT     decimal(19, 2) NOT NULL   -- без табака: ТО после скидки (I_CHECK.COST_DISCOUNT)
        , DISCOUNT          decimal(19, 2) NOT NULL   -- без табака: I_CHECK.DISCOUNT
        , CARD_DISCOUNT     decimal(19, 2) NOT NULL   -- без табака: I_CHECK.CARD_DISCOUNT
        , REAL_COST         decimal(19, 2) NOT NULL   -- без табака: ТО по реальной цене (I_CHECK.REAL_COST = REAL_PRICE × количество)
        , COST_TOB          decimal(19, 2) NOT NULL   -- табак: COST
        , COST_DISCOUNT_TOB decimal(19, 2) NOT NULL   -- табак: COST_DISCOUNT
        , DISCOUNT_TOB      decimal(19, 2) NOT NULL   -- табак: DISCOUNT
        , RUN_ID            int            NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_CLIENT_DAY ON dbo.I_DISC_MODEL_CLIENT_DAY;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DISC', 'U') IS NULL
BEGIN
    -- клиент × день × канал × строка скидки; только ненулевые
    CREATE TABLE dbo.I_DISC_MODEL_CLIENT_DISC (
          ID_COMPANY      int            NOT NULL
        , DATA            date           NOT NULL
        , ID_ORGANIZATION int            NOT NULL
        , ID_CONTACT      int            NOT NULL
        , ID_SHOP         int            NULL       -- магазин чека (у товара по нему ищется эпизод цены)
        , ID_CITY         int            NULL       -- город магазина (I_CHECK.ID_CITY)
        , LINE_TYPE       tinyint        NOT NULL   -- 1 — кампания Manzana; 2 — скидка на товар (цена продажи ниже регулярной цены
                                                    --   зоны); 3 — снижение регулярной цены (ниже прежней регулярной зоны дольше
                                                    --   2 месяцев — это не скидка, хранится для сверки, сумма — от прежней регулярной)
        , ID_LINE         bigint         NOT NULL   -- LINE_TYPE 1: ID_CAMPAIGN; 2 и 3: ID_PRODUCT; −1 — пустой в источнике
        , VALUE_POS       decimal(19, 2) NOT NULL   -- сумма положительных VALUE
        , VALUE_NEG       decimal(19, 2) NOT NULL   -- сумма отрицательных VALUE (со знаком минус); скидка = VALUE_POS − VALUE_NEG
                                                    -- LINE_TYPE 2: Σ(регулярная цена зоны × количество − сумма по цене продажи);
                                                    --   продажи — в VALUE_POS, возвраты — с минусом в VALUE_NEG
        , ROWS_ALL        int            NOT NULL   -- строк I_CHECK_RULE (LINE_TYPE 2 — позиций)
        , LINES           int            NOT NULL   -- позиций со скидкой (разных ID_CHEQUEITEM); строка без номера позиции (списание на чек целиком) — не позиция
        , CHECKS          int            NOT NULL   -- чеков со скидкой
        , QTY             decimal(18, 3) NULL       -- LINE_TYPE 2 и 3: количество товара в строках со скидкой (возвраты с минусом)
        , COST_SHELF      decimal(19, 2) NULL       -- LINE_TYPE 2 и 3: сумма этих строк по цене продажи (COST); по ним скидка
                                                    --   пересчитывается без чтения чеков, если регулярная цена зоны сменилась задним числом
        , RUN_ID          int            NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_CLIENT_DISC ON dbo.I_DISC_MODEL_CLIENT_DISC;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_DAY', 'U') IS NULL
BEGIN
    -- окно скидки по дням и городам: день × канал × город × строка скидки
    CREATE TABLE dbo.I_DISC_MODEL_LINE_DAY (
          ID_COMPANY      int            NOT NULL
        , DATA            date           NOT NULL
        , ID_ORGANIZATION int            NOT NULL
        , ID_CITY         int            NOT NULL   -- −1 — пустой в источнике
        , ID_ZONE         smallint       NOT NULL   -- ценовая зона магазина (I_DISC_MODEL_ZONE); −1 — регион магазина вне справочника
        , LINE_TYPE       tinyint        NOT NULL
        , ID_LINE         bigint         NOT NULL
        , VALUE_POS       decimal(19, 2) NOT NULL
        , VALUE_NEG       decimal(19, 2) NOT NULL
        , LINES           int            NOT NULL   -- позиций со скидкой
        , CHECKS          int            NOT NULL   -- чеков со скидкой
        , CLIENTS         int            NOT NULL   -- клиентов со скидкой за день в городе (по дням и городам не складывать)
        , SHOPS           int            NOT NULL   -- магазинов со скидкой
        , RUN_ID          int            NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_LINE_DAY ON dbo.I_DISC_MODEL_LINE_DAY;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE', 'U') IS NULL
BEGIN
    -- ценовые зоны (решение Елены 20.10.2026 по фактическим ценам в чеках, замер 19.10: 4 среды 23.09–14.10.2026, 448 штучных
    -- товаров из 500 самых распространённых; совпадение цен пары магазинов внутри зон 86–93 %, между большими зонами 76–83 %)
    CREATE TABLE dbo.I_DISC_MODEL_ZONE (
          ID_ZONE   smallint      NOT NULL CONSTRAINT PK_I_DISC_MODEL_ZONE PRIMARY KEY CLUSTERED
                                           CONSTRAINT CK_I_DISC_MODEL_ZONE_ID CHECK (ID_ZONE > 0)   -- 0 в реестре — «вся сеть»
        , ZONE_NAME nvarchar(120) NOT NULL
    );
    INSERT INTO dbo.I_DISC_MODEL_ZONE (ID_ZONE, ZONE_NAME) VALUES
          (1, N'Москва и МО')
        , (2, N'СПб, ЛО, Новгородская, Псковская')
        , (3, N'Тульская, Рязанская, Владимирская, Калужская, Смоленская, Брянская, Тверская')
        , (4, N'Ярославская, Ивановская, Орловская, Костромская')
        , (5, N'Малый формат Москвы и МО, ценовой лист 1')
        , (6, N'Малый формат Москвы и МО, ценовой лист 2')
        , (7, N'Малый формат СПб');
END

SET XACT_ABORT OFF;   -- ошибка вставки справочника не обрывает пакет: откат и снятие таблицы ниже
IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE_SHOP', 'U') IS NULL
BEGIN
    -- магазин → зона по замеру 19.10 (2 761 офлайн-магазин); −2 — вне зон: одиночки со своими ценами — магазины «ДИКСИ 77121»,
    -- «ДИКСИ 90264», «ДИКСИ 47275», «ДИКСИ 78369» (ID_SHOP 770, 2521, 3956, 4110). Магазина нет в таблице (новый или не
    -- продавал эти товары) — зона по области (I_DISC_MODEL_ZONE_CITY). Заполнение — одной транзакцией, проверка 2 761 строки
    CREATE TABLE dbo.I_DISC_MODEL_ZONE_SHOP (
          ID_COMPANY int      NOT NULL
        , ID_SHOP    int      NOT NULL
        , ID_ZONE    smallint NOT NULL   -- 1–7; −2 — вне зон
        , CONSTRAINT PK_I_DISC_MODEL_ZONE_SHOP PRIMARY KEY CLUSTERED (ID_COMPANY, ID_SHOP)
    );
    BEGIN TRANSACTION;
    INSERT INTO dbo.I_DISC_MODEL_ZONE_SHOP (ID_COMPANY, ID_SHOP, ID_ZONE) VALUES
      (1, 2, 1), (1, 4, 3), (1, 8, 3), (1, 9, 3), (1, 12, 3), (1, 13, 3), (1, 14, 3), (1, 16, 3), (1, 17, 3), (1, 18, 3)
    , (1, 20, 3), (1, 21, 3), (1, 22, 3), (1, 23, 3), (1, 24, 3), (1, 25, 3), (1, 27, 3), (1, 29, 3), (1, 31, 3), (1, 32, 3)
    , (1, 33, 3), (1, 34, 3), (1, 36, 3), (1, 37, 3), (1, 39, 3), (1, 41, 3), (1, 42, 3), (1, 43, 3), (1, 44, 3), (1, 46, 3)
    , (1, 48, 3), (1, 49, 3), (1, 50, 3), (1, 52, 3), (1, 53, 3), (1, 54, 3), (1, 55, 3), (1, 56, 3), (1, 57, 3), (1, 58, 3)
    , (1, 60, 3), (1, 61, 3), (1, 62, 3), (1, 63, 3), (1, 64, 3), (1, 65, 3), (1, 68, 3), (1, 69, 3), (1, 70, 3), (1, 71, 3)
    , (1, 72, 3), (1, 73, 3), (1, 75, 3), (1, 78, 3), (1, 80, 4), (1, 82, 4), (1, 83, 4), (1, 84, 4), (1, 85, 4), (1, 86, 4)
    , (1, 87, 4), (1, 88, 4), (1, 89, 4), (1, 90, 4), (1, 91, 4), (1, 92, 4), (1, 93, 4), (1, 94, 4), (1, 95, 4), (1, 96, 4)
    , (1, 97, 4), (1, 98, 4), (1, 99, 4), (1, 101, 3), (1, 104, 3), (1, 106, 3), (1, 108, 3), (1, 109, 3), (1, 110, 3), (1, 111, 3)
    , (1, 114, 3), (1, 115, 3), (1, 116, 3), (1, 120, 3), (1, 125, 3), (1, 127, 3), (1, 131, 3), (1, 133, 3), (1, 134, 3), (1, 137, 3)
    , (1, 138, 3), (1, 139, 3), (1, 140, 3), (1, 141, 3), (1, 145, 4), (1, 146, 4), (1, 147, 4), (1, 148, 4), (1, 149, 4), (1, 150, 4)
    , (1, 151, 4), (1, 152, 4), (1, 153, 4), (1, 154, 4), (1, 155, 4), (1, 156, 4), (1, 157, 4), (1, 158, 4), (1, 159, 4), (1, 160, 4)
    , (1, 161, 4), (1, 162, 4), (1, 163, 4), (1, 164, 4), (1, 165, 4), (1, 168, 4), (1, 170, 4), (1, 172, 4), (1, 173, 4), (1, 174, 4)
    , (1, 175, 4), (1, 176, 4), (1, 179, 4), (1, 180, 4), (1, 181, 4), (1, 182, 4), (1, 184, 4), (1, 185, 4), (1, 186, 4), (1, 187, 4)
    , (1, 188, 4), (1, 189, 4), (1, 190, 4), (1, 191, 4), (1, 196, 4), (1, 197, 4), (1, 198, 4), (1, 200, 4), (1, 201, 4), (1, 202, 4)
    , (1, 203, 4), (1, 205, 4), (1, 206, 4), (1, 207, 4), (1, 208, 4), (1, 209, 4), (1, 210, 4), (1, 211, 4), (1, 212, 4), (1, 213, 4)
    , (1, 214, 4), (1, 216, 4), (1, 217, 4), (1, 218, 4), (1, 219, 4), (1, 220, 4), (1, 221, 4), (1, 223, 4), (1, 226, 4), (1, 227, 4)
    , (1, 229, 4), (1, 230, 4), (1, 231, 2), (1, 232, 2), (1, 234, 2), (1, 235, 2), (1, 237, 2), (1, 238, 2), (1, 239, 2), (1, 240, 2)
    , (1, 241, 2), (1, 244, 2), (1, 245, 2), (1, 246, 2), (1, 247, 2), (1, 248, 2), (1, 249, 2), (1, 250, 2), (1, 251, 2), (1, 253, 2)
    , (1, 256, 2), (1, 258, 2), (1, 260, 2), (1, 262, 2), (1, 265, 2), (1, 266, 2), (1, 267, 2), (1, 269, 2), (1, 270, 2), (1, 271, 2)
    , (1, 272, 2), (1, 273, 2), (1, 274, 2), (1, 277, 2), (1, 278, 2), (1, 279, 2), (1, 280, 2), (1, 281, 2), (1, 282, 2), (1, 283, 2)
    , (1, 284, 2), (1, 287, 2), (1, 288, 2), (1, 292, 2), (1, 294, 2), (1, 295, 2), (1, 297, 2), (1, 298, 2), (1, 299, 2), (1, 300, 2)
    , (1, 302, 2), (1, 304, 2), (1, 306, 2), (1, 307, 2), (1, 308, 2), (1, 310, 2), (1, 311, 2), (1, 312, 2), (1, 314, 2), (1, 315, 2)
    , (1, 316, 2), (1, 317, 2), (1, 318, 2), (1, 319, 2), (1, 320, 2), (1, 321, 2), (1, 322, 2), (1, 323, 2), (1, 324, 2), (1, 325, 2)
    , (1, 326, 2), (1, 327, 2), (1, 328, 2), (1, 329, 2), (1, 330, 2), (1, 331, 2), (1, 332, 2), (1, 334, 2), (1, 335, 2), (1, 336, 2)
    , (1, 337, 2), (1, 338, 2), (1, 339, 2), (1, 340, 2), (1, 341, 2), (1, 342, 2), (1, 343, 2), (1, 344, 2), (1, 345, 2), (1, 346, 2)
    , (1, 347, 2), (1, 348, 2), (1, 349, 2), (1, 350, 2), (1, 351, 2), (1, 352, 2), (1, 353, 2), (1, 354, 2), (1, 355, 2), (1, 356, 2)
    , (1, 357, 2), (1, 358, 2), (1, 359, 2), (1, 360, 2), (1, 361, 2), (1, 362, 2), (1, 364, 2), (1, 365, 2), (1, 366, 2), (1, 368, 2)
    , (1, 369, 2), (1, 370, 2), (1, 374, 2), (1, 375, 2), (1, 378, 2), (1, 379, 2), (1, 381, 2), (1, 382, 2), (1, 383, 2), (1, 384, 2)
    , (1, 385, 2), (1, 386, 2), (1, 387, 2), (1, 389, 2), (1, 390, 2), (1, 391, 2), (1, 393, 2), (1, 394, 2), (1, 395, 2), (1, 396, 2)
    , (1, 397, 2), (1, 398, 2), (1, 399, 2), (1, 400, 2), (1, 401, 2), (1, 402, 2), (1, 403, 2), (1, 404, 2), (1, 405, 2), (1, 406, 2)
    , (1, 407, 2), (1, 408, 2), (1, 409, 2), (1, 410, 2), (1, 411, 2), (1, 412, 2), (1, 413, 2), (1, 414, 2), (1, 415, 2), (1, 416, 2)
    , (1, 417, 2), (1, 419, 2), (1, 420, 2), (1, 421, 2), (1, 422, 2), (1, 423, 2), (1, 424, 2), (1, 425, 2), (1, 426, 2), (1, 427, 2)
    , (1, 429, 2), (1, 430, 2), (1, 431, 2), (1, 432, 2), (1, 433, 2), (1, 434, 2), (1, 435, 2), (1, 436, 2), (1, 437, 2), (1, 439, 2)
    , (1, 441, 2), (1, 442, 2), (1, 445, 2), (1, 446, 2), (1, 447, 2), (1, 448, 2), (1, 449, 2), (1, 450, 2), (1, 451, 2), (1, 452, 2)
    , (1, 453, 2), (1, 454, 2), (1, 455, 2), (1, 456, 2), (1, 457, 2), (1, 458, 2), (1, 459, 2), (1, 460, 2), (1, 461, 2), (1, 462, 2)
    , (1, 464, 2), (1, 465, 2), (1, 467, 2), (1, 468, 2), (1, 469, 2), (1, 470, 2), (1, 471, 2), (1, 472, 2), (1, 473, 2), (1, 475, 2)
    , (1, 476, 2), (1, 478, 2), (1, 479, 2), (1, 481, 2), (1, 482, 2), (1, 483, 2), (1, 484, 2), (1, 485, 2), (1, 486, 2), (1, 487, 2)
    , (1, 488, 2), (1, 493, 2), (1, 494, 2), (1, 495, 2), (1, 496, 2), (1, 497, 2), (1, 498, 2), (1, 499, 2), (1, 502, 2), (1, 503, 2)
    , (1, 504, 2), (1, 505, 2), (1, 506, 2), (1, 507, 2), (1, 508, 2), (1, 509, 2), (1, 510, 2), (1, 511, 2), (1, 512, 2), (1, 514, 2)
    , (1, 515, 2), (1, 516, 2), (1, 518, 2), (1, 519, 2), (1, 520, 2), (1, 521, 2), (1, 523, 2), (1, 524, 2), (1, 525, 2), (1, 526, 2)
    , (1, 527, 2), (1, 528, 2), (1, 529, 2), (1, 530, 2), (1, 531, 2), (1, 532, 2), (1, 533, 2), (1, 534, 2), (1, 535, 2), (1, 536, 2)
    , (1, 537, 2), (1, 538, 2), (1, 539, 2), (1, 540, 2), (1, 541, 2), (1, 542, 2), (1, 543, 2), (1, 544, 2), (1, 545, 2), (1, 546, 2)
    , (1, 547, 2), (1, 548, 2), (1, 549, 2), (1, 550, 2), (1, 552, 2), (1, 553, 2), (1, 554, 2), (1, 555, 2), (1, 556, 2), (1, 557, 2)
    , (1, 558, 2), (1, 559, 2), (1, 560, 2), (1, 561, 2), (1, 562, 2), (1, 563, 2), (1, 564, 2), (1, 565, 2), (1, 566, 2), (1, 568, 2)
    , (1, 569, 2), (1, 570, 2), (1, 571, 2), (1, 573, 2), (1, 574, 2), (1, 575, 2), (1, 576, 2), (1, 577, 2), (1, 578, 2), (1, 579, 2)
    , (1, 580, 2), (1, 581, 2), (1, 582, 2), (1, 584, 2), (1, 585, 2), (1, 586, 2), (1, 587, 2), (1, 589, 2), (1, 590, 2), (1, 591, 2)
    , (1, 592, 2), (1, 593, 2), (1, 594, 2), (1, 595, 2), (1, 596, 2), (1, 597, 2), (1, 598, 2), (1, 599, 2), (1, 601, 2), (1, 602, 2)
    , (1, 603, 2), (1, 604, 2), (1, 605, 2), (1, 606, 2), (1, 607, 2), (1, 608, 2), (1, 612, 2), (1, 613, 2), (1, 614, 2), (1, 616, 2)
    , (1, 617, 2), (1, 619, 2), (1, 620, 2), (1, 621, 2), (1, 624, 2), (1, 625, 2), (1, 627, 2), (1, 628, 2), (1, 629, 2), (1, 630, 2)
    , (1, 631, 2), (1, 632, 2), (1, 633, 2), (1, 635, 2), (1, 636, 2), (1, 637, 2), (1, 638, 2), (1, 639, 2), (1, 640, 2), (1, 641, 2)
    , (1, 642, 2), (1, 643, 2), (1, 644, 2), (1, 645, 2), (1, 647, 2), (1, 648, 2), (1, 649, 2), (1, 650, 2), (1, 652, 2), (1, 653, 2)
    , (1, 654, 2), (1, 655, 2), (1, 656, 2), (1, 657, 2), (1, 658, 2), (1, 662, 2), (1, 663, 2), (1, 664, 2), (1, 665, 2), (1, 667, 2)
    , (1, 668, 2), (1, 669, 2), (1, 670, 2), (1, 671, 2), (1, 672, 2), (1, 673, 2), (1, 674, 2), (1, 675, 2), (1, 676, 2), (1, 677, 2)
    , (1, 678, 2), (1, 679, 2), (1, 680, 2), (1, 681, 2), (1, 683, 2), (1, 684, 2), (1, 685, 2), (1, 686, 2), (1, 687, 1), (1, 689, 1)
    , (1, 690, 1), (1, 691, 1), (1, 692, 1), (1, 693, 1), (1, 694, 1), (1, 695, 1), (1, 697, 1), (1, 698, 1), (1, 700, 1), (1, 701, 1)
    , (1, 703, 1), (1, 704, 1), (1, 705, 1), (1, 706, 1), (1, 707, 1), (1, 708, 1), (1, 709, 1), (1, 710, 1), (1, 711, 1), (1, 712, 1)
    , (1, 713, 1), (1, 715, 1), (1, 716, 1), (1, 718, 1), (1, 719, 1), (1, 720, 1), (1, 721, 1), (1, 722, 1), (1, 723, 1), (1, 725, 1)
    , (1, 726, 1), (1, 727, 1), (1, 728, 1), (1, 730, 1), (1, 731, 1), (1, 734, 1), (1, 735, 1), (1, 736, 1), (1, 737, 1), (1, 738, 1)
    , (1, 739, 1), (1, 740, 1), (1, 741, 1), (1, 742, 1), (1, 743, 1), (1, 744, 1), (1, 745, 1), (1, 746, 1), (1, 747, 1), (1, 749, 1)
    , (1, 750, 1), (1, 751, 1), (1, 752, 1), (1, 753, 1), (1, 754, 1), (1, 755, 1), (1, 756, 1), (1, 757, 1), (1, 758, 1), (1, 759, 1)
    , (1, 761, 1), (1, 762, 1), (1, 763, 1), (1, 765, 1), (1, 766, 1), (1, 768, 1), (1, 769, 1), (1, 770, -2), (1, 771, 1), (1, 772, 1)
    , (1, 773, 1), (1, 774, 1), (1, 775, 1), (1, 776, 1), (1, 777, 1), (1, 778, 1), (1, 779, 1), (1, 780, 1), (1, 781, 1), (1, 783, 1)
    , (1, 784, 1), (1, 785, 1), (1, 786, 1), (1, 787, 1), (1, 788, 1), (1, 789, 1), (1, 790, 1), (1, 792, 1), (1, 793, 1), (1, 794, 1)
    , (1, 795, 1), (1, 796, 1), (1, 798, 1), (1, 799, 1), (1, 800, 1), (1, 801, 1), (1, 803, 1), (1, 804, 1), (1, 805, 1), (1, 806, 1)
    , (1, 807, 1), (1, 808, 1), (1, 809, 1), (1, 810, 1), (1, 811, 1), (1, 812, 1), (1, 813, 1), (1, 814, 1), (1, 815, 1), (1, 816, 1)
    , (1, 817, 1), (1, 818, 1), (1, 819, 1), (1, 821, 1), (1, 822, 1), (1, 823, 1), (1, 824, 1), (1, 825, 1), (1, 826, 1), (1, 827, 1)
    , (1, 828, 1), (1, 829, 1), (1, 830, 1), (1, 831, 1), (1, 832, 1), (1, 833, 1), (1, 834, 1), (1, 835, 1), (1, 836, 1), (1, 837, 1)
    , (1, 838, 1), (1, 840, 1), (1, 841, 1), (1, 842, 1), (1, 843, 1), (1, 844, 1), (1, 845, 1), (1, 846, 1), (1, 847, 1), (1, 848, 1)
    , (1, 849, 1), (1, 850, 1), (1, 851, 1), (1, 852, 1), (1, 853, 1), (1, 854, 1), (1, 855, 1), (1, 856, 1), (1, 857, 1), (1, 858, 1)
    , (1, 859, 1), (1, 860, 1), (1, 861, 1), (1, 862, 1), (1, 863, 1), (1, 864, 1), (1, 865, 1), (1, 866, 1), (1, 867, 1), (1, 868, 1)
    , (1, 870, 3), (1, 871, 3), (1, 872, 1), (1, 873, 1), (1, 874, 1), (1, 875, 1), (1, 876, 1), (1, 877, 1), (1, 878, 1), (1, 879, 1)
    , (1, 880, 1), (1, 881, 1), (1, 882, 1), (1, 883, 1), (1, 884, 1), (1, 885, 1), (1, 886, 1), (1, 887, 1), (1, 889, 1), (1, 890, 1)
    , (1, 891, 1), (1, 892, 1), (1, 893, 1), (1, 894, 1), (1, 895, 1), (1, 896, 1), (1, 897, 1), (1, 898, 1), (1, 899, 1), (1, 900, 1)
    , (1, 901, 1), (1, 902, 1), (1, 903, 1), (1, 905, 1), (1, 906, 1), (1, 907, 1), (1, 908, 1), (1, 909, 1), (1, 910, 1), (1, 911, 1)
    , (1, 912, 1), (1, 913, 1), (1, 914, 1), (1, 915, 1), (1, 916, 1), (1, 917, 1), (1, 918, 1), (1, 919, 1), (1, 920, 1), (1, 921, 1)
    , (1, 922, 1), (1, 923, 1), (1, 924, 1), (1, 925, 1), (1, 926, 1), (1, 927, 1), (1, 928, 1), (1, 929, 1), (1, 930, 1), (1, 931, 1)
    , (1, 932, 1), (1, 933, 1), (1, 934, 1), (1, 935, 1), (1, 936, 1), (1, 937, 1), (1, 938, 1), (1, 939, 1), (1, 942, 1), (1, 943, 1)
    , (1, 944, 1), (1, 945, 1), (1, 946, 1), (1, 947, 1), (1, 948, 1), (1, 949, 1), (1, 950, 1), (1, 951, 1), (1, 952, 1), (1, 953, 1)
    , (1, 954, 1), (1, 955, 1), (1, 956, 1), (1, 957, 1), (1, 958, 1), (1, 959, 1), (1, 960, 1), (1, 961, 1), (1, 962, 1), (1, 963, 1)
    , (1, 964, 1), (1, 965, 1), (1, 966, 1), (1, 967, 1), (1, 968, 1), (1, 969, 1), (1, 970, 1), (1, 971, 1), (1, 972, 1), (1, 973, 1)
    , (1, 974, 1), (1, 976, 1), (1, 978, 1), (1, 980, 1), (1, 981, 1), (1, 982, 1), (1, 983, 1), (1, 984, 1), (1, 985, 1), (1, 986, 1)
    , (1, 987, 1), (1, 988, 1), (1, 989, 1), (1, 990, 1), (1, 991, 1), (1, 992, 1), (1, 993, 1), (1, 994, 1), (1, 995, 1), (1, 996, 1)
    , (1, 997, 1), (1, 998, 1), (1, 999, 1), (1, 1000, 1), (1, 1001, 1), (1, 1002, 1), (1, 1004, 1), (1, 1005, 1), (1, 1006, 1), (1, 1009, 1)
    , (1, 1011, 1), (1, 1012, 1), (1, 1013, 1), (1, 1014, 1), (1, 1015, 1), (1, 1016, 1), (1, 1017, 1), (1, 1019, 1), (1, 1021, 1), (1, 1022, 1)
    , (1, 1023, 1), (1, 1025, 1), (1, 1026, 1), (1, 1027, 1), (1, 1028, 1), (1, 1029, 1), (1, 1030, 1), (1, 1031, 1), (1, 1032, 1), (1, 1033, 1)
    , (1, 1034, 1), (1, 1035, 1), (1, 1036, 1), (1, 1037, 1), (1, 1039, 1), (1, 1040, 1), (1, 1041, 1), (1, 1042, 1), (1, 1043, 1), (1, 1044, 1)
    , (1, 1045, 1), (1, 1046, 1), (1, 1048, 1), (1, 1049, 1), (1, 1050, 1), (1, 1051, 1), (1, 1052, 1), (1, 1053, 1), (1, 1054, 1), (1, 1055, 1)
    , (1, 1056, 1), (1, 1057, 1), (1, 1059, 1), (1, 1060, 1), (1, 1061, 1), (1, 1062, 1), (1, 1063, 1), (1, 1064, 1), (1, 1065, 1), (1, 1066, 1)
    , (1, 1067, 1), (1, 1068, 1), (1, 1069, 1), (1, 1071, 1), (1, 1072, 1), (1, 1073, 1), (1, 1074, 1), (1, 1075, 1), (1, 1076, 1), (1, 1077, 1)
    , (1, 1078, 1), (1, 1079, 1), (1, 1080, 1), (1, 1081, 1), (1, 1082, 1), (1, 1083, 1), (1, 1084, 1), (1, 1085, 1), (1, 1086, 1), (1, 1087, 1);
    INSERT INTO dbo.I_DISC_MODEL_ZONE_SHOP (ID_COMPANY, ID_SHOP, ID_ZONE) VALUES
      (1, 1088, 1), (1, 1089, 1), (1, 1090, 1), (1, 1091, 1), (1, 1092, 1), (1, 1093, 1), (1, 1095, 1), (1, 1096, 1), (1, 1097, 1), (1, 1098, 1)
    , (1, 1099, 1), (1, 1100, 1), (1, 1101, 1), (1, 1102, 1), (1, 1103, 1), (1, 1104, 1), (1, 1105, 1), (1, 1106, 1), (1, 1107, 1), (1, 1108, 1)
    , (1, 1109, 1), (1, 1111, 1), (1, 1112, 1), (1, 1113, 1), (1, 1114, 1), (1, 1115, 1), (1, 1116, 1), (1, 1117, 1), (1, 1118, 1), (1, 1119, 1)
    , (1, 1120, 1), (1, 1121, 1), (1, 1122, 1), (1, 1123, 1), (1, 1125, 1), (1, 1126, 1), (1, 1127, 1), (1, 1129, 1), (1, 1130, 1), (1, 1131, 1)
    , (1, 1132, 1), (1, 1134, 1), (1, 1135, 1), (1, 1137, 1), (1, 1138, 1), (1, 1139, 1), (1, 1140, 1), (1, 1141, 1), (1, 1142, 1), (1, 1144, 1)
    , (1, 1145, 1), (1, 1147, 1), (1, 1148, 1), (1, 1149, 1), (1, 1150, 1), (1, 1151, 1), (1, 1152, 1), (1, 1153, 1), (1, 1154, 1), (1, 1155, 1)
    , (1, 1156, 1), (1, 1157, 1), (1, 1158, 1), (1, 1159, 1), (1, 1160, 1), (1, 1161, 1), (1, 1162, 1), (1, 1163, 1), (1, 1164, 1), (1, 1165, 1)
    , (1, 1166, 1), (1, 1167, 1), (1, 1168, 1), (1, 1169, 1), (1, 1170, 1), (1, 1171, 1), (1, 1173, 1), (1, 1174, 1), (1, 1176, 1), (1, 1177, 1)
    , (1, 1178, 1), (1, 1179, 1), (1, 1180, 1), (1, 1181, 1), (1, 1182, 1), (1, 1183, 1), (1, 1184, 1), (1, 1185, 1), (1, 1186, 1), (1, 1187, 1)
    , (1, 1188, 1), (1, 1189, 1), (1, 1190, 1), (1, 1191, 1), (1, 1192, 1), (1, 1196, 1), (1, 1197, 1), (1, 1198, 1), (1, 1199, 1), (1, 1200, 1)
    , (1, 1201, 1), (1, 1202, 1), (1, 1203, 1), (1, 1204, 1), (1, 1205, 1), (1, 1206, 1), (1, 1207, 1), (1, 1209, 1), (1, 1210, 1), (1, 1211, 1)
    , (1, 1213, 1), (1, 1214, 1), (1, 1215, 1), (1, 1216, 1), (1, 1217, 1), (1, 1218, 1), (1, 1219, 1), (1, 1220, 1), (1, 1221, 1), (1, 1222, 1)
    , (1, 1223, 1), (1, 1224, 1), (1, 1225, 1), (1, 1226, 1), (1, 1227, 1), (1, 1228, 1), (1, 1230, 1), (1, 1231, 1), (1, 1232, 1), (1, 1233, 1)
    , (1, 1234, 1), (1, 1235, 1), (1, 1236, 1), (1, 1237, 1), (1, 1238, 1), (1, 1239, 1), (1, 1240, 1), (1, 1241, 1), (1, 1242, 1), (1, 1243, 1)
    , (1, 1244, 1), (1, 1245, 1), (1, 1246, 1), (1, 1247, 1), (1, 1248, 1), (1, 1249, 1), (1, 1250, 1), (1, 1251, 1), (1, 1252, 1), (1, 1253, 1)
    , (1, 1254, 1), (1, 1255, 1), (1, 1256, 1), (1, 1257, 1), (1, 1258, 1), (1, 1259, 1), (1, 1260, 1), (1, 1261, 1), (1, 1262, 1), (1, 1263, 1)
    , (1, 1264, 1), (1, 1265, 1), (1, 1266, 1), (1, 1268, 1), (1, 1269, 1), (1, 1270, 1), (1, 1272, 1), (1, 1273, 1), (1, 1275, 1), (1, 1277, 1)
    , (1, 1278, 1), (1, 1279, 1), (1, 1280, 1), (1, 1282, 1), (1, 1284, 1), (1, 1285, 1), (1, 1286, 1), (1, 1287, 1), (1, 1288, 1), (1, 1290, 1)
    , (1, 1291, 1), (1, 1292, 1), (1, 1293, 1), (1, 1294, 1), (1, 1295, 1), (1, 1296, 1), (1, 1297, 1), (1, 1298, 1), (1, 1299, 1), (1, 1300, 1)
    , (1, 1302, 1), (1, 1303, 1), (1, 1304, 1), (1, 1306, 1), (1, 1307, 1), (1, 1308, 1), (1, 1309, 1), (1, 1310, 1), (1, 1311, 1), (1, 1312, 1)
    , (1, 1313, 1), (1, 1314, 1), (1, 1315, 1), (1, 1316, 1), (1, 1317, 1), (1, 1318, 1), (1, 1320, 1), (1, 1321, 1), (1, 1322, 1), (1, 1323, 1)
    , (1, 1324, 1), (1, 1325, 1), (1, 1326, 1), (1, 1329, 1), (1, 1330, 1), (1, 1331, 1), (1, 1332, 1), (1, 1333, 1), (1, 1334, 1), (1, 1335, 1)
    , (1, 1336, 1), (1, 1337, 1), (1, 1338, 1), (1, 1339, 1), (1, 1340, 1), (1, 1341, 1), (1, 1342, 1), (1, 1343, 1), (1, 1344, 3), (1, 1345, 3)
    , (1, 1346, 3), (1, 1347, 3), (1, 1348, 3), (1, 1349, 3), (1, 1350, 3), (1, 1351, 3), (1, 1352, 3), (1, 1353, 3), (1, 1354, 3), (1, 1355, 3)
    , (1, 1356, 3), (1, 1357, 3), (1, 1358, 3), (1, 1359, 3), (1, 1360, 3), (1, 1361, 3), (1, 1362, 3), (1, 1363, 3), (1, 1364, 3), (1, 1365, 3)
    , (1, 1366, 3), (1, 1367, 3), (1, 1369, 3), (1, 1370, 3), (1, 1371, 3), (1, 1373, 3), (1, 1376, 3), (1, 1377, 3), (1, 1378, 3), (1, 1379, 3)
    , (1, 1380, 3), (1, 1381, 3), (1, 1382, 3), (1, 1383, 3), (1, 1384, 3), (1, 1388, 3), (1, 1389, 3), (1, 1390, 3), (1, 1391, 3), (1, 1392, 3)
    , (1, 1393, 3), (1, 1394, 3), (1, 1396, 3), (1, 1397, 3), (1, 1398, 3), (1, 1399, 3), (1, 1400, 3), (1, 1401, 3), (1, 1402, 3), (1, 1403, 3)
    , (1, 1404, 3), (1, 1406, 3), (1, 1407, 3), (1, 1408, 3), (1, 1409, 3), (1, 1410, 3), (1, 1411, 3), (1, 1414, 3), (1, 1415, 3), (1, 1417, 3)
    , (1, 1418, 3), (1, 1419, 3), (1, 1420, 3), (1, 1421, 3), (1, 1422, 3), (1, 1423, 3), (1, 1425, 3), (1, 1426, 3), (1, 1427, 3), (1, 1428, 3)
    , (1, 1429, 3), (1, 1430, 3), (1, 1431, 3), (1, 1432, 3), (1, 1435, 3), (1, 1436, 3), (1, 1437, 3), (1, 1438, 3), (1, 1439, 3), (1, 1440, 3)
    , (1, 1441, 3), (1, 1442, 3), (1, 1443, 3), (1, 1444, 3), (1, 1445, 3), (1, 1446, 3), (1, 1447, 3), (1, 1448, 3), (1, 1449, 3), (1, 1450, 3)
    , (1, 1451, 3), (1, 1452, 3), (1, 1453, 3), (1, 1454, 3), (1, 1455, 3), (1, 1456, 3), (1, 1457, 3), (1, 1458, 3), (1, 1459, 3), (1, 1460, 3)
    , (1, 1461, 3), (1, 1462, 3), (1, 1463, 3), (1, 1464, 3), (1, 1465, 3), (1, 1466, 3), (1, 1468, 3), (1, 1469, 3), (1, 1470, 3), (1, 1471, 3)
    , (1, 1473, 3), (1, 1474, 3), (1, 1476, 3), (1, 1477, 3), (1, 1478, 3), (1, 1479, 3), (1, 1518, 2), (1, 1519, 2), (1, 1520, 2), (1, 1522, 2)
    , (1, 1523, 2), (1, 1524, 2), (1, 1525, 2), (1, 1528, 2), (1, 1529, 2), (1, 1536, 2), (1, 1537, 2), (1, 1625, 1), (1, 1626, 1), (1, 1627, 1)
    , (1, 1629, 1), (1, 1631, 1), (1, 1632, 1), (1, 1633, 1), (1, 1634, 1), (1, 1635, 1), (1, 1636, 1), (1, 1637, 1), (1, 1638, 1), (1, 1639, 1)
    , (1, 1640, 3), (1, 1647, 3), (1, 1650, 3), (1, 1651, 1), (1, 1652, 1), (1, 1653, 1), (1, 1654, 1), (1, 1655, 1), (1, 1656, 1), (1, 1657, 1)
    , (1, 1658, 1), (1, 1659, 3), (1, 1660, 3), (1, 1661, 3), (1, 1662, 3), (1, 1663, 3), (1, 1664, 1), (1, 1665, 1), (1, 1667, 1), (1, 1668, 1)
    , (1, 1669, 1), (1, 1670, 1), (1, 1671, 1), (1, 1672, 1), (1, 1673, 1), (1, 1674, 1), (1, 1675, 1), (1, 1676, 1), (1, 1677, 1), (1, 1678, 1)
    , (1, 1679, 1), (1, 1680, 1), (1, 1681, 1), (1, 1684, 3), (1, 1685, 3), (1, 1686, 3), (1, 1687, 3), (1, 1690, 1), (1, 1691, 1), (1, 1692, 1)
    , (1, 1693, 1), (1, 1694, 1), (1, 1695, 1), (1, 1696, 1), (1, 1698, 1), (1, 1699, 1), (1, 1700, 1), (1, 1701, 1), (1, 1702, 1), (1, 1703, 1)
    , (1, 1704, 1), (1, 1705, 1), (1, 1706, 1), (1, 1707, 3), (1, 1708, 3), (1, 1713, 1), (1, 1714, 1), (1, 1715, 1), (1, 1716, 1), (1, 1717, 1)
    , (1, 1718, 1), (1, 1719, 1), (1, 1720, 1), (1, 1721, 1), (1, 1722, 1), (1, 1723, 1), (1, 1724, 1), (1, 1725, 1), (1, 1727, 1), (1, 1728, 1)
    , (1, 1729, 1), (1, 1730, 1), (1, 1731, 1), (1, 1732, 1), (1, 1733, 1), (1, 1734, 1), (1, 1735, 1), (1, 1736, 1), (1, 1738, 1), (1, 1739, 1)
    , (1, 1740, 1), (1, 1741, 1), (1, 1742, 1), (1, 1743, 1), (1, 1744, 1), (1, 1745, 1), (1, 1746, 1), (1, 1748, 3), (1, 1749, 3), (1, 1750, 3)
    , (1, 1751, 3), (1, 1753, 1), (1, 1754, 3), (1, 1755, 3), (1, 1756, 1), (1, 1757, 1), (1, 1759, 1), (1, 1760, 1), (1, 1761, 1), (1, 1763, 1)
    , (1, 1764, 1), (1, 1769, 3), (1, 1770, 3), (1, 1772, 1), (1, 1773, 1), (1, 1774, 1), (1, 1776, 1), (1, 1777, 1), (1, 1778, 1), (1, 1779, 1)
    , (1, 1780, 1), (1, 1781, 1), (1, 1782, 1), (1, 1783, 1), (1, 1784, 1), (1, 1785, 1), (1, 1787, 2), (1, 1788, 2), (1, 1789, 3), (1, 1790, 3)
    , (1, 1791, 3), (1, 1792, 3), (1, 1793, 3), (1, 1794, 3), (1, 1795, 1), (1, 1796, 1), (1, 1797, 3), (1, 1798, 3), (1, 1799, 1), (1, 1802, 1)
    , (1, 1803, 1), (1, 1804, 3), (1, 1806, 3), (1, 1808, 1), (1, 1809, 1), (1, 1812, 1), (1, 1813, 1), (1, 1814, 1), (1, 1815, 1), (1, 1816, 1)
    , (1, 1817, 1), (1, 1819, 1), (1, 1820, 1), (1, 1821, 1), (1, 1822, 1), (1, 1823, 1), (1, 1824, 1), (1, 1825, 1), (1, 1826, 1), (1, 1830, 1)
    , (1, 1831, 1), (1, 1832, 1), (1, 1834, 1), (1, 1835, 1), (1, 1836, 1), (1, 1837, 1), (1, 1838, 1), (1, 1839, 1), (1, 1840, 1), (1, 1842, 1)
    , (1, 1844, 3), (1, 1846, 1), (1, 1848, 1), (1, 1849, 1), (1, 1850, 1), (1, 1851, 1), (1, 1852, 1), (1, 1853, 1), (1, 1854, 1), (1, 1855, 1)
    , (1, 1857, 1), (1, 1862, 3), (1, 1863, 1), (1, 1864, 1), (1, 1865, 1), (1, 1866, 1), (1, 1869, 3), (1, 1870, 1), (1, 1872, 1), (1, 1873, 1)
    , (1, 1874, 1), (1, 1875, 1), (1, 1876, 1), (1, 1877, 1), (1, 1878, 1), (1, 1879, 1), (1, 1880, 1), (1, 1881, 1), (1, 1882, 1), (1, 1883, 1)
    , (1, 1884, 1), (1, 1885, 1), (1, 1886, 1), (1, 1887, 1), (1, 1888, 1), (1, 1889, 1), (1, 1890, 3), (1, 1891, 3), (1, 1892, 3), (1, 1896, 3)
    , (1, 1897, 3), (1, 1898, 3), (1, 1901, 1), (1, 1902, 1), (1, 1903, 1), (1, 1904, 3), (1, 1905, 3), (1, 1906, 3), (1, 1907, 3), (1, 1908, 3)
    , (1, 1909, 1), (1, 1910, 1), (1, 1911, 1), (1, 1912, 1), (1, 1913, 1), (1, 1914, 1), (1, 1915, 1), (1, 1916, 1), (1, 1918, 1), (1, 1919, 1)
    , (1, 1920, 1), (1, 1921, 1), (1, 1922, 1), (1, 1923, 1), (1, 1924, 1), (1, 1925, 1), (1, 1926, 1), (1, 1930, 4), (1, 1932, 1), (1, 1933, 1)
    , (1, 1934, 1), (1, 1935, 1), (1, 1936, 1), (1, 1937, 1), (1, 1938, 1), (1, 1939, 1), (1, 1940, 1), (1, 1942, 2), (1, 1943, 3), (1, 1945, 3)
    , (1, 1946, 3), (1, 1948, 3), (1, 1949, 1), (1, 1950, 1), (1, 1951, 1), (1, 1952, 1), (1, 1953, 1), (1, 1954, 1), (1, 1955, 1), (1, 1956, 3)
    , (1, 1957, 3), (1, 1960, 1), (1, 1961, 1), (1, 1962, 1), (1, 1963, 1), (1, 1964, 1), (1, 1965, 1), (1, 1966, 1), (1, 1967, 1), (1, 1968, 1)
    , (1, 1969, 1), (1, 1970, 1), (1, 1971, 1), (1, 1972, 1), (1, 1973, 1), (1, 1974, 1), (1, 1975, 1), (1, 1976, 1), (1, 1977, 1), (1, 1981, 2)
    , (1, 1982, 2), (1, 1984, 3), (1, 1985, 3), (1, 1986, 3), (1, 1987, 3), (1, 1988, 3), (1, 1989, 3), (1, 1990, 3), (1, 1992, 3), (1, 1995, 1)
    , (1, 1996, 1), (1, 1997, 1), (1, 1998, 1), (1, 1999, 1), (1, 2000, 1), (1, 2001, 1), (1, 2002, 1), (1, 2003, 1), (1, 2004, 1), (1, 2005, 1)
    , (1, 2006, 1), (1, 2007, 1), (1, 2008, 1), (1, 2009, 1), (1, 2010, 1), (1, 2011, 1), (1, 2012, 1), (1, 2013, 1), (1, 2018, 3), (1, 2019, 3)
    , (1, 2020, 3), (1, 2022, 1), (1, 2024, 3), (1, 2025, 1), (1, 2026, 1), (1, 2027, 1), (1, 2028, 1), (1, 2029, 1), (1, 2030, 1), (1, 2031, 1)
    , (1, 2034, 1), (1, 2035, 1), (1, 2036, 1), (1, 2037, 1), (1, 2038, 1), (1, 2039, 1), (1, 2040, 1), (1, 2041, 1), (1, 2043, 3), (1, 2044, 3)
    , (1, 2049, 1), (1, 2051, 1), (1, 2052, 1), (1, 2053, 1), (1, 2055, 1), (1, 2056, 1), (1, 2057, 1), (1, 2058, 1), (1, 2059, 1), (1, 2061, 1)
    , (1, 2062, 1), (1, 2063, 1), (1, 2064, 1), (1, 2065, 1), (1, 2067, 1), (1, 2068, 1), (1, 2075, 2), (1, 2076, 3), (1, 2077, 3), (1, 2078, 3)
    , (1, 2080, 3), (1, 2082, 1), (1, 2084, 1), (1, 2085, 1), (1, 2087, 1), (1, 2088, 1), (1, 2089, 1), (1, 2090, 1), (1, 2091, 1), (1, 2092, 1)
    , (1, 2093, 1), (1, 2094, 1), (1, 2096, 1), (1, 2097, 3), (1, 2098, 1), (1, 2099, 1), (1, 2100, 1), (1, 2101, 1), (1, 2102, 1), (1, 2103, 1)
    , (1, 2104, 1), (1, 2105, 1), (1, 2107, 3), (1, 2108, 3), (1, 2109, 3), (1, 2110, 3), (1, 2121, 1), (1, 2122, 1), (1, 2123, 1), (1, 2124, 1)
    , (1, 2125, 1), (1, 2126, 1), (1, 2127, 1), (1, 2129, 1), (1, 2130, 1), (1, 2131, 1), (1, 2132, 1), (1, 2133, 1), (1, 2134, 1), (1, 2135, 1)
    , (1, 2136, 1), (1, 2137, 1), (1, 2139, 3), (1, 2140, 3), (1, 2141, 1), (1, 2142, 1), (1, 2143, 1), (1, 2144, 1), (1, 2145, 1), (1, 2147, 1)
    , (1, 2148, 1), (1, 2149, 1), (1, 2150, 1), (1, 2151, 1), (1, 2152, 1), (1, 2153, 1), (1, 2154, 1), (1, 2155, 1), (1, 2156, 1), (1, 2162, 2)
    , (1, 2164, 3), (1, 2165, 3), (1, 2166, 4), (1, 2167, 3), (1, 2169, 3), (1, 2170, 3), (1, 2171, 3), (1, 2172, 3), (1, 2173, 1), (1, 2174, 1)
    , (1, 2176, 1), (1, 2178, 1), (1, 2182, 3), (1, 2183, 1), (1, 2184, 1), (1, 2185, 1), (1, 2188, 1), (1, 2189, 1), (1, 2190, 1), (1, 2191, 1)
    , (1, 2192, 1), (1, 2193, 1), (1, 2194, 1), (1, 2195, 1), (1, 2197, 1), (1, 2198, 1), (1, 2199, 1), (1, 2201, 3), (1, 2202, 3), (1, 2205, 1)
    , (1, 2206, 1), (1, 2207, 1), (1, 2208, 1), (1, 2210, 3), (1, 2213, 1), (1, 2214, 1), (1, 2216, 1), (1, 2217, 1), (1, 2218, 1), (1, 2219, 1)
    , (1, 2220, 3), (1, 2221, 1), (1, 2222, 1), (1, 2223, 1), (1, 2224, 1), (1, 2226, 1), (1, 2227, 1), (1, 2228, 1), (1, 2229, 1), (1, 2230, 1)
    , (1, 2231, 1), (1, 2232, 1), (1, 2234, 1), (1, 2235, 1), (1, 2236, 1), (1, 2237, 1), (1, 2238, 1), (1, 2243, 4), (1, 2244, 3), (1, 2246, 3)
    , (1, 2247, 3), (1, 2248, 3), (1, 2249, 3), (1, 2250, 3), (1, 2251, 3), (1, 2253, 4), (1, 2255, 1), (1, 2256, 1), (1, 2257, 1), (1, 2258, 1)
    , (1, 2267, 1), (1, 2268, 1), (1, 2269, 1), (1, 2270, 1), (1, 2271, 1), (1, 2272, 4), (1, 2273, 1), (1, 2275, 1), (1, 2276, 1), (1, 2277, 1)
    , (1, 2278, 1), (1, 2281, 1), (1, 2282, 1), (1, 2283, 1), (1, 2284, 1), (1, 2285, 1), (1, 2287, 1), (1, 2288, 1), (1, 2290, 1), (1, 2291, 1)
    , (1, 2293, 1), (1, 2294, 1), (1, 2295, 1), (1, 2296, 1), (1, 2297, 1), (1, 2298, 1), (1, 2300, 1), (1, 2302, 1), (1, 2303, 1), (1, 2304, 3)
    , (1, 2305, 1), (1, 2306, 1), (1, 2308, 1), (1, 2309, 3), (1, 2311, 1), (1, 2312, 1), (1, 2313, 1), (1, 2314, 1), (1, 2315, 1), (1, 2316, 3)
    , (1, 2317, 4), (1, 2318, 1), (1, 2321, 1), (1, 2322, 1), (1, 2324, 1), (1, 2326, 1), (1, 2327, 1), (1, 2329, 1), (1, 2331, 1), (1, 2334, 1)
    , (1, 2335, 3), (1, 2336, 3), (1, 2340, 1), (1, 2341, 1), (1, 2343, 1), (1, 2345, 1), (1, 2346, 1), (1, 2347, 1), (1, 2349, 1), (1, 2350, 1);
    INSERT INTO dbo.I_DISC_MODEL_ZONE_SHOP (ID_COMPANY, ID_SHOP, ID_ZONE) VALUES
      (1, 2351, 1), (1, 2352, 3), (1, 2353, 3), (1, 2354, 3), (1, 2355, 4), (1, 2356, 4), (1, 2357, 4), (1, 2358, 4), (1, 2359, 4), (1, 2360, 1)
    , (1, 2361, 1), (1, 2362, 1), (1, 2363, 1), (1, 2365, 1), (1, 2366, 1), (1, 2367, 1), (1, 2368, 1), (1, 2369, 1), (1, 2370, 1), (1, 2371, 1)
    , (1, 2374, 1), (1, 2375, 3), (1, 2376, 3), (1, 2377, 1), (1, 2378, 1), (1, 2379, 1), (1, 2381, 1), (1, 2382, 3), (1, 2384, 1), (1, 2385, 1)
    , (1, 2387, 1), (1, 2388, 1), (1, 2389, 1), (1, 2390, 1), (1, 2391, 1), (1, 2392, 1), (1, 2394, 1), (1, 2395, 1), (1, 2397, 1), (1, 2398, 1)
    , (1, 2400, 1), (1, 2401, 1), (1, 2402, 1), (1, 2403, 1), (1, 2404, 1), (1, 2406, 1), (1, 2407, 1), (1, 2408, 1), (1, 2409, 1), (1, 2410, 1)
    , (1, 2412, 1), (1, 2413, 1), (1, 2414, 1), (1, 2415, 1), (1, 2417, 1), (1, 2418, 3), (1, 2420, 3), (1, 2422, 1), (1, 2423, 1), (1, 2424, 1)
    , (1, 2425, 1), (1, 2426, 1), (1, 2427, 1), (1, 2428, 1), (1, 2429, 1), (1, 2430, 1), (1, 2431, 1), (1, 2432, 1), (1, 2433, 1), (1, 2434, 1)
    , (1, 2435, 1), (1, 2436, 1), (1, 2437, 1), (1, 2438, 1), (1, 2439, 1), (1, 2440, 1), (1, 2441, 1), (1, 2442, 1), (1, 2443, 1), (1, 2444, 1)
    , (1, 2446, 1), (1, 2447, 1), (1, 2448, 1), (1, 2449, 1), (1, 2450, 3), (1, 2451, 1), (1, 2452, 1), (1, 2453, 1), (1, 2454, 1), (1, 2455, 1)
    , (1, 2456, 1), (1, 2458, 1), (1, 2459, 1), (1, 2460, 1), (1, 2461, 1), (1, 2462, 1), (1, 2463, 1), (1, 2465, 1), (1, 2466, 1), (1, 2468, 1)
    , (1, 2469, 1), (1, 2470, 1), (1, 2471, 1), (1, 2472, 1), (1, 2473, 1), (1, 2474, 1), (1, 2475, 1), (1, 2477, 1), (1, 2478, 1), (1, 2479, 1)
    , (1, 2480, 3), (1, 2481, 1), (1, 2482, 3), (1, 2483, 1), (1, 2484, 1), (1, 2485, 1), (1, 2486, 1), (1, 2487, 1), (1, 2488, 1), (1, 2489, 1)
    , (1, 2490, 1), (1, 2491, 1), (1, 2492, 1), (1, 2493, 1), (1, 2494, 1), (1, 2496, 1), (1, 2497, 1), (1, 2498, 1), (1, 2499, 1), (1, 2500, 1)
    , (1, 2501, 1), (1, 2502, 1), (1, 2503, 1), (1, 2504, 1), (1, 2505, 1), (1, 2506, 1), (1, 2507, 1), (1, 2508, 1), (1, 2509, 1), (1, 2510, 1)
    , (1, 2511, 1), (1, 2512, 1), (1, 2513, 1), (1, 2514, 1), (1, 2515, 1), (1, 2516, 1), (1, 2517, 1), (1, 2518, 1), (1, 2519, 1), (1, 2520, 1)
    , (1, 2521, -2), (1, 2522, 1), (1, 2523, 1), (1, 2524, 1), (1, 2525, 1), (1, 2526, 1), (1, 2527, 1), (1, 2528, 1), (1, 2529, 1), (1, 2530, 1)
    , (1, 2539, 1), (1, 2541, 1), (1, 2542, 1), (1, 2543, 1), (1, 2544, 3), (1, 2548, 1), (1, 2551, 1), (1, 2552, 3), (1, 2553, 1), (1, 2554, 1)
    , (1, 2556, 1), (1, 2557, 1), (1, 2558, 1), (1, 2559, 1), (1, 2560, 1), (1, 2561, 1), (1, 2564, 2), (1, 2565, 1), (1, 2566, 1), (1, 2567, 1)
    , (1, 2568, 1), (1, 2569, 1), (1, 2570, 1), (1, 2572, 1), (1, 2574, 1), (1, 2575, 1), (1, 2581, 1), (1, 2582, 1), (1, 2583, 1), (1, 2584, 1)
    , (1, 2585, 1), (1, 2586, 1), (1, 2587, 1), (1, 2588, 1), (1, 2589, 3), (1, 2592, 1), (1, 2593, 1), (1, 2594, 3), (1, 2595, 1), (1, 2596, 1)
    , (1, 2597, 1), (1, 2598, 1), (1, 2599, 1), (1, 2600, 1), (1, 2601, 1), (1, 2602, 1), (1, 2603, 1), (1, 2604, 1), (1, 2610, 1), (1, 2612, 1)
    , (1, 2614, 2), (1, 2615, 1), (1, 2616, 2), (1, 2627, 3), (1, 2628, 1), (1, 2633, 1), (1, 2634, 2), (1, 2635, 2), (1, 2637, 1), (1, 2638, 2)
    , (1, 2639, 1), (1, 2640, 1), (1, 2641, 2), (1, 2642, 1), (1, 2643, 2), (1, 2644, 1), (1, 2646, 2), (1, 2647, 1), (1, 2648, 2), (1, 2649, 2)
    , (1, 2651, 3), (1, 2652, 1), (1, 2653, 1), (1, 2654, 2), (1, 2655, 2), (1, 2656, 2), (1, 2657, 2), (1, 2658, 1), (1, 2660, 2), (1, 2661, 2)
    , (1, 2662, 2), (1, 2664, 1), (1, 2665, 1), (1, 2666, 2), (1, 2667, 1), (1, 2669, 2), (1, 2670, 2), (1, 2671, 1), (1, 2672, 1), (1, 2673, 1)
    , (1, 2674, 2), (1, 2675, 1), (1, 3676, 1), (1, 3677, 1), (1, 3678, 1), (1, 3679, 2), (1, 3680, 1), (1, 3681, 1), (1, 3683, 2), (1, 3684, 1)
    , (1, 3685, 1), (1, 3686, 2), (1, 3688, 1), (1, 3691, 2), (1, 3692, 2), (1, 3693, 1), (1, 3694, 1), (1, 3695, 1), (1, 3696, 2), (1, 3712, 2)
    , (1, 3713, 1), (1, 3714, 1), (1, 3715, 1), (1, 3716, 1), (1, 3717, 1), (1, 3718, 1), (1, 3719, 1), (1, 3720, 2), (1, 3721, 1), (1, 3722, 1)
    , (1, 3723, 2), (1, 3724, 1), (1, 3725, 1), (1, 3726, 1), (1, 3727, 1), (1, 3728, 2), (1, 3729, 2), (1, 3730, 1), (1, 3731, 1), (1, 3732, 1)
    , (1, 3733, 2), (1, 3734, 2), (1, 3735, 2), (1, 3736, 4), (1, 3737, 1), (1, 3738, 1), (1, 3739, 3), (1, 3740, 1), (1, 3741, 1), (1, 3742, 1)
    , (1, 3743, 1), (1, 3744, 1), (1, 3745, 1), (1, 3746, 1), (1, 3747, 2), (1, 3748, 1), (1, 3749, 1), (1, 3751, 2), (1, 3752, 1), (1, 3753, 2)
    , (1, 3754, 1), (1, 3755, 2), (1, 3756, 2), (1, 3757, 1), (1, 3758, 1), (1, 3759, 1), (1, 3760, 1), (1, 3761, 1), (1, 3764, 1), (1, 3765, 1)
    , (1, 3766, 1), (1, 3767, 2), (1, 3768, 1), (1, 3769, 2), (1, 3770, 3), (1, 3771, 1), (1, 3772, 2), (1, 3773, 2), (1, 3774, 3), (1, 3775, 1)
    , (1, 3776, 1), (1, 3777, 1), (1, 3778, 1), (1, 3780, 2), (1, 3781, 1), (1, 3782, 1), (1, 3783, 1), (1, 3784, 1), (1, 3785, 1), (1, 3787, 1)
    , (1, 3788, 1), (1, 3790, 2), (1, 3791, 3), (1, 3792, 1), (1, 3793, 2), (1, 3794, 1), (1, 3795, 2), (1, 3796, 2), (1, 3797, 1), (1, 3798, 1)
    , (1, 3799, 1), (1, 3800, 2), (1, 3801, 1), (1, 3802, 2), (1, 3803, 1), (1, 3804, 2), (1, 3805, 1), (1, 3806, 1), (1, 3808, 1), (1, 3809, 2)
    , (1, 3811, 1), (1, 3812, 1), (1, 3813, 2), (1, 3814, 1), (1, 3815, 1), (1, 3816, 1), (1, 3817, 2), (1, 3818, 1), (1, 3819, 1), (1, 3820, 2)
    , (1, 3821, 2), (1, 3822, 3), (1, 3823, 1), (1, 3824, 1), (1, 3825, 1), (1, 3826, 1), (1, 3827, 1), (1, 3828, 2), (1, 3829, 1), (1, 3830, 1)
    , (1, 3832, 5), (1, 3833, 1), (1, 3834, 1), (1, 3835, 1), (1, 3836, 1), (1, 3837, 2), (1, 3839, 5), (1, 3840, 1), (1, 3841, 2), (1, 3842, 6)
    , (1, 3843, 3), (1, 3844, 3), (1, 3845, 4), (1, 3846, 1), (1, 3847, 1), (1, 3848, 1), (1, 3849, 1), (1, 3850, 4), (1, 3851, 2), (1, 3852, 2)
    , (1, 3853, 1), (1, 3854, 4), (1, 3856, 2), (1, 3857, 4), (1, 3858, 1), (1, 3859, 1), (1, 3860, 1), (1, 3861, 2), (1, 3862, 1), (1, 3863, 1)
    , (1, 3864, 1), (1, 3865, 1), (1, 3866, 1), (1, 3868, 1), (1, 3870, 1), (1, 3871, 1), (1, 3872, 1), (1, 3873, 1), (1, 3876, 1), (1, 3877, 1)
    , (1, 3878, 4), (1, 3879, 1), (1, 3880, 1), (1, 3881, 2), (1, 3882, 1), (1, 3883, 2), (1, 3884, 2), (1, 3885, 1), (1, 3886, 1), (1, 3887, 2)
    , (1, 3888, 3), (1, 3889, 2), (1, 3891, 1), (1, 3892, 1), (1, 3893, 1), (1, 3894, 1), (1, 3895, 2), (1, 3896, 2), (1, 3897, 2), (1, 3898, 2)
    , (1, 3899, 1), (1, 3900, 2), (1, 3901, 2), (1, 3902, 2), (1, 3903, 3), (1, 3904, 1), (1, 3905, 1), (1, 3906, 1), (1, 3907, 1), (1, 3908, 1)
    , (1, 3909, 1), (1, 3911, 2), (1, 3912, 1), (1, 3913, 3), (1, 3914, 3), (1, 3915, 2), (1, 3916, 1), (1, 3917, 6), (1, 3918, 1), (1, 3919, 1)
    , (1, 3921, 5), (1, 3922, 1), (1, 3923, 1), (1, 3924, 1), (1, 3926, 1), (1, 3927, 1), (1, 3928, 5), (1, 3929, 1), (1, 3930, 1), (1, 3931, 5)
    , (1, 3932, 1), (1, 3933, 1), (1, 3934, 3), (1, 3935, 1), (1, 3936, 5), (1, 3937, 6), (1, 3938, 2), (1, 3939, 5), (1, 3940, 1), (1, 3941, 1)
    , (1, 3942, 5), (1, 3943, 1), (1, 3944, 5), (1, 3946, 2), (1, 3947, 1), (1, 3948, 3), (1, 3949, 6), (1, 3950, 6), (1, 3951, 1), (1, 3952, 1)
    , (1, 3953, 1), (1, 3954, 6), (1, 3956, -2), (1, 3957, 1), (1, 3958, 2), (1, 3959, 2), (1, 3960, 7), (1, 3961, 1), (1, 3962, 1), (1, 3963, 6)
    , (1, 3964, 1), (1, 3965, 1), (1, 3966, 5), (1, 3967, 1), (1, 3968, 1), (1, 3969, 6), (1, 3970, 5), (1, 3971, 1), (1, 3974, 1), (1, 3976, 1)
    , (1, 3977, 5), (1, 3979, 3), (1, 3980, 1), (1, 3982, 1), (1, 3983, 1), (1, 3984, 1), (1, 3985, 1), (1, 3986, 1), (1, 3987, 1), (1, 3988, 6)
    , (1, 3989, 6), (1, 3991, 5), (1, 3992, 1), (1, 3994, 5), (1, 3995, 1), (1, 3996, 5), (1, 3998, 5), (1, 3999, 4), (1, 4000, 4), (1, 4002, 5)
    , (1, 4003, 1), (1, 4005, 1), (1, 4006, 7), (1, 4007, 7), (1, 4008, 7), (1, 4009, 6), (1, 4010, 6), (1, 4012, 5), (1, 4013, 1), (1, 4014, 6)
    , (1, 4015, 1), (1, 4018, 2), (1, 4022, 1), (1, 4024, 1), (1, 4026, 1), (1, 4028, 5), (1, 4029, 6), (1, 4030, 6), (1, 4031, 1), (1, 4032, 1)
    , (1, 4033, 7), (1, 4034, 1), (1, 4035, 1), (1, 4037, 1), (1, 4038, 1), (1, 4039, 5), (1, 4040, 1), (1, 4041, 1), (1, 4042, 1), (1, 4045, 1)
    , (1, 4046, 1), (1, 4047, 1), (1, 4048, 2), (1, 4050, 1), (1, 4054, 7), (1, 4055, 1), (1, 4058, 1), (1, 4059, 1), (1, 4060, 5), (1, 4061, 7)
    , (1, 4062, 5), (1, 4063, 1), (1, 4065, 5), (1, 4067, 5), (1, 4068, 1), (1, 4069, 1), (1, 4071, 5), (1, 4072, 1), (1, 4073, 1), (1, 4076, 1)
    , (1, 4079, 2), (1, 4080, 6), (1, 4083, 1), (1, 4084, 1), (1, 4085, 5), (1, 4086, 1), (1, 4088, 2), (1, 4089, 5), (1, 4090, 2), (1, 4091, 1)
    , (1, 4092, 1), (1, 4093, 1), (1, 4096, 2), (1, 4098, 1), (1, 4099, 1), (1, 4100, 5), (1, 4102, 7), (1, 4103, 7), (1, 4104, 2), (1, 4105, 1)
    , (1, 4106, 1), (1, 4107, 1), (1, 4108, 1), (1, 4109, 5), (1, 4110, -2), (1, 4112, 6), (1, 4113, 1), (1, 4115, 2), (1, 4116, 1), (1, 4117, 5)
    , (1, 4118, 7), (1, 4119, 7), (1, 4120, 2), (1, 4121, 1), (1, 4122, 1), (1, 4123, 1), (1, 4124, 7), (1, 4125, 5), (1, 4126, 1), (1, 4127, 1)
    , (1, 4128, 1), (1, 4129, 1), (1, 4130, 5), (1, 4131, 1), (1, 4133, 2), (1, 4135, 1), (1, 4136, 1), (1, 4138, 6), (1, 4139, 1), (1, 4140, 1)
    , (1, 4142, 1), (1, 4143, 1), (1, 4144, 2), (1, 4145, 1), (1, 4146, 2), (1, 4147, 2), (1, 4148, 1), (1, 4150, 6), (1, 4151, 6), (1, 4152, 2)
    , (1, 4154, 1), (1, 4155, 1), (1, 4156, 1), (1, 4157, 1), (1, 4158, 5), (1, 4159, 1), (1, 4161, 2), (1, 4162, 5), (1, 4163, 1), (1, 4164, 2)
    , (1, 4166, 2), (1, 4167, 2), (1, 4168, 2), (1, 4169, 1), (1, 4170, 6), (1, 4171, 1), (1, 4172, 1), (1, 4173, 1), (1, 4174, 2), (1, 4175, 1)
    , (1, 4176, 2), (1, 4177, 1), (1, 4178, 2), (1, 4179, 2), (1, 4180, 1), (1, 4182, 1), (1, 4183, 2), (1, 4184, 2), (1, 4185, 2), (1, 4186, 2)
    , (1, 4188, 2), (1, 4189, 3), (1, 4190, 3), (1, 4191, 1), (1, 4192, 1), (1, 4193, 1), (1, 4194, 1), (1, 4195, 1), (1, 4196, 1), (1, 4197, 1)
    , (1, 4198, 1), (1, 4199, 1), (1, 4200, 1), (1, 4201, 2), (1, 4202, 2), (1, 4203, 2), (1, 4204, 1), (1, 4206, 1), (1, 4207, 1), (1, 4208, 1)
    , (1, 4209, 1), (1, 4210, 1), (1, 4211, 1), (1, 4212, 1), (1, 4213, 1), (1, 4215, 1), (1, 4216, 1), (1, 4219, 1), (1, 4220, 1), (1, 4223, 1)
    , (1, 4226, 2), (1, 4234, 2), (1, 4235, 2), (1, 4236, 3), (1, 4238, 5), (1, 4239, 1), (1, 4240, 1), (1, 4244, 3), (1, 4246, 1), (1, 4248, 3)
    , (1, 4250, 2), (1, 4251, 1), (1, 4253, 5), (1, 4254, 1), (1, 4255, 1), (1, 4257, 1), (1, 4258, 1), (1, 4260, 1), (1, 4268, 3), (1, 4270, 1)
    , (1, 4271, 1), (1, 4272, 1), (1, 4273, 2), (1, 4274, 1), (1, 4275, 1), (1, 4276, 1), (1, 4277, 1), (1, 4283, 3), (1, 4284, 4), (1, 4285, 4)
    , (1, 4288, 2), (1, 4301, 1), (1, 4303, 2), (1, 4304, 1), (1, 4306, 1), (1, 4307, 2), (1, 4308, 1), (1, 4309, 1), (1, 4310, 5), (1, 4311, 2)
    , (1, 4312, 1), (1, 4313, 1), (1, 4314, 1), (1, 4318, 1), (1, 4319, 1), (1, 4320, 1), (1, 4321, 1), (1, 4322, 1), (1, 4323, 1), (1, 4324, 1)
    , (1, 4325, 1), (1, 4326, 3), (1, 4327, 2), (1, 4328, 3), (1, 4331, 5), (1, 4332, 1), (1, 4333, 2), (1, 4334, 1), (1, 4335, 1), (1, 4336, 1)
    , (1, 4337, 1), (1, 4338, 1), (1, 4339, 1), (1, 4340, 1), (1, 4341, 1), (1, 4342, 1), (1, 4343, 1), (1, 4344, 1), (1, 4345, 2), (1, 4346, 1)
    , (1, 4347, 1), (1, 4348, 2), (1, 4349, 1), (1, 4350, 1), (1, 4351, 1), (1, 4529, 2), (1, 4530, 2), (1, 4531, 1), (1, 4532, 2), (1, 4538, 1)
    , (1, 4539, 1), (1, 4540, 3), (1, 4542, 1), (1, 4543, 1), (1, 4544, 4), (1, 4547, 1), (1, 4548, 1), (1, 4551, 1), (1, 4552, 1), (1, 4554, 1)
    , (1, 4556, 1), (1, 4557, 1), (1, 4559, 1), (1, 4560, 2), (1, 4561, 1), (1, 4562, 2), (1, 4563, 2), (1, 4564, 1), (1, 4565, 1), (1, 4566, 1)
    , (1, 4567, 2), (1, 4568, 1), (1, 4569, 1), (1, 4570, 1), (1, 4571, 1), (1, 4572, 1), (1, 4573, 1), (1, 4574, 1), (1, 4575, 1), (1, 4576, 1)
    , (1, 4577, 1), (1, 4578, 1), (1, 4579, 1), (1, 4580, 2), (1, 4581, 1), (1, 4582, 1), (1, 4583, 1), (1, 4584, 1), (1, 4585, 1), (1, 4586, 1)
    , (1, 4587, 2), (1, 4588, 1), (1, 4589, 2), (1, 4592, 1), (1, 4593, 1), (1, 4594, 1), (1, 4595, 3), (1, 4596, 1), (1, 4597, 1), (1, 4598, 1)
    , (1, 4599, 1), (1, 4601, 1), (1, 4602, 1), (1, 4603, 1), (1, 4604, 1), (1, 4605, 2), (1, 4606, 2), (1, 4607, 2), (1, 4608, 2), (1, 4609, 1)
    , (1, 4611, 1), (1, 4612, 2), (1, 4613, 1), (1, 4614, 1), (1, 4615, 2), (1, 4616, 1), (1, 4617, 2), (1, 4618, 2), (1, 4619, 1), (1, 4620, 1)
    , (1, 4621, 1), (1, 4622, 2), (1, 4623, 2), (1, 4624, 2), (1, 4625, 3), (1, 4626, 1), (1, 4627, 2), (1, 4628, 1), (1, 4629, 2), (1, 4630, 2)
    , (1, 4631, 1), (1, 4632, 1), (1, 4633, 1), (1, 4634, 1), (1, 4636, 1), (1, 4637, 2), (1, 4639, 1), (1, 4643, 2), (1, 4644, 1), (1, 4645, 1)
    , (1, 4646, 1), (1, 4647, 1), (1, 4648, 1), (1, 4649, 1), (1, 4650, 1), (1, 4651, 2), (1, 4652, 1), (1, 4653, 1), (1, 4654, 2), (1, 4655, 2)
    , (1, 4656, 1), (1, 4657, 1), (1, 4658, 2), (1, 4659, 2), (1, 4660, 2), (1, 4661, 1), (1, 4662, 1), (1, 4663, 1), (1, 4664, 1), (1, 4665, 1)
    , (1, 4666, 4), (1, 4667, 2), (1, 4669, 1), (1, 4670, 1), (1, 4671, 1), (1, 4672, 1), (1, 4673, 4), (1, 4674, 1), (1, 4675, 1), (1, 4676, 1);
    INSERT INTO dbo.I_DISC_MODEL_ZONE_SHOP (ID_COMPANY, ID_SHOP, ID_ZONE) VALUES
      (1, 4677, 1), (1, 4678, 1), (1, 4679, 1), (1, 4680, 3), (1, 4681, 1), (1, 4682, 1), (1, 4683, 1), (1, 4687, 2), (1, 4688, 2), (1, 4689, 1)
    , (1, 4697, 6), (1, 4698, 1), (1, 4699, 1), (1, 4700, 1), (1, 4701, 1), (1, 4703, 1), (1, 4704, 1), (1, 4705, 2), (1, 4706, 6), (1, 4707, 1)
    , (1, 4708, 1), (1, 4709, 2), (1, 4710, 1), (1, 4711, 1), (1, 4713, 2), (1, 4714, 1), (1, 4715, 1), (1, 4716, 1), (1, 4717, 3), (1, 4718, 2)
    , (1, 4719, 4), (1, 4720, 1), (1, 4721, 1), (1, 4722, 1), (1, 4724, 1), (1, 4726, 1), (1, 4728, 1), (1, 4729, 1), (1, 4730, 2), (1, 4731, 3)
    , (1, 4732, 2), (1, 4733, 1), (1, 4734, 1), (1, 4737, 1), (1, 4738, 1), (1, 4739, 4), (1, 4740, 1), (1, 4741, 1), (1, 4742, 1), (1, 4744, 1)
    , (1, 4745, 1), (1, 4746, 2), (1, 4747, 1), (1, 4750, 1), (1, 4752, 1), (1, 4753, 1), (1, 4756, 4), (1, 4757, 2), (1, 4758, 1), (1, 4759, 1)
    , (1, 4769, 1);
    IF @@TRANCOUNT = 0 OR (SELECT count(*) FROM dbo.I_DISC_MODEL_ZONE_SHOP (nolock)) <> 2761
    BEGIN
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DROP TABLE dbo.I_DISC_MODEL_ZONE_SHOP;
        RAISERROR(N'СТОП: справочник магазин → зона заполнился не целиком — таблица снята, перезапустите файл.', 16, 1);
    END
    ELSE COMMIT TRANSACTION;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE_CITY', 'U') IS NULL
BEGIN
    -- запасная зона по области магазина (I_SHOP.ID_CITY → I_CITY) — для магазинов, которых нет в I_DISC_MODEL_ZONE_SHOP;
    -- малый формат (зоны 5–7) — только по списку магазинов. Области с одним магазином (Белгородская, Воронежская,
    -- Сахалинская, город 21286) — по замеру в зоне Москвы и МО
    CREATE TABLE dbo.I_DISC_MODEL_ZONE_CITY (
          ID_COMPANY int      NOT NULL
        , ID_CITY    int      NOT NULL
        , ID_ZONE    smallint NOT NULL
        , CONSTRAINT PK_I_DISC_MODEL_ZONE_CITY PRIMARY KEY CLUSTERED (ID_COMPANY, ID_CITY)
    );
    INSERT INTO dbo.I_DISC_MODEL_ZONE_CITY (ID_COMPANY, ID_CITY, ID_ZONE) VALUES
          (1, 82, 1), (1, 10, 1), (1, 1, 1), (1, 4, 1), (1, 79, 1), (1, 21286, 1)
        , (1, 83, 2), (1, 24, 2), (1, 26, 2), (1, 27, 2)
        , (1, 2, 3), (1, 3, 3), (1, 6, 3), (1, 12, 3), (1, 13, 3), (1, 15, 3), (1, 16, 3)
        , (1, 5, 4), (1, 7, 4), (1, 11, 4), (1, 17, 4);
END

IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE_DAY', 'U') IS NULL
BEGIN
    -- цена зоны на день: зона × товар × день (офлайн, клиенты с картой, без табака). Цена продажи строки = COST / QUANTITY;
    -- цена магазина за день — самая частая у его строк; цена зоны — та, что стоит в большинстве магазинов зоны, продававших
    -- товар в этот день (при равенстве — меньшая). Весовой товар (у большинства строк дробное количество) — цены округлены
    -- до рубля (решение Елены 20.10). Десятки млн строк
    CREATE TABLE dbo.I_DISC_MODEL_ZONE_DAY (
          ID_COMPANY int            NOT NULL
        , DATA       date           NOT NULL
        , ID_ZONE    smallint       NOT NULL
        , ID_PRODUCT bigint         NOT NULL
        , PRICE      decimal(19, 2) NOT NULL   -- цена зоны
        , WEIGHTED   bit            NOT NULL   -- 1 — весовой: цены до рубля
        , SHOPS      int            NOT NULL   -- магазинов зоны с продажей товара в этот день
        , SHOPS_AT   int            NOT NULL   -- из них с ценой зоны
        , LINES      int            NOT NULL   -- строк чеков
        , RUN_ID     int            NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_ZONE_DAY ON dbo.I_DISC_MODEL_ZONE_DAY;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_ZONE_REG', 'U') IS NULL
BEGIN
    -- регулярная цена зоны на день (решение Елены 20.10): наибольшая цена зоны с начала истории; если цена зоны ниже неё
    -- держится дольше 2 месяцев (60 дней от первого до последнего дня ниже), с первого такого дня регулярной становится новая
    -- цена (отсчёт максимума заново). Пересобирается целиком процедурой dbo.disc_model_zone_reg. Строки — как в ZONE_DAY
    CREATE TABLE dbo.I_DISC_MODEL_ZONE_REG (
          ID_COMPANY  int            NOT NULL
        , DATA        date           NOT NULL
        , ID_ZONE     smallint       NOT NULL
        , ID_PRODUCT  bigint         NOT NULL
        , PRICE       decimal(19, 2) NOT NULL   -- цена зоны на день
        , WEIGHTED    bit            NOT NULL
        , REG_PRICE   decimal(19, 2) NOT NULL   -- регулярная цена зоны на день
        , BELOW       bit            NOT NULL   -- 1 — цена зоны ниже регулярной (акция в зоне)
        , EP_START    date           NULL       -- у BELOW = 1: первый день акции в зоне
        , EP_END      date           NULL       -- у BELOW = 1: последний день; NULL — идёт до последнего дня данных
        , PROVISIONAL bit            NOT NULL   -- 1 — акция идёт и ей меньше 2 месяцев: может стать снижением регулярной цены
        , IN_RESET    bit            NOT NULL   -- 1 — день снижения регулярной цены (ниже прежней регулярной дольше 2 месяцев)
        , REG_PREV    decimal(19, 2) NULL       -- у IN_RESET = 1: прежняя регулярная цена
        , BUILD_DT    datetime2(0)   NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_ZONE_REG ON dbo.I_DISC_MODEL_ZONE_REG;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_LINE', 'U') IS NULL
BEGIN
    -- реестр всех скидок (кампании и товары × каналы × зоны — до миллионов строк); пересобирается из I_DISC_MODEL_LINE_DAY
    CREATE TABLE dbo.I_DISC_MODEL_LINE (
          ID_COMPANY      int            NOT NULL
        , LINE_TYPE       tinyint        NOT NULL
        , ID_LINE         bigint         NOT NULL
        , ID_ORGANIZATION int            NOT NULL   -- 1 офлайн, 3 онлайн, 0 — оба канала
        , ID_ZONE         smallint       NOT NULL   -- ценовая зона; 0 — вся сеть; −1 — вне справочника зон
        , LINE_NAME       nvarchar(255)  NULL       -- LINE_TYPE 1: I_CAMPAIGN.CAMPAIGN_NAME; 2: I_PRODUCT.PRODUCT_NAME
        , ID_PROMO        int            NULL       -- акция CVM / ПП, к которой относится кампания (I_PROMO_CAMPAIGN_LINK, IS_USED = 1); у нескольких — меньший номер
        , PROMOS          int            NOT NULL   -- сколько акций связано с кампанией
        , DATE_FROM       date           NOT NULL   -- первый день со скидкой (самая ранняя продажа со скидкой в магазинах зоны)
        , DATE_TO         date           NOT NULL   -- последний день со скидкой
        , DAYS            int            NOT NULL   -- дней со скидкой
        , MONTHS          int            NOT NULL   -- месяцев со скидкой
        , CITIES          int            NOT NULL   -- городов со скидкой
        , VALUE_POS       decimal(19, 2) NOT NULL
        , VALUE_NEG       decimal(19, 2) NOT NULL
        , CHECKS          bigint         NOT NULL   -- чеков со скидкой
        , CALC_DT         datetime2(0)   NOT NULL
    );   -- ключ (ID_COMPANY, LINE_TYPE, ID_LINE, ID_ORGANIZATION, ID_ZONE) уникален по построению (GROUPING SETS)
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_LINE ON dbo.I_DISC_MODEL_LINE;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_LINE_MONTH', 'U') IS NULL
BEGIN
    -- реестр по месяцам: окно скидки в месяце (первый и последний день со скидкой)
    CREATE TABLE dbo.I_DISC_MODEL_LINE_MONTH (
          ID_COMPANY      int            NOT NULL
        , LINE_TYPE       tinyint        NOT NULL
        , ID_LINE         bigint         NOT NULL
        , ID_ORGANIZATION int            NOT NULL   -- 1, 3, 0 — оба
        , ID_ZONE         smallint       NOT NULL   -- ценовая зона; 0 — вся сеть
        , YEAR_MONTH      int            NOT NULL   -- ГГГГММ
        , DATE_FROM       date           NOT NULL
        , DATE_TO         date           NOT NULL
        , DAYS            int            NOT NULL
        , CITIES          int            NOT NULL
        , VALUE_POS       decimal(19, 2) NOT NULL
        , VALUE_NEG       decimal(19, 2) NOT NULL
        , CHECKS          bigint         NOT NULL
        , CALC_DT         datetime2(0)   NOT NULL
    );   -- ключ (..., ID_ZONE, YEAR_MONTH) уникален по построению
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_DISC_MODEL_LINE_MONTH ON dbo.I_DISC_MODEL_LINE_MONTH;
END

IF OBJECT_ID('dbo.I_DISC_MODEL_CAMPAIGN', 'U') IS NULL
BEGIN
    -- тип строки отчёта у кампании; заполняет Елена, процедура таблицу не пишет
    CREATE TABLE dbo.I_DISC_MODEL_CAMPAIGN (
          ID_COMPANY  int            NOT NULL
        , ID_CAMPAIGN int            NOT NULL
        , REPORT_TYPE nvarchar(50)   NULL       -- «Программа лояльности» / «Общие промо»
        , SET_DT      datetime2(0)   NOT NULL CONSTRAINT DF_I_DISC_MODEL_CAMPAIGN_SET_DT DEFAULT (sysdatetime())
        , CONSTRAINT PK_I_DISC_MODEL_CAMPAIGN PRIMARY KEY CLUSTERED (ID_COMPANY, ID_CAMPAIGN)
    );
END

IF OBJECT_ID('dbo.I_DISC_MODEL_STATE', 'U') IS NULL
BEGIN
    -- строка на день (сотни строк)
    CREATE TABLE dbo.I_DISC_MODEL_STATE (
          ID_COMPANY      int            NOT NULL
        , DATA            date           NOT NULL
        , STATUS          varchar(10)    NOT NULL   -- OK — посчитан; NO_CHECKS — строк чеков нет; ZONE — посчитаны только цены зон
        , CHECK_LINES     bigint         NOT NULL   -- строк I_CHECK дня (клиенты > 0, каналы 1 и 3)
        , CLIENT_DAYS     int            NOT NULL   -- строк I_DISC_MODEL_CLIENT_DAY
        , RULE_ROWS       bigint         NOT NULL   -- строк I_CHECK_RULE по чекам дня
        , RULE_ROWS_TOB   bigint         NOT NULL   -- из них на табачных позициях (в витрину не входят)
        , VALUE_TOB       decimal(19, 2) NOT NULL   -- Σ|VALUE| на табачных позициях
        , RULE_NO_ITEM    bigint         NOT NULL   -- строк с позицией, которой нет в I_CHECK (входят, табаком не считаются)
        , DUP_LINES       bigint         NOT NULL   -- повторов позиции (ID_CHECK, ID_CHEQUEITEM) в I_CHECK: суммы ТО идут как в источнике
        , ZONE_CALC_DT    datetime2(0)   NULL       -- когда посчитаны цены зон этого дня (I_DISC_MODEL_ZONE_DAY); NULL — не посчитаны
        , ZONE_ROWS       int            NULL       -- строк цен зон за день
        , RUN_ID          int            NOT NULL
        , CALC_DT         datetime2(0)   NOT NULL
        , LAST_TRY        datetime2(0)   NOT NULL
        , CONSTRAINT PK_I_DISC_MODEL_STATE PRIMARY KEY CLUSTERED (ID_COMPANY, DATA)
    );
END

IF OBJECT_ID('dbo.I_DISC_MODEL_LOG', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.I_DISC_MODEL_LOG (
          RUN_ID       int IDENTITY(1, 1) NOT NULL CONSTRAINT PK_I_DISC_MODEL_LOG PRIMARY KEY CLUSTERED
        , STARTED      datetime2(0)   NOT NULL
        , FINISHED     datetime2(0)   NULL
        , RUN_MODE     varchar(10)    NOT NULL   -- NIGHT / RANGE / ZONE_REG (регулярная цена зон)
        , D_FROM       date           NULL
        , D_TO         date           NULL
        , DAYS_PLANNED int            NULL
        , DAYS_DONE    int            NULL
        , STATUS       varchar(10)    NOT NULL   -- RUN / OK / STOPPED / ERROR
        , MAX_PART_SEC int            NULL       -- самая долгая порция запуска, с (оценка для следующего запуска с @stop_at)
        , REG_SEC      int            NULL       -- пересборка реестра, с (у ZONE_REG — вся пересборка регулярной цены)
        , ZONE_PART_SEC int           NULL       -- самая долгая порция цен зон, с
        , MSG          nvarchar(2000) NULL
    );
END
GO

-- ============================== 3а. ПРОЦЕДУРЫ ЦЕН ЗОН: dbo.disc_model_zone_day, dbo.disc_model_zone_reg ==============================
-- Решения Елены 20.10.2026: ценовые зоны — по фактическим ценам в чеках (I_DISC_MODEL_ZONE_SHOP / _ZONE_CITY); цена зоны на
--   день — цена большинства магазинов зоны; регулярная цена зоны — наибольшая цена зоны, а если цена зоны ниже неё держится
--   дольше 2 месяцев — с первого такого дня регулярной становится новая цена; скидка на покупку = (регулярная цена зоны −
--   цена продажи) × количество; эпизоды разных магазинов не склеиваются; весовые — цена зоны до рубля.
-- Обе процедуры вызывает dbo.disc_model_mart (ночью и в истории): сначала цены зон по дням, затем регулярная цена, затем
--   скидки клиентов. Отдельно их запускать не нужно.

-- dbo.disc_model_zone_day: цены зон за дни @d_from..@d_to — чтение I_CHECK (офлайн, клиенты с картой, без табака, продажи
--   с количеством и суммой больше 0), запись I_DISC_MODEL_ZONE_DAY (дни с чеками — DELETE + INSERT) и отметка ZONE_CALC_DT в
--   I_DISC_MODEL_STATE (новый день — строка со STATUS = 'ZONE': скидки клиентов за него ещё не посчитаны). День, где чеков
--   сейчас нет, а цены зон посчитаны раньше, остаётся как был.
CREATE OR ALTER PROCEDURE dbo.disc_model_zone_day
      @d_from date
    , @d_to   date
    , @run_id int = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET DEADLOCK_PRIORITY LOW;
    -- метка установщика: CVM_DISC_MODEL

    DECLARE @idc int = 1, @now datetime2(0), @n int = 0;

    CREATE TABLE #zd_zone (ID_SHOP int NOT NULL PRIMARY KEY, ID_ZONE smallint NOT NULL);
    CREATE TABLE #zd_tob  (ID_PRODUCT bigint NOT NULL PRIMARY KEY);
    CREATE TABLE #zd_days (DATA date NOT NULL PRIMARY KEY, N int NOT NULL);
    CREATE TABLE #zd_l (DATA date NOT NULL, ID_ZONE smallint NOT NULL, ID_SHOP int NOT NULL, ID_PRODUCT bigint NOT NULL
                      , U decimal(19, 4) NOT NULL, FR tinyint NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zd_l ON #zd_l;
    CREATE TABLE #zd_w (DATA date NOT NULL, ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, W tinyint NOT NULL, LINES int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zd_w ON #zd_w;
    CREATE TABLE #zd_sp (DATA date NOT NULL, ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, ID_SHOP int NOT NULL
                       , P decimal(19, 2) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zd_sp ON #zd_sp;
    CREATE TABLE #zd_out (DATA date NOT NULL, ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, PRICE decimal(19, 2) NOT NULL
                        , WEIGHTED bit NOT NULL, SHOPS int NOT NULL, SHOPS_AT int NOT NULL, LINES int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zd_out ON #zd_out;

    -- магазин → зона: список магазинов, иначе область; только офлайн (у онлайна зон нет — товарная скидка не считается)
    INSERT INTO #zd_zone (ID_SHOP, ID_ZONE)
    SELECT x.ID_SHOP, coalesce(zs.ID_ZONE, zc.ID_ZONE, -1)
    FROM (SELECT s.ID_SHOP, s.ID_CITY, s.ID_ORGANIZATION
               , rn = row_number() OVER (PARTITION BY s.ID_SHOP ORDER BY s.revision_date DESC, s.ID_CITY DESC)
          FROM dbo.I_SHOP AS s (nolock)
          WHERE s.ID_COMPANY = @idc AND s.ID_SHOP IS NOT NULL) AS x
         LEFT JOIN dbo.I_DISC_MODEL_ZONE_SHOP AS zs (nolock) ON zs.ID_COMPANY = @idc AND zs.ID_SHOP = x.ID_SHOP
         LEFT JOIN dbo.I_DISC_MODEL_ZONE_CITY AS zc (nolock) ON zc.ID_COMPANY = @idc AND zc.ID_CITY = x.ID_CITY
    WHERE x.rn = 1 AND x.ID_ORGANIZATION = 1;

    INSERT INTO #zd_tob (ID_PRODUCT)
    SELECT DISTINCT CAST(p.ID_PRODUCT AS bigint)
    FROM dbo.I_PRODUCT AS p (nolock)
    WHERE p.ID_PRODUCT IS NOT NULL
      AND (   p.ID_CATEGORY_5_ext LIKE N'610901%' OR p.ID_CATEGORY_5_ext LIKE N'610902%'
           OR p.ID_CATEGORY_5_ext LIKE N'610904%' OR p.ID_CATEGORY_5_ext LIKE N'610905%'
           OR p.ID_CATEGORY_5_ext LIKE N'610906%' OR p.ID_CATEGORY_5_ext LIKE N'70030101%');

    -- 1. Строки продаж дней: цена продажи и признак дробного количества (весовой)
    INSERT INTO #zd_l WITH (TABLOCK) (DATA, ID_ZONE, ID_SHOP, ID_PRODUCT, U, FR)
    SELECT c.DATA, z.ID_ZONE, c.ID_SHOP, c.ID_PRODUCT
         , CAST(c.COST / NULLIF(c.QUANTITY, 0) AS decimal(19, 4))
         , CASE WHEN c.QUANTITY <> round(c.QUANTITY, 0) THEN 1 ELSE 0 END
    FROM dbo.I_CHECK AS c (nolock)
         JOIN #zd_zone AS z ON z.ID_SHOP = c.ID_SHOP AND z.ID_ZONE > 0
    WHERE c.ID_COMPANY = @idc AND c.ID_ORGANIZATION = 1
      AND c.DATA >= @d_from AND c.DATA <= @d_to
      AND c.ID_CONTACT > 0 AND c.QUANTITY > 0 AND c.COST > 0 AND c.ID_PRODUCT IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM #zd_tob AS t WHERE t.ID_PRODUCT = c.ID_PRODUCT)
    OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);

    -- 2. Весовой ли товар в зоне за день: у большинства строк дробное количество
    INSERT INTO #zd_w WITH (TABLOCK) (DATA, ID_ZONE, ID_PRODUCT, W, LINES)
    SELECT DATA, ID_ZONE, ID_PRODUCT, CASE WHEN 2 * sum(FR) > count(*) THEN 1 ELSE 0 END, count(*)
    FROM #zd_l
    GROUP BY DATA, ID_ZONE, ID_PRODUCT
    OPTION (MAX_GRANT_PERCENT = 10);

    -- 3. Цена магазина за день — самая частая у его строк (при равенстве — меньшая); весовой — до рубля
    INSERT INTO #zd_sp WITH (TABLOCK) (DATA, ID_ZONE, ID_PRODUCT, ID_SHOP, P)
    SELECT y.DATA, y.ID_ZONE, y.ID_PRODUCT, y.ID_SHOP, y.P
    FROM (SELECT x.*, RN = row_number() OVER (PARTITION BY x.DATA, x.ID_ZONE, x.ID_PRODUCT, x.ID_SHOP ORDER BY x.N DESC, x.P)
          FROM (SELECT l.DATA, l.ID_ZONE, l.ID_PRODUCT, l.ID_SHOP
                     , P = CAST(CASE WHEN w.W = 1 THEN round(l.U, 0) ELSE round(l.U, 2) END AS decimal(19, 2)), N = count(*)
                FROM #zd_l AS l
                     JOIN #zd_w AS w ON w.DATA = l.DATA AND w.ID_ZONE = l.ID_ZONE AND w.ID_PRODUCT = l.ID_PRODUCT
                GROUP BY l.DATA, l.ID_ZONE, l.ID_PRODUCT, l.ID_SHOP, CAST(CASE WHEN w.W = 1 THEN round(l.U, 0) ELSE round(l.U, 2) END AS decimal(19, 2))) AS x) AS y
    WHERE y.RN = 1
    OPTION (MAX_GRANT_PERCENT = 10);

    -- 4. Цена зоны — та, что у большинства магазинов зоны (при равенстве — меньшая)
    INSERT INTO #zd_out WITH (TABLOCK) (DATA, ID_ZONE, ID_PRODUCT, PRICE, WEIGHTED, SHOPS, SHOPS_AT, LINES)
    SELECT y.DATA, y.ID_ZONE, y.ID_PRODUCT, y.P, w.W, y.SHOPS, y.STORES, w.LINES
    FROM (SELECT x.*, RN = row_number() OVER (PARTITION BY x.DATA, x.ID_ZONE, x.ID_PRODUCT ORDER BY x.STORES DESC, x.P)
               , SHOPS = sum(x.STORES) OVER (PARTITION BY x.DATA, x.ID_ZONE, x.ID_PRODUCT)
          FROM (SELECT DATA, ID_ZONE, ID_PRODUCT, P, STORES = count(*) FROM #zd_sp GROUP BY DATA, ID_ZONE, ID_PRODUCT, P) AS x) AS y
         JOIN #zd_w AS w ON w.DATA = y.DATA AND w.ID_ZONE = y.ID_ZONE AND w.ID_PRODUCT = y.ID_PRODUCT
    WHERE y.RN = 1
    OPTION (MAX_GRANT_PERCENT = 10);

    -- дни отрезка и сколько строк продаж в каждом
    WHILE dateadd(day, @n, @d_from) <= @d_to
    BEGIN
        INSERT INTO #zd_days (DATA, N) VALUES (dateadd(day, @n, @d_from), 0);
        SET @n += 1;
    END
    UPDATE d SET d.N = x.N
    FROM #zd_days AS d JOIN (SELECT DATA, N = count(*) FROM #zd_l GROUP BY DATA) AS x ON x.DATA = d.DATA;

    -- 5. Запись: дни с продажами — заново; день без продаж, посчитанный раньше, остаётся
    SET @now = sysdatetime();
    BEGIN TRANSACTION;

    DELETE v FROM dbo.I_DISC_MODEL_ZONE_DAY AS v
    WHERE v.ID_COMPANY = @idc AND v.DATA >= @d_from AND v.DATA <= @d_to
      AND EXISTS (SELECT 1 FROM #zd_days AS d WHERE d.DATA = v.DATA AND d.N > 0);

    INSERT INTO dbo.I_DISC_MODEL_ZONE_DAY WITH (TABLOCK) (ID_COMPANY, DATA, ID_ZONE, ID_PRODUCT, PRICE, WEIGHTED, SHOPS, SHOPS_AT, LINES, RUN_ID)
    SELECT @idc, DATA, ID_ZONE, ID_PRODUCT, PRICE, WEIGHTED, SHOPS, SHOPS_AT, LINES, @run_id FROM #zd_out;

    MERGE dbo.I_DISC_MODEL_STATE AS t
    USING (SELECT d.DATA, d.N, ROWS_Z = (SELECT count(*) FROM #zd_out AS o WHERE o.DATA = d.DATA) FROM #zd_days AS d) AS s
       ON t.ID_COMPANY = @idc AND t.DATA = s.DATA
    WHEN MATCHED AND (s.N > 0 OR t.ZONE_CALC_DT IS NULL) THEN
        UPDATE SET t.ZONE_CALC_DT = @now, t.ZONE_ROWS = s.ROWS_Z
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (ID_COMPANY, DATA, STATUS, CHECK_LINES, CLIENT_DAYS, RULE_ROWS, RULE_ROWS_TOB, VALUE_TOB, RULE_NO_ITEM, DUP_LINES
              , RUN_ID, CALC_DT, LAST_TRY, ZONE_CALC_DT, ZONE_ROWS)
        VALUES (@idc, s.DATA, 'ZONE', 0, 0, 0, 0, 0, 0, 0, @run_id, @now, @now, @now, s.ROWS_Z);

    COMMIT TRANSACTION;
END
GO

-- dbo.disc_model_zone_reg: регулярная цена зоны по всей истории I_DISC_MODEL_ZONE_DAY и пересмотр скидок задним числом.
--   По каждой зоне × товару по дням: регулярная = наибольшая цена зоны с начала отрезка; цена ниже регулярной больше копейки —
--   «ниже». Подряд идущие дни «ниже» (до дня по регулярной цене) — акция в зоне. Акция дольше @reg_days (60 дней от первого
--   до последнего дня ниже) — снижение регулярной цены: с её первого дня начинается новый отрезок, максимум считается заново
--   (задним числом, решение Елены 20.10). Отрезки ищутся по порядку: первое такое снижение ряда, пересчёт ряда, следующее.
--   Пересмотр: строки скидок клиентов (I_DISC_MODEL_CLIENT_DISC, LINE_TYPE 2 и 3), у которых регулярная цена зоны на их день
--   изменилась, пересчитываются из хранимых количества и суммы по цене продажи: ниже новой регулярной — скидка (2), ниже
--   прежней регулярной в дни снижения — 3, иначе строка убирается; окно по городам этих дней и товаров пересобирается из
--   строк клиентов. Чеки заново не читаются. Пересмотр, окно и замена таблицы — одной транзакцией.
CREATE OR ALTER PROCEDURE dbo.disc_model_zone_reg
      @reg_days int = 60
    , @run_id   int = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET DEADLOCK_PRIORITY LOW;
    -- метка установщика: CVM_DISC_MODEL

    DECLARE @idc int = 1, @it int = 0, @new int = 1, @t datetime2(0) = sysdatetime(), @msg nvarchar(2000), @lock int, @locked bit = 0;
    DECLARE @n_chg bigint = 0, @n_del bigint = 0, @n_ins bigint = 0, @n_ld int = 0, @min_d date, @log int, @n_reset int = 0;
    DECLARE @maxd date, @n_reread int = 0;

    IF @@TRANCOUNT > 0
        THROW 50001, N'disc_model_zone_reg: вызвана внутри открытой транзакции — не начинаю.', 1;

    CREATE TABLE #zr_d (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, DATA date NOT NULL, PRICE decimal(19, 2) NOT NULL
                      , WEIGHTED bit NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_d ON #zr_d;
    CREATE TABLE #zr_rs (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, R_FROM date NOT NULL, R_TO date NOT NULL
                       , REG_PREV decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_rs ON #zr_rs;
    CREATE TABLE #zr_act (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_act ON #zr_act;
    CREATE TABLE #zr_new (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, R_FROM date NOT NULL, R_TO date NOT NULL
                        , REG_PREV decimal(19, 2) NULL);
    CREATE TABLE #zr_w (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, DATA date NOT NULL, PRICE decimal(19, 2) NOT NULL
                      , WEIGHTED bit NOT NULL, SEG date NOT NULL, R decimal(19, 2) NOT NULL, BELOW tinyint NOT NULL, EG int NOT NULL
                      , LAST_D date NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_w ON #zr_w;
    CREATE TABLE #zr_fin (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, DATA date NOT NULL, PRICE decimal(19, 2) NOT NULL
                        , WEIGHTED bit NOT NULL, REG_PRICE decimal(19, 2) NOT NULL, BELOW bit NOT NULL, EP_START date NULL, EP_END date NULL
                        , PROVISIONAL bit NOT NULL, IN_RESET bit NOT NULL, REG_PREV decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_fin ON #zr_fin;
    CREATE TABLE #zr_chg (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, DATA date NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_chg ON #zr_chg;
    CREATE TABLE #zr_cmp (ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, DATA date NOT NULL, ACT char(1) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_cmp ON #zr_cmp;
    CREATE TABLE #zr_zone (ID_SHOP int NOT NULL PRIMARY KEY, ID_ZONE smallint NOT NULL);
    CREATE TABLE #zr_old (ID_COMPANY int NOT NULL, DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL
                        , ID_SHOP int NULL, ID_CITY int NULL, LINE_TYPE tinyint NOT NULL, ID_LINE bigint NOT NULL
                        , ROWS_ALL int NOT NULL, LINES int NOT NULL, CHECKS int NOT NULL, QTY decimal(18, 3) NULL
                        , COST_SHELF decimal(19, 2) NULL, RUN_ID int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_old ON #zr_old;
    CREATE TABLE #zr_key (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_LINE bigint NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_zr_key ON #zr_key;

    BEGIN TRY
        -- та же блокировка, что у disc_model_mart (из него — повторный захват в той же сессии, не ждёт)
        EXEC @lock = sp_getapplock @Resource = N'dbo.disc_model_mart', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
        IF @lock < 0
            THROW 50001, N'disc_model_zone_reg: идёт запуск витрины модели скидок — не начинаю.', 1;
        SET @locked = 1;

        INSERT INTO dbo.I_DISC_MODEL_LOG (STARTED, RUN_MODE, STATUS) VALUES (@t, 'ZONE_REG', 'RUN');
        SET @log = SCOPE_IDENTITY();

        INSERT INTO #zr_d WITH (TABLOCK) (ID_ZONE, ID_PRODUCT, DATA, PRICE, WEIGHTED)
        SELECT ID_ZONE, ID_PRODUCT, DATA, PRICE, WEIGHTED
        FROM dbo.I_DISC_MODEL_ZONE_DAY (nolock)
        WHERE ID_COMPANY = @idc
        OPTION (MAX_GRANT_PERCENT = 10);
        SET @maxd = (SELECT max(DATA) FROM #zr_d);

        -- 1. Отрезки и регулярная цена: по кругу, пока в каком-то ряду находится новое снижение регулярной цены
        WHILE @new > 0 AND @it < 30
        BEGIN
            SET @it += 1;
            TRUNCATE TABLE #zr_w;
            TRUNCATE TABLE #zr_new;

            -- ряды этого круга: в первом — все; дальше — только ряды с новым снижением в прошлом круге
            INSERT INTO #zr_w WITH (TABLOCK) (ID_ZONE, ID_PRODUCT, DATA, PRICE, WEIGHTED, SEG, R, BELOW, EG, LAST_D)
            SELECT c.ID_ZONE, c.ID_PRODUCT, c.DATA, c.PRICE, c.WEIGHTED, c.SEG, c.R, c.BELOW
                 , sum(1 - c.BELOW) OVER (PARTITION BY c.ID_ZONE, c.ID_PRODUCT, c.SEG ORDER BY c.DATA ROWS UNBOUNDED PRECEDING)
                 , c.LAST_D
            FROM (SELECT b.*, BELOW = CASE WHEN b.PRICE < b.R - 0.01 THEN 1 ELSE 0 END
                  FROM (SELECT a.*
                             , R = max(a.PRICE) OVER (PARTITION BY a.ID_ZONE, a.ID_PRODUCT, a.SEG ORDER BY a.DATA ROWS UNBOUNDED PRECEDING)
                        FROM (SELECT d.ID_ZONE, d.ID_PRODUCT, d.DATA, d.PRICE, d.WEIGHTED
                                   , SEG = isnull(max(CASE WHEN rs.R_FROM IS NOT NULL THEN d.DATA END)
                                                    OVER (PARTITION BY d.ID_ZONE, d.ID_PRODUCT ORDER BY d.DATA ROWS UNBOUNDED PRECEDING)
                                                 , CAST('19000101' AS date))
                                   , LAST_D = max(d.DATA) OVER (PARTITION BY d.ID_ZONE, d.ID_PRODUCT)
                              FROM #zr_d AS d
                                   LEFT JOIN #zr_rs AS rs ON rs.ID_ZONE = d.ID_ZONE AND rs.ID_PRODUCT = d.ID_PRODUCT AND rs.R_FROM = d.DATA
                              WHERE @it = 1
                                 OR EXISTS (SELECT 1 FROM #zr_act AS ac WHERE ac.ID_ZONE = d.ID_ZONE AND ac.ID_PRODUCT = d.ID_PRODUCT)) AS a) AS b) AS c
            OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);

            -- новое снижение ряда: самая ранняя акция дольше @reg_days, начавшаяся после последнего снижения ряда
            INSERT INTO #zr_new (ID_ZONE, ID_PRODUCT, R_FROM, R_TO, REG_PREV)
            SELECT y.ID_ZONE, y.ID_PRODUCT, y.F, y.L, y.RP
            FROM (SELECT e.*, RN = row_number() OVER (PARTITION BY e.ID_ZONE, e.ID_PRODUCT ORDER BY e.F)
                  FROM (SELECT w.ID_ZONE, w.ID_PRODUCT
                             , F = min(CASE WHEN w.BELOW = 1 THEN w.DATA END), L = max(CASE WHEN w.BELOW = 1 THEN w.DATA END)
                             , RP = max(CASE WHEN w.BELOW = 0 THEN w.R END)
                        FROM #zr_w AS w
                        GROUP BY w.ID_ZONE, w.ID_PRODUCT, w.SEG, w.EG
                        HAVING max(w.BELOW) = 1) AS e
                  WHERE datediff(day, e.F, e.L) + 1 > @reg_days
                    AND e.F > isnull((SELECT max(r.R_FROM) FROM #zr_rs AS r WHERE r.ID_ZONE = e.ID_ZONE AND r.ID_PRODUCT = e.ID_PRODUCT)
                                    , CAST('19000101' AS date))) AS y
            WHERE y.RN = 1
            OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);
            SET @new = @@ROWCOUNT;

            -- ряды без нового снижения — готовы: в итог с акциями и днями снижения
            INSERT INTO #zr_fin WITH (TABLOCK) (ID_ZONE, ID_PRODUCT, DATA, PRICE, WEIGHTED, REG_PRICE, BELOW, EP_START, EP_END, PROVISIONAL
                                              , IN_RESET, REG_PREV)
            SELECT g.ID_ZONE, g.ID_PRODUCT, g.DATA, g.PRICE, g.WEIGHTED, g.R, g.BELOW
                 , CASE WHEN g.BELOW = 1 THEN g.F END
                 -- акция идёт (EP_END NULL) — только если ниже регулярной до последнего дня ряда и ряд продаётся в последний день
                 -- данных; товар перестал продаваться, пока был ниже, — акция закончилась в последний день продажи
                 , CASE WHEN g.BELOW = 1 AND NOT (g.L = g.LAST_D AND g.LAST_D >= @maxd) THEN g.L END
                 , CASE WHEN g.BELOW = 1 AND g.L = g.LAST_D AND g.LAST_D >= @maxd AND datediff(day, g.F, g.L) + 1 <= @reg_days THEN 1 ELSE 0 END
                 , CASE WHEN rs.R_FROM IS NOT NULL THEN 1 ELSE 0 END, rs.REG_PREV
            FROM (SELECT w.*
                       , F = min(CASE WHEN w.BELOW = 1 THEN w.DATA END) OVER (PARTITION BY w.ID_ZONE, w.ID_PRODUCT, w.SEG, w.EG)
                       , L = max(CASE WHEN w.BELOW = 1 THEN w.DATA END) OVER (PARTITION BY w.ID_ZONE, w.ID_PRODUCT, w.SEG, w.EG)
                  FROM #zr_w AS w
                  WHERE NOT EXISTS (SELECT 1 FROM #zr_new AS n WHERE n.ID_ZONE = w.ID_ZONE AND n.ID_PRODUCT = w.ID_PRODUCT)
                     OR @it = 30) AS g
                 LEFT JOIN #zr_rs AS rs ON rs.ID_ZONE = g.ID_ZONE AND rs.ID_PRODUCT = g.ID_PRODUCT
                                       AND g.DATA >= rs.R_FROM AND g.DATA <= rs.R_TO   -- снижения ряда не налегают (см. ниже)
            OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);

            TRUNCATE TABLE #zr_act;
            INSERT INTO #zr_act (ID_ZONE, ID_PRODUCT) SELECT ID_ZONE, ID_PRODUCT FROM #zr_new;
            -- новое снижение налегает на прежнее (цена опустилась ещё раз внутри долгой акции) — прежнее обрезается: в днях
            -- нового «прежняя регулярная» — ближайшая, а не исходная
            UPDATE r SET r.R_TO = dateadd(day, -1, n.R_FROM)
            FROM #zr_rs AS r JOIN #zr_new AS n ON n.ID_ZONE = r.ID_ZONE AND n.ID_PRODUCT = r.ID_PRODUCT AND r.R_TO >= n.R_FROM;
            INSERT INTO #zr_rs (ID_ZONE, ID_PRODUCT, R_FROM, R_TO, REG_PREV) SELECT ID_ZONE, ID_PRODUCT, R_FROM, R_TO, REG_PREV FROM #zr_new;
            SET @n_reset += @new;
        END
        IF @new > 0
            RAISERROR(N'ВНИМАНИЕ: disc_model_zone_reg: после 30 кругов у части рядов есть ещё снижения регулярной цены — взяты как есть.', 10, 1) WITH NOWAIT;

        -- 2. Что изменилось против прошлой пересборки (по дню ряда). Порог скидки дня = регулярная, а в дни снижения — прежняя
        --    регулярная, если выше. Порог не вырос (только снизился или сменился статус) — строки скидок клиентов
        --    пересчитываются из хранимых количества и суммы. Порог вырос, сменился признак весового, дня ряда не было в
        --    прошлой пересборке или он из неё пропал — в витрине могут быть не все позиции со скидкой: такие дни скидок
        --    клиентов помечаются к пересчёту из чеков (STATUS 'ZONE' — следующий запуск пересчитает их, ночь — до 31 дня)
        INSERT INTO #zr_cmp WITH (TABLOCK) (ID_ZONE, ID_PRODUCT, DATA, ACT)
        SELECT isnull(n.ID_ZONE, o.ID_ZONE), isnull(n.ID_PRODUCT, o.ID_PRODUCT), isnull(n.DATA, o.DATA)
             , CASE WHEN n.DATA IS NULL OR o.DATA IS NULL OR n.WEIGHTED <> o.WEIGHTED
                         OR (CASE WHEN n.IN_RESET = 1 AND n.REG_PREV > n.REG_PRICE THEN n.REG_PREV ELSE n.REG_PRICE END)
                          > (CASE WHEN o.IN_RESET = 1 AND o.REG_PREV > o.REG_PRICE THEN o.REG_PREV ELSE o.REG_PRICE END)
                    THEN 'D' ELSE 'R' END
        FROM #zr_fin AS n
             FULL JOIN (SELECT ID_ZONE, ID_PRODUCT, DATA, REG_PRICE, IN_RESET, REG_PREV, WEIGHTED
                        FROM dbo.I_DISC_MODEL_ZONE_REG (nolock) WHERE ID_COMPANY = @idc) AS o
                    ON o.ID_ZONE = n.ID_ZONE AND o.ID_PRODUCT = n.ID_PRODUCT AND o.DATA = n.DATA
        WHERE n.DATA IS NULL OR o.DATA IS NULL
           OR n.REG_PRICE <> o.REG_PRICE OR n.IN_RESET <> o.IN_RESET OR n.WEIGHTED <> o.WEIGHTED
           OR isnull(n.REG_PREV, -1) <> isnull(o.REG_PREV, -1)
        OPTION (MAX_GRANT_PERCENT = 10);

        INSERT INTO #zr_chg WITH (TABLOCK) (ID_ZONE, ID_PRODUCT, DATA)
        SELECT ID_ZONE, ID_PRODUCT, DATA FROM #zr_cmp WHERE ACT = 'R';
        SET @n_chg = @@ROWCOUNT;
        SET @min_d = (SELECT min(DATA) FROM #zr_chg);

        INSERT INTO #zr_zone (ID_SHOP, ID_ZONE)
        SELECT x.ID_SHOP, coalesce(zs.ID_ZONE, zc.ID_ZONE, -1)
        FROM (SELECT s.ID_SHOP, s.ID_CITY, s.ID_ORGANIZATION
                   , rn = row_number() OVER (PARTITION BY s.ID_SHOP ORDER BY s.revision_date DESC, s.ID_CITY DESC)
              FROM dbo.I_SHOP AS s (nolock)
              WHERE s.ID_COMPANY = @idc AND s.ID_SHOP IS NOT NULL) AS x
             LEFT JOIN dbo.I_DISC_MODEL_ZONE_SHOP AS zs (nolock) ON zs.ID_COMPANY = @idc AND zs.ID_SHOP = x.ID_SHOP
             LEFT JOIN dbo.I_DISC_MODEL_ZONE_CITY AS zc (nolock) ON zc.ID_COMPANY = @idc AND zc.ID_CITY = x.ID_CITY
        WHERE x.rn = 1 AND x.ID_ORGANIZATION = 1;

        -- 3. Запись одной транзакцией: пересмотр строк скидок клиентов, окно по городам, замена регулярной цены
        BEGIN TRANSACTION;

        IF @n_chg > 0
        BEGIN
            DELETE c
            OUTPUT deleted.ID_COMPANY, deleted.DATA, deleted.ID_ORGANIZATION, deleted.ID_CONTACT, deleted.ID_SHOP, deleted.ID_CITY
                 , deleted.LINE_TYPE, deleted.ID_LINE, deleted.ROWS_ALL, deleted.LINES, deleted.CHECKS, deleted.QTY, deleted.COST_SHELF
                 , deleted.RUN_ID
            INTO #zr_old (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, LINE_TYPE, ID_LINE, ROWS_ALL, LINES, CHECKS
                        , QTY, COST_SHELF, RUN_ID)
            FROM dbo.I_DISC_MODEL_CLIENT_DISC AS c
                 JOIN #zr_zone AS z ON z.ID_SHOP = c.ID_SHOP
                 JOIN #zr_chg AS k ON k.ID_ZONE = z.ID_ZONE AND k.ID_PRODUCT = c.ID_LINE AND k.DATA = c.DATA
            WHERE c.ID_COMPANY = @idc AND c.LINE_TYPE IN (2, 3) AND c.DATA >= @min_d
            OPTION (MAX_GRANT_PERCENT = 10);
            SET @n_del = @@ROWCOUNT;
        END

        IF @n_del > 0
        BEGIN
            INSERT INTO dbo.I_DISC_MODEL_CLIENT_DISC WITH (TABLOCK)
                  (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, ROWS_ALL
                 , LINES, CHECKS, QTY, COST_SHELF, RUN_ID)
            SELECT o.ID_COMPANY, o.DATA, o.ID_ORGANIZATION, o.ID_CONTACT, o.ID_SHOP, o.ID_CITY, v.LT, o.ID_LINE
                 , CASE WHEN v.VAL > 0 THEN v.VAL ELSE 0 END, CASE WHEN v.VAL < 0 THEN v.VAL ELSE 0 END
                 , o.ROWS_ALL, o.LINES, o.CHECKS, o.QTY, o.COST_SHELF, o.RUN_ID
            FROM #zr_old AS o
                 JOIN #zr_zone AS z ON z.ID_SHOP = o.ID_SHOP
                 JOIN #zr_fin AS f ON f.ID_ZONE = z.ID_ZONE AND f.ID_PRODUCT = o.ID_LINE AND f.DATA = o.DATA
                 CROSS APPLY (SELECT U = o.COST_SHELF / NULLIF(o.QTY, 0)) AS u
                 CROSS APPLY (SELECT CMP = CASE WHEN f.WEIGHTED = 1 THEN round(u.U, 0) ELSE u.U END) AS m
                 CROSS APPLY (SELECT LT = CASE WHEN m.CMP < f.REG_PRICE - 0.01 THEN 2
                                               WHEN f.IN_RESET = 1 AND m.CMP < f.REG_PREV - 0.01 THEN 3 END) AS t
                 CROSS APPLY (SELECT LT = t.LT
                                   , VAL = CAST(CASE t.LT WHEN 2 THEN f.REG_PRICE * o.QTY - o.COST_SHELF
                                                          WHEN 3 THEN f.REG_PREV * o.QTY - o.COST_SHELF END AS decimal(19, 2))) AS v
            WHERE v.LT IS NOT NULL AND v.VAL <> 0
            OPTION (MAX_GRANT_PERCENT = 10);
            SET @n_ins = @@ROWCOUNT;

            INSERT INTO #zr_key WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_LINE)
            SELECT DISTINCT DATA, ID_ORGANIZATION, ID_LINE FROM #zr_old;

            DELETE v FROM dbo.I_DISC_MODEL_LINE_DAY AS v
            WHERE v.ID_COMPANY = @idc AND v.LINE_TYPE IN (2, 3) AND v.DATA >= @min_d
              AND EXISTS (SELECT 1 FROM #zr_key AS k WHERE k.DATA = v.DATA AND k.ID_ORGANIZATION = v.ID_ORGANIZATION AND k.ID_LINE = v.ID_LINE);

            INSERT INTO dbo.I_DISC_MODEL_LINE_DAY WITH (TABLOCK)
                  (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CITY, ID_ZONE, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, LINES, CHECKS, CLIENTS, SHOPS, RUN_ID)
            SELECT @idc, c.DATA, c.ID_ORGANIZATION, isnull(c.ID_CITY, -1), isnull(zn.ID_ZONE, -1), c.LINE_TYPE, c.ID_LINE
                 , sum(c.VALUE_POS), sum(c.VALUE_NEG), sum(c.LINES), sum(c.CHECKS), count(DISTINCT c.ID_CONTACT), count(DISTINCT c.ID_SHOP), @run_id
            FROM dbo.I_DISC_MODEL_CLIENT_DISC AS c (nolock)
                 LEFT JOIN #zr_zone AS zn ON zn.ID_SHOP = c.ID_SHOP
            WHERE c.ID_COMPANY = @idc AND c.LINE_TYPE IN (2, 3) AND c.DATA >= @min_d
              AND EXISTS (SELECT 1 FROM #zr_key AS k WHERE k.DATA = c.DATA AND k.ID_ORGANIZATION = c.ID_ORGANIZATION AND k.ID_LINE = c.ID_LINE)
            GROUP BY c.DATA, c.ID_ORGANIZATION, isnull(c.ID_CITY, -1), isnull(zn.ID_ZONE, -1), c.LINE_TYPE, c.ID_LINE
            OPTION (MAX_GRANT_PERCENT = 10);
            SET @n_ld = @@ROWCOUNT;
        END

        -- дни к пересчёту из чеков: посчитанные дни скидок клиентов → STATUS 'ZONE' (цены зон остаются)
        UPDATE t SET t.STATUS = 'ZONE'
        FROM dbo.I_DISC_MODEL_STATE AS t
        WHERE t.ID_COMPANY = @idc AND t.STATUS = 'OK'
          AND EXISTS (SELECT 1 FROM #zr_cmp AS c WHERE c.ACT = 'D' AND c.DATA = t.DATA);
        SET @n_reread = @@ROWCOUNT;

        -- порядок по дню — колоночный индекс отсекает группы строк при чтении дней порции
        TRUNCATE TABLE dbo.I_DISC_MODEL_ZONE_REG;   -- в таблице только ID_COMPANY = 1; в транзакции откатывается
        INSERT INTO dbo.I_DISC_MODEL_ZONE_REG WITH (TABLOCK)
              (ID_COMPANY, DATA, ID_ZONE, ID_PRODUCT, PRICE, WEIGHTED, REG_PRICE, BELOW, EP_START, EP_END, PROVISIONAL, IN_RESET, REG_PREV, BUILD_DT)
        SELECT @idc, DATA, ID_ZONE, ID_PRODUCT, PRICE, WEIGHTED, REG_PRICE, BELOW, EP_START, EP_END, PROVISIONAL, IN_RESET, REG_PREV, sysdatetime()
        FROM #zr_fin
        ORDER BY DATA
        OPTION (MAXDOP 1, MAX_GRANT_PERCENT = 10);

        COMMIT TRANSACTION;

        UPDATE dbo.I_DISC_MODEL_LOG SET FINISHED = sysdatetime(), STATUS = 'OK', REG_SEC = datediff(second, @t, sysdatetime())
        WHERE RUN_ID = @log;
        SET @msg = concat(N'disc_model_zone_reg: рядов зона × товар ', (SELECT count(*) FROM (SELECT DISTINCT ID_ZONE, ID_PRODUCT FROM #zr_d) AS x)
                        , N', дней ', (SELECT count_big(*) FROM #zr_fin), N'; снижений регулярной цены ', @n_reset, N' (кругов ', @it
                        , N'); изменилось дней ', @n_chg, N'; строк скидок клиентов пересчитано ', @n_del, N' → ', @n_ins
                        , N'; окно по городам — строк ', @n_ld, N'; дней скидок клиентов к пересчёту из чеков ', @n_reread
                        , N' — ', datediff(second, @t, sysdatetime()) / 60, N' мин.');
        RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

        EXEC sp_releaseapplock @Resource = N'dbo.disc_model_mart', @LockOwner = 'Session';
        SET @locked = 0;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = concat(N'disc_model_zone_reg: ошибка ', ERROR_NUMBER(), N': ', ERROR_MESSAGE(), N' Регулярная цена и строки витрины не изменены.');
        IF @log IS NOT NULL
            UPDATE dbo.I_DISC_MODEL_LOG SET FINISHED = sysdatetime(), STATUS = 'ERROR', MSG = left(@msg, 2000) WHERE RUN_ID = @log;
        IF @locked = 1
            EXEC sp_releaseapplock @Resource = N'dbo.disc_model_mart', @LockOwner = 'Session';
        THROW 50005, @msg, 1;
    END CATCH
END
GO

-- ============================== 3. ПРОЦЕДУРА dbo.disc_model_mart ==============================
-- Временные таблицы — #mc_*: процедура видит временные таблицы вызвавшей сессии, общие имена задели бы чужие.
CREATE OR ALTER PROCEDURE dbo.disc_model_mart
      @DATE_FROM date     = NULL   -- пересчёт периода: первый день; NULL — НОЧНОЙ режим
    , @DATE_TO   date     = NULL   -- пересчёт периода: последний день; NULL — вчера
    , @hist_days int      = 0      -- НОЧНОЙ: дней истории за запуск (от новых к старым); 0 — без истории, NULL — вся
    , @part_days int      = 7      -- порция чтения, дней
    , @stop_at   datetime = NULL   -- к этому времени запуск заканчивается: порция начинается, только если успеет (по самой
                                   --   долгой порции запуска); только время — сегодня
    , @max_days  int      = 31     -- НОЧНОЙ: дней окна и догона за ночь (после перезаливки первой пятницы ~90 дней
                                   --   досчитываются за несколько ночей, следующие шаги задания не ждут часами); NULL — все
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET IMPLICIT_TRANSACTIONS OFF;
    -- шаг 1 MCI чистит отметки дней I_DISC_MODEL_STATE: при взаимоблокировке жертвой будет витрина, порция откатится,
    -- следующий запуск досчитает
    SET DEADLOCK_PRIORITY LOW;
    -- метка установщика: CVM_DISC_MODEL

    DECLARE @idc       int    = 1;
    DECLARE @hist_from date   = dateadd(day, -105, CAST('20250901' AS date));   -- 19.05.2025: давность Оттока у скидок с 01.09.2025
    DECLARE @node      bigint = 4398046511104;                                  -- 2^42: ID_CHECK = номер чека + нода × 2^42
    DECLARE @today     date   = CAST(getdate() AS date);
    DECLARE @yesterday date   = dateadd(day, -1, @today);
    DECLARE @mode      varchar(10) = CASE WHEN @DATE_FROM IS NULL THEN 'NIGHT' ELSE 'RANGE' END;
    DECLARE @stop_dt   datetime2(0) = CASE WHEN @stop_at IS NULL THEN NULL
                                           WHEN CAST(@stop_at AS date) = '19000101'
                                                THEN dateadd(second, datediff(second, CAST('19000101' AS datetime), @stop_at), CAST(@today AS datetime2(0)))
                                           ELSE CAST(@stop_at AS datetime2(0)) END;

    DECLARE @run_id int, @t_run datetime2(0) = sysdatetime(), @t_part datetime2(0), @now datetime2(0);
    DECLARE @msg nvarchar(2000), @warn nvarchar(1000), @lock int, @locked bit = 0, @n int, @i int, @c_min bigint, @c_max bigint;
    DECLARE @last_done date, @night_from date, @gap_days int = 0, @parts int, @part int = 0, @p_from date, @p_to date;
    DECLARE @kind nvarchar(20), @days_done int = 0, @lines bigint, @rules bigint, @ins_cd int, @ins_disc int, @ins_ld int;
    DECLARE @kept nvarchar(400), @stopped bit = 0, @dup bigint, @cut int = 0, @min_ok date, @hole date;
    DECLARE @max_part_sec int = 0;   -- самая долгая порция (прошлого запуска, затем этого), с: порция начинается, только если успеет до @stop_at
    DECLARE @reg_sec int = 0, @t_reg datetime2(0);   -- пересборка реестра (прошлый запуск), с: тоже должна успеть
    DECLARE @zpart int = 0, @zparts int = 0, @max_zsec int = 0, @zreg_sec int = 0, @zleft int = 0;   -- проход цен зон

    CREATE TABLE #mc_num   (N int NOT NULL PRIMARY KEY);
    CREATE TABLE #mc_need  (DATA date NOT NULL PRIMARY KEY, PRIO tinyint NOT NULL, KIND nvarchar(20) COLLATE DATABASE_DEFAULT NOT NULL);
    CREATE TABLE #mc_parts (PART_NO int IDENTITY(1, 1) NOT NULL PRIMARY KEY, P_FROM date NOT NULL, P_TO date NOT NULL
                          , KIND nvarchar(20) COLLATE DATABASE_DEFAULT NOT NULL);
    CREATE TABLE #mc_tob   (ID_PRODUCT bigint NOT NULL PRIMARY KEY);
    -- большие временные таблицы — колоночные (как в витрине скидок): меньше tempdb, агрегаты в пакетном режиме
    CREATE TABLE #mc_line  (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_SHOP int NULL, ID_CITY int NULL, ID_CONTACT int NOT NULL
                          , ID_CHECK bigint NOT NULL, ID_CHEQUEITEM bigint NULL, TOB bit NOT NULL, ID_PRODUCT bigint NULL
                          , COST decimal(19, 2) NULL, COST_DISCOUNT decimal(19, 2) NULL, DISCOUNT decimal(19, 2) NULL
                          , CARD_DISCOUNT decimal(19, 2) NULL, REAL_COST decimal(19, 2) NULL, QUANTITY decimal(18, 3) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_line ON #mc_line;
    -- уникальность чека и позиции даёт GROUP BY при заполнении
    CREATE TABLE #mc_hdr   (ID_CHECK bigint NOT NULL, DATA date NOT NULL, ID_ORGANIZATION int NOT NULL
                          , ID_SHOP int NULL, ID_CITY int NULL, ID_CONTACT int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_hdr ON #mc_hdr;
    CREATE TABLE #mc_item  (ID_CHECK bigint NOT NULL, ID_CHEQUEITEM bigint NOT NULL, TOB bit NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_item ON #mc_item;
    CREATE TABLE #mc_rng   (RN int NOT NULL PRIMARY KEY, C_MIN bigint NOT NULL, C_MAX bigint NOT NULL);
    CREATE TABLE #mc_rule  (ID_CHECK bigint NOT NULL, ID_CHEQUEITEM bigint NULL, ID_CAMPAIGN int NULL, VAL decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_rule ON #mc_rule;
    CREATE TABLE #mc_rl    (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CITY int NULL, ID_SHOP int NULL, ID_CONTACT int NOT NULL
                          , ID_CHECK bigint NOT NULL, ID_CHEQUEITEM bigint NULL, LINE_TYPE tinyint NOT NULL, ID_LINE bigint NOT NULL
                          , VAL decimal(19, 2) NOT NULL, TOB bit NOT NULL, NO_ITEM bit NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_rl ON #mc_rl;
    -- чек: суммы и позиции без табака / табака
    CREATE TABLE #mc_ck    (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_SHOP int NULL, ID_CITY int NULL
                          , ID_CHECK bigint NOT NULL, NT_LINES int NOT NULL
                          , COST decimal(19, 2) NOT NULL, COST_DISCOUNT decimal(19, 2) NOT NULL, DISCOUNT decimal(19, 2) NOT NULL
                          , CARD_DISCOUNT decimal(19, 2) NOT NULL, REAL_COST decimal(19, 2) NOT NULL, COST_TOB decimal(19, 2) NOT NULL
                          , COST_DISCOUNT_TOB decimal(19, 2) NOT NULL, DISCOUNT_TOB decimal(19, 2) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_ck ON #mc_ck;
    CREATE TABLE #mc_cs    (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_SHOP int NULL, ID_CITY int NULL
                          , CHECKS int NOT NULL, CHECKS_ALL int NOT NULL, LINES int NOT NULL
                          , COST decimal(19, 2) NOT NULL, COST_DISCOUNT decimal(19, 2) NOT NULL, DISCOUNT decimal(19, 2) NOT NULL
                          , CARD_DISCOUNT decimal(19, 2) NOT NULL, REAL_COST decimal(19, 2) NOT NULL, COST_TOB decimal(19, 2) NOT NULL
                          , COST_DISCOUNT_TOB decimal(19, 2) NOT NULL, DISCOUNT_TOB decimal(19, 2) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_cs ON #mc_cs;
    -- готовые строки витрин порции (считаются до транзакции записи)
    CREATE TABLE #mc_o_cd  (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_SHOP int NULL, ID_CITY int NULL
                          , SHOPS int NOT NULL, CHECKS int NOT NULL, CHECKS_ALL int NOT NULL, LINES int NOT NULL
                          , COST decimal(19, 2) NOT NULL, COST_DISCOUNT decimal(19, 2) NOT NULL, DISCOUNT decimal(19, 2) NOT NULL
                          , CARD_DISCOUNT decimal(19, 2) NOT NULL, REAL_COST decimal(19, 2) NOT NULL, COST_TOB decimal(19, 2) NOT NULL
                          , COST_DISCOUNT_TOB decimal(19, 2) NOT NULL, DISCOUNT_TOB decimal(19, 2) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_o_cd ON #mc_o_cd;
    CREATE TABLE #mc_o_disc (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_SHOP int NULL, ID_CITY int NULL
                           , LINE_TYPE tinyint NOT NULL, ID_LINE bigint NOT NULL
                           , VALUE_POS decimal(19, 2) NOT NULL, VALUE_NEG decimal(19, 2) NOT NULL, ROWS_ALL int NOT NULL
                           , LINES int NOT NULL, CHECKS int NOT NULL, QTY decimal(18, 3) NULL, COST_SHELF decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_o_disc ON #mc_o_disc;
    CREATE TABLE #mc_zone  (ID_SHOP int NOT NULL PRIMARY KEY, ID_ZONE smallint NOT NULL);   -- магазин → ценовая зона (тысячи строк)
    CREATE TABLE #mc_o_ld  (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CITY int NOT NULL, ID_ZONE smallint NOT NULL
                          , LINE_TYPE tinyint NOT NULL, ID_LINE bigint NOT NULL
                          , VALUE_POS decimal(19, 2) NOT NULL, VALUE_NEG decimal(19, 2) NOT NULL, LINES int NOT NULL, CHECKS int NOT NULL
                          , CLIENTS int NOT NULL, SHOPS int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_o_ld ON #mc_o_ld;
    -- регулярная цена зон на дни порции (I_DISC_MODEL_ZONE_REG)
    CREATE TABLE #mc_reg   (DATA date NOT NULL, ID_ZONE smallint NOT NULL, ID_PRODUCT bigint NOT NULL, WEIGHTED bit NOT NULL
                          , REG_PRICE decimal(19, 2) NOT NULL, IN_RESET bit NOT NULL, REG_PREV decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_reg ON #mc_reg;
    -- дни и порции прохода цен зон
    CREATE TABLE #mc_zneed (DATA date NOT NULL PRIMARY KEY);
    CREATE TABLE #mc_zparts (PART_NO int IDENTITY(1, 1) NOT NULL PRIMARY KEY, P_FROM date NOT NULL, P_TO date NOT NULL);
    CREATE TABLE #mc_st    (DATA date NOT NULL PRIMARY KEY, STATUS varchar(10) COLLATE DATABASE_DEFAULT NOT NULL
                          , WAS_OK bit NOT NULL   -- день посчитан в витрине до этой порции (по I_DISC_MODEL_STATE)
                          , CHECK_LINES bigint NOT NULL, CLIENT_DAYS int NOT NULL, RULE_ROWS bigint NOT NULL, RULE_ROWS_TOB bigint NOT NULL
                          , VALUE_TOB decimal(19, 2) NOT NULL, RULE_NO_ITEM bigint NOT NULL, DUP_LINES bigint NOT NULL);

    IF @@TRANCOUNT > 0
        THROW 50001, N'disc_model_mart: вызвана внутри открытой транзакции — не начинаю (порции пишутся своими транзакциями).', 1;

    BEGIN TRY
        EXEC @lock = sp_getapplock @Resource = N'dbo.disc_model_mart', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
        IF @lock < 0
            THROW 50001, N'disc_model_mart: уже идёт другой запуск — этот не начат, ничего не записано.', 1;
        SET @locked = 1;

        IF @part_days IS NULL OR @part_days < 1 SET @part_days = 7;

        -- оценки времени из прошлых запусков: самая долгая порция и пересборка реестра
        SELECT TOP (1) @max_part_sec = g.MAX_PART_SEC FROM dbo.I_DISC_MODEL_LOG AS g (nolock)
        WHERE g.MAX_PART_SEC IS NOT NULL AND g.RUN_MODE IN ('NIGHT', 'RANGE') ORDER BY g.RUN_ID DESC;
        SELECT TOP (1) @reg_sec = g.REG_SEC FROM dbo.I_DISC_MODEL_LOG AS g (nolock)
        WHERE g.REG_SEC IS NOT NULL AND g.RUN_MODE IN ('NIGHT', 'RANGE') ORDER BY g.RUN_ID DESC;
        SELECT @max_part_sec = isnull(@max_part_sec, 0), @reg_sec = isnull(@reg_sec, 0);

        INSERT INTO #mc_num (N)
        SELECT TOP (3000) row_number() OVER (ORDER BY (SELECT NULL)) - 1
        FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;

        -- магазин → ценовая зона: список магазинов (I_DISC_MODEL_ZONE_SHOP), иначе область (I_DISC_MODEL_ZONE_CITY); только
        -- офлайн; вне зон — −2 (одиночки), вне справочников и онлайн — −1
        INSERT INTO #mc_zone (ID_SHOP, ID_ZONE)
        SELECT x.ID_SHOP, CASE WHEN x.ID_ORGANIZATION = 1 THEN coalesce(zs.ID_ZONE, zc.ID_ZONE, -1) ELSE -1 END
        FROM (SELECT s.ID_SHOP, s.ID_CITY, s.ID_ORGANIZATION
                   , rn = row_number() OVER (PARTITION BY s.ID_SHOP ORDER BY s.revision_date DESC, s.ID_CITY DESC)
              FROM dbo.I_SHOP AS s (nolock)
              WHERE s.ID_COMPANY = @idc AND s.ID_SHOP IS NOT NULL) AS x
             LEFT JOIN dbo.I_DISC_MODEL_ZONE_SHOP AS zs (nolock) ON zs.ID_COMPANY = @idc AND zs.ID_SHOP = x.ID_SHOP
             LEFT JOIN dbo.I_DISC_MODEL_ZONE_CITY AS zc (nolock) ON zc.ID_COMPANY = @idc AND zc.ID_CITY = x.ID_CITY
        WHERE x.rn = 1;

        -- табак: вне ТО и вне скидки (правило моделей); зажигалки 610903 — не табак
        INSERT INTO #mc_tob (ID_PRODUCT)
        SELECT DISTINCT CAST(p.ID_PRODUCT AS bigint)
        FROM dbo.I_PRODUCT AS p (nolock)
        WHERE p.ID_PRODUCT IS NOT NULL
          AND (   p.ID_CATEGORY_5_ext LIKE N'610901%' OR p.ID_CATEGORY_5_ext LIKE N'610902%'
               OR p.ID_CATEGORY_5_ext LIKE N'610904%' OR p.ID_CATEGORY_5_ext LIKE N'610905%'
               OR p.ID_CATEGORY_5_ext LIKE N'610906%' OR p.ID_CATEGORY_5_ext LIKE N'70030101%');

        -- ---------- какие дни считать ----------
        IF @mode = 'RANGE'
        BEGIN
            IF @DATE_TO IS NULL OR @DATE_TO > @yesterday SET @DATE_TO = @yesterday;
            INSERT INTO #mc_need (DATA, PRIO, KIND)
            SELECT dateadd(day, n.N, @DATE_FROM), 0, N'период'
            FROM #mc_num AS n
            WHERE n.N <= datediff(day, @DATE_FROM, @DATE_TO);
        END
        ELSE
        BEGIN
            SET @last_done = (SELECT max(s.DATA) FROM dbo.I_DISC_MODEL_STATE AS s (nolock) WHERE s.ID_COMPANY = @idc AND s.STATUS = 'OK');
            SET @night_from = dateadd(day, -10, @today);
            -- досчёт — с первой дыры выше самого раннего посчитанного дня (дыры истории — ниже него: история идёт от новых
            -- к старым); так ручной RANGE или раздел 4 по свежим дням не обрывает досчёт после перезаливки
            SET @min_ok = (SELECT min(s.DATA) FROM dbo.I_DISC_MODEL_STATE AS s (nolock) WHERE s.ID_COMPANY = @idc AND s.STATUS = 'OK');
            SET @hole = (SELECT min(dateadd(day, n.N, @hist_from))
                         FROM #mc_num AS n
                         WHERE n.N <= datediff(day, @hist_from, @yesterday)
                           AND dateadd(day, n.N, @hist_from) > @min_ok
                           AND NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
                                           WHERE s.ID_COMPANY = @idc AND s.DATA = dateadd(day, n.N, @hist_from)
                                             AND s.STATUS IN ('OK', 'NO_CHECKS')));
            IF @hole < @night_from SET @night_from = @hole;
            IF @night_from < @hist_from SET @night_from = @hist_from;
            INSERT INTO #mc_need (DATA, PRIO, KIND)
            SELECT dateadd(day, n.N, @night_from), 0, N'окно и новые дни'
            FROM #mc_num AS n
            WHERE n.N <= datediff(day, @night_from, @yesterday);
            -- больше @max_days — ночь берёт первые по дате, остальные досчитают следующие ночи (от последнего посчитанного дня)
            IF @max_days IS NOT NULL AND @max_days > 0
            BEGIN
                DELETE x FROM (SELECT DATA, RN = row_number() OVER (ORDER BY DATA) FROM #mc_need) AS x WHERE x.RN > @max_days;
                SET @cut = @@ROWCOUNT;
            END

            -- строки чистки в шаге 1 на месте? Без них перезалитые дни старше окна 10 дней не пересчитаются
            IF isnull(OBJECT_DEFINITION(OBJECT_ID(N'dbo.clear_data_update')), N'') NOT LIKE N'%CVM[_]DISC[_]MODEL%'
               OR isnull(OBJECT_DEFINITION(OBJECT_ID(N'dbo.reload_clear_data')), N'') NOT LIKE N'%CVM[_]DISC[_]MODEL%'
                SET @warn = left(concat(@warn + N' ', N'ВНИМАНИЕ: в clear_data_update или reload_clear_data нет строк чистки витрины модели скидок (раздел 5 CVM_DISC_MODEL.sql) — перезалитые дни старше 10 дней витрина не пересчитает.'), 1000);

            -- история: дни с 19.05.2025, где скидки клиентов не посчитаны (нет отметки или только цены зон), от новых к старым
            SELECT @gap_days = count(*)
            FROM #mc_num AS n
            WHERE n.N <= datediff(day, @hist_from, @yesterday)
              AND NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
                              WHERE s.ID_COMPANY = @idc AND s.DATA = dateadd(day, n.N, @hist_from) AND s.STATUS IN ('OK', 'NO_CHECKS'))
              AND NOT EXISTS (SELECT 1 FROM #mc_need AS e WHERE e.DATA = dateadd(day, n.N, @hist_from));

            IF @hist_days IS NULL OR @hist_days > 0
                INSERT INTO #mc_need (DATA, PRIO, KIND)
                SELECT TOP (isnull(@hist_days, 100000)) dateadd(day, n.N, @hist_from), 1, N'история'
                FROM #mc_num AS n
                WHERE n.N <= datediff(day, @hist_from, @yesterday)
                  AND NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
                                  WHERE s.ID_COMPANY = @idc AND s.DATA = dateadd(day, n.N, @hist_from) AND s.STATUS IN ('OK', 'NO_CHECKS'))
                  AND NOT EXISTS (SELECT 1 FROM #mc_need AS e WHERE e.DATA = dateadd(day, n.N, @hist_from))
                ORDER BY n.N DESC;
            ELSE IF @last_done IS NULL
                RAISERROR(N'ВНИМАНИЕ: disc_model_mart: в витрине нет посчитанных дней — история не посчитана (раздел 4 CVM_DISC_MODEL.sql); сейчас посчитано только окно 10 дней.', 10, 1) WITH NOWAIT;
        END

        -- порции: подряд идущие дни одного приоритета, не длиннее @part_days; сначала окно и новые дни, потом история
        -- от новых к старым
        INSERT INTO #mc_parts (P_FROM, P_TO, KIND)
        SELECT min(c.DATA), max(c.DATA), max(c.KIND)
        FROM (SELECT i.DATA, i.PRIO, i.KIND, i.ISL
                   , CH = (row_number() OVER (PARTITION BY i.PRIO, i.ISL ORDER BY i.DATA) - 1) / @part_days
              FROM (SELECT n.DATA, n.PRIO, n.KIND
                         , ISL = dateadd(day, -CAST(row_number() OVER (PARTITION BY n.PRIO ORDER BY n.DATA) AS int), n.DATA)
                    FROM #mc_need AS n) AS i) AS c
        GROUP BY c.PRIO, c.ISL, c.CH
        ORDER BY c.PRIO
               , CASE WHEN c.PRIO = 1 THEN -datediff(day, '19000101', max(c.DATA)) ELSE datediff(day, '19000101', min(c.DATA)) END;
        SET @parts = (SELECT count(*) FROM #mc_parts);

        INSERT INTO dbo.I_DISC_MODEL_LOG (STARTED, RUN_MODE, D_FROM, D_TO, DAYS_PLANNED, DAYS_DONE, STATUS)
        SELECT @t_run, @mode, min(DATA), max(DATA), count(*), 0, 'RUN' FROM #mc_need;
        SET @run_id = SCOPE_IDENTITY();

        SET @msg = CASE WHEN @mode = 'RANGE'
                        THEN concat(N'disc_model_mart: пересчёт ', convert(char(10), @DATE_FROM, 104), N'–', convert(char(10), @DATE_TO, 104))
                        ELSE concat(N'disc_model_mart: НОЧНОЙ — последний посчитанный день ', isnull(convert(char(10), @last_done, 104), N'нет')
                                  , N'; окно и новые дни: ', convert(char(10), @night_from, 104), N'–', convert(char(10), @yesterday, 104)
                                  , CASE WHEN @cut > 0 THEN concat(N' (взято ', @max_days, N' дней, ', @cut, N' — следующими ночами)') END
                                  , N'; непосчитанных дней истории: ', @gap_days, N', из них сейчас: '
                                  , (SELECT count(*) FROM #mc_need WHERE PRIO = 1)) END
                 + concat(N'; дней всего ', (SELECT count(*) FROM #mc_need), N', порций ', @parts
                        , CASE WHEN @stop_dt IS NOT NULL THEN concat(N', новые порции — до ', format(@stop_dt, N'dd.MM.yyyy HH:mm')) END
                        , N' (запуск ', @run_id, N').');
        RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
        IF @warn IS NOT NULL RAISERROR(N'%s', 10, 1, @warn) WITH NOWAIT;

        -- ---------- ЦЕНЫ ЗОН (решения Елены 20.10): сначала цены зон по дням, потом регулярная цена — скидки клиентов считаются
        --            от неё. Дни цен зон: дни скидок этого запуска + (ночь с историей) все дни истории без цен зон ----------
        INSERT INTO #mc_zneed (DATA) SELECT DATA FROM #mc_need;
        IF @mode = 'NIGHT' AND (@hist_days IS NULL OR @hist_days > 0)
            INSERT INTO #mc_zneed (DATA)
            SELECT dateadd(day, n.N, @hist_from)
            FROM #mc_num AS n
            WHERE n.N <= datediff(day, @hist_from, @yesterday)
              AND NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
                              WHERE s.ID_COMPANY = @idc AND s.DATA = dateadd(day, n.N, @hist_from) AND s.ZONE_CALC_DT IS NOT NULL)
              AND NOT EXISTS (SELECT 1 FROM #mc_zneed AS e WHERE e.DATA = dateadd(day, n.N, @hist_from));

        INSERT INTO #mc_zparts (P_FROM, P_TO)
        SELECT min(c.DATA), max(c.DATA)
        FROM (SELECT i.DATA, i.ISL, CH = (row_number() OVER (PARTITION BY i.ISL ORDER BY i.DATA) - 1) / @part_days
              FROM (SELECT z.DATA, ISL = dateadd(day, -CAST(row_number() OVER (ORDER BY z.DATA) AS int), z.DATA) FROM #mc_zneed AS z) AS i) AS c
        GROUP BY c.ISL, c.CH
        ORDER BY min(c.DATA);
        SET @zparts = (SELECT count(*) FROM #mc_zparts);

        SELECT TOP (1) @zreg_sec = g.REG_SEC FROM dbo.I_DISC_MODEL_LOG AS g (nolock)
        WHERE g.RUN_MODE = 'ZONE_REG' AND g.STATUS = 'OK' AND g.REG_SEC IS NOT NULL ORDER BY g.RUN_ID DESC;
        SELECT TOP (1) @max_zsec = g.ZONE_PART_SEC FROM dbo.I_DISC_MODEL_LOG AS g (nolock)
        WHERE g.ZONE_PART_SEC IS NOT NULL ORDER BY g.RUN_ID DESC;
        SELECT @zreg_sec = isnull(@zreg_sec, 0), @max_zsec = isnull(@max_zsec, 0);

        WHILE 1 = 1
        BEGIN
            SELECT TOP (1) @zpart = PART_NO, @p_from = P_FROM, @p_to = P_TO FROM #mc_zparts WHERE PART_NO > @zpart ORDER BY PART_NO;
            IF @@ROWCOUNT = 0 BREAK;
            IF @stop_dt IS NOT NULL AND dateadd(second, @max_zsec + @zreg_sec, sysdatetime()) >= @stop_dt
            BEGIN
                SET @stopped = 1;
                SET @msg = concat(N'disc_model_mart: цены зон — следующая порция не успеет до ', format(@stop_dt, N'dd.MM HH:mm')
                                , N'; осталось порций ', (SELECT count(*) FROM #mc_zparts WHERE PART_NO >= @zpart), N'. Продолжит следующий запуск.');
                RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
                BREAK;
            END
            SET @t_part = sysdatetime();
            EXEC dbo.disc_model_zone_day @d_from = @p_from, @d_to = @p_to, @run_id = @run_id;
            IF datediff(second, @t_part, sysdatetime()) > @max_zsec SET @max_zsec = datediff(second, @t_part, sysdatetime());
            SET @msg = concat(N'disc_model_mart: цены зон, порция ', @zpart, N' из ', @zparts, N' ', convert(char(10), @p_from, 104), N'–'
                            , convert(char(10), @p_to, 104), N' — ', datediff(second, @t_part, sysdatetime()), N' с.');
            RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
        END
        SELECT @p_from = NULL, @p_to = NULL;

        -- дни истории без цен зон остались (остановка по времени) — регулярную цену и скидки клиентов не считаю: регулярная
        -- цена задним числом зависит от всех дней
        SELECT @zleft = count(*)
        FROM #mc_num AS n
        WHERE n.N <= datediff(day, @hist_from, @yesterday)
          AND NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
                          WHERE s.ID_COMPANY = @idc AND s.DATA = dateadd(day, n.N, @hist_from) AND s.ZONE_CALC_DT IS NOT NULL);
        IF @stopped = 1 OR (@zleft > 0 AND @mode = 'NIGHT' AND (@hist_days IS NULL OR @hist_days > 0))
        BEGIN
            SET @stopped = 1;
            RAISERROR(N'disc_model_mart: цены зон посчитаны не за все дни истории (осталось %d) — регулярную цену и скидки клиентов посчитает следующий запуск.', 0, 1, @zleft) WITH NOWAIT;
            TRUNCATE TABLE #mc_parts;
        END
        ELSE
        BEGIN
            IF @zleft > 0
                RAISERROR(N'ВНИМАНИЕ: disc_model_mart: цен зон нет за %d дней истории (история не посчитана, раздел 4) — регулярная цена по неполной истории.', 10, 1, @zleft) WITH NOWAIT;
            IF @stop_dt IS NOT NULL AND dateadd(second, @zreg_sec, sysdatetime()) >= @stop_dt
            BEGIN
                SET @stopped = 1;
                RAISERROR(N'disc_model_mart: регулярная цена зон не успеет до времени остановки — скидки клиентов посчитает следующий запуск.', 0, 1) WITH NOWAIT;
                TRUNCATE TABLE #mc_parts;
            END
            ELSE
            BEGIN
                BEGIN TRY
                    EXEC dbo.disc_model_zone_reg @run_id = @run_id;
                END TRY
                BEGIN CATCH
                    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
                    SET @msg = concat(N'ВНИМАНИЕ: disc_model_mart: регулярная цена зон не пересобрана (', ERROR_MESSAGE(), N') — скидки клиентов в этот запуск не считаю, посчитает следующий.');
                    RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
                    SET @warn = left(concat(@warn + N' ', @msg), 1000);
                    SET @stopped = 1;
                    TRUNCATE TABLE #mc_parts;
                END CATCH
            END
        END

        -- ---------- порции ----------
        WHILE 1 = 1
        BEGIN
            SELECT TOP (1) @part = PART_NO, @p_from = P_FROM, @p_to = P_TO, @kind = KIND
            FROM #mc_parts
            WHERE PART_NO > @part
            ORDER BY PART_NO;
            IF @@ROWCOUNT = 0 BREAK;

            IF @stop_dt IS NOT NULL AND dateadd(second, @max_part_sec + @reg_sec, sysdatetime()) >= @stop_dt
            BEGIN
                SET @stopped = 1;
                SET @msg = concat(N'disc_model_mart: следующая порция не успеет до ', format(@stop_dt, N'dd.MM HH:mm')
                                , N' (самая долгая порция — ', @max_part_sec / 60, N' мин, реестр — ', @reg_sec / 60, N' мин) — новые порции не начинаю; осталось порций '
                                , (SELECT count(*) FROM #mc_parts WHERE PART_NO >= @part), N'. Продолжит следующий запуск.');
                RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
                BREAK;
            END

            SET @t_part = sysdatetime();
            SELECT @kept = NULL, @ins_cd = 0, @ins_disc = 0, @ins_ld = 0;
            TRUNCATE TABLE #mc_line; TRUNCATE TABLE #mc_hdr;  TRUNCATE TABLE #mc_item;   TRUNCATE TABLE #mc_rng;
            TRUNCATE TABLE #mc_rule; TRUNCATE TABLE #mc_rl;   TRUNCATE TABLE #mc_ck;     TRUNCATE TABLE #mc_cs;
            TRUNCATE TABLE #mc_o_cd; TRUNCATE TABLE #mc_o_disc; TRUNCATE TABLE #mc_o_ld; TRUNCATE TABLE #mc_st;

            -- 1. Строки чеков дней порции: клиенты > 0, каналы 1 и 3, признак табака
            INSERT INTO #mc_line WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_SHOP, ID_CITY, ID_CONTACT, ID_CHECK, ID_CHEQUEITEM, TOB
                                               , ID_PRODUCT, COST, COST_DISCOUNT, DISCOUNT, CARD_DISCOUNT, REAL_COST, QUANTITY)
            SELECT c.DATA, c.ID_ORGANIZATION, c.ID_SHOP, c.ID_CITY, c.ID_CONTACT, c.ID_CHECK, c.ID_CHEQUEITEM
                 , CASE WHEN t.ID_PRODUCT IS NOT NULL THEN 1 ELSE 0 END
                 , c.ID_PRODUCT, c.COST, c.COST_DISCOUNT, c.DISCOUNT, c.CARD_DISCOUNT, c.REAL_COST, c.QUANTITY
            FROM dbo.I_CHECK AS c (nolock)
                 LEFT JOIN #mc_tob AS t ON t.ID_PRODUCT = c.ID_PRODUCT
            WHERE c.ID_COMPANY = @idc
              AND c.DATA >= @p_from AND c.DATA <= @p_to
              AND c.ID_ORGANIZATION IN (1, 3)
              AND c.ID_CONTACT > 0
              AND c.ID_CHECK IS NOT NULL
            OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);
            SET @lines = @@ROWCOUNT;

            -- 2. Чеки (для списаний): день, канал, магазин, город, клиент
            INSERT INTO #mc_hdr WITH (TABLOCK) (ID_CHECK, DATA, ID_ORGANIZATION, ID_SHOP, ID_CITY, ID_CONTACT)
            SELECT l.ID_CHECK, min(l.DATA), min(l.ID_ORGANIZATION), min(l.ID_SHOP), min(l.ID_CITY), min(l.ID_CONTACT)
            FROM #mc_line AS l
            GROUP BY l.ID_CHECK
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 3. Списания по чекам порции: I_CHECK_RULE (даты нет) — только диапазонами ID_CHECK по нодам
            INSERT INTO #mc_rng (RN, C_MIN, C_MAX)
            SELECT row_number() OVER (ORDER BY min(ID_CHECK)), min(ID_CHECK), max(ID_CHECK)
            FROM #mc_hdr
            GROUP BY ID_CHECK / @node;

            SELECT @n = count(*) FROM #mc_rng;
            SET @i = 1;
            WHILE @i <= @n
            BEGIN
                SELECT @c_min = C_MIN, @c_max = C_MAX FROM #mc_rng WHERE RN = @i;

                INSERT INTO #mc_rule WITH (TABLOCK) (ID_CHECK, ID_CHEQUEITEM, ID_CAMPAIGN, VAL)
                SELECT r.ID_CHECK, r.ID_CHEQUEITEM, r.ID_CAMPAIGN, r.[VALUE]
                FROM dbo.I_CHECK_RULE AS r (nolock)
                WHERE r.ID_COMPANY = @idc
                  AND r.ID_CHECK BETWEEN @c_min AND @c_max
                  AND EXISTS (SELECT 1 FROM #mc_hdr AS h WHERE h.ID_CHECK = r.ID_CHECK)
                OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);

                SET @i += 1;
            END
            SET @rules = (SELECT count_big(*) FROM #mc_rule);

            -- 3а. Позиции со списаниями — табак или нет (позиция повторена в I_CHECK — склеивается: списание не задвоится)
            INSERT INTO #mc_item WITH (TABLOCK) (ID_CHECK, ID_CHEQUEITEM, TOB)
            SELECT l.ID_CHECK, l.ID_CHEQUEITEM, CAST(max(CAST(l.TOB AS tinyint)) AS bit)
            FROM #mc_line AS l
            WHERE l.ID_CHEQUEITEM IS NOT NULL
              AND EXISTS (SELECT 1 FROM #mc_rule AS r WHERE r.ID_CHECK = l.ID_CHECK AND r.ID_CHEQUEITEM = l.ID_CHEQUEITEM)
            GROUP BY l.ID_CHECK, l.ID_CHEQUEITEM
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 4. Списания с днём, клиентом, городом, магазином; табачная позиция — TOB = 1 (в витрину не идёт);
            --    позиция не нашлась в I_CHECK — NO_ITEM = 1 (входит); на чек целиком (без позиции) — входит
            INSERT INTO #mc_rl WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CITY, ID_SHOP, ID_CONTACT, ID_CHECK, ID_CHEQUEITEM, LINE_TYPE, ID_LINE
                                             , VAL, TOB, NO_ITEM)
            SELECT h.DATA, h.ID_ORGANIZATION, h.ID_CITY, h.ID_SHOP, h.ID_CONTACT, r.ID_CHECK, r.ID_CHEQUEITEM, 1, isnull(r.ID_CAMPAIGN, -1)
                 , isnull(r.VAL, 0)
                 , isnull(it.TOB, 0)
                 , CASE WHEN r.ID_CHEQUEITEM IS NOT NULL AND it.ID_CHECK IS NULL THEN 1 ELSE 0 END
            FROM #mc_rule AS r
                 JOIN #mc_hdr AS h ON h.ID_CHECK = r.ID_CHECK
                 LEFT JOIN #mc_item AS it ON it.ID_CHECK = r.ID_CHECK AND it.ID_CHEQUEITEM = r.ID_CHEQUEITEM
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 5. Чек → клиент × день × магазин (чек целиком в одном магазине): чеков = строк, без count(DISTINCT)
            INSERT INTO #mc_ck WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, ID_CHECK, NT_LINES
                                             , COST, COST_DISCOUNT, DISCOUNT, CARD_DISCOUNT, REAL_COST, COST_TOB, COST_DISCOUNT_TOB, DISCOUNT_TOB)
            SELECT l.DATA, l.ID_ORGANIZATION, l.ID_CONTACT, l.ID_SHOP, min(l.ID_CITY), l.ID_CHECK
                 , sum(CASE WHEN l.TOB = 0 THEN 1 ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 0 THEN isnull(l.COST, 0)          ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 0 THEN isnull(l.COST_DISCOUNT, 0) ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 0 THEN isnull(l.DISCOUNT, 0)      ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 0 THEN isnull(l.CARD_DISCOUNT, 0) ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 0 THEN isnull(l.REAL_COST, 0)     ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 1 THEN isnull(l.COST, 0)          ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 1 THEN isnull(l.COST_DISCOUNT, 0) ELSE 0 END)
                 , sum(CASE WHEN l.TOB = 1 THEN isnull(l.DISCOUNT, 0)      ELSE 0 END)
            FROM #mc_line AS l
            GROUP BY l.DATA, l.ID_ORGANIZATION, l.ID_CONTACT, l.ID_SHOP, l.ID_CHECK
            OPTION (MAX_GRANT_PERCENT = 10);

            INSERT INTO #mc_cs WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, CHECKS, CHECKS_ALL, LINES
                                             , COST, COST_DISCOUNT, DISCOUNT, CARD_DISCOUNT, REAL_COST, COST_TOB, COST_DISCOUNT_TOB, DISCOUNT_TOB)
            SELECT k.DATA, k.ID_ORGANIZATION, k.ID_CONTACT, k.ID_SHOP, min(k.ID_CITY)
                 , sum(CASE WHEN k.NT_LINES > 0 THEN 1 ELSE 0 END), count(*), sum(k.NT_LINES)
                 , sum(k.COST), sum(k.COST_DISCOUNT), sum(k.DISCOUNT), sum(k.CARD_DISCOUNT), sum(k.REAL_COST)
                 , sum(k.COST_TOB), sum(k.COST_DISCOUNT_TOB), sum(k.DISCOUNT_TOB)
            FROM #mc_ck AS k
            GROUP BY k.DATA, k.ID_ORGANIZATION, k.ID_CONTACT, k.ID_SHOP
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 6. Готовые строки витрин (до транзакции записи)
            -- клиент × день: суммы по магазинам; основной магазин — большая сумма без табака, затем больше чеков, затем меньший номер
            INSERT INTO #mc_o_cd WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, SHOPS, CHECKS, CHECKS_ALL, LINES
                                               , COST, COST_DISCOUNT, DISCOUNT, CARD_DISCOUNT, REAL_COST, COST_TOB, COST_DISCOUNT_TOB, DISCOUNT_TOB)
            SELECT x.DATA, x.ID_ORGANIZATION, x.ID_CONTACT
                 , max(CASE WHEN x.RN = 1 THEN x.ID_SHOP END), max(CASE WHEN x.RN = 1 THEN x.ID_CITY END)
                 , count(*), sum(x.CHECKS), sum(x.CHECKS_ALL), sum(x.LINES)
                 , sum(x.COST), sum(x.COST_DISCOUNT), sum(x.DISCOUNT), sum(x.CARD_DISCOUNT), sum(x.REAL_COST)
                 , sum(x.COST_TOB), sum(x.COST_DISCOUNT_TOB), sum(x.DISCOUNT_TOB)
            FROM (SELECT c.*, RN = row_number() OVER (PARTITION BY c.DATA, c.ID_ORGANIZATION, c.ID_CONTACT
                                                      ORDER BY c.COST_DISCOUNT DESC, c.CHECKS_ALL DESC, c.ID_SHOP)
                  FROM #mc_cs AS c) AS x
            GROUP BY x.DATA, x.ID_ORGANIZATION, x.ID_CONTACT
            OPTION (MAX_GRANT_PERCENT = 10);

            -- клиент × день × кампания: только ненулевые, без табачных позиций
            INSERT INTO #mc_o_disc WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, ROWS_ALL, LINES, CHECKS)
            SELECT r.DATA, r.ID_ORGANIZATION, r.ID_CONTACT, r.ID_SHOP, r.ID_CITY, r.LINE_TYPE, r.ID_LINE
                 , sum(CASE WHEN r.VAL > 0 THEN r.VAL ELSE 0 END), sum(CASE WHEN r.VAL < 0 THEN r.VAL ELSE 0 END)
                 , count(*)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_CHEQUEITEM END)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_CHECK END)
            FROM #mc_rl AS r
            WHERE r.TOB = 0
            GROUP BY r.DATA, r.ID_ORGANIZATION, r.ID_CONTACT, r.ID_SHOP, r.ID_CITY, r.LINE_TYPE, r.ID_LINE
            HAVING sum(abs(r.VAL)) > 0
            OPTION (MAX_GRANT_PERCENT = 10);

            -- окно скидки: день × канал × город × кампания
            INSERT INTO #mc_o_ld WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CITY, ID_ZONE, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, LINES, CHECKS, CLIENTS, SHOPS)
            SELECT r.DATA, r.ID_ORGANIZATION, isnull(r.ID_CITY, -1), isnull(zn.ID_ZONE, -1), r.LINE_TYPE, r.ID_LINE
                 , sum(CASE WHEN r.VAL > 0 THEN r.VAL ELSE 0 END), sum(CASE WHEN r.VAL < 0 THEN r.VAL ELSE 0 END)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_CHEQUEITEM END)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_CHECK END)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_CONTACT END)
                 , count(DISTINCT CASE WHEN r.VAL <> 0 THEN r.ID_SHOP END)
            FROM #mc_rl AS r
                 LEFT JOIN #mc_zone AS zn ON zn.ID_SHOP = r.ID_SHOP
            WHERE r.TOB = 0
            GROUP BY r.DATA, r.ID_ORGANIZATION, isnull(r.ID_CITY, -1), isnull(zn.ID_ZONE, -1), r.LINE_TYPE, r.ID_LINE
            HAVING sum(abs(r.VAL)) > 0
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 6а. Товарная скидка (решения Елены 20.10): цена продажи строки (COST / QUANTITY) ниже регулярной цены зоны магазина
            --     на этот день (I_DISC_MODEL_ZONE_REG) → скидка = регулярная цена зоны × количество − сумма по цене продажи,
            --     строка LINE_TYPE 2. В дни снижения регулярной цены (IN_RESET) цена ниже прежней, но не ниже новой регулярной —
            --     не скидка: LINE_TYPE 3 для сверки, сумма — от прежней регулярной. Весовой товар — цена продажи до рубля, как у
            --     зоны. Копейки не скидка: ниже больше чем на 0,01 ₽ за единицу. Возврат — с минусом (количество < 0).
            --     Только офлайн-магазины зон 1–7; одиночки вне зон и онлайн — без товарной скидки. Табак не входит.
            --     Кассовая скидка и списания кампаний — в строках кампаний, здесь не задваиваются: сумма — от цены продажи
            TRUNCATE TABLE #mc_reg;
            INSERT INTO #mc_reg WITH (TABLOCK) (DATA, ID_ZONE, ID_PRODUCT, WEIGHTED, REG_PRICE, IN_RESET, REG_PREV)
            SELECT g.DATA, g.ID_ZONE, g.ID_PRODUCT, g.WEIGHTED, g.REG_PRICE, g.IN_RESET, g.REG_PREV
            FROM dbo.I_DISC_MODEL_ZONE_REG AS g (nolock)
            WHERE g.ID_COMPANY = @idc AND g.DATA >= @p_from AND g.DATA <= @p_to
            OPTION (MAX_GRANT_PERCENT = 10);

            INSERT INTO #mc_o_disc WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG
                                                 , ROWS_ALL, LINES, CHECKS, QTY, COST_SHELF)
            -- строка клиента — ещё и по знаку количества и цене продажи: её сумма и количество тогда дают ту же скидку при
            -- пересчёте задним числом (dbo.disc_model_zone_reg), что и расчёт по позициям
            SELECT l.DATA, l.ID_ORGANIZATION, l.ID_CONTACT, l.ID_SHOP, l.ID_CITY, l.LT, l.ID_PRODUCT
                 , sum(CASE WHEN l.VAL > 0 THEN l.VAL ELSE 0 END), sum(CASE WHEN l.VAL < 0 THEN l.VAL ELSE 0 END)
                 , count(*), count(DISTINCT l.ID_CHEQUEITEM), count(DISTINCT l.CK), sum(l.QUANTITY), sum(l.COST)
            FROM (SELECT x.*, v.LT, m.CMP, SG = sign(x.QUANTITY)
                       -- чек — один раз на клиента × товар × тип строки (строка делится по знаку и цене, чеки потом складываются)
                       , CK = CASE WHEN row_number() OVER (PARTITION BY x.ID_CHECK, x.ID_PRODUCT, v.LT ORDER BY sign(x.QUANTITY) DESC, m.CMP) = 1
                                   THEN x.ID_CHECK END
                       , VAL = CASE v.LT WHEN 2 THEN g.REG_PRICE * x.QUANTITY - x.COST WHEN 3 THEN g.REG_PREV * x.QUANTITY - x.COST END
                  FROM #mc_line AS x
                       JOIN #mc_zone AS zn ON zn.ID_SHOP = x.ID_SHOP AND zn.ID_ZONE > 0
                       JOIN #mc_reg AS g ON g.DATA = x.DATA AND g.ID_ZONE = zn.ID_ZONE AND g.ID_PRODUCT = x.ID_PRODUCT
                       CROSS APPLY (SELECT U = x.COST / NULLIF(x.QUANTITY, 0)) AS u
                       CROSS APPLY (SELECT CMP = CASE WHEN g.WEIGHTED = 1 THEN round(u.U, 0) ELSE u.U END) AS m
                       CROSS APPLY (SELECT LT = CASE WHEN m.CMP < g.REG_PRICE - 0.01 THEN 2
                                                     WHEN g.IN_RESET = 1 AND m.CMP < g.REG_PREV - 0.01 THEN 3 END) AS v
                  WHERE x.TOB = 0 AND x.ID_ORGANIZATION = 1 AND x.ID_PRODUCT IS NOT NULL
                    AND x.QUANTITY <> 0 AND x.COST <> 0 AND sign(x.COST) = sign(x.QUANTITY)   -- ошибки чека (разные знаки) — не скидка
                    AND v.LT IS NOT NULL) AS l
            GROUP BY l.DATA, l.ID_ORGANIZATION, l.ID_CONTACT, l.ID_SHOP, l.ID_CITY, l.LT, l.ID_PRODUCT, l.SG, l.CMP
            HAVING sum(abs(l.VAL)) > 0
            OPTION (MAX_GRANT_PERCENT = 10);

            -- окно скидки на товар по городам — из строк клиентов (так же собирает пересмотр в disc_model_zone_reg): чек одного
            -- клиента и засчитан в одной строке клиента × товара × типа, поэтому чеки складываются без повторов
            INSERT INTO #mc_o_ld WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CITY, ID_ZONE, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, LINES, CHECKS, CLIENTS, SHOPS)
            SELECT o.DATA, o.ID_ORGANIZATION, isnull(o.ID_CITY, -1), isnull(zn.ID_ZONE, -1), o.LINE_TYPE, o.ID_LINE
                 , sum(o.VALUE_POS), sum(o.VALUE_NEG), sum(o.LINES), sum(o.CHECKS), count(DISTINCT o.ID_CONTACT), count(DISTINCT o.ID_SHOP)
            FROM #mc_o_disc AS o
                 LEFT JOIN #mc_zone AS zn ON zn.ID_SHOP = o.ID_SHOP
            WHERE o.LINE_TYPE IN (2, 3)
            GROUP BY o.DATA, o.ID_ORGANIZATION, isnull(o.ID_CITY, -1), isnull(zn.ID_ZONE, -1), o.LINE_TYPE, o.ID_LINE
            OPTION (MAX_GRANT_PERCENT = 10);

            -- 7. Состояние дней порции: строк чеков нет — NO_CHECKS; WAS_OK — день был посчитан до этой порции
            INSERT INTO #mc_st (DATA, STATUS, WAS_OK, CHECK_LINES, CLIENT_DAYS, RULE_ROWS, RULE_ROWS_TOB, VALUE_TOB, RULE_NO_ITEM, DUP_LINES)
            SELECT d.DATA
                 , CASE WHEN isnull(l.N, 0) > 0 THEN 'OK' ELSE 'NO_CHECKS' END
                 , CASE WHEN EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS t (nolock)
                                     WHERE t.ID_COMPANY = @idc AND t.DATA = d.DATA
                                       AND (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0))) THEN 1 ELSE 0 END
                 , isnull(l.N, 0), isnull(c.N, 0), isnull(r.N, 0), isnull(r.NT, 0), isnull(r.VT, 0), isnull(r.NI, 0)
                 , isnull(l.NP, 0) - isnull(l.UP, 0)
            FROM #mc_need AS d
                 LEFT JOIN (SELECT DATA, N = sum(K), NP = sum(CASE WHEN ID_CHEQUEITEM IS NOT NULL THEN K ELSE 0 END)
                                 , UP = sum(CASE WHEN ID_CHEQUEITEM IS NOT NULL THEN 1 ELSE 0 END)
                            FROM (SELECT DATA, ID_CHECK, ID_CHEQUEITEM, K = count_big(*)
                                  FROM #mc_line GROUP BY DATA, ID_CHECK, ID_CHEQUEITEM) AS q
                            GROUP BY DATA) AS l ON l.DATA = d.DATA
                 LEFT JOIN (SELECT DATA, N = count(*) FROM #mc_o_cd GROUP BY DATA) AS c ON c.DATA = d.DATA
                 LEFT JOIN (SELECT DATA, N = count_big(*)
                                 , NT = sum(CASE WHEN TOB = 1 THEN 1 ELSE 0 END)
                                 , VT = sum(CASE WHEN TOB = 1 THEN abs(VAL) ELSE 0 END)
                                 , NI = sum(CASE WHEN NO_ITEM = 1 THEN 1 ELSE 0 END)
                            FROM #mc_rl WHERE LINE_TYPE = 1 GROUP BY DATA) AS r ON r.DATA = d.DATA
            WHERE d.DATA >= @p_from AND d.DATA <= @p_to
            OPTION (MAX_GRANT_PERCENT = 10);

            SELECT @kept = left(STRING_AGG(CAST(convert(char(10), s.DATA, 104) AS nvarchar(max)), N', '), 400)
            FROM #mc_st AS s
            WHERE s.STATUS = 'NO_CHECKS' AND s.WAS_OK = 1;
            SET @dup = (SELECT sum(DUP_LINES) FROM #mc_st);

            -- 8. Запись порции одной транзакцией. Строки дня стираются, если день посчитан сейчас (OK) или не был посчитан
            --    раньше (WAS_OK = 0: шаг 1 MCI при перезаливке стирает только отметки дней в I_DISC_MODEL_STATE, старые строки
            --    перезалитого дня убираются здесь); день без чеков, посчитанный раньше, остаётся как был
            SET @now = sysdatetime();
            BEGIN TRANSACTION;

            DELETE v FROM dbo.I_DISC_MODEL_CLIENT_DAY AS v
            WHERE v.ID_COMPANY = @idc AND v.DATA >= @p_from AND v.DATA <= @p_to
              AND EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = v.DATA AND (s.STATUS = 'OK' OR s.WAS_OK = 0));
            DELETE v FROM dbo.I_DISC_MODEL_CLIENT_DISC AS v
            WHERE v.ID_COMPANY = @idc AND v.DATA >= @p_from AND v.DATA <= @p_to
              AND EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = v.DATA AND (s.STATUS = 'OK' OR s.WAS_OK = 0));
            DELETE v FROM dbo.I_DISC_MODEL_LINE_DAY AS v
            WHERE v.ID_COMPANY = @idc AND v.DATA >= @p_from AND v.DATA <= @p_to
              AND EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = v.DATA AND (s.STATUS = 'OK' OR s.WAS_OK = 0));

            INSERT INTO dbo.I_DISC_MODEL_CLIENT_DAY WITH (TABLOCK)
                  (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, SHOPS, CHECKS, CHECKS_ALL, LINES
                 , COST, COST_DISCOUNT, DISCOUNT, CARD_DISCOUNT, REAL_COST, COST_TOB, COST_DISCOUNT_TOB, DISCOUNT_TOB, RUN_ID)
            SELECT @idc, o.DATA, o.ID_ORGANIZATION, o.ID_CONTACT, o.ID_SHOP, o.ID_CITY, o.SHOPS, o.CHECKS, o.CHECKS_ALL, o.LINES
                 , o.COST, o.COST_DISCOUNT, o.DISCOUNT, o.CARD_DISCOUNT, o.REAL_COST, o.COST_TOB, o.COST_DISCOUNT_TOB, o.DISCOUNT_TOB, @run_id
            FROM #mc_o_cd AS o
            WHERE EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = o.DATA AND s.STATUS = 'OK');
            SET @ins_cd = @@ROWCOUNT;

            INSERT INTO dbo.I_DISC_MODEL_CLIENT_DISC WITH (TABLOCK)
                  (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CONTACT, ID_SHOP, ID_CITY, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, ROWS_ALL, LINES, CHECKS
                 , QTY, COST_SHELF, RUN_ID)
            SELECT @idc, o.DATA, o.ID_ORGANIZATION, o.ID_CONTACT, o.ID_SHOP, o.ID_CITY, o.LINE_TYPE, o.ID_LINE, o.VALUE_POS, o.VALUE_NEG, o.ROWS_ALL, o.LINES, o.CHECKS
                 , o.QTY, o.COST_SHELF, @run_id
            FROM #mc_o_disc AS o
            WHERE EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = o.DATA AND s.STATUS = 'OK');
            SET @ins_disc = @@ROWCOUNT;

            INSERT INTO dbo.I_DISC_MODEL_LINE_DAY WITH (TABLOCK)
                  (ID_COMPANY, DATA, ID_ORGANIZATION, ID_CITY, ID_ZONE, LINE_TYPE, ID_LINE, VALUE_POS, VALUE_NEG, LINES, CHECKS, CLIENTS, SHOPS, RUN_ID)
            SELECT @idc, o.DATA, o.ID_ORGANIZATION, o.ID_CITY, o.ID_ZONE, o.LINE_TYPE, o.ID_LINE, o.VALUE_POS, o.VALUE_NEG, o.LINES, o.CHECKS, o.CLIENTS, o.SHOPS, @run_id
            FROM #mc_o_ld AS o
            WHERE EXISTS (SELECT 1 FROM #mc_st AS s WHERE s.DATA = o.DATA AND s.STATUS = 'OK');
            SET @ins_ld = @@ROWCOUNT;

            MERGE dbo.I_DISC_MODEL_STATE AS t
            USING #mc_st AS s
               ON t.ID_COMPANY = @idc AND t.DATA = s.DATA
            -- посчитанный день (OK) переписывается; день без чеков — только если раньше не был посчитан; LAST_TRY — у всех
            WHEN MATCHED THEN
                UPDATE SET t.STATUS        = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.STATUS        ELSE t.STATUS        END
                         , t.CHECK_LINES   = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.CHECK_LINES   ELSE t.CHECK_LINES   END
                         , t.CLIENT_DAYS   = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.CLIENT_DAYS   ELSE t.CLIENT_DAYS   END
                         , t.RULE_ROWS     = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.RULE_ROWS     ELSE t.RULE_ROWS     END
                         , t.RULE_ROWS_TOB = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.RULE_ROWS_TOB ELSE t.RULE_ROWS_TOB END
                         , t.VALUE_TOB     = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.VALUE_TOB     ELSE t.VALUE_TOB     END
                         , t.RULE_NO_ITEM  = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.RULE_NO_ITEM  ELSE t.RULE_NO_ITEM  END
                         , t.DUP_LINES     = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN s.DUP_LINES     ELSE t.DUP_LINES     END
                         , t.RUN_ID        = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN @run_id         ELSE t.RUN_ID        END
                         , t.CALC_DT       = CASE WHEN s.STATUS = 'OK' OR NOT (t.STATUS = 'OK' OR (t.STATUS = 'ZONE' AND t.CLIENT_DAYS > 0)) THEN @now            ELSE t.CALC_DT       END
                         , t.LAST_TRY      = @now
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (ID_COMPANY, DATA, STATUS, CHECK_LINES, CLIENT_DAYS, RULE_ROWS, RULE_ROWS_TOB, VALUE_TOB, RULE_NO_ITEM, DUP_LINES
                      , RUN_ID, CALC_DT, LAST_TRY)
                VALUES (@idc, s.DATA, s.STATUS, s.CHECK_LINES, s.CLIENT_DAYS, s.RULE_ROWS, s.RULE_ROWS_TOB, s.VALUE_TOB, s.RULE_NO_ITEM
                      , s.DUP_LINES, @run_id, @now, @now);

            COMMIT TRANSACTION;

            SET @days_done += (SELECT count(*) FROM #mc_st);
            UPDATE dbo.I_DISC_MODEL_LOG SET DAYS_DONE = @days_done WHERE RUN_ID = @run_id;

            SET @msg = concat(N'disc_model_mart: порция ', @part, N' из ', @parts, N' (', @kind, N') '
                            , convert(char(10), @p_from, 104), N'–', convert(char(10), @p_to, 104)
                            , N': строк чеков ', @lines, N', строк списаний ', @rules
                            , N'; записано: клиенто-дней ', @ins_cd, N', клиент × скидка ', @ins_disc, N', окно по городам ', @ins_ld
                            , N' — ', datediff(second, @t_part, sysdatetime()), N' с.');
            IF datediff(second, @t_part, sysdatetime()) > @max_part_sec SET @max_part_sec = datediff(second, @t_part, sysdatetime());
            RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
            IF @dup > 0
            BEGIN
                SET @msg = concat(N'ВНИМАНИЕ: disc_model_mart: за ', convert(char(10), @p_from, 104), N'–', convert(char(10), @p_to, 104)
                                , N' в I_CHECK повторены позиции чека: ', @dup, N' строк — списания кампаний склеены, ТО и товарная скидка взяты как в источнике (I_DISC_MODEL_STATE.DUP_LINES).');
                RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
            END
            IF @kept IS NOT NULL
                RAISERROR(N'ВНИМАНИЕ: disc_model_mart: за %s строк чеков сейчас нет, а день уже посчитан — оставлен как был (идёт перезаливка или сбой загрузки).', 10, 1, @kept) WITH NOWAIT;
        END

        SELECT @p_from = NULL, @p_to = NULL;   -- дальше ошибка — уже не порции

        -- ---------- реестр всех скидок: строка скидки × канал (и оба канала), целиком и по месяцам ----------
        IF @days_done > 0 OR NOT EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_LINE (nolock))
        BEGIN
            SET @t_reg = sysdatetime();
            -- названия товаров — один раз
            SELECT ID_PRODUCT = CAST(p.ID_PRODUCT AS bigint), PRODUCT_NAME = max(p.PRODUCT_NAME)
            INTO #mc_pname
            FROM dbo.I_PRODUCT AS p (nolock)
            WHERE p.ID_PRODUCT IS NOT NULL
            GROUP BY CAST(p.ID_PRODUCT AS bigint);
            CREATE UNIQUE CLUSTERED INDEX UX_mc_pname ON #mc_pname (ID_PRODUCT);

            -- посчитанные дни окна скидок — один раз (строки дня, ждущего досчёта после перезаливки, в реестр не идут)
            SELECT v.DATA, YM = year(v.DATA) * 100 + month(v.DATA), v.ID_ORGANIZATION, v.ID_ZONE, v.ID_CITY, v.LINE_TYPE, v.ID_LINE
                 , v.VALUE_POS, v.VALUE_NEG, v.CHECKS
            INTO #mc_ldr
            FROM dbo.I_DISC_MODEL_LINE_DAY AS v (nolock)
            WHERE v.ID_COMPANY = @idc
              AND EXISTS (SELECT 1 FROM dbo.I_DISC_MODEL_STATE AS t (nolock)
                          WHERE t.ID_COMPANY = @idc AND t.DATA = v.DATA AND t.STATUS = 'OK')
            OPTION (MAX_GRANT_PERCENT = 10);
            CREATE CLUSTERED COLUMNSTORE INDEX CCI_mc_ldr ON #mc_ldr;

            SET @now = sysdatetime();
            BEGIN TRANSACTION;

            DELETE FROM dbo.I_DISC_MODEL_LINE WHERE ID_COMPANY = @idc;
            INSERT INTO dbo.I_DISC_MODEL_LINE (ID_COMPANY, LINE_TYPE, ID_LINE, ID_ORGANIZATION, ID_ZONE, LINE_NAME, ID_PROMO, PROMOS
                                             , DATE_FROM, DATE_TO, DAYS, MONTHS, CITIES, VALUE_POS, VALUE_NEG, CHECKS, CALC_DT)
            SELECT @idc, d.LINE_TYPE, d.ID_LINE, d.ORG, d.ZONE, left(coalesce(cn.CAMPAIGN_NAME, pn.PRODUCT_NAME), 255), pl.ID_PROMO, isnull(pl.PROMOS, 0)
                 , d.DATE_FROM, d.DATE_TO, d.DAYS, d.MONTHS, d.CITIES, d.VALUE_POS, d.VALUE_NEG, d.CHECKS, @now
            FROM (SELECT v.LINE_TYPE, v.ID_LINE
                       , ORG  = CASE WHEN GROUPING(v.ID_ORGANIZATION) = 1 THEN 0 ELSE v.ID_ORGANIZATION END
                       , ZONE = CASE WHEN GROUPING(v.ID_ZONE) = 1 THEN 0 ELSE v.ID_ZONE END
                       , DATE_FROM = min(v.DATA), DATE_TO = max(v.DATA), DAYS = count(DISTINCT v.DATA)
                       , MONTHS = count(DISTINCT v.YM)
                       , CITIES = count(DISTINCT CASE WHEN v.ID_CITY <> -1 THEN v.ID_CITY END)
                       , VALUE_POS = sum(v.VALUE_POS), VALUE_NEG = sum(v.VALUE_NEG), CHECKS = sum(CAST(v.CHECKS AS bigint))
                  FROM #mc_ldr AS v
                  GROUP BY GROUPING SETS ((v.LINE_TYPE, v.ID_LINE, v.ID_ORGANIZATION, v.ID_ZONE), (v.LINE_TYPE, v.ID_LINE, v.ID_ZONE)
                                        , (v.LINE_TYPE, v.ID_LINE, v.ID_ORGANIZATION), (v.LINE_TYPE, v.ID_LINE))) AS d
                 OUTER APPLY (SELECT TOP (1) c.CAMPAIGN_NAME FROM dbo.I_CAMPAIGN AS c (nolock)
                              WHERE d.LINE_TYPE = 1 AND c.ID_COMPANY = @idc AND c.ID_CAMPAIGN = d.ID_LINE) AS cn
                 LEFT JOIN #mc_pname AS pn ON d.LINE_TYPE IN (2, 3) AND pn.ID_PRODUCT = d.ID_LINE
                 OUTER APPLY (SELECT ID_PROMO = min(l.ID_PROMO), PROMOS = count(DISTINCT l.ID_PROMO)
                              FROM dbo.I_PROMO_CAMPAIGN_LINK AS l (nolock)
                              WHERE d.LINE_TYPE = 1 AND l.ID_COMPANY = @idc AND l.ID_CAMPAIGN = d.ID_LINE
                                AND l.IS_USED = 1 AND l.IS_CURRENT = 1) AS pl
            OPTION (MAX_GRANT_PERCENT = 10);

            DELETE FROM dbo.I_DISC_MODEL_LINE_MONTH WHERE ID_COMPANY = @idc;
            INSERT INTO dbo.I_DISC_MODEL_LINE_MONTH (ID_COMPANY, LINE_TYPE, ID_LINE, ID_ORGANIZATION, ID_ZONE, YEAR_MONTH, DATE_FROM, DATE_TO
                                                   , DAYS, CITIES, VALUE_POS, VALUE_NEG, CHECKS, CALC_DT)
            SELECT @idc, v.LINE_TYPE, v.ID_LINE
                 , CASE WHEN GROUPING(v.ID_ORGANIZATION) = 1 THEN 0 ELSE v.ID_ORGANIZATION END
                 , CASE WHEN GROUPING(v.ID_ZONE) = 1 THEN 0 ELSE v.ID_ZONE END, v.YM
                 , min(v.DATA), max(v.DATA), count(DISTINCT v.DATA)
                 , count(DISTINCT CASE WHEN v.ID_CITY <> -1 THEN v.ID_CITY END)
                 , sum(v.VALUE_POS), sum(v.VALUE_NEG), sum(CAST(v.CHECKS AS bigint)), @now
            FROM #mc_ldr AS v
            GROUP BY GROUPING SETS ((v.LINE_TYPE, v.ID_LINE, v.ID_ORGANIZATION, v.ID_ZONE, v.YM), (v.LINE_TYPE, v.ID_LINE, v.ID_ZONE, v.YM)
                                  , (v.LINE_TYPE, v.ID_LINE, v.ID_ORGANIZATION, v.YM), (v.LINE_TYPE, v.ID_LINE, v.YM))
            OPTION (MAX_GRANT_PERCENT = 10);

            COMMIT TRANSACTION;
            SET @reg_sec = datediff(second, @t_reg, sysdatetime());
            SET @msg = concat(N'disc_model_mart: реестр скидок пересобран — строк ', (SELECT count(*) FROM dbo.I_DISC_MODEL_LINE (nolock) WHERE ID_COMPANY = @idc)
                            , N', по месяцам ', (SELECT count(*) FROM dbo.I_DISC_MODEL_LINE_MONTH (nolock) WHERE ID_COMPANY = @idc), N'.');
            RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
        END

        -- воскресной ночью — сжать колоночные индексы: ночные DELETE копят удалённые строки (07.01.1900 — воскресенье)
        IF @mode = 'NIGHT' AND datediff(day, '19000107', @today) % 7 = 0
        BEGIN
            BEGIN TRY
                ALTER INDEX CCI_I_DISC_MODEL_CLIENT_DAY  ON dbo.I_DISC_MODEL_CLIENT_DAY  REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
                ALTER INDEX CCI_I_DISC_MODEL_CLIENT_DISC ON dbo.I_DISC_MODEL_CLIENT_DISC REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
                ALTER INDEX CCI_I_DISC_MODEL_LINE_DAY    ON dbo.I_DISC_MODEL_LINE_DAY    REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
                ALTER INDEX CCI_I_DISC_MODEL_ZONE_DAY    ON dbo.I_DISC_MODEL_ZONE_DAY    REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
                RAISERROR(N'disc_model_mart: колоночные индексы витрины сжаты (воскресенье).', 0, 1) WITH NOWAIT;
            END TRY
            BEGIN CATCH
                SET @msg = concat(N'ВНИМАНИЕ: disc_model_mart: сжатие индексов не прошло (', ERROR_MESSAGE(), N') — витрина посчитана, сжатие повторится в следующее воскресенье.');
                RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
            END CATCH
        END

        UPDATE dbo.I_DISC_MODEL_LOG
        SET FINISHED = sysdatetime(), STATUS = IIF(@stopped = 1, 'STOPPED', 'OK'), DAYS_DONE = @days_done, MSG = @warn
          , MAX_PART_SEC = nullif(@max_part_sec, 0), REG_SEC = @reg_sec, ZONE_PART_SEC = nullif(@max_zsec, 0)
        WHERE RUN_ID = @run_id;

        SET @msg = concat(N'disc_model_mart: готово — дней ', @days_done, N', '
                        , datediff(second, @t_run, sysdatetime()) / 60, N' мин ', datediff(second, @t_run, sysdatetime()) % 60, N' с.'
                        , IIF(@stopped = 1, N' Остановлено по @stop_at — продолжит следующий запуск.', N''));
        RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

        -- снять блокировку целиком (и повторный захват из disc_model_zone_reg, если тот оборвался без снятия)
        WHILE APPLOCK_MODE('public', N'dbo.disc_model_mart', 'Session') <> 'NoLock'
            EXEC sp_releaseapplock @Resource = N'dbo.disc_model_mart', @LockOwner = 'Session';
        SET @locked = 0;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = concat(N'disc_model_mart: ', IIF(@p_from IS NOT NULL, concat(N'порция ', convert(char(10), @p_from, 104), N'–'
                        , convert(char(10), @p_to, 104), N' не записана; '), N''), N'ошибка ', ERROR_NUMBER(), N': ', ERROR_MESSAGE()
                        , N' Записанные порции остаются, следующий запуск досчитает.');
        IF @run_id IS NOT NULL
            UPDATE dbo.I_DISC_MODEL_LOG SET FINISHED = sysdatetime(), STATUS = 'ERROR', MSG = left(concat(@warn + N' ', @msg), 2000)
            WHERE RUN_ID = @run_id;
        IF @locked = 1
            WHILE APPLOCK_MODE('public', N'dbo.disc_model_mart', 'Session') <> 'NoLock'
                EXEC sp_releaseapplock @Resource = N'dbo.disc_model_mart', @LockOwner = 'Session';
        THROW 50004, @msg, 1;
    END CATCH
END
GO

-- ============================== 4. ВСЯ ИСТОРИЯ — СРАЗУ, ЦЕЛИКОМ (до добавления в обновление) ==============================
-- Елена 20.10: «скрипт для старта целиком и отсчитать всё сейчас, а не ждать обновлений». Раздел считает всю историю
--   с 19.05.2025 по вчера за один запуск, без остановки по времени: цены зон за все дни → регулярная цена зон → скидки
--   клиентов за все дни → реестр. Дальше раздел 7 выводит результат. Время — ориентировочно 10–20 часов (замера нет).
--   Если запуск застанет ночную загрузку MCI и упадёт на блокировке (витрина уступает задаче MCI), записанные порции
--   остаются: выделить и выполнить этот раздел ещё раз (затем разделы 5–6) — продолжит с непосчитанного. Прогресс — во вкладке «Сообщения».
USE mci_model
GO

IF OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DAY', 'U') IS NULL OR OBJECT_ID('dbo.I_DISC_MODEL_CLIENT_DISC', 'U') IS NULL
   OR OBJECT_ID('dbo.I_DISC_MODEL_LINE_DAY', 'U') IS NULL OR OBJECT_ID('dbo.I_DISC_MODEL_STATE', 'U') IS NULL
   OR OBJECT_ID('dbo.I_DISC_MODEL_ZONE_DAY', 'U') IS NULL OR OBJECT_ID('dbo.I_DISC_MODEL_ZONE_REG', 'U') IS NULL
   OR isnull(OBJECT_DEFINITION(OBJECT_ID('dbo.disc_model_mart')), N'') NOT LIKE N'%CVM[_]DISC[_]MODEL%'
    RAISERROR(N'СТОП: таблицы или процедура витрины не создались — историю не считаю.', 16, 1);
ELSE
BEGIN
    RAISERROR(N'История витрины целиком: цены зон → регулярная цена → скидки клиентов → реестр, без остановки по времени.', 0, 1) WITH NOWAIT;
    EXEC dbo.disc_model_mart @hist_days = NULL, @max_days = NULL;

    DECLARE @left int = datediff(day, '20250519', dateadd(day, -1, CAST(getdate() AS date))) + 1
                      - (SELECT count(*) FROM dbo.I_DISC_MODEL_STATE (nolock) WHERE ID_COMPANY = 1 AND STATUS IN ('OK', 'NO_CHECKS'));
    DECLARE @m nvarchar(400) = CASE WHEN @left <= 0 THEN N'История посчитана целиком.'
                                    ELSE concat(N'ВНИМАНИЕ: не посчитано дней ', @left, N' — выполнить раздел 4 ещё раз, продолжит.') END;
    RAISERROR(N'%s', 10, 1, @m) WITH NOWAIT;
END
GO

-- ============================== 5. ЧИСТКА ВИТРИНЫ ПРИ ПЕРЕЗАЛИВКЕ: шаг 1 MCI (clear_data_update, reload_clear_data) ==============================
-- Как раздел 4 CVM_DISCOUNT_MART.sql: в ЖИВОЙ текст каждой процедуры перед первой строкой с #DELS_CH вставляются
--   комментарий и ОДНА строка DELETE — отметки дней I_DISC_MODEL_STATE с первого перезалитого дня (сотни строк); CREATE →
--   ALTER с исходными ANSI_NULLS / QUOTED_IDENTIFIER; исходный текст — в dbo.I_DISCOUNT_PROC_BACKUP; остальной текст (и
--   строки витрины скидок) не меняется. Строка уже есть (метка CVM_DISC_MODEL) — процедура не трогается.
-- Сами строки витрины шаг 1 не стирает: в первую пятницу это ~180 млн строк колоночных таблиц внутри интеграционной
--   процедуры (ревью sql-senior 19.10). Дни без отметки пересчитывает процедура: стирает старые строки дня и пишет новые
--   (день без чеков и без прежней отметки — только стирает).
-- ВАЖНО: пока строка стоит в шаге 1, таблицу I_DISC_MODEL_STATE не удалять и не переименовывать — шаг 1 упал бы;
--   снимать витрину — только после отката ниже.
USE mci_model
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('dbo.I_DISCOUNT_PROC_BACKUP') IS NULL
BEGIN
    CREATE TABLE dbo.I_DISCOUNT_PROC_BACKUP (
          PROC_NAME  sysname        NOT NULL
        , SAVED_AT   datetime2(0)   NOT NULL
        , DEFINITION nvarchar(max)  NOT NULL
        , CONSTRAINT PK_I_DISCOUNT_PROC_BACKUP PRIMARY KEY CLUSTERED (PROC_NAME, SAVED_AT)
    );
END

DROP TABLE IF EXISTS #mc_patch;
CREATE TABLE #mc_patch (RN int NOT NULL, PROC_NAME sysname NOT NULL, IDC_VAR nvarchar(30) NOT NULL
                      , DEF nvarchar(max) NULL, NEW_DEF nvarchar(max) NULL, ANSI bit NULL, QI bit NULL
                      , ACT nvarchar(20) COLLATE DATABASE_DEFAULT NULL, WHY nvarchar(400) COLLATE DATABASE_DEFAULT NULL);
INSERT INTO #mc_patch (RN, PROC_NAME, IDC_VAR) VALUES (1, N'dbo.clear_data_update', N'@idc'), (2, N'dbo.reload_clear_data', N'@ID_COMPANY');

DECLARE @i int = 1, @name sysname, @var nvarchar(30), @def nvarchar(max), @ansi bit, @qi bit, @crlf nchar(2) = NCHAR(13) + NCHAR(10);
DECLARE @pos_create int, @pos_anchor int, @back int, @line_start int, @block nvarchar(max), @new nvarchar(max), @msg nvarchar(2000);
DECLARE @wrap nvarchar(400);

-- 1. Проверки и новый текст обеих процедур — до любых изменений
WHILE @i <= 2
BEGIN
    SELECT @name = PROC_NAME, @var = IDC_VAR FROM #mc_patch WHERE RN = @i;
    SELECT @def = NULL, @ansi = NULL, @qi = NULL;
    SELECT @def = m.definition, @ansi = m.uses_ansi_nulls, @qi = m.uses_quoted_identifier
    FROM sys.sql_modules AS m (nolock)
    WHERE m.object_id = OBJECT_ID(@name);

    SET @pos_create = PATINDEX(N'%CREATE%', isnull(@def, N''));
    SET @pos_anchor = CHARINDEX(N'#DELS_CH', isnull(@def, N''));
    SET @back = CASE WHEN @pos_anchor > 1 THEN CHARINDEX(NCHAR(10), REVERSE(LEFT(@def, @pos_anchor - 1))) ELSE 0 END;
    SET @line_start = @pos_anchor - @back + 1;   -- начало строки с #DELS_CH

    IF @def IS NULL
        UPDATE #mc_patch SET ACT = N'СТОП', WHY = N'процедуры нет или нет прав читать её текст' WHERE RN = @i;
    ELSE IF @def LIKE N'%CVM[_]DISC[_]MODEL%'
         AND (   @def LIKE N'%I[_]DISC[_]MODEL[_]CLIENT[_]DAY%' OR @def LIKE N'%I[_]DISC[_]MODEL[_]CLIENT[_]DISC%'
              OR @def LIKE N'%I[_]DISC[_]MODEL[_]LINE[_]DAY%' OR @def NOT LIKE N'%delete from [[]dbo].I[_]DISC[_]MODEL[_]STATE where DATA >= @d[_]from%')
        UPDATE #mc_patch SET ACT = N'СТОП', WHY = N'в тексте стоит другой вариант строк витрины модели скидок (не одна строка STATE) — не трогаю, нужна ручная правка' WHERE RN = @i;
    ELSE IF @def LIKE N'%CVM[_]DISC[_]MODEL%'
        UPDATE #mc_patch SET ACT = N'уже стоит', WHY = N'строка витрины модели скидок уже есть' WHERE RN = @i;
    ELSE IF @pos_create = 0
         OR ltrim(rtrim(replace(replace(replace(left(@def, @pos_create - 1), NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N''))) <> N''
         OR substring(@def, @pos_create, 40) NOT LIKE N'CREATE%PROC%'
        UPDATE #mc_patch SET ACT = N'СТОП', WHY = N'текст начинается не с CREATE PROCEDURE' WHERE RN = @i;
    ELSE IF @pos_anchor = 0 OR @back = 0
         OR @def NOT LIKE N'%@d[_]from%' OR @def NOT LIKE N'%' + @var + N'%'
        UPDATE #mc_patch SET ACT = N'СТОП', WHY = concat(N'в тексте нет строки с #DELS_CH, @d_from или ', @var, N' — текст изменился') WHERE RN = @i;
    ELSE IF OBJECT_ID(N'dbo.I_DISC_MODEL_STATE', N'U') IS NULL
        UPDATE #mc_patch SET ACT = N'СТОП', WHY = N'таблицы I_DISC_MODEL_STATE нет (раздел 2 не отработал) — строка чистки уронила бы шаг 1' WHERE RN = @i;
    ELSE
    BEGIN
        SET @block = concat(
              NCHAR(9), N'-- модель эффекта скидок (CVM_DISC_MODEL, решение 16.10.2026): дни перезаливки пересчитывает ночной шаг DISC_MODEL_MART', @crlf
            , NCHAR(9), N'delete from [dbo].I_DISC_MODEL_STATE where DATA >= @d_from and ID_COMPANY=', @var, @crlf
            , @crlf);
        -- сначала вставка строк (дальше по тексту), потом CREATE → ALTER (раньше по тексту — позиция не сдвинулась)
        SET @new = STUFF(STUFF(@def, @line_start, 0, @block), @pos_create, 6, N'ALTER');
        UPDATE #mc_patch SET DEF = @def, NEW_DEF = @new, ANSI = @ansi, QI = @qi, ACT = N'правка' WHERE RN = @i;
    END

    SET @i += 1;
END

-- 2. Правка — каждая процедура своей транзакцией: бэкап, ALTER, проверка «строки на месте, настройки те же»
SET @i = 1;
WHILE @i <= 2
BEGIN
    SELECT @name = NULL, @new = NULL, @ansi = NULL, @qi = NULL;
    SELECT @name = PROC_NAME, @new = NEW_DEF, @ansi = ANSI, @qi = QI FROM #mc_patch WHERE RN = @i AND ACT = N'правка';
    IF @new IS NOT NULL
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
            SELECT PROC_NAME, sysdatetime(), DEF FROM #mc_patch WHERE RN = @i;

            SET @wrap = concat(N'SET ANSI_NULLS ', IIF(@ansi = 1, N'ON', N'OFF'), N'; SET QUOTED_IDENTIFIER ', IIF(@qi = 1, N'ON', N'OFF')
                             , N'; EXEC sp_executesql @x;');
            EXEC sp_executesql @wrap, N'@x nvarchar(max)', @x = @new;

            IF NOT EXISTS (SELECT 1 FROM sys.sql_modules AS m (nolock)
                           WHERE m.object_id = OBJECT_ID(@name)
                             AND m.definition LIKE N'%CVM[_]DISC[_]MODEL%'
                             AND m.uses_ansi_nulls = @ansi AND m.uses_quoted_identifier = @qi)
                THROW 50001, N'после правки в тексте процедуры нет строк витрины модели скидок или изменились настройки — откат.', 1;

            COMMIT TRANSACTION;
            UPDATE #mc_patch SET ACT = N'добавлено' WHERE RN = @i;
            RAISERROR(N'Чистка шага 1: в %s добавлены строки чистки витрины модели скидок (исходный текст — в dbo.I_DISCOUNT_PROC_BACKUP).', 10, 1, @name) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            UPDATE #mc_patch SET ACT = N'СТОП', WHY = left(concat(N'ошибка ', ERROR_NUMBER(), N' — ', ERROR_MESSAGE(), N'; откачено'), 400) WHERE RN = @i;
        END CATCH
    END
    SET @i += 1;
END

IF EXISTS (SELECT 1 FROM #mc_patch WHERE ACT = N'СТОП')
BEGIN
    SELECT @msg = STRING_AGG(CAST(concat(PROC_NAME, N': ', WHY) AS nvarchar(max)), N'; ') FROM #mc_patch WHERE ACT = N'СТОП';
    RAISERROR(N'СТОП по чистке шага 1: %s. Эта процедура не изменена; без строк в clear_data_update шаг задания (раздел 6) не поставится — напиши мне.', 16, 1, @msg);
END

SELECT [Процедура] = p.PROC_NAME
     , [Что сделано] = p.ACT
     , [Строки витрины модели скидок в тексте] = CASE WHEN m.definition LIKE N'%CVM[_]DISC[_]MODEL%' THEN N'есть' ELSE N'НЕТ' END
FROM #mc_patch AS p
     LEFT JOIN sys.sql_modules AS m (nolock) ON m.object_id = OBJECT_ID(p.PROC_NAME)
ORDER BY p.RN;
GO

-- ОТКАТ ЧИСТКИ (не выполняется; выделить и выполнить вручную): убрать из живого текста только строки этой витрины.
-- USE mci_model;
-- SET XACT_ABORT ON;
-- DECLARE @n sysname, @v nvarchar(30), @d nvarchar(max), @b nvarchar(max), @w nvarchar(400), @c nchar(2) = NCHAR(13) + NCHAR(10)
--       , @k int = 1, @p int, @an bit, @qi bit;
-- WHILE @k <= 2
-- BEGIN
--     SELECT @n = CASE @k WHEN 1 THEN N'dbo.clear_data_update' ELSE N'dbo.reload_clear_data' END
--          , @v = CASE @k WHEN 1 THEN N'@idc' ELSE N'@ID_COMPANY' END;
--     SELECT @d = NULL, @an = NULL, @qi = NULL;
--     SELECT @d = m.definition, @an = m.uses_ansi_nulls, @qi = m.uses_quoted_identifier
--     FROM sys.sql_modules AS m (nolock) WHERE m.object_id = OBJECT_ID(@n);
--     SET @b = concat(NCHAR(9), N'-- модель эффекта скидок (CVM_DISC_MODEL, решение 16.10.2026): дни перезаливки пересчитывает ночной шаг DISC_MODEL_MART', @c
--                   , NCHAR(9), N'delete from [dbo].I_DISC_MODEL_STATE where DATA >= @d_from and ID_COMPANY=', @v, @c, @c);
--     SET @p = PATINDEX(N'%CREATE%', @d);
--     IF CHARINDEX(@b, @d) > 0 AND @p > 0 AND substring(@d, @p, 40) LIKE N'CREATE%PROC%'
--        AND ltrim(rtrim(replace(replace(replace(left(@d, @p - 1), NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N''))) = N''
--     BEGIN
--         BEGIN TRY
--             BEGIN TRANSACTION;
--             INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION) VALUES (@n, sysdatetime(), @d);
--             SET @d = STUFF(REPLACE(@d, @b, N''), @p, 6, N'ALTER');
--             SET @w = concat(N'SET ANSI_NULLS ', IIF(@an = 1, N'ON', N'OFF'), N'; SET QUOTED_IDENTIFIER ', IIF(@qi = 1, N'ON', N'OFF'), N'; EXEC sp_executesql @x;');
--             EXEC sp_executesql @w, N'@x nvarchar(max)', @x = @d;
--             IF isnull(OBJECT_DEFINITION(OBJECT_ID(@n)), N'') LIKE N'%CVM[_]DISC[_]MODEL%'
--                OR NOT EXISTS (SELECT 1 FROM sys.sql_modules AS m (nolock)
--                               WHERE m.object_id = OBJECT_ID(@n) AND m.uses_ansi_nulls = @an AND m.uses_quoted_identifier = @qi)
--                 THROW 50001, N'строки не убраны или изменились настройки — откат', 1;
--             COMMIT TRANSACTION;
--             PRINT concat(@n, N': строки витрины модели скидок убраны');
--         END TRY
--         BEGIN CATCH
--             IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
--             PRINT concat(@n, N': НЕ изменена — ', ERROR_MESSAGE());
--         END CATCH
--     END
--     ELSE PRINT concat(@n, N': строк витрины модели скидок нет или текст начинается не с CREATE PROCEDURE — не трогаю');
--     SET @k += 1;
-- END

-- ============================== 6. ШАГ DISC_MODEL_MART в «MCI Regular update» сразу после DISCOUNT_MART ==============================
-- Порядок задания с 16.10.2026: каждый шаг при успехе — следующий. sp_add_jobstep с @step_id = (DISCOUNT_MART + 1)
-- сдвигает следующие шаги на 1 и сам пересчитывает их переходы. Повторный запуск шаг не задвоит. Шаг ставится, только
-- если строки чистки стоят в clear_data_update (раздел 5).
USE msdb
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'MCI Regular update';
DECLARE @dm int, @dm_ok int, @rc int = 0;

SELECT @dm = s.step_id, @dm_ok = s.on_success_action
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
     JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = @job AND s.step_name = N'DISCOUNT_MART';

-- текст процедуры mci_model — через mci_model.sys.sql_modules (OBJECT_DEFINITION в контексте msdb ищет в msdb)
IF OBJECT_ID(N'mci_model.dbo.disc_model_mart', N'P') IS NULL
   OR NOT EXISTS (SELECT 1 FROM mci_model.sys.sql_modules AS m WITH (NOLOCK)
                  WHERE m.object_id = OBJECT_ID(N'mci_model.dbo.clear_data_update') AND m.definition LIKE N'%CVM[_]DISC[_]MODEL%')
    RAISERROR(N'СТОП: нет процедуры dbo.disc_model_mart или строк чистки в clear_data_update (разделы 3 и 5) — задание не изменено.', 16, 1);
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK) JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
                WHERE j.name = @job AND s.step_name = N'DISC_MODEL_MART')
    RAISERROR(N'Шаг DISC_MODEL_MART уже есть — задание не меняю.', 10, 1);
ELSE IF @dm IS NULL OR @dm_ok <> 3
    RAISERROR(N'СТОП: шаг DISCOUNT_MART не найден или при успехе идёт не на следующий шаг — задание не изменено.', 16, 1);
ELSE
BEGIN
    SET @dm += 1;
    EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_id = @dm, @step_name = N'DISC_MODEL_MART',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.disc_model_mart;',
         @on_success_action = 3, @on_fail_action = 3;
    IF (@@ERROR <> 0 OR @rc <> 0)
        RAISERROR(N'СТОП: шаг не добавлен (ошибка sp_add_jobstep) — задание не изменено.', 16, 1);
    ELSE
        RAISERROR(N'Шаг DISC_MODEL_MART добавлен сразу после DISCOUNT_MART.', 10, 1);
END
GO

SELECT [Шаг] = s.step_id, [Имя] = s.step_name, [Команда] = s.command
     , [При успехе] = s.on_success_action, [При успехе → шаг] = s.on_success_step_id
     , [При ошибке] = s.on_fail_action, [При ошибке → шаг] = s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
     JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update'
ORDER BY s.step_id;
GO

-- ============================== 7. ИТОГ ==============================
USE mci_model
GO

-- 7.1 Дни витрины
SELECT [Дней посчитано] = sum(CASE WHEN s.STATUS = 'OK' THEN 1 ELSE 0 END)
     , [Дней без чеков] = sum(CASE WHEN s.STATUS = 'NO_CHECKS' THEN 1 ELSE 0 END)
     , [Первый день] = min(CASE WHEN s.STATUS = 'OK' THEN s.DATA END)
     , [Последний день] = max(CASE WHEN s.STATUS = 'OK' THEN s.DATA END)
     , [Непосчитанных дней с 19.05.2025] = datediff(day, '20250519', dateadd(day, -1, CAST(getdate() AS date))) + 1
                                          - sum(CASE WHEN s.STATUS IN ('OK', 'NO_CHECKS') THEN 1 ELSE 0 END)
     , [Дней с ценами зон] = sum(CASE WHEN s.ZONE_CALC_DT IS NOT NULL THEN 1 ELSE 0 END)
     , [Клиенто-дней] = sum(CAST(s.CLIENT_DAYS AS bigint))
     , [Строк списаний на табак (не входят)] = sum(s.RULE_ROWS_TOB)
     , [Списано на табак, ₽ (не входит)] = sum(s.VALUE_TOB)
     , [Строк списаний без позиции в I_CHECK] = sum(s.RULE_NO_ITEM)
FROM dbo.I_DISC_MODEL_STATE AS s (nolock)
WHERE s.ID_COMPANY = 1;

-- 7.2 Скидки по месяцам и каналам за последние три месяца: кампаний со скидкой, списано, клиентов со скидкой (разных за месяц)
SELECT [Месяц] = d.YM
     , [Канал] = CASE d.ID_ORGANIZATION WHEN 1 THEN N'офлайн' WHEN 3 THEN N'онлайн' END
     , [Строка скидки] = CASE d.LINE_TYPE WHEN 1 THEN N'кампании Manzana' WHEN 2 THEN N'скидка на товар' WHEN 3 THEN N'снижение регулярной цены (не скидка)' END
     , [Строк скидки] = count(DISTINCT d.ID_LINE)
     , [Скидка, ₽] = sum(d.VALUE_POS - d.VALUE_NEG)
     , [Клиентов со скидкой] = count(DISTINCT d.ID_CONTACT)
FROM (SELECT c.ID_ORGANIZATION, c.LINE_TYPE, c.ID_LINE, c.ID_CONTACT, c.VALUE_POS, c.VALUE_NEG, YM = year(c.DATA) * 100 + month(c.DATA)
      FROM dbo.I_DISC_MODEL_CLIENT_DISC AS c (nolock)
      WHERE c.ID_COMPANY = 1
        AND c.DATA >= dateadd(month, -2, datefromparts(year(getdate()), month(getdate()), 1))) AS d
GROUP BY d.YM, d.ID_ORGANIZATION, d.LINE_TYPE
ORDER BY d.YM, d.ID_ORGANIZATION, d.LINE_TYPE
OPTION (MAX_GRANT_PERCENT = 10);

-- 7.2а Цены зон: по зонам — товаро-дней, доля дней ниже регулярной цены (акция в зоне), дней снижения регулярной цены;
--      средняя доля магазинов зоны, державших цену зоны (товаро-дни с ≥ 5 магазинами)
SELECT [Зона] = isnull(z.ZONE_NAME, concat(N'зона ', g.ID_ZONE))
     , [Товаро-дней] = count_big(*)
     , [Товаров] = count(DISTINCT g.ID_PRODUCT)
     , [Ниже регулярной (акция), %] = cast(100.0 * avg(CAST(g.BELOW AS float)) AS decimal(9, 1))
     , [Дней снижения регулярной цены, %] = cast(100.0 * avg(CAST(g.IN_RESET AS float)) AS decimal(9, 1))
     , [Акция идёт и моложе 2 мес. (предварительно), %] = cast(100.0 * avg(CAST(g.PROVISIONAL AS float)) AS decimal(9, 1))
FROM dbo.I_DISC_MODEL_ZONE_REG AS g (nolock)
     LEFT JOIN dbo.I_DISC_MODEL_ZONE AS z (nolock) ON z.ID_ZONE = g.ID_ZONE
WHERE g.ID_COMPANY = 1
GROUP BY isnull(z.ZONE_NAME, concat(N'зона ', g.ID_ZONE))
ORDER BY 1
OPTION (MAX_GRANT_PERCENT = 10);

SELECT [Зона] = isnull(z.ZONE_NAME, concat(N'зона ', x.ID_ZONE))
     , [Магазинов держат цену зоны, % (в среднем по товаро-дням, где продавали ≥ 5 магазинов)] = cast(avg(100.0 * x.SHOPS_AT / x.SHOPS) AS decimal(9, 1))
FROM dbo.I_DISC_MODEL_ZONE_DAY AS x (nolock)
     LEFT JOIN dbo.I_DISC_MODEL_ZONE AS z (nolock) ON z.ID_ZONE = x.ID_ZONE
WHERE x.ID_COMPANY = 1 AND x.SHOPS >= 5
GROUP BY isnull(z.ZONE_NAME, concat(N'зона ', x.ID_ZONE))
ORDER BY 1
OPTION (MAX_GRANT_PERCENT = 10);

-- 7.3 Сверка с витриной скидок за последние 14 посчитанных дней: витрина скидок (чеки с клиентом, уровень 4 —
--   все кампании дня по каналу) = эта витрина + списания на табак. Разница — чеки клиентов, которых нет в I_CHECK
--   (строки списаний без строк чека), или день, посчитанный в двух витринах в разное время.
SELECT [День] = s.DATA
     , [Витрина скидок, ₽ (клиенты)] = dd.V
     , [Эта витрина, ₽] = md.V
     , [Табак, ₽] = s.VALUE_TOB
     , [Разница, ₽] = dd.V - isnull(md.V, 0) - s.VALUE_TOB
FROM (SELECT TOP (14) x.DATA, x.VALUE_TOB FROM dbo.I_DISC_MODEL_STATE AS x (nolock)
      WHERE x.ID_COMPANY = 1 AND x.STATUS = 'OK' ORDER BY x.DATA DESC) AS s
     OUTER APPLY (SELECT V = sum(d.VALUE_POS_CL - d.VALUE_NEG_CL) FROM dbo.I_DISCOUNT_DAY AS d (nolock)
                  WHERE d.ID_COMPANY = 1 AND d.DATA = s.DATA AND d.LVL = 4) AS dd
     OUTER APPLY (SELECT V = sum(m.VALUE_POS - m.VALUE_NEG) FROM dbo.I_DISC_MODEL_LINE_DAY AS m (nolock)
                  WHERE m.ID_COMPANY = 1 AND m.DATA = s.DATA AND m.LINE_TYPE = 1) AS md
ORDER BY s.DATA DESC;
GO

SET NOEXEC OFF;
GO
