-- ============================================================================================
-- CVM_EFFECT_PROC — отчёт эффекта CVM-акций по методике 2026-10 (решения Елены 12–13.10.2026):
--   витрины dbo.I_CVM_EFFECT (общий эффект: строка на акцию) и dbo.I_CVM_EFFECT_CG (данные по слоям),
--   процедура dbo.CVM_EFFECT_TODAY (заменяет старую), строки чистки витрин в шаге 1 MCI.
-- Запуск: целиком в SSMS (F5), ПОСЛЕ CVM_DISCOUNT_MART.sql. Процедуру файл не запускает: ручной прогон с замером —
--   в конце файла (закомментирован); шаги задания, вечерний догон истории и удаление старых объектов —
--   CVM_EFFECT_JOB.sql.
-- Порядок файла: 0 проверки → 1 витрины (старые I_CVM_EFFECT / I_CVM_EFFECT_CG удаляются и создаются заново) →
--   2 процедура → 3 чистка шага 1 (clear_data_update, reload_clear_data) → 4 итог.
--
-- Методика — тестовый расчёт CVM_EFFECT_TEST_101374_101384.sql (режим выбросов DROP) с дополнениями 13.10:
--   акции    — ID_COMPANY = 1, старт с 01.09.2025: 101000–101999 и 500000–599999 (TYPE_PROMO = 'CVM') — по офлайн-
--              чекам; 110000–111999 — по онлайн-чекам (ID_ORGANIZATION = 3); организация 2 не берётся нигде;
--   список   — I_PROMO_OFFER: CONTROL_GROUP 0 — ЦГ, 1 — КГ (обе метки — ЦГ); фрод (I_FRAUD_CONTACT) — вне оценки,
--              его скидка — в затратах; ошибочные контакты не исключаются;
--   сегмент  — на дату старта по чекам канала акции: последняя покупка ≤ 42 дней — Активные (первая тоже ≤ 42 —
--              Новые), 43–105 — Отток, больше 105 — Спящие; покупок нет — вне оценки. Первая покупка и давность
--              Спящих — из месячного снимка I_CVM_CONTACT_DATA (последний REFERENCE_DATE до старта, строка канала);
--   «до»     — Активные и Новые: ТО до скидки (COST_DISCOUNT + DISCOUNT) за 42 дня до старта; Отток и Спящие — ТО до
--              скидки за всю историю до старта (решение 14.10): по дату снимка — из LTV и долей скидок снимка
--              (LTV · (1 + CARD_SALE_ALL / (1 − REAL_SALE_ALL))), после снимка — по чекам; в оценку — клиенты с «до» > 0;
--   выбросы  — граница из данных G = exp(m + z·s) по ln: m — медиана, s = (верхний квартиль − m) / 0,674, z —
--              квантиль нормального распределения для 1 − κ/n; «до» — по всем в оценке, «во время» и «после» —
--              m и s по покупателям КГ, n — покупатели ЦГ и КГ; клиент за границей хотя бы в одном периоде
--              исключается из всего расчёта; граница — по акции и сегменту;
--   Доп ТО   — в каждом слое ΣY_ЦГ − ΣX_ЦГ · ΣY_КГ / ΣX_КГ, сумма слоёв, сумма сегментов. Слои (решения 19.10) — недели
--              без слияния: Активные — недели давности 1–6, Новые — недели стажа 1–6, Отток — недели давности 7–15,
--              Спящие — 4-недельные блоки давности; неделя меньше 30 клиентов КГ не сравнивается. В неделе ЦГ и КГ
--              выравниваются по сумме покупок «до»: интервалы шкалы I_INTERVALS_BUDGET_100 (100 ₽; у Оттока и Спящих —
--              I_INTERVALS_BUDGET_500, 500 ₽), доля КГ в интервале;
--              соседние интервалы сливаются, пока в объединённом нет 30 КГ; в сравнении — интервалы с ЦГ; минимальная доля
--              КГ среди них (не ниже 3 %) — доля недели;
--              где доля выше — случайно убираются лишние КГ, где ниже — лишние ЦГ. Доп ТО и показатели —
--              на выровненном списке недели; «на всю ЦГ» — Доп ТО на клиента недели × все клиенты ЦГ недели в оценке, у
--              недель без сравнения — Доп ТО на клиента сегмента. Каналы (решение 14.10) — каждый на своей базе:
--              офлайн — сегмент, слои, «до», выбросы и ТО периода по офлайн-чекам, онлайн — по онлайн-чекам (снимок своего
--              канала); «все каналы» = офлайн + онлайн (ошибки — сумма квадратов). Регрессия по КГ — сверка;
--              ошибка (SE² слоёв и сегментов складываются), ДИ 95 %, MDE = 2,8 · SE, значимость — как в тесте;
--              «итого» — по сумме клиента «в акцию» + «после»;
--   отклик   — доля купивших ЦГ − КГ среди вошедших, по слоям; инкрементальные покупатели — сумма слоёв;
--   проверка «до старта» (решения 19.10) — та же формула на выровненном списке: «период» — последний отрезок «до» (неделя
--              последней покупки; у Спящих — месяц последней покупки по снимкам I_CVM_CONTACT_DATA, они есть с 31.08.2025),
--              «до» — остальное «до»; псевдоэффект «Все каналы» дальше 3,29 ошибки — «КГ несопоставима», акция не входит в
--              общий эффект месяца (цифры её эффекта в витрине остаются);
--   шесть показателей (обязательны в отчёте, 18.10): клиентов ЦГ → ARPU → средний чек → чеков на клиента → SKU в чеке →
--              цена SKU — «в акцию» и «после», ЦГ, КГ и инкремент (₽ / шт. и %). Чеки — строки I_CHECKHEADER, SKU — его
--              поле SKU, ТО — до скидки. ARPU и чеков на клиента — на КУПИВШИХ в периоде (решение 19.10: «ТО клиентов,
--              делённый на количество клиентов, которые купили»), средний чек — на чек, SKU в чеке и цена SKU — на чек и SKU.
--              КГ — ожидание ЦГ без акции с той же привязкой к «до», что у Доп ТО: в каждой неделе ТО, чеки, SKU и купившие КГ
--              × ΣX_ЦГ / ΣX_КГ, сумма недель; ARPU = чеков на клиента × средний чек — и у ЦГ, и у КГ. Клиентов ЦГ — выровненный
--              список недель в итоге; «Все каналы» — ТО, чеки и SKU офлайна + онлайна, купившие — без двойного счёта купивших
--              в обоих каналах (у КГ — по её доле таких);
--   «после»  — финиш + 1 … финиш + 4 по всем клиентам оценки. У ОДНОДНЕВНОЙ акции (старт = финиш) с механикой вне
--              @cashback_mechanics «после» не считается (пусто, выбросы «после» не ищутся), Доп ТО итого = в акцию;
--   скидка   — Σ|VALUE| из витрины скидок: строки dbo.I_DISCOUNT_CHECK кампаний акции (I_PROMO_CAMPAIGN_LINK,
--              IS_USED = 1) у всех клиентов списка — ЦГ и КГ, с фродом и исключёнными; за старт … финиш + 4 и
--              отдельно за дни акции. Кампаний нет — скидка 0 и пометка (решение 12.10). Не посчитан день витрины
--              скидок в окне акции, у дня нет чеков или он ждёт поздней связи кампании — акция в этот запуск не
--              считается (сообщение), прежняя строка витрины остаётся;
--   PL       — (Доп ТО × 0,3 − скидка) / 1,15; «PL в акцию» — скидка дней акции (@pl_in_discount = 'PROMO_DAYS')
--              или старт … финиш + 4 ('ALL'), «PL итого» — старт … финиш + 4 (и у акций без «после»: затраты
--              остаются затратами); ДИ PL — из ДИ Доп ТО;
--   доставка — отчёт по доставкам: I_DELIVERY_ACTION / I_DELIVERY_DAY, Push, группа CVM.
--   Сегмент, где в оценке меньше 2 клиентов КГ (ни отношение, ни ошибку не посчитать), в итог не входит —
--   пометка и колонки «Сегменты без оценки»; не оценивается ни один сегмент — Доп ТО и PL пустые.
--
-- Процедура dbo.CVM_EFFECT_TODAY (все параметры с умолчаниями; шаг задания — EXEC dbo.CVM_EFFECT_TODAY):
--   EXEC dbo.CVM_EFFECT_TODAY;                         — НОЧЬ: акции, которых нет в I_CVM_EFFECT (новые и стёртые
--                                                        шагом 1), со стартом не раньше сегодня − @night_days (120);
--                                                        акции, у которых финиш + 4 не раньше сегодня − 10 (окно
--                                                        перезаливки; помечаются «предварительная»); прошлые
--                                                        предварительные; акции с новой связью с кампанией Manzana
--                                                        после расчёта. Старше @night_days — только история
--   EXEC dbo.CVM_EFFECT_TODAY @date_from = '2026-09-01', @date_to = '2026-09-30';
--                                                      — ИСТОРИЯ: акции со стартом в периоде, от новых к старым
--                                                        (@only_missing = 1 — только те, которых нет в витрине;
--                                                        @calc_before = '2026-10-18 20:00' — ещё и посчитанные раньше)
--   EXEC dbo.CVM_EFFECT_TODAY @id_promo = 101384;      — одна акция
--   @stop_at = '23:30'                                 — после этого времени новые порции и акции не начинаются
--                                                        (только время — сегодня; можно дату и время)
--   @left OUTPUT                                       — сколько акций выборки (ночь — горизонт @night_days,
--                                                        история — период) после запуска нет в I_CVM_EFFECT
--   @kappa = 0.5, @cashback_mechanics = '1,2,8,9,10,15,16,17', @pl_in_discount = 'PROMO_DAYS' — методика;
--   @part_promos = 10, @part_days = 7                  — порция: акций и разброс дат старта (чеки — один проход
--                                                        на порцию)
--   Запись — по акции: DELETE + INSERT в обе витрины одной транзакцией. Двойной запуск не идёт: блокировка
--   sp_getapplock на сессию (второй запуск сразу падает с сообщением). Ошибка порции не останавливает
--   остальные; в конце, если ошибки были, процедура завершается ошибкой (видно в истории задания).
--   Журнал по акциям — во вкладке «Сообщения» (в истории шага задания). Ручной запуск в SSMS остановлен кнопкой
--   «Отмена» — закрыть вкладку (снимается блокировка запуска) или выполнить в ней SELECT @@TRANCOUNT (не 0 —
--   ROLLBACK) и EXEC sp_releaseapplock @Resource = N'dbo.CVM_EFFECT_TODAY', @LockOwner = 'Session';
--
-- Витрины (колоночный кластерный индекс):
--   dbo.I_CVM_EFFECT    — три строки на акцию по каналу продаж: CHANNEL_ID 0 все каналы (итог акции = офлайн + онлайн),
--                         1 офлайн, 3 онлайн — каждый канал считается на своей базе: сегмент, слои давности, «до» и выбросы
--                         по чекам канала (решение 14.10); КГ списка некорректна (нет или доля вне 5–10 % дальше 3,29
--                         ошибки) — контроль из ГКГ на дату старта ([Источник КГ]); все сегменты; [Дата старта] и ID_COMPANY — по ним шаг 1 стирает окно перезаливки;
--                         [ID] — номер акции; доставка — только в строке CHANNEL_ID = 0.
--   dbo.I_CVM_EFFECT_CG — акция × сегмент × слой × период (1 в акцию, 2 после, 3 итого) × канал (1 офлайн,
--                         3 онлайн — база канала; сумма каналов — в I_CVM_EFFECT): клиенты, ТО «до» и периода ЦГ / КГ, Доп ТО, SE, регрессия, купившие,
--                         чеки и SKU ЦГ / КГ и их ожидание у ЦГ без акции;
--                         IN_TOTAL = 1 — слой входит в итог акции; START_DATE и ID_COMPANY — для шага 1.
--
-- Источники только читаются (nolock), I_PROMO не меняется; полных проходов I_CHECKHEADER, I_PROMO_OFFER, I_CHECK,
--   I_CHECK_RULE, I_PROMO_DELIVERY_REPORT нет (чеки — по окну дат порции и клиентам списков, списки — по номеру
--   акции; I_CHECK, I_CHECK_RULE и I_PROMO_DELIVERY_REPORT не читаются). Проверки «идёт ли задание MCI» нет
--   (решение 13.10).
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
DECLARE @abort bit = 0, @msg nvarchar(2000);

IF DB_NAME() <> N'mci_model'
BEGIN
    RAISERROR(N'СТОП: файл выполняется не в базе mci_model — ничего не меняю.', 16, 1);
    SET @abort = 1;
END

-- кириллица в названиях колонок и в тексте процедуры должна дойти до сервера как есть: файл открыт в UTF-8
IF N'Отток' <> NCHAR(1054) + NCHAR(1090) + NCHAR(1090) + NCHAR(1086) + NCHAR(1082)
BEGIN
    RAISERROR(N'СТОП: файл открыт не в UTF-8 — названия колонок витрины и сегментов исказились бы, ничего не меняю.', 16, 1);
    SET @abort = 1;
END

-- колонки источников, которые читает процедура
IF EXISTS (SELECT 1
           FROM (VALUES ('dbo.I_PROMO', 'ID_PROMO'), ('dbo.I_PROMO', 'PROMO_NAME'), ('dbo.I_PROMO', 'START_DATE')
                      , ('dbo.I_PROMO', 'FINISH_DATE'), ('dbo.I_PROMO', 'SEGMENT'), ('dbo.I_PROMO', 'MECHANICS')
                      , ('dbo.I_PROMO', 'ID_MECHANICS'), ('dbo.I_PROMO', 'CHANNEL'), ('dbo.I_PROMO', 'TYPE_PROMO')
                      , ('dbo.I_PROMO', 'ID_COMPANY')
                      , ('dbo.I_PROMO_OFFER', 'ID_PROMO'), ('dbo.I_PROMO_OFFER', 'ID_CONTACT'), ('dbo.I_PROMO_OFFER', 'CONTROL_GROUP')
                      , ('dbo.I_PROMO_OFFER', 'ID_COMPANY')
                      , ('dbo.I_CHECKHEADER', 'ID_CONTACT'), ('dbo.I_CHECKHEADER', 'DATA'), ('dbo.I_CHECKHEADER', 'ID_ORGANIZATION')
                      , ('dbo.I_CHECKHEADER', 'COST_DISCOUNT'), ('dbo.I_CHECKHEADER', 'DISCOUNT'), ('dbo.I_CHECKHEADER', 'ID_COMPANY')
                      , ('dbo.I_CHECKHEADER', 'SKU')
                      , ('dbo.I_CVM_CONTACT_DATA', 'ID_COMPANY'), ('dbo.I_CVM_CONTACT_DATA', 'ID_ORGANIZATION')
                      , ('dbo.I_CVM_CONTACT_DATA', 'ID_CONTACT'), ('dbo.I_CVM_CONTACT_DATA', 'REFERENCE_DATE')
                      , ('dbo.I_CVM_CONTACT_DATA', 'FIRST_DATA'), ('dbo.I_CVM_CONTACT_DATA', 'LAST_DATA'), ('dbo.I_CVM_CONTACT_DATA', 'LTV')
                      , ('dbo.I_CVM_CONTACT_DATA', 'CARD_SALE_ALL'), ('dbo.I_CVM_CONTACT_DATA', 'REAL_SALE_ALL')
                      , ('dbo.I_FRAUD_CONTACT', 'ID_CONTACT'), ('dbo.I_FRAUD_CONTACT', 'ID_COMPANY')
                      , ('dbo.I_INTERVALS_BUDGET_100', 'ID_INTERVAL'), ('dbo.I_INTERVALS_BUDGET_100', 'FINISH_SUM')
                      , ('dbo.I_INTERVALS_BUDGET_500', 'ID_INTERVAL'), ('dbo.I_INTERVALS_BUDGET_500', 'FINISH_SUM')) AS c (T, C)
           WHERE COL_LENGTH(c.T, c.C) IS NULL)
BEGIN
    RAISERROR(N'СТОП: в источниках (I_PROMO, I_PROMO_OFFER, I_CHECKHEADER, I_CVM_CONTACT_DATA, I_FRAUD_CONTACT, I_INTERVALS_BUDGET_100 / _500) нет нужных колонок — ничего не меняю.', 16, 1);
    SET @abort = 1;
END

-- витрина скидок (CVM_DISCOUNT_MART.sql) и отчёт по доставкам: без них процедура ставится, но без витрины скидок
-- акции не считаются (скидка не берётся нулём), без доставки — колонки доставки пустые
IF EXISTS (SELECT 1
           FROM (VALUES ('dbo.I_DISCOUNT_CHECK', 'ID_COMPANY'), ('dbo.I_DISCOUNT_CHECK', 'DATA'), ('dbo.I_DISCOUNT_CHECK', 'ID_CONTACT')
                      , ('dbo.I_DISCOUNT_CHECK', 'ID_CAMPAIGN'), ('dbo.I_DISCOUNT_CHECK', 'VALUE_TYPE'), ('dbo.I_DISCOUNT_CHECK', 'VALUE')
                      , ('dbo.I_PROMO_CAMPAIGN_LINK', 'ID_COMPANY'), ('dbo.I_PROMO_CAMPAIGN_LINK', 'ID_PROMO')
                      , ('dbo.I_PROMO_CAMPAIGN_LINK', 'ID_CAMPAIGN'), ('dbo.I_PROMO_CAMPAIGN_LINK', 'IS_USED')
                      , ('dbo.I_PROMO_CAMPAIGN_LINK', 'CURRENT_SINCE')
                      , ('dbo.I_DISCOUNT_STATE', 'ID_COMPANY'), ('dbo.I_DISCOUNT_STATE', 'DATA'), ('dbo.I_DISCOUNT_STATE', 'STATUS')
                      , ('dbo.I_DISCOUNT_STATE', 'LAST_TRY')
                      , ('dbo.I_DISCOUNT_DAY', 'ID_COMPANY'), ('dbo.I_DISCOUNT_DAY', 'DATA'), ('dbo.I_DISCOUNT_DAY', 'LVL')
                      , ('dbo.I_DISCOUNT_DAY', 'ID_CAMPAIGN')) AS c (T, C)
           WHERE COL_LENGTH(c.T, c.C) IS NULL)
    RAISERROR(N'ВНИМАНИЕ: витрины скидок нет (сначала CVM_DISCOUNT_MART.sql) — процедура ставится, но акции считать не будет, пока витрины нет.', 10, 1) WITH NOWAIT;

IF OBJECT_ID('dbo.I_DELIVERY_ACTION', 'U') IS NULL OR OBJECT_ID('dbo.I_DELIVERY_DAY', 'U') IS NULL
   OR OBJECT_ID('dbo.I_DELIVERY_STATUS', 'U') IS NULL
    RAISERROR(N'ВНИМАНИЕ: нет таблиц отчёта по доставкам (I_DELIVERY_*) — колонки доставки будут пустыми.', 10, 1) WITH NOWAIT;

-- таблицы с такими именами, но ни старые (процедура 2025 года), ни этого файла — не трогаем
IF (OBJECT_ID('dbo.I_CVM_EFFECT') IS NOT NULL
    AND COL_LENGTH('dbo.I_CVM_EFFECT', 'sample_size_note') IS NULL AND COL_LENGTH('dbo.I_CVM_EFFECT', N'Пометки') IS NULL)
   OR (OBJECT_ID('dbo.I_CVM_EFFECT_CG') IS NOT NULL
    AND COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'SHARE_IN') IS NULL AND COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'IN_TOTAL') IS NULL)
BEGIN
    RAISERROR(N'СТОП: dbo.I_CVM_EFFECT или dbo.I_CVM_EFFECT_CG есть, но это ни старая витрина, ни витрина этого файла — ничего не меняю, напиши мне.', 16, 1);
    SET @abort = 1;
END

-- перед удалением старых витрин: их не должен упоминать ни один модуль, кроме старой CVM_EFFECT_TODAY (её текст
-- заменяется) и процедур шага 1 (их строки правит раздел 3). Имя — целым словом (I_CVM_EFFECT_MONTH и подобные
-- не в счёт); источники — тексты модулей и зависимости
SET @msg = NULL;
IF COL_LENGTH('dbo.I_CVM_EFFECT', 'sample_size_note') IS NOT NULL OR COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'SHARE_IN') IS NOT NULL
   OR (COL_LENGTH('dbo.I_CVM_EFFECT', N'Пометки') IS NOT NULL
       AND (COL_LENGTH('dbo.I_CVM_EFFECT', 'CHANNEL_ID') IS NULL OR COL_LENGTH('dbo.I_CVM_EFFECT', N'Источник КГ') IS NULL))
    SELECT @msg = left(STRING_AGG(CAST(x.NM AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY x.NM), 1500)
    FROM (SELECT NM = concat(OBJECT_SCHEMA_NAME(m.object_id), N'.', OBJECT_NAME(m.object_id))
          FROM sys.sql_modules AS m (nolock)
          WHERE (   m.definition LIKE N'%[^A-Za-z0-9_]I[_]CVM[_]EFFECT[^A-Za-z0-9_]%'
                 OR m.definition LIKE N'%[^A-Za-z0-9_]I[_]CVM[_]EFFECT'
                 OR m.definition LIKE N'%[^A-Za-z0-9_]I[_]CVM[_]EFFECT[_]CG[^A-Za-z0-9_]%'
                 OR m.definition LIKE N'%[^A-Za-z0-9_]I[_]CVM[_]EFFECT[_]CG')
            AND m.object_id NOT IN (isnull(OBJECT_ID(N'dbo.CVM_EFFECT_TODAY'), 0), isnull(OBJECT_ID(N'dbo.clear_data_update'), 0)
                                  , isnull(OBJECT_ID(N'dbo.reload_clear_data'), 0))
          UNION
          SELECT concat(OBJECT_SCHEMA_NAME(d.referencing_id), N'.', OBJECT_NAME(d.referencing_id))
          FROM sys.sql_expression_dependencies AS d (nolock)
          WHERE d.referenced_entity_name IN (N'I_CVM_EFFECT', N'I_CVM_EFFECT_CG')
            AND d.referencing_id NOT IN (isnull(OBJECT_ID(N'dbo.CVM_EFFECT_TODAY'), 0), isnull(OBJECT_ID(N'dbo.clear_data_update'), 0)
                                       , isnull(OBJECT_ID(N'dbo.reload_clear_data'), 0))) AS x;
IF @msg IS NOT NULL
BEGIN
    RAISERROR(N'СТОП: dbo.I_CVM_EFFECT / dbo.I_CVM_EFFECT_CG упоминают другие модули (%s) — витрины не пересоздаю, ничего не меняю; напиши мне.', 16, 1, @msg);
    SET @abort = 1;
END

IF @abort = 1 SET NOEXEC ON;   -- всё ниже не выполняется — до SET NOEXEC OFF в конце файла
GO

-- ============================== 1. ВИТРИНЫ ==============================
-- Старые витрины (колонки процедуры 2025 года) удаляются и создаются заново (решение Елены 13.10: «перепиши старую
-- таблицу, удали её»); витрина этого файла прежнего вида (без канала продаж) пересоздаётся; текущего вида — не трогается.
IF OBJECT_ID('dbo.I_CVM_EFFECT', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_CVM_EFFECT', 'sample_size_note') IS NOT NULL
BEGIN
    DROP TABLE dbo.I_CVM_EFFECT;
    RAISERROR(N'1. Старая dbo.I_CVM_EFFECT удалена.', 0, 1) WITH NOWAIT;
END
-- витрина этого файла в прежнем виде (строка на акцию, без канала продаж) — пересоздаётся, строки считаются заново (14.10)
IF OBJECT_ID('dbo.I_CVM_EFFECT', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_CVM_EFFECT', N'Пометки') IS NOT NULL
   AND (COL_LENGTH('dbo.I_CVM_EFFECT', 'CHANNEL_ID') IS NULL OR COL_LENGTH('dbo.I_CVM_EFFECT', N'Источник КГ') IS NULL)
BEGIN
    DROP TABLE dbo.I_CVM_EFFECT;
    RAISERROR(N'1. dbo.I_CVM_EFFECT прежнего вида (без канала продаж) удалена — создаётся заново, акции считаются заново.', 0, 1) WITH NOWAIT;
END
IF OBJECT_ID('dbo.I_CVM_EFFECT_CG', 'U') IS NOT NULL AND COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'SHARE_IN') IS NOT NULL
BEGIN
    DROP TABLE dbo.I_CVM_EFFECT_CG;
    RAISERROR(N'1. Старая dbo.I_CVM_EFFECT_CG удалена.', 0, 1) WITH NOWAIT;
END
GO

IF OBJECT_ID('dbo.I_CVM_EFFECT', 'U') IS NULL
BEGIN
    -- общий эффект по акции: строка на акцию (все сегменты, все каналы); суммы — в рублях, доли — в процентах
    CREATE TABLE dbo.I_CVM_EFFECT (
          [Дата расчета]                  datetime2(0)   NOT NULL
        , ID_COMPANY                      int            NOT NULL
        , [ID]                            int            NOT NULL   -- номер акции
        , [Название]                      nvarchar(250)  NULL
        , [Год]                           int            NOT NULL   -- по дате старта
        , [Месяц]                         int            NOT NULL
        , [Дата старта]                   date           NOT NULL   -- по ней шаг 1 стирает окно перезаливки
        , [Дата окончания]                date           NOT NULL
        , [После с]                       date           NULL       -- пусто — «после» не считается
        , [После по]                      date           NULL
        , [Сегмент]                       nvarchar(50)   NULL       -- из I_PROMO
        , [Механика]                      nvarchar(100)  NULL       -- из I_PROMO
        , ID_MECHANICS                    int            NULL
        , [Канал]                         nvarchar(50)   NULL       -- канал рассылки из I_PROMO
        , ID_ORGANIZATION                 int            NOT NULL   -- канал акции: 1 офлайн, 3 онлайн (по его чекам сегмент и «до»)
        , [Канал акции]                   nvarchar(10)   NOT NULL
        , CHANNEL_ID                      tinyint        NOT NULL   -- канал продаж строки: 0 все каналы (офлайн + онлайн), 1 офлайн, 3 онлайн
        , [Канал продаж]                  nvarchar(20)   NOT NULL   -- суммы по акции — строка «Все каналы»
        , [Источник КГ]                   nvarchar(60)   NOT NULL   -- «список» или ГКГ (КГ списка некорректна)
        , [Доля КГ в списке, %]           decimal(9, 3)  NULL
        , [Клиентов ЦГ]                   int            NOT NULL   -- весь список, с фродом
        , [Клиентов КГ]                   int            NOT NULL
        , [Фрод ЦГ]                       int            NOT NULL   -- вне оценки, скидка — в затратах
        , [Фрод КГ]                       int            NOT NULL
        , [Без покупок до ЦГ]             int            NOT NULL   -- «до» = 0 (в том числе без покупок в канале): вне оценки
        , [Без покупок до КГ]             int            NOT NULL
        , [Исключено выбросами ЦГ]        int            NOT NULL
        , [Исключено выбросами КГ]        int            NOT NULL
        , [Не с кем сравнить ЦГ]          int            NULL       -- неделя или интервал суммы «до» меньше 30 КГ
        , [Не с кем сравнить КГ]          int            NULL
        , [Убрано при выравнивании ЦГ]    int            NULL       -- лишние ЦГ / КГ в интервалах: доля КГ выровнена до доли недели
        , [Убрано при выравнивании КГ]    int            NULL
        , [Сегменты без оценки ЦГ]        int            NOT NULL   -- в сегменте меньше 2 клиентов КГ в оценке
        , [Сегменты без оценки КГ]        int            NOT NULL
        , [В оценке ЦГ]                   int            NOT NULL
        , [В оценке КГ]                   int            NOT NULL
        , [Слоёв]                         int            NULL
        , [Доп ТО в акцию, ₽]              decimal(19, 2) NULL       -- по слоям, сумма сегментов, все каналы
        , [Доп ТО в акцию ДИ от, ₽]        decimal(19, 2) NULL       -- интервал 95 %
        , [Доп ТО в акцию ДИ до, ₽]        decimal(19, 2) NULL
        , [Доп ТО в акцию мин. различимый эффект, ₽]          decimal(19, 2) NULL       -- 2,8 × ошибка
        , [Значимость в акцию]            nvarchar(40)   NULL
        , [Доп ТО после, ₽]                decimal(19, 2) NULL       -- пусто — «после» не считается
        , [Доп ТО после ДИ от, ₽]          decimal(19, 2) NULL
        , [Доп ТО после ДИ до, ₽]          decimal(19, 2) NULL
        , [Доп ТО после мин. различимый эффект, ₽]            decimal(19, 2) NULL
        , [Значимость после]              nvarchar(40)   NULL
        , [Доп ТО итого, ₽]                decimal(19, 2) NULL       -- в акцию + после (без «после» — = в акцию)
        , [Доп ТО итого ДИ от, ₽]          decimal(19, 2) NULL
        , [Доп ТО итого ДИ до, ₽]          decimal(19, 2) NULL
        , [Доп ТО итого мин. различимый эффект, ₽]            decimal(19, 2) NULL
        , [Значимость итого]              nvarchar(40)   NULL
        , [Доп ТО в акцию регрессия, ₽]    decimal(19, 2) NULL       -- сверка: регрессия по КГ в слоях
        , [Доп ТО итого регрессия, ₽]      decimal(19, 2) NULL
        , [Псевдоэффект до старта, ₽]      decimal(19, 2) NULL       -- та же формула на последнем отрезке «до» (до акции): должен быть ≈ 0
        , [Псевдоэффект ДИ от, ₽]          decimal(19, 2) NULL
        , [Псевдоэффект ДИ до, ₽]          decimal(19, 2) NULL
        , [КГ несопоставима]              bit            NULL       -- псевдоэффект «Все каналы» дальше 3,29 ошибки: вне общего эффекта месяца
        , [Доп ТО в акцию на клиента, ₽]   decimal(19, 2) NULL       -- на клиента ЦГ выровненного списка
        , [Доп ТО после на клиента, ₽]     decimal(19, 2) NULL
        , [Доп ТО итого на клиента, ₽]     decimal(19, 2) NULL
        , [Доп ТО в акцию на всю ЦГ, ₽]    decimal(19, 2) NULL       -- пересчёт на всех клиентов ЦГ в оценке (примерный)
        , [Доп ТО после на всю ЦГ, ₽]      decimal(19, 2) NULL
        , [Доп ТО итого на всю ЦГ, ₽]      decimal(19, 2) NULL
        , [Купили в акцию ЦГ %]           decimal(9, 2)  NULL
        , [Купили в акцию КГ при составе ЦГ %]           decimal(9, 2)  NULL       -- при составе ЦГ по слоям: ЦГ − отклик
        , [Отклик в акцию п.п.]           decimal(9, 2)  NULL
        , [Инкр. покупателей в акцию]     decimal(19, 1) NULL
        , [Купили после ЦГ %]             decimal(9, 2)  NULL
        , [Купили после КГ при составе ЦГ %]             decimal(9, 2)  NULL
        , [Отклик после п.п.]             decimal(9, 2)  NULL
        , [Инкр. покупателей после]       decimal(19, 1) NULL
        -- шесть показателей: ЦГ, КГ (ожидание ЦГ без акции с привязкой к «до»), инкремент — в акцию и после
        , [Клиентов ЦГ в акцию]                     int            NULL
        , [ARPU в акцию ЦГ, ₽]                      decimal(19, 2) NULL
        , [ARPU в акцию КГ, ₽]                      decimal(19, 2) NULL
        , [ARPU в акцию инкр., ₽]                   decimal(19, 2) NULL
        , [ARPU в акцию инкр., %]                   decimal(9, 2)  NULL
        , [Ср. чек в акцию ЦГ, ₽]                   decimal(19, 2) NULL
        , [Ср. чек в акцию КГ, ₽]                   decimal(19, 2) NULL
        , [Ср. чек в акцию инкр., ₽]                decimal(19, 2) NULL
        , [Ср. чек в акцию инкр., %]                decimal(9, 2)  NULL
        , [Чеков на клиента в акцию ЦГ]             decimal(19, 4) NULL
        , [Чеков на клиента в акцию КГ]             decimal(19, 4) NULL
        , [Чеков на клиента в акцию инкр.]          decimal(19, 4) NULL
        , [Чеков на клиента в акцию инкр., %]       decimal(9, 2)  NULL
        , [SKU в чеке в акцию ЦГ]                   decimal(19, 4) NULL
        , [SKU в чеке в акцию КГ]                   decimal(19, 4) NULL
        , [SKU в чеке в акцию инкр.]                decimal(19, 4) NULL
        , [SKU в чеке в акцию инкр., %]             decimal(9, 2)  NULL
        , [Цена SKU в акцию ЦГ, ₽]                  decimal(19, 2) NULL
        , [Цена SKU в акцию КГ, ₽]                  decimal(19, 2) NULL
        , [Цена SKU в акцию инкр., ₽]               decimal(19, 2) NULL
        , [Цена SKU в акцию инкр., %]               decimal(9, 2)  NULL
        , [Клиентов ЦГ после]                       int            NULL
        , [ARPU после ЦГ, ₽]                        decimal(19, 2) NULL
        , [ARPU после КГ, ₽]                        decimal(19, 2) NULL
        , [ARPU после инкр., ₽]                     decimal(19, 2) NULL
        , [ARPU после инкр., %]                     decimal(9, 2)  NULL
        , [Ср. чек после ЦГ, ₽]                     decimal(19, 2) NULL
        , [Ср. чек после КГ, ₽]                     decimal(19, 2) NULL
        , [Ср. чек после инкр., ₽]                  decimal(19, 2) NULL
        , [Ср. чек после инкр., %]                  decimal(9, 2)  NULL
        , [Чеков на клиента после ЦГ]               decimal(19, 4) NULL
        , [Чеков на клиента после КГ]               decimal(19, 4) NULL
        , [Чеков на клиента после инкр.]            decimal(19, 4) NULL
        , [Чеков на клиента после инкр., %]         decimal(9, 2)  NULL
        , [SKU в чеке после ЦГ]                     decimal(19, 4) NULL
        , [SKU в чеке после КГ]                     decimal(19, 4) NULL
        , [SKU в чеке после инкр.]                  decimal(19, 4) NULL
        , [SKU в чеке после инкр., %]               decimal(9, 2)  NULL
        , [Цена SKU после ЦГ, ₽]                    decimal(19, 2) NULL
        , [Цена SKU после КГ, ₽]                    decimal(19, 2) NULL
        , [Цена SKU после инкр., ₽]                 decimal(19, 2) NULL
        , [Цена SKU после инкр., %]                 decimal(9, 2)  NULL
        , [Скидка ЦГ, ₽]                   decimal(19, 2) NOT NULL   -- старт … финиш + 4, по модулю
        , [Скидка КГ, ₽]                   decimal(19, 2) NOT NULL
        , [Скидка всего, ₽]                decimal(19, 2) NOT NULL
        , [Скидка в дни акции, ₽]          decimal(19, 2) NOT NULL   -- старт … финиш, ЦГ и КГ
        , [Скидка в PL в акцию]           nvarchar(30)   NOT NULL   -- за какие дни скидка вычтена в «PL в акцию»
        , [PL в акцию, ₽]                  decimal(19, 2) NULL
        , [PL в акцию ДИ от, ₽]            decimal(19, 2) NULL
        , [PL в акцию ДИ до, ₽]            decimal(19, 2) NULL
        , [PL итого, ₽]                    decimal(19, 2) NULL
        , [PL итого ДИ от, ₽]              decimal(19, 2) NULL
        , [PL итого ДИ до, ₽]              decimal(19, 2) NULL
        , [PL итого на всю ЦГ, ₽]          decimal(19, 2) NULL       -- из Доп ТО итого на всю ЦГ
        , [Рассылок]                     int            NULL       -- рассылок акции в отчёте по доставкам (Push)
        , [Отправлено сообщений]                    bigint         NULL
        , [Отправлено на 100 клиентов ЦГ] decimal(9, 2)  NULL
        , [Доставлено %]                  decimal(9, 2)  NULL       -- доставлено / отправлено
        , [Переходы %]                    decimal(9, 2)  NULL       -- переходы / доставлено
        , [Ошибки %]                      decimal(9, 2)  NULL       -- ошибки / (отправлено + ошибки)
        , [Каппа выбросов]                float          NOT NULL
        , [Предварительная]               bit            NOT NULL   -- финиш + 4 в окне перезаливки чеков — пересчитается
        , [После не считается]            bit            NOT NULL   -- однодневная акция без кешбэка
        , [Нет КГ]                        bit            NOT NULL
        , [Кампании не найдены]           bit            NOT NULL   -- скидка 0
        , [Пометки]                       nvarchar(2000) NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_EFFECT ON dbo.I_CVM_EFFECT;
    RAISERROR(N'1. dbo.I_CVM_EFFECT создана.', 0, 1) WITH NOWAIT;
END

IF OBJECT_ID('dbo.I_CVM_EFFECT_CG', 'U') IS NULL
BEGIN
    -- данные по слоям: акция × сегмент × слой × период × канал
    CREATE TABLE dbo.I_CVM_EFFECT_CG (
          ID_COMPANY      int            NOT NULL
        , ID_PROMO        int            NOT NULL
        , START_DATE      date           NOT NULL   -- старт акции: по нему шаг 1 стирает окно перезаливки
        , FINISH_DATE     date           NOT NULL
        , ID_ORGANIZATION int            NOT NULL   -- канал акции: 1 офлайн, 3 онлайн
        , SEGMENT_ID      tinyint        NOT NULL   -- сегмент на старте: 2 Активные, 1 Новые, 5 Отток, 6 Спящие
        , SEGMENT_NAME    nvarchar(20)   NOT NULL
        , LAYER_NO        smallint       NOT NULL   -- неделя давности (у Новых — стажа, у Спящих — 4-недельный блок): 1 — самая свежая
        , LAYER_BASIS     nvarchar(40)   NOT NULL   -- по чему слой
        , WEEK_FROM       smallint       NOT NULL   -- недели слоя (давности или стажа), с … по
        , WEEK_TO         smallint       NOT NULL
        , X_BASIS         nvarchar(40)   NOT NULL   -- что взято за ТО «до»
        , PERIOD_ID       tinyint        NOT NULL   -- 1 в акцию, 2 после, 3 итого
        , PERIOD_NAME     nvarchar(10)   NOT NULL
        , DATE_FROM       date           NOT NULL
        , DATE_TO         date           NOT NULL
        , CHANNEL_ID      tinyint        NOT NULL   -- база канала: 1 офлайн, 3 онлайн (сумма — в I_CVM_EFFECT, CHANNEL_ID = 0)
        , CHANNEL_NAME    nvarchar(12)   NOT NULL
        , CLIENTS_TG      int            NOT NULL   -- клиентов в оценке ЦГ
        , CLIENTS_CG      int            NOT NULL   -- клиентов в оценке КГ
        , TO_BEFORE_TG    decimal(19, 2) NOT NULL   -- ТО «до» ЦГ (ΣX)
        , TO_BEFORE_CG    decimal(19, 2) NOT NULL
        , TO_PERIOD_TG    decimal(19, 2) NOT NULL   -- ТО периода ЦГ в канале (ΣY), до скидки
        , TO_PERIOD_CG    decimal(19, 2) NOT NULL
        , EXPECTED_TG     decimal(19, 2) NULL       -- ожидаемый ТО ЦГ без акции = ΣX_ЦГ · ΣY_КГ / ΣX_КГ
        , ADD_TO          decimal(19, 2) NULL       -- Доп ТО = ΣY_ЦГ − ожидаемый
        , ADD_TO_SE       decimal(19, 2) NULL       -- ошибка Доп ТО
        , ADD_TO_REG      decimal(19, 2) NULL       -- Доп ТО регрессией по КГ (сверка)
        , BUYERS_TG       int            NULL       -- купивших в периоде и канале (итого — пусто)
        , BUYERS_CG       int            NULL
        , CHECKS_TG       int            NULL       -- чеков периода ЦГ в канале
        , CHECKS_CG       int            NULL
        , SKU_TG          decimal(19, 3) NULL       -- SKU периода ЦГ в канале (сумма поля SKU чеков)
        , SKU_CG          decimal(19, 3) NULL
        , EXP_CHECKS_TG   decimal(19, 3) NULL       -- ожидаемые чеки ЦГ без акции = чеки КГ · ΣX_ЦГ / ΣX_КГ
        , EXP_SKU_TG      decimal(19, 3) NULL       -- ожидаемые SKU ЦГ без акции = SKU КГ · ΣX_ЦГ / ΣX_КГ
        , CLIENTS_TG_ALL  int            NULL       -- клиентов ЦГ / КГ недели в оценке до выравнивания по сумме покупок «до»
        , CLIENTS_CG_ALL  int            NULL
        , KG_SHARE        decimal(9, 3)  NULL       -- доля КГ недели после выравнивания, % (минимальная по интервалам суммы, не ниже 3 %)
        , INTERVALS       int            NULL       -- интервалов суммы «до» в сравнении (соседние интервалы шкалы слиты до 30 КГ)
        , IN_TOTAL        bit            NOT NULL   -- 1 — слой входит в итог акции (в I_CVM_EFFECT)
        , PRELIM          bit            NOT NULL   -- предварительная
        , CALC_DT         datetime2(0)   NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_EFFECT_CG ON dbo.I_CVM_EFFECT_CG;
    RAISERROR(N'1. dbo.I_CVM_EFFECT_CG создана.', 0, 1) WITH NOWAIT;
END
GO


-- витрины на месте и устроены так, как ждёт процедура (если они от прошлой версии этого файла — СТОП)
IF EXISTS (SELECT 1
           FROM (VALUES (N'Дата расчета'), (N'ID_COMPANY'), (N'ID'), (N'Дата старта'), (N'Дата окончания'), (N'После с')
                      , (N'Клиентов ЦГ'), (N'Сегменты без оценки КГ'), (N'В оценке КГ'), (N'Доп ТО итого, ₽')
                      , (N'Доп ТО итого регрессия, ₽'), (N'Инкр. покупателей после'), (N'Скидка в дни акции, ₽')
                      , (N'Скидка в PL в акцию'), (N'PL итого ДИ до, ₽'), (N'Ошибки %'), (N'Каппа выбросов')
                      , (N'Кампании не найдены'), (N'Пометки'), (N'CHANNEL_ID'), (N'Источник КГ')) AS c (C)
           WHERE COL_LENGTH('dbo.I_CVM_EFFECT', c.C) IS NULL)
   OR EXISTS (SELECT 1
              FROM (VALUES ('ID_COMPANY'), ('ID_PROMO'), ('START_DATE'), ('SEGMENT_ID'), ('LAYER_NO'), ('WEEK_TO')
                         , ('X_BASIS'), ('PERIOD_ID'), ('CHANNEL_ID'), ('EXPECTED_TG'), ('ADD_TO_SE'), ('ADD_TO_REG')
                         , ('BUYERS_CG'), ('IN_TOTAL'), ('PRELIM'), ('CALC_DT')) AS c (C)
              WHERE COL_LENGTH('dbo.I_CVM_EFFECT_CG', c.C) IS NULL)
BEGIN
    RAISERROR(N'СТОП: витрины dbo.I_CVM_EFFECT / dbo.I_CVM_EFFECT_CG не создались или устроены иначе — процедуру и чистку шага 1 не ставлю; напиши мне.', 16, 1);
    SET NOEXEC ON;
END
GO
-- (после проверки устройства) витрины текущего вида без шести показателей (до 18.10) — колонки добавляются, строки остаются до пересчёта
-- (портал не пустеет); новые колонки пустые, пока акция не пересчитана (CVM_EFFECT_RUN.sql)
DECLARE @c sysname, @t nvarchar(30), @sql nvarchar(400), @added int = 0;
DECLARE cur_add CURSOR LOCAL FAST_FORWARD FOR
    SELECT N'dbo.I_CVM_EFFECT', v.C, v.T
    FROM (VALUES
                 (N'Псевдоэффект до старта, ₽', N'decimal(19, 2)'),
                 (N'Псевдоэффект ДИ от, ₽', N'decimal(19, 2)'),
                 (N'Псевдоэффект ДИ до, ₽', N'decimal(19, 2)'),
                 (N'КГ несопоставима', N'bit'),
                 (N'Не с кем сравнить ЦГ', N'int'),
                 (N'Не с кем сравнить КГ', N'int'),
                 (N'Убрано при выравнивании ЦГ', N'int'),
                 (N'Убрано при выравнивании КГ', N'int'),
                 (N'Доп ТО в акцию на клиента, ₽', N'decimal(19, 2)'),
                 (N'Доп ТО после на клиента, ₽', N'decimal(19, 2)'),
                 (N'Доп ТО итого на клиента, ₽', N'decimal(19, 2)'),
                 (N'Доп ТО в акцию на всю ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Доп ТО после на всю ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Доп ТО итого на всю ЦГ, ₽', N'decimal(19, 2)'),
                 (N'PL итого на всю ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Клиентов ЦГ в акцию', N'int'),
                 (N'ARPU в акцию ЦГ, ₽', N'decimal(19, 2)'),
                 (N'ARPU в акцию КГ, ₽', N'decimal(19, 2)'),
                 (N'ARPU в акцию инкр., ₽', N'decimal(19, 2)'),
                 (N'ARPU в акцию инкр., %', N'decimal(9, 2)'),
                 (N'Ср. чек в акцию ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Ср. чек в акцию КГ, ₽', N'decimal(19, 2)'),
                 (N'Ср. чек в акцию инкр., ₽', N'decimal(19, 2)'),
                 (N'Ср. чек в акцию инкр., %', N'decimal(9, 2)'),
                 (N'Чеков на клиента в акцию ЦГ', N'decimal(19, 4)'),
                 (N'Чеков на клиента в акцию КГ', N'decimal(19, 4)'),
                 (N'Чеков на клиента в акцию инкр.', N'decimal(19, 4)'),
                 (N'Чеков на клиента в акцию инкр., %', N'decimal(9, 2)'),
                 (N'SKU в чеке в акцию ЦГ', N'decimal(19, 4)'),
                 (N'SKU в чеке в акцию КГ', N'decimal(19, 4)'),
                 (N'SKU в чеке в акцию инкр.', N'decimal(19, 4)'),
                 (N'SKU в чеке в акцию инкр., %', N'decimal(9, 2)'),
                 (N'Цена SKU в акцию ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Цена SKU в акцию КГ, ₽', N'decimal(19, 2)'),
                 (N'Цена SKU в акцию инкр., ₽', N'decimal(19, 2)'),
                 (N'Цена SKU в акцию инкр., %', N'decimal(9, 2)'),
                 (N'Клиентов ЦГ после', N'int'),
                 (N'ARPU после ЦГ, ₽', N'decimal(19, 2)'),
                 (N'ARPU после КГ, ₽', N'decimal(19, 2)'),
                 (N'ARPU после инкр., ₽', N'decimal(19, 2)'),
                 (N'ARPU после инкр., %', N'decimal(9, 2)'),
                 (N'Ср. чек после ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Ср. чек после КГ, ₽', N'decimal(19, 2)'),
                 (N'Ср. чек после инкр., ₽', N'decimal(19, 2)'),
                 (N'Ср. чек после инкр., %', N'decimal(9, 2)'),
                 (N'Чеков на клиента после ЦГ', N'decimal(19, 4)'),
                 (N'Чеков на клиента после КГ', N'decimal(19, 4)'),
                 (N'Чеков на клиента после инкр.', N'decimal(19, 4)'),
                 (N'Чеков на клиента после инкр., %', N'decimal(9, 2)'),
                 (N'SKU в чеке после ЦГ', N'decimal(19, 4)'),
                 (N'SKU в чеке после КГ', N'decimal(19, 4)'),
                 (N'SKU в чеке после инкр.', N'decimal(19, 4)'),
                 (N'SKU в чеке после инкр., %', N'decimal(9, 2)'),
                 (N'Цена SKU после ЦГ, ₽', N'decimal(19, 2)'),
                 (N'Цена SKU после КГ, ₽', N'decimal(19, 2)'),
                 (N'Цена SKU после инкр., ₽', N'decimal(19, 2)'),
                 (N'Цена SKU после инкр., %', N'decimal(9, 2)')) AS v (C, T)
    WHERE COL_LENGTH('dbo.I_CVM_EFFECT', v.C) IS NULL
    UNION ALL
    SELECT N'dbo.I_CVM_EFFECT_CG', v.C, v.T
    FROM (VALUES
                 (N'CHECKS_TG', N'int'),
                 (N'CHECKS_CG', N'int'),
                 (N'SKU_TG', N'decimal(19, 3)'),
                 (N'SKU_CG', N'decimal(19, 3)'),
                 (N'EXP_CHECKS_TG', N'decimal(19, 3)'),
                 (N'EXP_SKU_TG', N'decimal(19, 3)'),
                 (N'CLIENTS_TG_ALL', N'int'),
                 (N'CLIENTS_CG_ALL', N'int'),
                 (N'KG_SHARE', N'decimal(9, 3)'),
                 (N'INTERVALS', N'int')) AS v (C, T)
    WHERE COL_LENGTH('dbo.I_CVM_EFFECT_CG', v.C) IS NULL;
DECLARE @tbl sysname;
OPEN cur_add;
FETCH NEXT FROM cur_add INTO @tbl, @c, @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = concat(N'ALTER TABLE ', @tbl, N' ADD ', QUOTENAME(@c), N' ', @t, N' NULL;');
    EXEC sys.sp_executesql @sql;
    SET @added += 1;
    FETCH NEXT FROM cur_add INTO @tbl, @c, @t;
END
CLOSE cur_add;
DEALLOCATE cur_add;
IF @added > 0
    RAISERROR(N'1. В витрины добавлены колонки шести показателей: %d (пустые до пересчёта акций).', 0, 1, @added) WITH NOWAIT;
GO

IF COL_LENGTH('dbo.I_CVM_EFFECT', N'Клиентов ЦГ в акцию') IS NULL OR COL_LENGTH('dbo.I_CVM_EFFECT', N'PL итого на всю ЦГ, ₽') IS NULL
   OR COL_LENGTH('dbo.I_CVM_EFFECT', N'КГ несопоставима') IS NULL OR COL_LENGTH('dbo.I_CVM_EFFECT', N'Цена SKU после инкр., %') IS NULL
   OR COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'CHECKS_TG') IS NULL OR COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'INTERVALS') IS NULL
BEGIN
    RAISERROR(N'СТОП: колонки шести показателей в витрины не добавились — процедуру и чистку шага 1 не ставлю; напиши мне.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 2. ПРОЦЕДУРА dbo.CVM_EFFECT_TODAY ==============================
-- Временные таблицы — #ce_*: процедура видит временные таблицы вызвавшей сессии, общие имена задели бы рабочие
-- таблицы Елены. Строковые колонки #-таблиц — COLLATE DATABASE_DEFAULT.
CREATE OR ALTER PROCEDURE dbo.CVM_EFFECT_TODAY
      @id_promo           int          = NULL                -- одна акция
    , @date_from          date         = NULL                -- ИСТОРИЯ: старт акции с … (задан только @date_to — с 01.09.2025)
    , @date_to            date         = NULL                -- … по (пусто или позже вчера — вчера); от новых к старым
    , @only_missing       bit          = 0                   -- ИСТОРИЯ: 1 — только акции, которых нет в I_CVM_EFFECT
    , @calc_before        datetime2(0) = NULL                -- ИСТОРИЯ: ещё и акции, посчитанные раньше этого времени (полный
                                                             --   пересчёт по частям: повторный запуск продолжает с непересчитанных)
    , @stop_at            datetime2(0) = NULL                -- после этого времени новые порции и акции не начинаются;
                                                             --   только время ('23:30') — сегодня в это время
    , @night_days         int          = 120                 -- НОЧЬ: акции, которых нет в витрине, — со стартом не раньше
                                                             --   сегодня − @night_days (перезаливка первой пятницы ~3 мес.)
    , @part_promos        int          = 10                  -- акций в порции (чеки — один проход на порцию)
    , @part_days          int          = 7                   -- разброс дат старта в порции, дней
    , @kappa              float        = 0.5                 -- κ выбросов
    , @cashback_mechanics varchar(200) = '1,2,8,9,10,15,16,17'  -- ID_MECHANICS с кешбэком: однодневным акциям с другой
                                                             --   механикой «после» не считается
    , @pl_in_discount     varchar(10)  = 'PROMO_DAYS'        -- «PL в акцию»: 'PROMO_DAYS' — скидка дней акции, 'ALL' — старт … финиш + 4
    , @left               int          = NULL OUTPUT         -- после запуска: акций выборки, которых нет в I_CVM_EFFECT
                                                             --   (с @calc_before — и посчитанных раньше него)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET IMPLICIT_TRANSACTIONS OFF;
    -- CVM_EFFECT_TODAY: методика 2026-10

    DECLARE @idc          int   = 1;
    DECLARE @hist_from    date  = '20250901';           -- акции со стартом с 01.09.2025 (решение 12.10)
    DECLARE @pre_days     int   = 42;                   -- «до» Активных и Новых: старт − 42 … старт − 1; граница Активных
    DECLARE @hist_days    int   = 105;                  -- граница Оттока; окно чеков для сегмента: старт − 105 … старт − 1
    DECLARE @after_days   int   = 4;                    -- «после»: финиш + 1 … финиш + 4
    DECLARE @layer_min_cg int   = 30;                   -- в неделе и в интервале суммы — не меньше 30 клиентов КГ
    DECLARE @kg_share_min float = 0.03;                 -- нижняя граница доли КГ недели (решение 19.10)
    DECLARE @iv_last1     int, @iv_last2 int;           -- последний интервал шкал 100 ₽ / 500 ₽ (сумма выше всех границ)
    DECLARE @z_pseudo     float = 3.29;                 -- «КГ несопоставима»: псевдоэффект «до старта» дальше 3,29 ошибки (решение 19.10)
    DECLARE @margin       float = 0.3;                  -- PL: маржа
    DECLARE @vat          float = 1.15;                 -- PL: средний НДС
    DECLARE @q75          float = 0.6744897501960817;   -- квантиль нормального распределения для 0,75
    DECLARE @reload_days  int   = 10;                   -- ежедневная перезаливка чеков: последние 10 дней
    DECLARE @today        date  = CAST(getdate() AS date);
    DECLARE @yesterday    date  = dateadd(day, -1, CAST(getdate() AS date));   -- сегодняшних чеков ещё нет

    DECLARE @mode varchar(10), @lock int, @locked bit = 0, @disc_ok bit = 1, @dlv_ok bit = 1;
    DECLARE @msg nvarchar(2000), @t_run datetime2(0) = sysdatetime(), @t datetime2(0), @t_part datetime2(0), @now datetime2(0);
    DECLARE @n int, @i int, @k int, @part int, @parts int, @pid int, @ord int, @sd date, @fd date, @org int, @ref date;
    DECLARE @hf date, @pf date, @pt date, @af date, @at date, @d_from date, @d_to date, @p_first date, @p_cnt int, @refs_from date
          , @ref1 date, @ref3 date, @gb date;
    DECLARE @z_kg float = 3.29;   -- доля КГ списка ниже 5 % дальше 3,29 ошибки (p < 0,001, шкала значимости отчёта) — пометка
    DECLARE @pass int;
    DECLARE @done int = 0, @errors int = 0, @rows bigint, @tok varchar(200), @pos int, @val int, @stopped bit = 0;

    IF @@TRANCOUNT > 0
        THROW 50001, N'CVM_EFFECT_TODAY: запуск внутри открытой транзакции — запустить без BEGIN TRANSACTION; ничего не записано.', 1;
    IF @kappa IS NULL OR @kappa <= 0
        THROW 50001, N'CVM_EFFECT_TODAY: @kappa — больше 0; ничего не записано.', 1;
    IF @pl_in_discount IS NULL OR @pl_in_discount NOT IN ('PROMO_DAYS', 'ALL')
        THROW 50001, N'CVM_EFFECT_TODAY: @pl_in_discount — ''PROMO_DAYS'' или ''ALL''; ничего не записано.', 1;
    IF @part_promos IS NULL OR @part_promos < 1 OR @part_days IS NULL OR @part_days < 0 OR @night_days IS NULL OR @night_days < 0
       OR @only_missing IS NULL
        THROW 50001, N'CVM_EFFECT_TODAY: @part_promos — не меньше 1, @part_days и @night_days — не меньше 0, @only_missing — 0 или 1; ничего не записано.', 1;

    SET @mode = CASE WHEN @id_promo IS NOT NULL THEN 'PROMO'
                     WHEN @date_from IS NOT NULL OR @date_to IS NOT NULL THEN 'RANGE'
                     ELSE 'NIGHT' END;
    IF @mode = 'RANGE'
    BEGIN
        IF @date_from IS NULL SET @date_from = @hist_from;
        IF @date_to IS NULL OR @date_to > @yesterday SET @date_to = @yesterday;
        IF @date_from > @date_to
        BEGIN
            SET @msg = concat(N'CVM_EFFECT_TODAY: в периоде ', convert(nchar(10), @date_from, 104), N'–', convert(nchar(10), @date_to, 104)
                            , N' нет дней (старт акции — не позже вчера); ничего не записано.');
            THROW 50001, @msg, 1;
        END
    END
    -- только время ('23:30') — сегодня в это время
    IF @stop_at IS NOT NULL AND @stop_at < '20000101'
        SET @stop_at = dateadd(second, datediff(second, CAST('19000101' AS datetime2(0)), @stop_at), CAST(@today AS datetime2(0)));

    -- механики с кешбэком — из списка через запятую
    CREATE TABLE #ce_mech (ID_MECHANICS int NOT NULL PRIMARY KEY);
    SET @tok = replace(isnull(@cashback_mechanics, ''), ' ', '');
    WHILE len(@tok) > 0
    BEGIN
        SET @pos = CHARINDEX(',', @tok);
        SET @val = TRY_CAST(CASE WHEN @pos > 0 THEN left(@tok, @pos - 1) ELSE @tok END AS int);
        IF @val IS NULL
            THROW 50001, N'CVM_EFFECT_TODAY: @cashback_mechanics — номера механик через запятую (например ''1,2,8''); ничего не записано.', 1;
        IF NOT EXISTS (SELECT 1 FROM #ce_mech WHERE ID_MECHANICS = @val)
            INSERT INTO #ce_mech (ID_MECHANICS) VALUES (@val);
        SET @tok = CASE WHEN @pos > 0 THEN substring(@tok, @pos + 1, 200) ELSE '' END;
    END

    -- ---------- временные таблицы ----------
    -- акции отчёта (сотни строк), уже посчитанные, выбранные к расчёту
    CREATE TABLE #ce_promo (ID_PROMO int NOT NULL PRIMARY KEY, PROMO_NAME nvarchar(250) COLLATE DATABASE_DEFAULT NULL
                          , START_DATE date NULL, FINISH_DATE date NULL, SEGMENT nvarchar(50) COLLATE DATABASE_DEFAULT NULL
                          , MECHANICS nvarchar(100) COLLATE DATABASE_DEFAULT NULL, ID_MECHANICS int NULL
                          , CHANNEL nvarchar(50) COLLATE DATABASE_DEFAULT NULL
                          , ORG int NOT NULL              -- канал акции: 1 офлайн, 3 онлайн
                          , CASHBACK tinyint NOT NULL     -- механика из @cashback_mechanics
                          , DATE_ROWS int NOT NULL        -- строк I_PROMO с разными датами
                          , BAD tinyint NOT NULL          -- нет дат старта / окончания или финиш раньше старта
                          , THEM tinyint NOT NULL);       -- тематическая рассылка — «после» не считается (решение 14.10)
    CREATE TABLE #ce_done  (ID_PROMO int NOT NULL PRIMARY KEY, PRELIM tinyint NOT NULL, CALC_DT datetime2(0) NULL);
    CREATE TABLE #ce_sel   (ID_PROMO int NOT NULL PRIMARY KEY, START_DATE date NOT NULL, FINISH_DATE date NOT NULL
                          , CHK_TO date NOT NULL          -- последний день окна скидки, не позже вчера
                          , WHY nvarchar(40) COLLATE DATABASE_DEFAULT NOT NULL
                          , READY tinyint NOT NULL, NOT_READY nvarchar(200) COLLATE DATABASE_DEFAULT NULL
                          , ORD int NULL, PART_NO int NULL);
    CREATE TABLE #ce_lcand (ID_PROMO int NOT NULL, DATA date NOT NULL, ID_CAMPAIGN int NOT NULL);   -- поздние связи (обычно пусто)
    -- акции порции и опорные даты
    CREATE TABLE #ce_pr    (ID_PROMO int NOT NULL PRIMARY KEY, START_DATE date NOT NULL, FINISH_DATE date NOT NULL, ORG int NOT NULL
                          , HIST_FROM date NOT NULL, PRE_FROM date NOT NULL, PRE_TO date NOT NULL
                          , AFT_FROM date NOT NULL, AFT_TO date NOT NULL
                          , AFTER_ON tinyint NOT NULL     -- 1 — «после» считается
                          , PRELIM tinyint NOT NULL       -- 1 — предварительная
                          , REF_DATE date NULL            -- снимок I_CVM_CONTACT_DATA канала акции
                          , REF1 date NULL, REF3 date NULL -- снимки офлайн / онлайн (база канала)
                          , CTRL tinyint NOT NULL DEFAULT 0  -- контроль: 0 — КГ списка, 1 — ГКГ (КГ списка некорректна)
                          , CTRL_SRC nvarchar(60) COLLATE DATABASE_DEFAULT NULL
                          , KG_SHARE float NULL           -- доля КГ в списке
                          , ORD int NOT NULL);
    -- акция на базе канала продаж: ID = номер акции · 10 + канал (1 офлайн, 3 онлайн); сегмент, слои, «до» и выбросы — по чекам канала
    CREATE TABLE #ce_vp    (ID_PROMO int NOT NULL PRIMARY KEY, REAL_ID int NOT NULL, BASIS tinyint NOT NULL, AFTER_ON tinyint NOT NULL);
    -- состав ГКГ на дату (кэш на весь запуск): K — дата партии, 1900-01-01 — архив прежней ГКГ, 1900-01-02 — текущая ГКГ
    CREATE TABLE #ce_gcg   (K date NOT NULL, ID_CONTACT int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_gcg ON #ce_gcg;
    CREATE TABLE #ce_rd    (RN int NOT NULL PRIMARY KEY, D date NOT NULL, ORG int NOT NULL);
    CREATE TABLE #ce_fraud (ID_CONTACT int NOT NULL PRIMARY KEY);
    -- клиенты и чеки порции (миллионы строк) — колоночные
    CREATE TABLE #ce_offer (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, HAS_TG tinyint NOT NULL, HAS_CG tinyint NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_offer ON #ce_offer;
    CREATE TABLE #ce_list  (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL
                          , CG tinyint NOT NULL           -- 0 ЦГ, 1 КГ
                          , IS_FRAUD tinyint NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_list ON #ce_list;
    CREATE TABLE #ce_cust  (ID_CONTACT int NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_cust ON #ce_cust;
    CREATE TABLE #ce_snap  (REF_DATE date NOT NULL, ORG int NOT NULL, ID_CONTACT int NOT NULL
                          , FIRST_DATA date NULL, LAST_DATA date NULL, LTV float NULL
                          , X_ALL float NULL);            -- ТО до скидки за всю историю по дату снимка
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_snap ON #ce_snap;
    CREATE TABLE #ce_ch    (ID_CONTACT int NOT NULL, DATA date NOT NULL, ID_ORGANIZATION int NOT NULL
                          , TO_RUB decimal(19, 2) NOT NULL     -- ТО чека до скидки
                          , SKU int NOT NULL);                 -- SKU чека (поле I_CHECKHEADER.SKU); строка = чек
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_ch ON #ce_ch;
    CREATE TABLE #ce_agg   (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL
                          , LAST_W1 date NULL, FIRST_W1 date NULL, LAST_W3 date NULL, FIRST_W3 date NULL  -- последняя / первая покупка: офлайн / онлайн, старт − 105 … старт − 1
                          , X42_1 decimal(19, 2) NOT NULL, X42_3 decimal(19, 2) NOT NULL  -- ТО за 42 дня до старта: офлайн / онлайн
                          , XG1 decimal(19, 2) NOT NULL, XG3 decimal(19, 2) NOT NULL      -- ТО после даты снимка канала … старт − 1
                          , Y1 decimal(19, 2) NOT NULL, Y3 decimal(19, 2) NOT NULL   -- ТО в дни акции: офлайн / онлайн
                          , A1 decimal(19, 2) NOT NULL, A3 decimal(19, 2) NOT NULL   -- ТО «после»: офлайн / онлайн
                          , B_IN tinyint NOT NULL, B_IN1 tinyint NOT NULL, B_IN3 tinyint NOT NULL       -- купил в дни акции: любой / офлайн / онлайн
                          , B_AFT tinyint NOT NULL, B_AFT1 tinyint NOT NULL, B_AFT3 tinyint NOT NULL   -- купил «после»
                          , C1 int NOT NULL, C3 int NOT NULL, K1 int NOT NULL, K3 int NOT NULL       -- чеки и SKU в дни акции: офлайн / онлайн
                          , CA1 int NOT NULL, CA3 int NOT NULL, KA1 int NOT NULL, KA3 int NOT NULL  -- чеки и SKU «после»
                          -- ТО по неделям давности 1–15 (неделя w — дни старт − 7w … старт − 7w + 6): офлайн _1, онлайн _3 —
                          -- для проверки «до старта» (неделя последней покупки против «до» раньше неё)
                          , W1_1 decimal(19, 2) NOT NULL, W2_1 decimal(19, 2) NOT NULL, W3_1 decimal(19, 2) NOT NULL, W4_1 decimal(19, 2) NOT NULL, W5_1 decimal(19, 2) NOT NULL, W6_1 decimal(19, 2) NOT NULL, W7_1 decimal(19, 2) NOT NULL, W8_1 decimal(19, 2) NOT NULL, W9_1 decimal(19, 2) NOT NULL, W10_1 decimal(19, 2) NOT NULL, W11_1 decimal(19, 2) NOT NULL, W12_1 decimal(19, 2) NOT NULL, W13_1 decimal(19, 2) NOT NULL, W14_1 decimal(19, 2) NOT NULL, W15_1 decimal(19, 2) NOT NULL
                          , W1_3 decimal(19, 2) NOT NULL, W2_3 decimal(19, 2) NOT NULL, W3_3 decimal(19, 2) NOT NULL, W4_3 decimal(19, 2) NOT NULL, W5_3 decimal(19, 2) NOT NULL, W6_3 decimal(19, 2) NOT NULL, W7_3 decimal(19, 2) NOT NULL, W8_3 decimal(19, 2) NOT NULL, W9_3 decimal(19, 2) NOT NULL, W10_3 decimal(19, 2) NOT NULL, W11_3 decimal(19, 2) NOT NULL, W12_3 decimal(19, 2) NOT NULL, W13_3 decimal(19, 2) NOT NULL, W14_3 decimal(19, 2) NOT NULL, W15_3 decimal(19, 2) NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_agg ON #ce_agg;
    CREATE TABLE #ce_pc    (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, CG tinyint NOT NULL
                          , SEG tinyint NOT NULL          -- 2 Активные, 1 Новые, 5 Отток, 6 Спящие, 0 нет покупок в канале
                          , UNIT smallint NULL            -- неделя слоя (Активные, Отток — давности, Новые — стажа), у Спящих — 4-недельный блок
                          , X float NOT NULL              -- «до»: ТО до скидки за 42 дня (Активные, Новые) или за всю историю (Отток, Спящие)
                          , Y1 float NOT NULL, Y3 float NOT NULL, A1 float NOT NULL, A3 float NOT NULL
                          , B_IN tinyint NOT NULL, B_IN1 tinyint NOT NULL, B_IN3 tinyint NOT NULL
                          , B_AFT tinyint NOT NULL, B_AFT1 tinyint NOT NULL, B_AFT3 tinyint NOT NULL
                          , CY float NOT NULL, KY float NOT NULL, CA float NOT NULL, KA float NOT NULL  -- чеки и SKU канала базы: в дни акции / «после»
                          , P float NULL);                -- проверка «до старта»: ТО недели последней покупки (Активные, Новые, Отток); Спящие — позже
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_pc ON #ce_pc;
    CREATE TABLE #ce_cl    (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, CG tinyint NOT NULL, AON tinyint NOT NULL
                          , X float NOT NULL, Y1 float NOT NULL, Y3 float NOT NULL, A1 float NOT NULL, A3 float NOT NULL
                          , B_IN tinyint NOT NULL, B_IN1 tinyint NOT NULL, B_IN3 tinyint NOT NULL
                          , B_AFT tinyint NOT NULL, B_AFT1 tinyint NOT NULL, B_AFT3 tinyint NOT NULL
                          , CY float NOT NULL, KY float NOT NULL, CA float NOT NULL, KA float NOT NULL
                          , P float NULL);   -- проверка «до старта»: ТО последнего отрезка «до» (Q = X − P); пусто — не проверяется
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_cl ON #ce_cl;
    CREATE TABLE #ce_dsc   (ID_PROMO int NOT NULL, ID_CONTACT int NULL, DATA date NOT NULL, ORG int NOT NULL
                          , VALUE_TYPE nvarchar(50) COLLATE DATABASE_DEFAULT NULL, VAL decimal(19, 2) NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_dsc ON #ce_dsc;
    -- выбросы, слои, оценки, итоги (тысячи строк)
    CREATE TABLE #ce_thr   (ID_PROMO int NOT NULL, SEG tinyint NOT NULL
                          , STEP tinyint NOT NULL         -- 0 «до», 1 «во время», 2 «после»
                          , N_ALL int NOT NULL, N_CG1 int NOT NULL
                          , M float NULL, Q3 float NULL, S float NULL, Z float NULL
                          , T float NULL                  -- граница, ₽ за период
                          , APPLIED tinyint NOT NULL);
    CREATE TABLE #ce_xpre  (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL);   -- исключены по «до»
    CREATE TABLE #ce_xdrop (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL);   -- исключены по «во время» / «после»
    CREATE TABLE #ce_lw    (RN int NOT NULL PRIMARY KEY, ID_PROMO int NOT NULL, SEG tinyint NOT NULL, UNIT smallint NOT NULL, N1 int NOT NULL);
    CREATE TABLE #ce_lmap  (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, UNIT smallint NOT NULL, LAYER smallint NOT NULL);
    CREATE TABLE #ce_lrng  (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER smallint NOT NULL
                          , WEEK_FROM smallint NOT NULL, WEEK_TO smallint NOT NULL);
    -- выравнивание ЦГ и КГ в неделе по сумме покупок «до» (решения 19.10)
    -- шкалы суммы «до»: SC 1 — I_INTERVALS_BUDGET_100 (Активные, Новые), 2 — I_INTERVALS_BUDGET_500 (Отток, Спящие; решение 19.10)
    CREATE TABLE #ce_ivt   (SC tinyint NOT NULL, ID_INTERVAL int NOT NULL, FINISH_SUM float NOT NULL, PRIMARY KEY (SC, FINISH_SUM, ID_INTERVAL));
    CREATE TABLE #ce_ev    (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, SEG tinyint NOT NULL, CG tinyint NOT NULL
                          , RL smallint NOT NULL, IV int NOT NULL);           -- клиент в оценке: неделя, интервал суммы «до»
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_ev ON #ce_ev;
    CREATE TABLE #ce_is0   (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, RL smallint NOT NULL, IV int NOT NULL   -- интервал шкалы
                          , RN int NOT NULL                                     -- порядок непустых интервалов в неделе
                          , N0 int NOT NULL, N1 int NOT NULL, PRIMARY KEY (ID_PROMO, SEG, RL, IV), UNIQUE (ID_PROMO, SEG, RL, RN));
    CREATE TABLE #ce_ivm   (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, RL smallint NOT NULL, IV int NOT NULL, G int NOT NULL
                          , PRIMARY KEY (ID_PROMO, SEG, RL, IV));                -- интервал шкалы → группа (её первый интервал)
    -- интервалы сравнения — группы соседних интервалов шкалы (IV = первый интервал группы)
    CREATE TABLE #ce_is    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, RL smallint NOT NULL, IV int NOT NULL
                          , N0 int NOT NULL, N1 int NOT NULL, OK tinyint NOT NULL
                          , KEEP0 int NULL, KEEP1 int NULL   -- ЦГ и КГ, остающиеся после выравнивания
                          , PRIMARY KEY (ID_PROMO, SEG, RL, IV));
    CREATE TABLE #ce_ws    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, RL smallint NOT NULL, SHARE float NOT NULL   -- доля КГ недели
                          , PRIMARY KEY (ID_PROMO, SEG, RL));
    CREATE TABLE #ce_kp    (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, RL smallint NOT NULL);   -- выровненный список
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_kp ON #ce_kp;
    -- проверка «до старта» (решения 19.10): Спящие — месяц последней покупки по снимкам; оценка псевдоэффекта
    CREATE TABLE #ce_spk   (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, ORG int NOT NULL, M1 date NOT NULL, M0 date NOT NULL
                          , FIRST_DATA date NULL);
    CREATE TABLE #ce_srd   (RN int NOT NULL PRIMARY KEY, RD date NOT NULL, ORG int NOT NULL);   -- даты снимков для Спящих
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_spk ON #ce_spk;
    CREATE TABLE #ce_sx    (ID_CONTACT int NOT NULL, ORG int NOT NULL, RD date NOT NULL, XA float NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_sx ON #ce_sx;
    CREATE TABLE #ce_sp    (ID_PROMO int NOT NULL, ID_CONTACT int NOT NULL, P float NOT NULL);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ce_sp ON #ce_sp;
    CREATE TABLE #ce_q1    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, CG tinyint NOT NULL
                          , N int NOT NULL, SQ float NOT NULL, SP float NOT NULL);
    CREATE TABLE #ce_qu    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL
                          , N0 int NOT NULL, SQ0 float NOT NULL, SP0 float NOT NULL, N1 int NOT NULL, SQ1 float NOT NULL, SP1 float NOT NULL
                          , R float NULL, EBAR0 float NULL);
    CREATE TABLE #ce_q2    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, CG tinyint NOT NULL, SS float NULL);
    CREATE TABLE #ce_qt    (ID_PROMO int NOT NULL, CH tinyint NOT NULL, PS float NULL, PSE2 float NULL, PRIMARY KEY (ID_PROMO, CH));
    CREATE TABLE #ce_wn    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, RL smallint NOT NULL
                          , N0ALL int NOT NULL, N1ALL int NOT NULL, IVS int NOT NULL   -- ЦГ и КГ недели до выравнивания, интервалов в сравнении
                          , PRIMARY KEY (ID_PROMO, SEG, RL));
    CREATE TABLE #ce_segok (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, N0 int NOT NULL, N1 int NOT NULL, OK tinyint NOT NULL);
    CREATE TABLE #ce_e1    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, PER tinyint NOT NULL, CH tinyint NOT NULL
                          , CG tinyint NOT NULL, N int NOT NULL, SX float NOT NULL, SY float NOT NULL, SB int NULL
                          , SC float NULL, SK float NULL);   -- чеки и SKU периода
    CREATE TABLE #ce_u     (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, PER tinyint NOT NULL, CH tinyint NOT NULL
                          , N0 int NOT NULL, SX0 float NOT NULL, SY0 float NOT NULL, SB0 int NULL
                          , N1 int NOT NULL, SX1 float NOT NULL, SY1 float NOT NULL, SB1 int NULL
                          , R float NULL, ADD_A float NULL, EBAR0 float NULL, XBAR1 float NULL, YBAR1 float NULL
                          , SC0 float NULL, SK0 float NULL, SC1 float NULL, SK1 float NULL);
    CREATE TABLE #ce_e2    (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, PER tinyint NOT NULL, CH tinyint NOT NULL
                          , CG tinyint NOT NULL, SS float NULL, SXY float NULL, SXX float NULL);
    CREATE TABLE #ce_res   (ID_PROMO int NOT NULL, SEG tinyint NOT NULL, LAYER int NOT NULL, PER tinyint NOT NULL, CH tinyint NOT NULL
                          , N0 int NOT NULL, N1 int NOT NULL, SX0 float NOT NULL, SX1 float NOT NULL, SY0 float NOT NULL, SY1 float NOT NULL
                          , SB0 int NULL, SB1 int NULL, EXPV float NULL, ADD_A float NULL, SE2 float NULL, ADD_REG float NULL
                          , INC float NULL, IN_TOTAL tinyint NOT NULL
                          , SC0 float NULL, SC1 float NULL, SK0 float NULL, SK1 float NULL
                          , EXPC float NULL, EXPK float NULL     -- ожидаемые чеки и SKU ЦГ без акции: КГ · ΣX_ЦГ / ΣX_КГ
                          , EXPB float NULL);                    -- ожидаемые купившие ЦГ без акции: купившие КГ · ΣX_ЦГ / ΣX_КГ
    CREATE TABLE #ce_tot   (ID_PROMO int NOT NULL, CH tinyint NOT NULL, LAYERS int NULL
                          , ADD1 float NULL, SE21 float NULL, REG1 float NULL, INC1 float NULL, SB01 float NULL, N01 float NULL
                          , ADD2 float NULL, SE22 float NULL, INC2 float NULL, SB02 float NULL, N02 float NULL
                          , ADD3 float NULL, SE23 float NULL, REG3 float NULL
                          -- шесть показателей, в акцию (1) и после (2): клиентов ЦГ, ТО, чеки, SKU ЦГ и ожидание ЦГ без акции (E…)
                          , NE1 float NULL, SY01 float NULL, EY1 float NULL, SC01 float NULL, EC1 float NULL, SK01 float NULL, EK1 float NULL
                          , NE2 float NULL, SY02 float NULL, EY2 float NULL, SC02 float NULL, EC2 float NULL, SK02 float NULL, EK2 float NULL
                          -- купившие ЦГ, ожидаемые купившие ЦГ без акции, купившие КГ — знаменатель ARPU и чеков на клиента
                          , NB1 float NULL, EB1 float NULL, KB1 float NULL, NB2 float NULL, EB2 float NULL, KB2 float NULL
                          -- Доп ТО на всю ЦГ (решение 19.10): в акцию, после, итого
                          , ALL1 float NULL, ALL2 float NULL, ALL3 float NULL
                          , PRIMARY KEY (ID_PROMO, CH));
    -- клиенты выровненных списков обеих баз, купившие в обоих каналах (для «Все каналы» — без двойного счёта): ЦГ и КГ
    CREATE TABLE #ce_bb    (ID_PROMO int NOT NULL, CG tinyint NOT NULL, D1 int NOT NULL, D2 int NOT NULL, PRIMARY KEY (ID_PROMO, CG));
    -- Доп ТО на всю ЦГ по акции на базе канала и периоду
    CREATE TABLE #ce_all   (ID_PROMO int NOT NULL, PER tinyint NOT NULL, ADDALL float NULL, PRIMARY KEY (ID_PROMO, PER));
    CREATE TABLE #ce_cnt   (ID_PROMO int NOT NULL, CH tinyint NOT NULL, L0 int NOT NULL, L1 int NOT NULL, F0 int NOT NULL, F1 int NOT NULL
                          , D0 int NOT NULL, D1 int NOT NULL, O0 int NOT NULL, O1 int NOT NULL, U0 int NOT NULL, U1 int NOT NULL
                          , I0 int NOT NULL, I1 int NOT NULL, U_SEGS nvarchar(400) COLLATE DATABASE_DEFAULT NULL
                          , P0 int NOT NULL DEFAULT 0, P1 int NOT NULL DEFAULT 0   -- не с кем сравнить (неделя или интервал суммы): ЦГ / КГ
                          , R0 int NOT NULL DEFAULT 0, R1 int NOT NULL DEFAULT 0   -- убраны при выравнивании доли: ЦГ / КГ
                          , PRIMARY KEY (ID_PROMO, CH));
    CREATE TABLE #ce_link  (ID_PROMO int NOT NULL, ID_CAMPAIGN int NOT NULL);
    CREATE TABLE #ce_dagg  (ID_PROMO int NOT NULL, CH tinyint NOT NULL, D0 decimal(19, 2) NOT NULL, D1 decimal(19, 2) NOT NULL
                          , DT decimal(19, 2) NOT NULL, DIN decimal(19, 2) NOT NULL, MIXED tinyint NOT NULL, NO_CAMP tinyint NOT NULL
                          , PRIMARY KEY (ID_PROMO, CH));
    CREATE TABLE #ce_dlv   (ID_PROMO int NOT NULL PRIMARY KEY, MSGS int NULL, SENT bigint NULL, DELIV bigint NULL
                          , CLICKED bigint NULL, ERRORS bigint NULL);

    BEGIN TRY
        -- один запуск за раз (ночной шаг, догон истории, ручной EXEC): второй сразу падает с сообщением
        EXEC @lock = sp_getapplock @Resource = N'dbo.CVM_EFFECT_TODAY', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
        IF @lock < 0
            THROW 50002, N'CVM_EFFECT_TODAY: уже идёт другой запуск (ночной шаг, догон истории или ручной) — этот не начат, ничего не записано.', 1;
        SET @locked = 1;

        -- шкалы интервалов суммы покупок «до» — один раз на запуск: 100 ₽ (Активные, Новые), 500 ₽ (Отток, Спящие)
        IF OBJECT_ID(N'dbo.I_INTERVALS_BUDGET_100', N'U') IS NULL OR OBJECT_ID(N'dbo.I_INTERVALS_BUDGET_500', N'U') IS NULL
            THROW 50001, N'CVM_EFFECT_TODAY: нет шкал интервалов dbo.I_INTERVALS_BUDGET_100 / _500 — ничего не записано.', 1;
        INSERT INTO #ce_ivt (SC, ID_INTERVAL, FINISH_SUM)
        SELECT 1, t.ID_INTERVAL, CAST(t.FINISH_SUM AS float) FROM dbo.I_INTERVALS_BUDGET_100 AS t (nolock) WHERE t.FINISH_SUM IS NOT NULL;
        INSERT INTO #ce_ivt (SC, ID_INTERVAL, FINISH_SUM)
        SELECT 2, t.ID_INTERVAL, CAST(t.FINISH_SUM AS float) FROM dbo.I_INTERVALS_BUDGET_500 AS t (nolock) WHERE t.FINISH_SUM IS NOT NULL;
        SELECT TOP (1) @iv_last1 = ID_INTERVAL FROM #ce_ivt WHERE SC = 1 ORDER BY FINISH_SUM DESC, ID_INTERVAL DESC;
        SELECT TOP (1) @iv_last2 = ID_INTERVAL FROM #ce_ivt WHERE SC = 2 ORDER BY FINISH_SUM DESC, ID_INTERVAL DESC;
        IF @iv_last1 IS NULL OR @iv_last2 IS NULL
            THROW 50001, N'CVM_EFFECT_TODAY: шкала dbo.I_INTERVALS_BUDGET_100 или _500 пустая — ничего не записано.', 1;

        IF OBJECT_ID('dbo.I_DISCOUNT_CHECK', 'U') IS NULL OR OBJECT_ID('dbo.I_PROMO_CAMPAIGN_LINK', 'U') IS NULL
           OR OBJECT_ID('dbo.I_DISCOUNT_STATE', 'U') IS NULL OR OBJECT_ID('dbo.I_DISCOUNT_DAY', 'U') IS NULL
            SET @disc_ok = 0;
        IF OBJECT_ID('dbo.I_DELIVERY_ACTION', 'U') IS NULL OR OBJECT_ID('dbo.I_DELIVERY_DAY', 'U') IS NULL
           OR OBJECT_ID('dbo.I_DELIVERY_STATUS', 'U') IS NULL
            SET @dlv_ok = 0;

        -- ---------- акции отчёта: даты из I_PROMO (I_PROMO только читается) ----------
        -- строк у номера несколько — самый ранний старт и самый поздний финиш; механика с кешбэком — если она у любой строки
        INSERT INTO #ce_promo (ID_PROMO, PROMO_NAME, START_DATE, FINISH_DATE, SEGMENT, MECHANICS, ID_MECHANICS, CHANNEL
                             , ORG, CASHBACK, DATE_ROWS, BAD, THEM)
        SELECT p.ID_PROMO, max(p.PROMO_NAME), min(p.START_DATE), max(p.FINISH_DATE), max(p.SEGMENT), max(p.MECHANICS)
             , max(p.ID_MECHANICS), max(p.CHANNEL)
             , CASE WHEN p.ID_PROMO BETWEEN 110000 AND 111999 THEN 3 ELSE 1 END
             , max(CASE WHEN m.ID_MECHANICS IS NOT NULL THEN 1 ELSE 0 END)
             , count(DISTINCT concat(p.START_DATE, N'|', p.FINISH_DATE))
             , CASE WHEN min(p.START_DATE) IS NULL OR max(p.FINISH_DATE) IS NULL OR max(p.FINISH_DATE) < min(p.START_DATE)
                    THEN 1 ELSE 0 END
             , max(CASE WHEN p.PROMO_NAME LIKE N'%Тематическ% рассылк%' THEN 1 ELSE 0 END)
        FROM dbo.I_PROMO AS p (nolock)
             LEFT JOIN #ce_mech AS m ON m.ID_MECHANICS = p.ID_MECHANICS
        WHERE p.ID_COMPANY = @idc
          AND p.ID_PROMO <> 1013000
          AND (   p.ID_PROMO BETWEEN 101000 AND 101999
               OR p.ID_PROMO BETWEEN 110000 AND 111999
               OR (p.ID_PROMO BETWEEN 500000 AND 599999 AND p.TYPE_PROMO = N'CVM'))
        GROUP BY p.ID_PROMO
        HAVING max(CASE WHEN p.START_DATE >= @hist_from THEN 1 ELSE 0 END) = 1;

        -- уже посчитанные
        INSERT INTO #ce_done (ID_PROMO, PRELIM, CALC_DT)
        SELECT e.[ID], max(CAST(e.[Предварительная] AS tinyint)), max(e.[Дата расчета])
        FROM dbo.I_CVM_EFFECT AS e (nolock)
        WHERE e.ID_COMPANY = @idc
        GROUP BY e.[ID];

        IF @disc_ok = 0
            RAISERROR(N'CVM_EFFECT_TODAY: витрины скидок нет (CVM_DISCOUNT_MART.sql) — акции не считаются: скидка нулём не берётся.', 10, 1) WITH NOWAIT;
        ELSE
        BEGIN
            -- ---------- выбор акций ----------
            IF @mode = 'PROMO'
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM #ce_promo WHERE ID_PROMO = @id_promo)
                    RAISERROR(N'CVM_EFFECT_TODAY: акции %d нет среди акций отчёта (101000–101999, 500000–599999 с типом CVM, 110000–111999; старт с 01.09.2025) — не считаю.', 10, 1, @id_promo) WITH NOWAIT;
                ELSE IF EXISTS (SELECT 1 FROM #ce_promo WHERE ID_PROMO = @id_promo AND BAD = 1)
                    RAISERROR(N'CVM_EFFECT_TODAY: у акции %d в I_PROMO нет даты старта или окончания (или финиш раньше старта) — не считаю.', 10, 1, @id_promo) WITH NOWAIT;
                ELSE IF EXISTS (SELECT 1 FROM #ce_promo WHERE ID_PROMO = @id_promo AND START_DATE > @yesterday)
                    RAISERROR(N'CVM_EFFECT_TODAY: акция %d ещё не началась (чеков за дни акции нет) — не считаю.', 10, 1, @id_promo) WITH NOWAIT;
                ELSE
                    INSERT INTO #ce_sel (ID_PROMO, START_DATE, FINISH_DATE, CHK_TO, WHY, READY)
                    SELECT p.ID_PROMO, p.START_DATE, p.FINISH_DATE
                         , CASE WHEN dateadd(day, @after_days, p.FINISH_DATE) < @yesterday THEN dateadd(day, @after_days, p.FINISH_DATE) ELSE @yesterday END
                         , N'одна акция', 1
                    FROM #ce_promo AS p
                    WHERE p.ID_PROMO = @id_promo;
            END
            ELSE IF @mode = 'RANGE'
                INSERT INTO #ce_sel (ID_PROMO, START_DATE, FINISH_DATE, CHK_TO, WHY, READY)
                SELECT p.ID_PROMO, p.START_DATE, p.FINISH_DATE
                     , CASE WHEN dateadd(day, @after_days, p.FINISH_DATE) < @yesterday THEN dateadd(day, @after_days, p.FINISH_DATE) ELSE @yesterday END
                     , N'история', 1
                FROM #ce_promo AS p
                WHERE p.BAD = 0
                  AND p.START_DATE >= @date_from AND p.START_DATE <= @date_to
                  AND (   (@only_missing = 0 AND @calc_before IS NULL)
                       OR NOT EXISTS (SELECT 1 FROM #ce_done AS d
                                      WHERE d.ID_PROMO = p.ID_PROMO
                                        AND (@calc_before IS NULL OR d.CALC_DT >= @calc_before)));
            ELSE
                -- НОЧЬ: нет в витрине (новые и стёртые шагом 1) в горизонте @night_days; финиш + 4 в окне перезаливки;
                -- прошлые предварительные (дошли до окончательных); новая связь с кампанией после расчёта
                INSERT INTO #ce_sel (ID_PROMO, START_DATE, FINISH_DATE, CHK_TO, WHY, READY)
                SELECT p.ID_PROMO, p.START_DATE, p.FINISH_DATE
                     , CASE WHEN dateadd(day, @after_days, p.FINISH_DATE) < @yesterday THEN dateadd(day, @after_days, p.FINISH_DATE) ELSE @yesterday END
                     , CASE WHEN d.ID_PROMO IS NULL THEN N'новая или стёртая шагом 1'
                            WHEN dateadd(day, @after_days, p.FINISH_DATE) >= dateadd(day, -@reload_days, @today) THEN N'окно перезаливки'
                            WHEN d.PRELIM = 1 THEN N'была предварительной'
                            ELSE N'новая связь с кампанией' END
                     , 1
                FROM #ce_promo AS p
                     LEFT JOIN #ce_done AS d ON d.ID_PROMO = p.ID_PROMO
                WHERE p.BAD = 0
                  AND p.START_DATE <= @yesterday
                  AND (   (d.ID_PROMO IS NULL AND p.START_DATE >= dateadd(day, -@night_days, @today))
                       OR dateadd(day, @after_days, p.FINISH_DATE) >= dateadd(day, -@reload_days, @today)
                       OR d.PRELIM = 1
                       OR EXISTS (SELECT 1 FROM dbo.I_PROMO_CAMPAIGN_LINK AS l (nolock)
                                  WHERE l.ID_COMPANY = @idc AND l.ID_PROMO = p.ID_PROMO AND l.IS_USED = 1
                                    AND l.CURRENT_SINCE > d.CALC_DT));

            -- ---------- готовность скидки (шапка CVM_DISCOUNT_MART.sql): иначе акция ждёт следующего запуска ----------
            -- 1) каждый день старт … финиш + 4 (по вчера) готов в витрине скидок: STATUS = 'OK' и есть итоговая строка дня
            --    I_DISCOUNT_DAY с LVL = 5 (условие готовности из шапки CVM_DISCOUNT_MART.sql); иначе скидка дня была бы нулём
            UPDATE s
            SET READY = 0
              , NOT_READY = CASE WHEN x.N_STATE < datediff(day, s.START_DATE, s.CHK_TO) + 1
                                 THEN concat(N'витрина скидок: не посчитано дней — ', datediff(day, s.START_DATE, s.CHK_TO) + 1 - x.N_STATE)
                                 WHEN x.N_NOCHK > 0 THEN concat(N'нет чеков за дней — ', x.N_NOCHK)
                                 ELSE concat(N'витрина скидок: дни не готовы (статус не OK или нет итога дня) — ', x.N_STATE - x.N_OK) END
            FROM #ce_sel AS s
                 CROSS APPLY (SELECT N_STATE = count(*)
                                   , N_NOCHK = isnull(sum(CASE WHEN st.STATUS = 'NO_CHECKS' THEN 1 ELSE 0 END), 0)
                                   , N_OK    = isnull(sum(CASE WHEN st.STATUS = 'OK' AND d5.HAS5 = 1 THEN 1 ELSE 0 END), 0)
                              FROM dbo.I_DISCOUNT_STATE AS st (nolock)
                                   OUTER APPLY (SELECT TOP (1) HAS5 = 1
                                                FROM dbo.I_DISCOUNT_DAY AS dd (nolock)
                                                WHERE dd.ID_COMPANY = @idc AND dd.DATA = st.DATA AND dd.LVL = 5) AS d5
                              WHERE st.ID_COMPANY = @idc AND st.DATA >= s.START_DATE AND st.DATA <= s.CHK_TO) AS x
            WHERE x.N_OK < datediff(day, s.START_DATE, s.CHK_TO) + 1;

            -- 2) поздняя связь: кампания акции за день есть в дневной витрине, строк кампании за день в I_DISCOUNT_CHECK нет,
            --    а день посчитан раньше, чем появилась связь (то же условие, по которому витрина скидок пересчитывает дни).
            --    Сначала кандидаты по маленьким таблицам — обычно их нет, и I_DISCOUNT_CHECK не читается
            INSERT INTO #ce_lcand (ID_PROMO, DATA, ID_CAMPAIGN)
            SELECT DISTINCT s.ID_PROMO, d.DATA, d.ID_CAMPAIGN
            FROM #ce_sel AS s
                 JOIN dbo.I_PROMO_CAMPAIGN_LINK AS l (nolock) ON l.ID_COMPANY = @idc AND l.ID_PROMO = s.ID_PROMO AND l.IS_USED = 1
                 JOIN dbo.I_DISCOUNT_DAY AS d (nolock) ON d.ID_COMPANY = @idc AND d.LVL = 2 AND d.ID_CAMPAIGN = l.ID_CAMPAIGN
                                                      AND d.DATA >= s.START_DATE AND d.DATA <= s.CHK_TO
                 JOIN dbo.I_DISCOUNT_STATE AS st (nolock) ON st.ID_COMPANY = @idc AND st.DATA = d.DATA
            WHERE s.READY = 1
              AND st.LAST_TRY < l.CURRENT_SINCE
            OPTION (RECOMPILE);

            IF EXISTS (SELECT 1 FROM #ce_lcand)
            BEGIN
                SELECT @d_from = min(DATA), @d_to = max(DATA) FROM #ce_lcand;
                DELETE k
                FROM #ce_lcand AS k
                WHERE EXISTS (SELECT 1 FROM dbo.I_DISCOUNT_CHECK AS c (nolock)
                              WHERE c.ID_COMPANY = @idc
                                AND c.DATA >= @d_from AND c.DATA <= @d_to
                                AND c.DATA = k.DATA AND c.ID_CAMPAIGN = k.ID_CAMPAIGN)
                OPTION (RECOMPILE);
                UPDATE s
                SET READY = 0, NOT_READY = N'витрина скидок: дни ждут пересчёта по новой связи кампании'
                FROM #ce_sel AS s
                WHERE EXISTS (SELECT 1 FROM #ce_lcand AS k WHERE k.ID_PROMO = s.ID_PROMO);
            END

            -- ---------- порядок и порции: от новых к старым; в порции не больше @part_promos акций и разброс старта
            --            не больше @part_days дней ----------
            UPDATE s
            SET ORD = o.RN
            FROM #ce_sel AS s
                 JOIN (SELECT ID_PROMO, RN = ROW_NUMBER() OVER (ORDER BY START_DATE DESC, ID_PROMO DESC)
                       FROM #ce_sel
                       WHERE READY = 1) AS o ON o.ID_PROMO = s.ID_PROMO;

            SELECT @n = count(*) FROM #ce_sel WHERE READY = 1;
            SELECT @i = 1, @part = 0, @p_first = NULL, @p_cnt = 0;
            WHILE @i <= @n
            BEGIN
                SELECT @sd = START_DATE FROM #ce_sel WHERE ORD = @i;
                IF @part = 0 OR @p_cnt >= @part_promos OR datediff(day, @sd, @p_first) > @part_days
                    SELECT @part += 1, @p_first = @sd, @p_cnt = 0;
                UPDATE #ce_sel SET PART_NO = @part WHERE ORD = @i;
                SELECT @p_cnt += 1, @i += 1;
            END
            SET @parts = @part;

            SET @msg = concat(N'CVM_EFFECT_TODAY: ', CASE @mode WHEN 'NIGHT' THEN N'НОЧЬ'
                                                          WHEN 'RANGE' THEN concat(N'ИСТОРИЯ ', convert(nchar(10), @date_from, 104), N'–'
                                                                                 , convert(nchar(10), @date_to, 104)
                                                                                 , IIF(@only_missing = 1, N' (только недостающие)', N'')
                                                                                 , IIF(@calc_before IS NULL, N''
                                                                                     , concat(N' (недостающие и посчитанные до ', convert(nchar(16), @calc_before, 120), N')')))
                                                          ELSE concat(N'АКЦИЯ ', @id_promo) END
                            , N'; выбрано акций ', (SELECT count(*) FROM #ce_sel), N', готовы к расчёту ', @n, N', порций ', @parts
                            , N'; κ = ', @kappa, N'; «PL в акцию» — скидка ', IIF(@pl_in_discount = 'ALL', N'старт … финиш + 4', N'дней акции')
                            , IIF(@stop_at IS NULL, N'', concat(N'; новые акции не начинаются после ', convert(nchar(16), @stop_at, 120))), N'.');
            RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

            IF @mode = 'NIGHT'
            BEGIN
                SELECT @msg = STRING_AGG(CAST(concat(x.WHY, N' — ', x.N) AS nvarchar(max)), N'; ')
                FROM (SELECT WHY, N = count(*) FROM #ce_sel GROUP BY WHY) AS x;
                IF @msg IS NOT NULL
                    RAISERROR(N'CVM_EFFECT_TODAY: почему выбраны: %s.', 0, 1, @msg) WITH NOWAIT;
            END

            SET @msg = NULL;
            SELECT @msg = left(STRING_AGG(CAST(concat(ID_PROMO, N' (', NOT_READY, N')') AS nvarchar(max)), N', ')
                                    WITHIN GROUP (ORDER BY START_DATE DESC, ID_PROMO DESC), 1500)
            FROM #ce_sel
            WHERE READY = 0;
            IF @msg IS NOT NULL
                RAISERROR(N'ВНИМАНИЕ: CVM_EFFECT_TODAY: скидка не готова — акции в этот запуск не считаются, прежние строки витрины остаются: %s.', 10, 1, @msg) WITH NOWAIT;

            SET @msg = NULL;
            SELECT @msg = left(STRING_AGG(CAST(ID_PROMO AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY ID_PROMO), 1500)
            FROM #ce_promo
            WHERE BAD = 1
              AND (   (@mode = 'NIGHT' AND START_DATE >= dateadd(day, -@night_days, @today) AND START_DATE <= @yesterday)
                   OR (@mode = 'RANGE' AND START_DATE >= @date_from AND START_DATE <= @date_to));
            IF @msg IS NOT NULL
                RAISERROR(N'ВНИМАНИЕ: CVM_EFFECT_TODAY: у акций %s в I_PROMO нет даты старта или окончания (или финиш раньше старта) — не считаются.', 10, 1, @msg) WITH NOWAIT;

            -- фрод — текущий внешний список (даты в нём нет)
            INSERT INTO #ce_fraud (ID_CONTACT)
            SELECT DISTINCT f.ID_CONTACT
            FROM dbo.I_FRAUD_CONTACT AS f (nolock)
            WHERE f.ID_COMPANY = @idc AND f.ID_CONTACT IS NOT NULL;

            -- снимок акции — последний месячный REFERENCE_DATE I_CVM_CONTACT_DATA до старта (строка канала), как в тесте; не
            -- старше полугода до самого раннего старта запуска (снимка старше нет — пусто)
            SELECT @refs_from = dateadd(month, -6, min(START_DATE)) FROM #ce_sel WHERE READY = 1;

            -- ============================== ПОРЦИИ АКЦИЙ ==============================
            SET @part = 0;
            WHILE 1 = 1
            BEGIN
                SELECT TOP (1) @part = PART_NO FROM #ce_sel WHERE READY = 1 AND PART_NO > @part ORDER BY PART_NO;
                IF @@ROWCOUNT = 0 BREAK;
                IF @stop_at IS NOT NULL AND sysdatetime() >= @stop_at
                BEGIN
                    SET @stopped = 1;
                    BREAK;
                END
                SELECT @t_part = sysdatetime(), @t = sysdatetime();

                BEGIN TRY
                    TRUNCATE TABLE #ce_vp;
                    TRUNCATE TABLE #ce_pr;    TRUNCATE TABLE #ce_offer; TRUNCATE TABLE #ce_list;  TRUNCATE TABLE #ce_cust;
                    TRUNCATE TABLE #ce_snap;  TRUNCATE TABLE #ce_ch;    TRUNCATE TABLE #ce_agg;   TRUNCATE TABLE #ce_pc;
                    TRUNCATE TABLE #ce_cl;    TRUNCATE TABLE #ce_dsc;   TRUNCATE TABLE #ce_thr;   TRUNCATE TABLE #ce_xpre;
                    TRUNCATE TABLE #ce_xdrop; TRUNCATE TABLE #ce_lw;    TRUNCATE TABLE #ce_lmap;  TRUNCATE TABLE #ce_lrng;
                    TRUNCATE TABLE #ce_ev;    TRUNCATE TABLE #ce_is;    TRUNCATE TABLE #ce_ws;    TRUNCATE TABLE #ce_kp;
                    TRUNCATE TABLE #ce_is0;   TRUNCATE TABLE #ce_ivm;
                    TRUNCATE TABLE #ce_wn;    TRUNCATE TABLE #ce_bb;    TRUNCATE TABLE #ce_all;
                    TRUNCATE TABLE #ce_spk;   TRUNCATE TABLE #ce_srd;   TRUNCATE TABLE #ce_sx;    TRUNCATE TABLE #ce_sp;    TRUNCATE TABLE #ce_q1;
                    TRUNCATE TABLE #ce_qu;    TRUNCATE TABLE #ce_q2;    TRUNCATE TABLE #ce_qt;
                    TRUNCATE TABLE #ce_segok; TRUNCATE TABLE #ce_e1;    TRUNCATE TABLE #ce_u;     TRUNCATE TABLE #ce_e2;
                    TRUNCATE TABLE #ce_res;   TRUNCATE TABLE #ce_tot;   TRUNCATE TABLE #ce_cnt;   TRUNCATE TABLE #ce_link;
                    TRUNCATE TABLE #ce_dagg;  TRUNCATE TABLE #ce_dlv;

                    -- ---------- 1. Акции порции и окна ----------
                    INSERT INTO #ce_pr (ID_PROMO, START_DATE, FINISH_DATE, ORG, HIST_FROM, PRE_FROM, PRE_TO, AFT_FROM, AFT_TO
                                      , AFTER_ON, PRELIM, REF_DATE, ORD)
                    SELECT s.ID_PROMO, s.START_DATE, s.FINISH_DATE, p.ORG
                         , dateadd(day, -@hist_days, s.START_DATE)             -- окно чеков для сегмента
                         , dateadd(day, -@pre_days, s.START_DATE)              -- «до» Активных и Новых
                         , dateadd(day, -1, s.START_DATE)
                         , dateadd(day, 1, s.FINISH_DATE)                      -- «после»
                         , dateadd(day, @after_days, s.FINISH_DATE)
                         , CASE WHEN (s.START_DATE = s.FINISH_DATE AND p.CASHBACK = 0) OR p.THEM = 1 THEN 0 ELSE 1 END
                         , CASE WHEN dateadd(day, @after_days, s.FINISH_DATE) >= dateadd(day, -@reload_days, @today) THEN 1 ELSE 0 END
                         , NULL, s.ORD
                    FROM #ce_sel AS s
                         JOIN #ce_promo AS p ON p.ID_PROMO = s.ID_PROMO
                    WHERE s.READY = 1 AND s.PART_NO = @part;

                    -- снимок для первой покупки, давности Спящих и «до» Оттока и Спящих (вся история по дату снимка): последний месячный REFERENCE_DATE до старта, строка канала
                    UPDATE p
                    SET REF1 = (SELECT max(d.REFERENCE_DATE)
                                FROM dbo.I_CVM_CONTACT_DATA AS d (nolock)
                                WHERE d.ID_COMPANY = @idc AND d.ID_ORGANIZATION = 1
                                  AND d.REFERENCE_DATE >= @refs_from AND d.REFERENCE_DATE < p.START_DATE)
                      , REF3 = (SELECT max(d.REFERENCE_DATE)
                                FROM dbo.I_CVM_CONTACT_DATA AS d (nolock)
                                WHERE d.ID_COMPANY = @idc AND d.ID_ORGANIZATION = 3
                                  AND d.REFERENCE_DATE >= @refs_from AND d.REFERENCE_DATE < p.START_DATE)
                    FROM #ce_pr AS p;
                    UPDATE #ce_pr SET REF_DATE = CASE ORG WHEN 3 THEN REF3 ELSE REF1 END;

                    -- базы каналов: у офлайн-акций — офлайн и онлайн; у онлайн-акций 110xxx — только онлайн (решение 19.10: «по офлайну
                    -- эффект заведомо некорректный, выборка делалась по онлайн-покупкам»)
                    INSERT INTO #ce_vp (ID_PROMO, REAL_ID, BASIS, AFTER_ON)
                    SELECT p.ID_PROMO * 10 + b.B, p.ID_PROMO, b.B, p.AFTER_ON
                    FROM #ce_pr AS p
                         CROSS JOIN (VALUES (cast(1 AS tinyint)), (cast(3 AS tinyint))) AS b (B)
                    WHERE p.ORG = 1 OR b.B = 3;

                    -- ---------- 2. Списки — по номеру акции; фрод отмечается ----------
                    SET @k = 0;
                    WHILE 1 = 1
                    BEGIN
                        SELECT TOP (1) @pid = ID_PROMO FROM #ce_pr WHERE ID_PROMO > @k ORDER BY ID_PROMO;
                        IF @@ROWCOUNT = 0 BREAK;
                        SET @k = @pid;

                        INSERT INTO #ce_offer WITH (TABLOCK) (ID_PROMO, ID_CONTACT, HAS_TG, HAS_CG)
                        SELECT @pid, o.ID_CONTACT
                             , max(CASE WHEN o.CONTROL_GROUP = 0 THEN 1 ELSE 0 END)
                             , max(CASE WHEN o.CONTROL_GROUP = 1 THEN 1 ELSE 0 END)
                        FROM dbo.I_PROMO_OFFER AS o (nolock)
                        WHERE o.ID_PROMO = @pid
                          AND o.ID_COMPANY = @idc
                          AND o.ID_CONTACT IS NOT NULL
                        GROUP BY o.ID_CONTACT
                        OPTION (RECOMPILE);
                    END

                    -- Контроль — КГ списка, в том числе при доле ниже 5 % (решение 19.10: «из списка КГ, но по слоям анализируем
                    -- заново» — слои собираются по своей КГ, по 30 клиентов КГ в слое). Подмена ГКГ при низкой доле снята: ГКГ —
                    -- случайная часть всей базы без подборки и фильтров отбора списка, «до» ЦГ в 2 раза выше — оценка смещена.
                    -- ГКГ — только если КГ в списке нет совсем (решение 14.10). Низкая доля — пометкой
                    UPDATE p
                    SET KG_SHARE = x.SH
                      , CTRL = CASE WHEN x.N = 0 THEN 0
                                    WHEN x.L1 = 0 THEN 1                                         -- КГ нет — ГКГ (решение 14.10)
                                    ELSE 0 END
                    FROM #ce_pr AS p
                         JOIN (SELECT ID_PROMO
                                    , N  = sum(CASE WHEN HAS_TG = 1 OR HAS_CG = 1 THEN 1 ELSE 0 END)
                                    , L1 = sum(CASE WHEN HAS_TG = 0 AND HAS_CG = 1 THEN 1 ELSE 0 END)
                                    , SH = sum(CASE WHEN HAS_TG = 0 AND HAS_CG = 1 THEN 1e0 ELSE 0e0 END)
                                           / nullif(sum(CASE WHEN HAS_TG = 1 OR HAS_CG = 1 THEN 1 ELSE 0 END), 0)
                               FROM #ce_offer
                               GROUP BY ID_PROMO) AS x ON x.ID_PROMO = p.ID_PROMO;

                    INSERT INTO #ce_list WITH (TABLOCK) (ID_PROMO, ID_CONTACT, CG, IS_FRAUD)
                    SELECT o.ID_PROMO, o.ID_CONTACT
                         , CASE WHEN o.HAS_TG = 1 THEN 0 ELSE 1 END            -- обе метки — ЦГ
                         , CASE WHEN f.ID_CONTACT IS NULL THEN 0 ELSE 1 END
                    FROM #ce_offer AS o
                         JOIN #ce_pr AS p ON p.ID_PROMO = o.ID_PROMO
                         LEFT JOIN #ce_fraud AS f ON f.ID_CONTACT = o.ID_CONTACT
                    WHERE (o.HAS_TG = 1 OR o.HAS_CG = 1)                       -- другие метки не берутся
                      AND (o.HAS_TG = 1 OR p.CTRL = 0);                         -- КГ некорректна — КГ списка не берётся

                    -- контроль из ГКГ, действовавшей на дату старта: партии жеребьёвки (I_GCG_BATCH, нарезаны не позже старта,
                    -- состав — I_GCG_PRE), до первой партии — архив прежней ГКГ (I_GLOBAL_CG_2025_2026), нет его — текущая ГКГ;
                    -- клиенты из списка акции (с любой меткой) не берутся
                    SET @k = 0;
                    WHILE 1 = 1
                    BEGIN
                        SELECT TOP (1) @pid = ID_PROMO, @sd = START_DATE FROM #ce_pr WHERE CTRL = 1 AND ID_PROMO > @k ORDER BY ID_PROMO;
                        IF @@ROWCOUNT = 0 BREAK;
                        SET @k = @pid;
                        SET @gb = NULL;
                        IF OBJECT_ID(N'dbo.I_GCG_BATCH', N'U') IS NOT NULL AND OBJECT_ID(N'dbo.I_GCG_PRE', N'U') IS NOT NULL
                            SELECT @gb = max(b.DRAW_DATE) FROM dbo.I_GCG_BATCH AS b (nolock) WHERE b.DRAW_DATE <= @sd;

                        IF @gb IS NULL
                            SET @gb = CASE WHEN OBJECT_ID(N'dbo.I_GLOBAL_CG_2025_2026', N'U') IS NOT NULL THEN '19000101' ELSE '19000102' END;
                        IF NOT EXISTS (SELECT 1 FROM #ce_gcg WHERE K = @gb)
                        BEGIN
                            IF @gb > '19000102'
                                INSERT INTO #ce_gcg WITH (TABLOCK) (K, ID_CONTACT)
                                SELECT DISTINCT @gb, e.ID_CONTACT
                                FROM dbo.I_GCG_PRE AS e (nolock)
                                     JOIN dbo.I_GCG_BATCH AS b (nolock) ON b.BATCH_ID = e.BATCH_ID
                                WHERE e.GCG = 1 AND b.DRAW_DATE <= @gb;
                            ELSE IF @gb = '19000101'
                                INSERT INTO #ce_gcg WITH (TABLOCK) (K, ID_CONTACT)
                                SELECT DISTINCT @gb, CAST(e.ID_CONTACT AS int)
                                FROM dbo.I_GLOBAL_CG_2025_2026 AS e (nolock)
                                WHERE e.ID_COMPANY = @idc AND e.CONTROL_GROUP = 1
                                  AND e.ID_CONTACT > 0 AND e.ID_CONTACT <= 2147483647;
                            ELSE
                                INSERT INTO #ce_gcg WITH (TABLOCK) (K, ID_CONTACT)
                                SELECT DISTINCT @gb, e.ID_CONTACT
                                FROM dbo.I_GLOBAL_CG AS e (nolock)
                                WHERE e.ID_COMPANY = @idc AND e.CONTROL_GROUP = 1 AND e.ID_CONTACT IS NOT NULL;
                        END

                        INSERT INTO #ce_list WITH (TABLOCK) (ID_PROMO, ID_CONTACT, CG, IS_FRAUD)
                        SELECT @pid, g.ID_CONTACT, 1, CASE WHEN f.ID_CONTACT IS NULL THEN 0 ELSE 1 END
                        FROM #ce_gcg AS g
                             LEFT JOIN #ce_fraud AS f ON f.ID_CONTACT = g.ID_CONTACT
                        WHERE g.K = @gb
                          AND NOT EXISTS (SELECT 1 FROM #ce_offer AS o WHERE o.ID_PROMO = @pid AND o.ID_CONTACT = g.ID_CONTACT);
                        UPDATE #ce_pr
                        SET CTRL_SRC = CASE WHEN @gb > '19000102' THEN concat(N'партии ГКГ по ', convert(nchar(10), @gb, 104))
                                            WHEN @gb = '19000101' THEN N'архив прежней ГКГ'
                                            ELSE N'текущая ГКГ — нарезана после старта акции' END
                        WHERE ID_PROMO = @pid;
                        RAISERROR(N'CVM_EFFECT_TODAY: акция %d — КГ списка некорректна, контроль из ГКГ.', 0, 1, @pid) WITH NOWAIT;
                    END

                    -- клиенты для чеков и снимка — без фрода (его скидка — из витрины скидок, чеки не нужны)
                    INSERT INTO #ce_cust WITH (TABLOCK) (ID_CONTACT)
                    SELECT DISTINCT ID_CONTACT FROM #ce_list WHERE IS_FRAUD = 0;

                    -- ---------- 3. Снимок I_CVM_CONTACT_DATA: первая и последняя покупка, ТО до скидки за всю историю ----------
                    TRUNCATE TABLE #ce_rd;
                    INSERT INTO #ce_rd (RN, D, ORG)
                    SELECT ROW_NUMBER() OVER (ORDER BY x.REF_DATE, x.ORG), x.REF_DATE, x.ORG
                    FROM (SELECT REF_DATE = REF1, ORG = 1 FROM #ce_pr WHERE REF1 IS NOT NULL
                          UNION
                          SELECT REF3, 3 FROM #ce_pr WHERE REF3 IS NOT NULL) AS x;
                    SELECT @n = count(*) FROM #ce_rd;
                    SET @i = 1;
                    WHILE @i <= @n
                    BEGIN
                        SELECT @ref = D, @org = ORG FROM #ce_rd WHERE RN = @i;

                        -- X_ALL — ТО до скидки (COST_DISCOUNT + DISCOUNT) за всю историю по дату снимка: LTV = Σ COST_DISCOUNT,
                        -- CARD_SALE_ALL = Σ DISCOUNT / T, REAL_SALE_ALL = Σ REAL_DISCOUNT / T, T = Σ REAL_DISCOUNT + Σ COST_DISCOUNT
                        -- (процедура CVM_CONTACT_DATA, вся история) → X_ALL = LTV · (1 + CARD_SALE_ALL / (1 − REAL_SALE_ALL))
                        INSERT INTO #ce_snap WITH (TABLOCK) (REF_DATE, ORG, ID_CONTACT, FIRST_DATA, LAST_DATA, LTV, X_ALL)
                        SELECT @ref, @org, d.ID_CONTACT, min(d.FIRST_DATA), max(d.LAST_DATA), max(CAST(d.LTV AS float))
                             , max(CAST(d.LTV AS float)
                                   * (1e0 + isnull(CAST(d.CARD_SALE_ALL AS float)
                                                   / nullif(1e0 - isnull(CAST(d.REAL_SALE_ALL AS float), 0e0), 0e0), 0e0)))
                        FROM dbo.I_CVM_CONTACT_DATA AS d (nolock)
                        WHERE d.ID_COMPANY = @idc
                          AND d.ID_ORGANIZATION = @org
                          AND d.REFERENCE_DATE = @ref
                          AND EXISTS (SELECT 1 FROM #ce_cust AS k WHERE k.ID_CONTACT = d.ID_CONTACT)
                        GROUP BY d.ID_CONTACT
                        OPTION (RECOMPILE);

                        SET @i += 1;
                    END

                    -- ---------- 4. Чеки клиентов списков: один проход I_CHECKHEADER на порцию ----------
                    -- окно — от самого раннего «старт − 105» до самого позднего «финиш + 4» (не позже вчера); каналы 1 и 3
                    SELECT @d_from = min(HIST_FROM), @d_to = max(AFT_TO) FROM #ce_pr;
                    IF @d_to > @yesterday SET @d_to = @yesterday;

                    INSERT INTO #ce_ch WITH (TABLOCK) (ID_CONTACT, DATA, ID_ORGANIZATION, TO_RUB, SKU)
                    SELECT c.ID_CONTACT, c.DATA, c.ID_ORGANIZATION
                         , isnull(c.COST_DISCOUNT, 0) + isnull(c.DISCOUNT, 0)   -- ТО до скидки
                         , isnull(c.SKU, 0)
                    FROM dbo.I_CHECKHEADER AS c (nolock)
                    WHERE c.ID_COMPANY = @idc
                      AND c.ID_ORGANIZATION IN (1, 3)
                      AND c.DATA >= @d_from AND c.DATA <= @d_to
                      AND EXISTS (SELECT 1 FROM #ce_cust AS k WHERE k.ID_CONTACT = c.ID_CONTACT)
                    OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);
                    SET @rows = @@ROWCOUNT;

                    SET @msg = concat(N'CVM_EFFECT_TODAY: порция ', @part, N' из ', @parts, N' — акции '
                                    , (SELECT STRING_AGG(CAST(ID_PROMO AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY ORD) FROM #ce_pr)
                                    , N'; клиентов списков (без фрода) ', (SELECT count(*) FROM #ce_cust)
                                    , N'; чеки ', convert(nchar(10), @d_from, 104), N'–', convert(nchar(10), @d_to, 104), N': ', @rows
                                    , N' строк — ', datediff(second, @t, sysdatetime()), N' с.');
                    RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
                    SET @t = sysdatetime();

                    -- ---------- 5. Клиент × акция: покупки и ТО по периодам и каналам — по акции (память — на одну акцию) ----------
                    SELECT @ord = 0, @k = 0;
                    WHILE 1 = 1
                    BEGIN
                        SELECT TOP (1) @pid = ID_PROMO, @ord = ORD, @sd = START_DATE, @fd = FINISH_DATE, @org = ORG
                                     , @hf = HIST_FROM, @pf = PRE_FROM, @pt = PRE_TO, @af = AFT_FROM, @at = AFT_TO
                                     , @ref1 = CASE WHEN REF1 >= HIST_FROM THEN REF1 ELSE dateadd(day, -1, HIST_FROM) END
                                     , @ref3 = CASE WHEN REF3 >= HIST_FROM THEN REF3 ELSE dateadd(day, -1, HIST_FROM) END
                        FROM #ce_pr
                        WHERE ORD > @ord
                        ORDER BY ORD;
                        IF @@ROWCOUNT = 0 BREAK;

                        -- время остановки: оставшиеся акции порции не начинаются (их строки списка дальше не участвуют)
                        IF @k > 0 AND @stop_at IS NOT NULL AND sysdatetime() >= @stop_at
                        BEGIN
                            SET @stopped = 1;
                            DELETE FROM #ce_pr WHERE ORD >= @ord;
                            DELETE v FROM #ce_vp AS v WHERE NOT EXISTS (SELECT 1 FROM #ce_pr AS p WHERE p.ID_PROMO = v.REAL_ID);
                            BREAK;
                        END

                        INSERT INTO #ce_agg WITH (TABLOCK) (ID_PROMO, ID_CONTACT, LAST_W1, FIRST_W1, LAST_W3, FIRST_W3, X42_1, X42_3, XG1, XG3, Y1, Y3, A1, A3
                                                          , B_IN, B_IN1, B_IN3, B_AFT, B_AFT1, B_AFT3, C1, C3, K1, K3, CA1, CA3, KA1, KA3
                                                          , W1_1, W2_1, W3_1, W4_1, W5_1, W6_1, W7_1, W8_1, W9_1, W10_1, W11_1, W12_1, W13_1, W14_1, W15_1, W1_3, W2_3, W3_3, W4_3, W5_3, W6_3, W7_3, W8_3, W9_3, W10_3, W11_3, W12_3, W13_3, W14_3, W15_3)
                        SELECT l.ID_PROMO, l.ID_CONTACT
                             , LAST_W1  = max(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt THEN c.DATA END)
                             , FIRST_W1 = min(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt THEN c.DATA END)
                             , LAST_W3  = max(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt THEN c.DATA END)
                             , FIRST_W3 = min(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt THEN c.DATA END)
                             , X42_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @pf AND c.DATA <= @pt THEN c.TO_RUB ELSE 0 END)
                             , X42_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @pf AND c.DATA <= @pt THEN c.TO_RUB ELSE 0 END)
                             -- после даты снимка канала (снимка нет или он старше окна чеков — всё окно) … старт − 1
                             , XG1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA > @ref1 AND c.DATA <= @pt THEN c.TO_RUB ELSE 0 END)
                             , XG3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA > @ref3 AND c.DATA <= @pt THEN c.TO_RUB ELSE 0 END)
                             , Y1  = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @sd AND c.DATA <= @fd THEN c.TO_RUB ELSE 0 END)
                             , Y3  = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @sd AND c.DATA <= @fd THEN c.TO_RUB ELSE 0 END)
                             , A1  = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @af AND c.DATA <= @at THEN c.TO_RUB ELSE 0 END)
                             , A3  = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @af AND c.DATA <= @at THEN c.TO_RUB ELSE 0 END)
                             , B_IN   = max(CASE WHEN c.DATA >= @sd AND c.DATA <= @fd THEN 1 ELSE 0 END)
                             , B_IN1  = max(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @sd AND c.DATA <= @fd THEN 1 ELSE 0 END)
                             , B_IN3  = max(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @sd AND c.DATA <= @fd THEN 1 ELSE 0 END)
                             , B_AFT  = max(CASE WHEN c.DATA >= @af AND c.DATA <= @at THEN 1 ELSE 0 END)
                             , B_AFT1 = max(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @af AND c.DATA <= @at THEN 1 ELSE 0 END)
                             , B_AFT3 = max(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @af AND c.DATA <= @at THEN 1 ELSE 0 END)
                             , C1  = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @sd AND c.DATA <= @fd THEN 1 ELSE 0 END)
                             , C3  = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @sd AND c.DATA <= @fd THEN 1 ELSE 0 END)
                             , K1  = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @sd AND c.DATA <= @fd THEN c.SKU ELSE 0 END)
                             , K3  = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @sd AND c.DATA <= @fd THEN c.SKU ELSE 0 END)
                             , CA1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @af AND c.DATA <= @at THEN 1 ELSE 0 END)
                             , CA3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @af AND c.DATA <= @at THEN 1 ELSE 0 END)
                             , KA1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA >= @af AND c.DATA <= @at THEN c.SKU ELSE 0 END)
                             , KA3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA >= @af AND c.DATA <= @at THEN c.SKU ELSE 0 END)
                             , W1_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 1 THEN c.TO_RUB ELSE 0 END)
                             , W2_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 2 THEN c.TO_RUB ELSE 0 END)
                             , W3_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 3 THEN c.TO_RUB ELSE 0 END)
                             , W4_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 4 THEN c.TO_RUB ELSE 0 END)
                             , W5_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 5 THEN c.TO_RUB ELSE 0 END)
                             , W6_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 6 THEN c.TO_RUB ELSE 0 END)
                             , W7_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 7 THEN c.TO_RUB ELSE 0 END)
                             , W8_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 8 THEN c.TO_RUB ELSE 0 END)
                             , W9_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 9 THEN c.TO_RUB ELSE 0 END)
                             , W10_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 10 THEN c.TO_RUB ELSE 0 END)
                             , W11_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 11 THEN c.TO_RUB ELSE 0 END)
                             , W12_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 12 THEN c.TO_RUB ELSE 0 END)
                             , W13_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 13 THEN c.TO_RUB ELSE 0 END)
                             , W14_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 14 THEN c.TO_RUB ELSE 0 END)
                             , W15_1 = sum(CASE WHEN c.ID_ORGANIZATION = 1 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 15 THEN c.TO_RUB ELSE 0 END)
                             , W1_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 1 THEN c.TO_RUB ELSE 0 END)
                             , W2_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 2 THEN c.TO_RUB ELSE 0 END)
                             , W3_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 3 THEN c.TO_RUB ELSE 0 END)
                             , W4_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 4 THEN c.TO_RUB ELSE 0 END)
                             , W5_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 5 THEN c.TO_RUB ELSE 0 END)
                             , W6_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 6 THEN c.TO_RUB ELSE 0 END)
                             , W7_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 7 THEN c.TO_RUB ELSE 0 END)
                             , W8_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 8 THEN c.TO_RUB ELSE 0 END)
                             , W9_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 9 THEN c.TO_RUB ELSE 0 END)
                             , W10_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 10 THEN c.TO_RUB ELSE 0 END)
                             , W11_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 11 THEN c.TO_RUB ELSE 0 END)
                             , W12_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 12 THEN c.TO_RUB ELSE 0 END)
                             , W13_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 13 THEN c.TO_RUB ELSE 0 END)
                             , W14_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 14 THEN c.TO_RUB ELSE 0 END)
                             , W15_3 = sum(CASE WHEN c.ID_ORGANIZATION = 3 AND c.DATA <= @pt AND (datediff(day, c.DATA, @sd) - 1) / 7 + 1 = 15 THEN c.TO_RUB ELSE 0 END)
                        FROM #ce_list AS l
                             JOIN #ce_ch AS c ON c.ID_CONTACT = l.ID_CONTACT AND c.DATA >= @hf AND c.DATA <= @at
                        WHERE l.ID_PROMO = @pid
                          AND l.IS_FRAUD = 0
                        GROUP BY l.ID_PROMO, l.ID_CONTACT
                        OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);

                        SET @k += 1;
                    END

                    IF @stopped = 1
                        RAISERROR(N'CVM_EFFECT_TODAY: время остановки — остальные акции порции не начинаются, считаются уже начатые.', 0, 1) WITH NOWAIT;

                    -- ---------- 6. Сегмент на старте, единица слоя и «до» ----------
                    -- первая покупка в канале — более ранняя из снимка и окна чеков; последняя — из окна чеков (старт − 105 …
                    -- старт − 1), а без покупок в окне — из снимка, если она раньше окна (Спящие). Слой: Активные и Отток —
                    -- неделя давности, Новые — неделя стажа, Спящие — 4-недельный блок давности (недели 16–19 — блок 1 …)
                    INSERT INTO #ce_pc WITH (TABLOCK) (ID_PROMO, ID_CONTACT, CG, SEG, UNIT, X, Y1, Y3, A1, A3
                                                     , B_IN, B_IN1, B_IN3, B_AFT, B_AFT1, B_AFT3, CY, KY, CA, KA, P)
                    SELECT l.ID_PROMO * 10 + b.B, l.ID_CONTACT, l.CG, g.SEG
                         , UNIT = CASE g.SEG WHEN 2 THEN (d.REC - 1) / 7 + 1
                                             WHEN 1 THEN (d.TEN - 1) / 7 + 1
                                             WHEN 5 THEN (d.REC - 1) / 7 + 1
                                             WHEN 6 THEN ((d.REC - 1) / 7 + 1 - 16) / 4 + 1 END
                         , X = CASE WHEN g.SEG IN (1, 2) THEN isnull(CAST(v.X42 AS float), 0e0)   -- ТО канала за 42 дня до старта
                                    WHEN g.SEG IN (5, 6) THEN isnull(s.X_ALL, 0e0)                -- вся история канала по дату снимка
                                                              + isnull(CAST(v.XG AS float), 0e0)  -- + чеки канала после снимка до старта
                                    ELSE 0e0 END
                         -- ТО и покупки периода — только в канале базы (в колонках офлайна; онлайн-колонки нулевые)
                         , isnull(v.YB, 0), 0, isnull(v.AB, 0), 0
                         , isnull(v.BI, 0), isnull(v.BI, 0), 0
                         , isnull(v.BA, 0), isnull(v.BA, 0), 0
                         , isnull(CAST(v.CB AS float), 0e0), isnull(CAST(v.KB AS float), 0e0)
                         , isnull(CAST(v.CAB AS float), 0e0), isnull(CAST(v.KAB AS float), 0e0)
                         -- ТО недели последней покупки в канале базы (неделя давности; Спящие — по снимкам, ниже)
                         , P = CASE WHEN g.SEG IN (1, 2, 5) THEN CAST(CASE (d.REC - 1) / 7 + 1 WHEN 1 THEN CASE b.B WHEN 1 THEN a.W1_1 ELSE a.W1_3 END WHEN 2 THEN CASE b.B WHEN 1 THEN a.W2_1 ELSE a.W2_3 END WHEN 3 THEN CASE b.B WHEN 1 THEN a.W3_1 ELSE a.W3_3 END WHEN 4 THEN CASE b.B WHEN 1 THEN a.W4_1 ELSE a.W4_3 END WHEN 5 THEN CASE b.B WHEN 1 THEN a.W5_1 ELSE a.W5_3 END WHEN 6 THEN CASE b.B WHEN 1 THEN a.W6_1 ELSE a.W6_3 END WHEN 7 THEN CASE b.B WHEN 1 THEN a.W7_1 ELSE a.W7_3 END WHEN 8 THEN CASE b.B WHEN 1 THEN a.W8_1 ELSE a.W8_3 END WHEN 9 THEN CASE b.B WHEN 1 THEN a.W9_1 ELSE a.W9_3 END WHEN 10 THEN CASE b.B WHEN 1 THEN a.W10_1 ELSE a.W10_3 END WHEN 11 THEN CASE b.B WHEN 1 THEN a.W11_1 ELSE a.W11_3 END WHEN 12 THEN CASE b.B WHEN 1 THEN a.W12_1 ELSE a.W12_3 END WHEN 13 THEN CASE b.B WHEN 1 THEN a.W13_1 ELSE a.W13_3 END WHEN 14 THEN CASE b.B WHEN 1 THEN a.W14_1 ELSE a.W14_3 END WHEN 15 THEN CASE b.B WHEN 1 THEN a.W15_1 ELSE a.W15_3 END END AS float) END
                    FROM #ce_list AS l
                         JOIN #ce_pr AS p ON p.ID_PROMO = l.ID_PROMO
                         CROSS JOIN (VALUES (cast(1 AS tinyint)), (cast(3 AS tinyint))) AS b (B)   -- база: 1 офлайн, 3 онлайн
                         LEFT JOIN #ce_agg  AS a ON a.ID_PROMO = l.ID_PROMO AND a.ID_CONTACT = l.ID_CONTACT
                         LEFT JOIN #ce_snap AS s ON s.REF_DATE = CASE b.B WHEN 1 THEN p.REF1 ELSE p.REF3 END AND s.ORG = b.B
                                                AND s.ID_CONTACT = l.ID_CONTACT
                         CROSS APPLY (SELECT LW  = CASE b.B WHEN 1 THEN a.LAST_W1  ELSE a.LAST_W3  END
                                           , FW  = CASE b.B WHEN 1 THEN a.FIRST_W1 ELSE a.FIRST_W3 END
                                           , X42 = CASE b.B WHEN 1 THEN a.X42_1    ELSE a.X42_3    END
                                           , XG  = CASE b.B WHEN 1 THEN a.XG1      ELSE a.XG3      END
                                           , YB  = CASE b.B WHEN 1 THEN a.Y1       ELSE a.Y3       END
                                           , AB  = CASE b.B WHEN 1 THEN a.A1       ELSE a.A3       END
                                           , BI  = CASE b.B WHEN 1 THEN a.B_IN1    ELSE a.B_IN3    END
                                           , BA  = CASE b.B WHEN 1 THEN a.B_AFT1   ELSE a.B_AFT3   END
                                           , CB  = CASE b.B WHEN 1 THEN a.C1       ELSE a.C3       END
                                           , KB  = CASE b.B WHEN 1 THEN a.K1       ELSE a.K3       END
                                           , CAB = CASE b.B WHEN 1 THEN a.CA1      ELSE a.CA3      END
                                           , KAB = CASE b.B WHEN 1 THEN a.KA1      ELSE a.KA3      END) AS v
                         CROSS APPLY (SELECT FIRST_OFF = CASE WHEN s.FIRST_DATA IS NULL                         THEN v.FW
                                                              WHEN v.FW IS NULL OR s.FIRST_DATA <= v.FW         THEN s.FIRST_DATA
                                                              ELSE v.FW END
                                           , LAST_OFF  = CASE WHEN v.LW IS NOT NULL             THEN v.LW
                                                              WHEN s.LAST_DATA < p.HIST_FROM    THEN s.LAST_DATA END) AS f
                         CROSS APPLY (SELECT REC = datediff(day, f.LAST_OFF, p.START_DATE)       -- давность последней покупки
                                           , TEN = datediff(day, f.FIRST_OFF, p.START_DATE)) AS d -- стаж
                         CROSS APPLY (SELECT SEG = CASE WHEN d.REC IS NULL                             THEN 0   -- покупок в канале нет
                                                        WHEN d.REC <= @pre_days AND d.TEN <= @pre_days THEN 1   -- Новые
                                                        WHEN d.REC <= @pre_days                        THEN 2   -- Активные
                                                        WHEN d.REC <= @hist_days                       THEN 5   -- Отток
                                                        ELSE 6 END) AS g                                        -- Спящие
                    WHERE l.IS_FRAUD = 0
                      AND (p.ORG = 1 OR b.B = 3);                                -- у онлайн-акций 110xxx — только онлайн-база

                    -- ---------- 7. Выбросы: граница G = exp(m + z·s) по ln, по акции и сегменту ----------
                    --   «до» — по всем в оценке (ЦГ и КГ: до акции группы одинаковы), клиент за границей исключается;
                    --   «во время» и «после» — m и s по покупателям КГ, n — покупатели ЦГ и КГ, по оставшимся после «до»;
                    --   меньше 4 покупателей в КГ — шаг не применяется; «после» — только если «после» считается.
                    --   Квантили — как PERCENTILE_CONT (линейная интерполяция) через номера строк.
                    SET @pass = 1;
                    WHILE @pass <= 2
                    BEGIN
                        IF @pass = 1
                        BEGIN
                            INSERT INTO #ce_thr (ID_PROMO, SEG, STEP, N_ALL, N_CG1, M, Q3, APPLIED)
                            SELECT q.ID_PROMO, q.SEG, 0, q.N, q.N1
                                 , q.M_LO + q.M_FR * (q.M_HI - q.M_LO)
                                 , q.Q_LO + q.Q_FR * (q.Q_HI - q.Q_LO)
                                 , 0
                            FROM (SELECT v.ID_PROMO, v.SEG
                                       , N    = max(v.N)
                                       , N1   = sum(CASE WHEN v.CG = 1 THEN 1 ELSE 0 END)
                                       , M_LO = max(CASE WHEN v.RN = floor(0.5e0    * (v.N - 1)) + 1 THEN v.LNV END)
                                       , M_HI = max(CASE WHEN v.RN = ceiling(0.5e0  * (v.N - 1)) + 1 THEN v.LNV END)
                                       , Q_LO = max(CASE WHEN v.RN = floor(0.75e0   * (v.N - 1)) + 1 THEN v.LNV END)
                                       , Q_HI = max(CASE WHEN v.RN = ceiling(0.75e0 * (v.N - 1)) + 1 THEN v.LNV END)
                                       , M_FR = max(0.5e0  * (v.N - 1) - floor(0.5e0  * (v.N - 1)))
                                       , Q_FR = max(0.75e0 * (v.N - 1) - floor(0.75e0 * (v.N - 1)))
                                  FROM (SELECT c.ID_PROMO, c.SEG, c.CG
                                             , LNV = log(c.X)
                                             , RN  = ROW_NUMBER() OVER (PARTITION BY c.ID_PROMO, c.SEG ORDER BY c.X)
                                             , N   = COUNT(*)     OVER (PARTITION BY c.ID_PROMO, c.SEG)
                                        FROM #ce_pc AS c
                                        WHERE c.SEG > 0 AND c.X > 0) AS v
                                  GROUP BY v.ID_PROMO, v.SEG) AS q;
                        END
                        ELSE
                        BEGIN
                            INSERT INTO #ce_thr (ID_PROMO, SEG, STEP, N_ALL, N_CG1, M, Q3, APPLIED)
                            SELECT a.ID_PROMO, a.SEG, a.STEP, a.N_ALL, a.N1
                                 , q.M_LO + q.M_FR * (q.M_HI - q.M_LO)
                                 , q.Q_LO + q.Q_FR * (q.Q_HI - q.Q_LO)
                                 , 0
                            FROM (SELECT c.ID_PROMO, c.SEG, b.STEP
                                       , N_ALL = count(*)
                                       , N1    = sum(CASE WHEN c.CG = 1 THEN 1 ELSE 0 END)
                                  FROM #ce_pc AS c
                                       JOIN #ce_vp AS p ON p.ID_PROMO = c.ID_PROMO
                                       CROSS APPLY (VALUES (cast(1 AS tinyint), c.Y1 + c.Y3), (cast(2 AS tinyint), c.A1 + c.A3)) AS b (STEP, V)
                                  WHERE c.SEG > 0 AND c.X > 0
                                    AND b.V > 0
                                    AND (b.STEP = 1 OR p.AFTER_ON = 1)
                                    AND NOT EXISTS (SELECT 1 FROM #ce_xpre AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)
                                  GROUP BY c.ID_PROMO, c.SEG, b.STEP) AS a
                                 LEFT JOIN
                                 (SELECT v.ID_PROMO, v.SEG, v.STEP
                                       , M_LO = max(CASE WHEN v.RN1 = floor(0.5e0    * (v.N1 - 1)) + 1 THEN v.LNV END)
                                       , M_HI = max(CASE WHEN v.RN1 = ceiling(0.5e0  * (v.N1 - 1)) + 1 THEN v.LNV END)
                                       , Q_LO = max(CASE WHEN v.RN1 = floor(0.75e0   * (v.N1 - 1)) + 1 THEN v.LNV END)
                                       , Q_HI = max(CASE WHEN v.RN1 = ceiling(0.75e0 * (v.N1 - 1)) + 1 THEN v.LNV END)
                                       , M_FR = max(0.5e0  * (v.N1 - 1) - floor(0.5e0  * (v.N1 - 1)))
                                       , Q_FR = max(0.75e0 * (v.N1 - 1) - floor(0.75e0 * (v.N1 - 1)))
                                  FROM (SELECT c.ID_PROMO, c.SEG, b.STEP
                                             , LNV = log(b.V)
                                             , RN1 = ROW_NUMBER() OVER (PARTITION BY c.ID_PROMO, c.SEG, b.STEP ORDER BY b.V)
                                             , N1  = COUNT(*)     OVER (PARTITION BY c.ID_PROMO, c.SEG, b.STEP)
                                        FROM #ce_pc AS c
                                             JOIN #ce_vp AS p ON p.ID_PROMO = c.ID_PROMO
                                             CROSS APPLY (VALUES (cast(1 AS tinyint), c.Y1 + c.Y3), (cast(2 AS tinyint), c.A1 + c.A3)) AS b (STEP, V)
                                        WHERE c.CG = 1
                                          AND c.SEG > 0 AND c.X > 0
                                          AND b.V > 0
                                          AND (b.STEP = 1 OR p.AFTER_ON = 1)
                                          AND NOT EXISTS (SELECT 1 FROM #ce_xpre AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)) AS v
                                  GROUP BY v.ID_PROMO, v.SEG, v.STEP) AS q
                                   ON q.ID_PROMO = a.ID_PROMO AND q.SEG = a.SEG AND q.STEP = a.STEP;
                        END

                        -- z — обратная функция нормального распределения от 1 − κ/n (приближение Acklam, погрешность ~1e-9;
                        -- хвост TL = κ/n считается напрямую, без потери точности)
                        UPDATE t
                        SET S       = k.S
                          , Z       = CASE WHEN a.OK = 1 THEN z.Z END
                          , T       = CASE WHEN a.OK = 1 THEN exp(t.M + z.Z * k.S) END
                          , APPLIED = a.OK
                        FROM #ce_thr AS t
                             CROSS APPLY (SELECT S    = (t.Q3 - t.M) / @q75
                                               , TAIL = @kappa / nullif(t.N_ALL, 0)) AS k
                             CROSS APPLY (SELECT OK = CASE WHEN t.STEP > 0 AND t.N_CG1 < 4      THEN 0
                                                           WHEN t.N_ALL < 4                     THEN 0
                                                           WHEN k.TAIL IS NULL OR k.TAIL >= 1  THEN 0
                                                           WHEN k.S IS NULL OR k.S <= 0         THEN 0
                                                           ELSE 1 END) AS a
                             CROSS APPLY (SELECT TL = CASE WHEN k.TAIL > 0 AND k.TAIL < 1 THEN k.TAIL ELSE 0.5e0 END) AS h
                             CROSS APPLY (SELECT QU = sqrt(-2e0 * log(h.TL))
                                               , QC = 0.5e0 - h.TL
                                               , R2 = square(0.5e0 - h.TL)
                                               , QL = sqrt(-2e0 * log(1e0 - h.TL))) AS q
                             CROSS APPLY (SELECT Z = CASE
                                     WHEN h.TL < 0.02425e0 THEN
                                         -(((((-7.784894002430293e-03 * q.QU - 3.223964580411365e-01) * q.QU - 2.400758277161838e+00) * q.QU
                                              - 2.549732539343734e+00) * q.QU + 4.374664141464968e+00) * q.QU + 2.938163982698783e+00)
                                         / ((((7.784695709041462e-03 * q.QU + 3.224671290700398e-01) * q.QU + 2.445134137142996e+00) * q.QU
                                             + 3.754408661907416e+00) * q.QU + 1e0)
                                     WHEN h.TL <= 0.97575e0 THEN
                                         (((((-3.969683028665376e+01 * q.R2 + 2.209460984245205e+02) * q.R2 - 2.759285104469687e+02) * q.R2
                                            + 1.383577518672690e+02) * q.R2 - 3.066479806614716e+01) * q.R2 + 2.506628277459239e+00) * q.QC
                                         / (((((-5.447609879822406e+01 * q.R2 + 1.615858368580409e+02) * q.R2 - 1.556989798598866e+02) * q.R2
                                              + 6.680131188771972e+01) * q.R2 - 1.328068155288572e+01) * q.R2 + 1e0)
                                     ELSE
                                         (((((-7.784894002430293e-03 * q.QL - 3.223964580411365e-01) * q.QL - 2.400758277161838e+00) * q.QL
                                            - 2.549732539343734e+00) * q.QL + 4.374664141464968e+00) * q.QL + 2.938163982698783e+00)
                                         / ((((7.784695709041462e-03 * q.QL + 3.224671290700398e-01) * q.QL + 2.445134137142996e+00) * q.QL
                                             + 3.754408661907416e+00) * q.QL + 1e0)
                                     END) AS z
                        WHERE (@pass = 1 AND t.STEP = 0) OR (@pass = 2 AND t.STEP > 0);

                        IF @pass = 1
                            INSERT INTO #ce_xpre (ID_PROMO, ID_CONTACT)
                            SELECT c.ID_PROMO, c.ID_CONTACT
                            FROM #ce_pc AS c
                                 JOIN #ce_thr AS t ON t.ID_PROMO = c.ID_PROMO AND t.SEG = c.SEG AND t.STEP = 0 AND t.APPLIED = 1
                            WHERE c.SEG > 0 AND c.X > t.T;

                        SET @pass += 1;
                    END

                    -- «во время» / «после» за границей — клиент исключается из всего расчёта (DROP)
                    INSERT INTO #ce_xdrop (ID_PROMO, ID_CONTACT)
                    SELECT DISTINCT c.ID_PROMO, c.ID_CONTACT
                    FROM #ce_pc AS c
                         JOIN #ce_thr AS t ON t.ID_PROMO = c.ID_PROMO AND t.SEG = c.SEG AND t.STEP > 0 AND t.APPLIED = 1
                    WHERE c.SEG > 0 AND c.X > 0
                      AND NOT EXISTS (SELECT 1 FROM #ce_xpre AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)
                      AND (   (t.STEP = 1 AND c.Y1 + c.Y3 > t.T)
                           OR (t.STEP = 2 AND c.A1 + c.A3 > t.T));

                    -- ---------- 8. Слои — недели давности без слияния (решение 19.10; у Новых — недели стажа, у Спящих — 4-недельные
                    --            блоки); неделя, где в оценке меньше 30 клиентов КГ, не сравнивается («не с кем сравнить») ----------
                    INSERT INTO #ce_lw (RN, ID_PROMO, SEG, UNIT, N1)
                    SELECT ROW_NUMBER() OVER (ORDER BY c.ID_PROMO, c.SEG, c.UNIT), c.ID_PROMO, c.SEG, c.UNIT
                         , sum(CASE WHEN c.CG = 1 THEN 1 ELSE 0 END)
                    FROM #ce_pc AS c
                    WHERE c.SEG > 0 AND c.X > 0
                      AND NOT EXISTS (SELECT 1 FROM #ce_xpre  AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)
                      AND NOT EXISTS (SELECT 1 FROM #ce_xdrop AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)
                    GROUP BY c.ID_PROMO, c.SEG, c.UNIT;

                    INSERT INTO #ce_lmap (ID_PROMO, SEG, UNIT, LAYER)
                    SELECT ID_PROMO, SEG, UNIT, UNIT FROM #ce_lw;

                    -- недели слоя: у Спящих блок b — недели 16 + 4(b − 1) … 19 + 4(b − 1)
                    INSERT INTO #ce_lrng (ID_PROMO, SEG, LAYER, WEEK_FROM, WEEK_TO)
                    SELECT ID_PROMO, SEG, LAYER
                         , CASE WHEN SEG = 6 THEN 16 + 4 * (min(UNIT) - 1) ELSE min(UNIT) END
                         , CASE WHEN SEG = 6 THEN 19 + 4 * (max(UNIT) - 1) ELSE max(UNIT) END
                    FROM #ce_lmap
                    GROUP BY ID_PROMO, SEG, LAYER;

                    -- ---------- 8а. Выравнивание ЦГ и КГ в неделе по сумме покупок «до» (решения 19.10): интервалы суммы «до» —
                    --            шкала dbo.I_INTERVALS_BUDGET_100 (шаг 100 ₽) у Активных и Новых, dbo.I_INTERVALS_BUDGET_500 (500 ₽) у Оттока и
                    --            Спящих («до» — вся история); клиент — в первый интервал, чей FINISH_SUM не меньше его суммы; доля КГ в интервале = КГ / (ЦГ + КГ). Сравниваются интервалы, где есть ЦГ и не меньше
                    --            30 клиентов КГ (соседние интервалы шкалы сливаются, пока в объединённом нет 30 КГ — решение 19.10);
                    --            доля недели — минимальная доля КГ среди них, но не ниже @kg_share_min (3 %).
                    --            В интервале с долей выше — случайно убираются лишние КГ, с долей ниже — лишние ЦГ («лишние ЦГ
                    --            тоже убираются, если для них не нашлось КГ»); случайно и воспроизводимо — по MD5 номера клиента.
                    --            Неделя меньше 30 КГ и интервал без ЦГ — «не с кем сравнить».
                    --            Дальше слой = неделя: Доп ТО и показатели — на выровненном списке недели ----------
                    INSERT INTO #ce_ev WITH (TABLOCK) (ID_PROMO, ID_CONTACT, SEG, CG, RL, IV)
                    SELECT c.ID_PROMO, c.ID_CONTACT, c.SEG, c.CG, m.LAYER
                         , isnull(iv.ID_INTERVAL, CASE WHEN c.SEG IN (5, 6) THEN @iv_last2 ELSE @iv_last1 END)
                    FROM #ce_pc AS c
                         JOIN #ce_lmap AS m ON m.ID_PROMO = c.ID_PROMO AND m.SEG = c.SEG AND m.UNIT = c.UNIT
                         OUTER APPLY (SELECT TOP (1) t.ID_INTERVAL FROM #ce_ivt AS t
                                      WHERE t.SC = CASE WHEN c.SEG IN (5, 6) THEN 2 ELSE 1 END AND t.FINISH_SUM >= c.X
                                      ORDER BY t.FINISH_SUM, t.ID_INTERVAL) AS iv
                    WHERE c.SEG > 0 AND c.X > 0
                      AND NOT EXISTS (SELECT 1 FROM #ce_xpre  AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT)
                      AND NOT EXISTS (SELECT 1 FROM #ce_xdrop AS x WHERE x.ID_PROMO = c.ID_PROMO AND x.ID_CONTACT = c.ID_CONTACT);

                    -- интервалы шкалы недели: ЦГ, КГ
                    INSERT INTO #ce_is0 (ID_PROMO, SEG, RL, IV, RN, N0, N1)
                    SELECT e.ID_PROMO, e.SEG, e.RL, e.IV, ROW_NUMBER() OVER (PARTITION BY e.ID_PROMO, e.SEG, e.RL ORDER BY e.IV)
                         , sum(CASE WHEN e.CG = 0 THEN 1 ELSE 0 END), sum(CASE WHEN e.CG = 1 THEN 1 ELSE 0 END)
                    FROM #ce_ev AS e
                    GROUP BY e.ID_PROMO, e.SEG, e.RL, e.IV;

                    -- соседние интервалы шкалы сливаются, пока в объединённом нет 30 КГ (решение 19.10): по порядку шкалы группа
                    -- закрывается на первом интервале, на котором в ней накопилось 30 КГ;
                    -- хвост меньше 30 КГ в конце недели — с предыдущей группой
                    ;WITH r AS (
                        SELECT ID_PROMO, SEG, RL, RN, IV, GS = IV, ACC = N1
                        FROM #ce_is0
                        WHERE RN = 1
                        UNION ALL
                        SELECT o.ID_PROMO, o.SEG, o.RL, o.RN, o.IV
                             , CASE WHEN r.ACC >= @layer_min_cg THEN o.IV ELSE r.GS END
                             , CASE WHEN r.ACC >= @layer_min_cg THEN o.N1 ELSE r.ACC + o.N1 END
                        FROM r
                             JOIN #ce_is0 AS o ON o.ID_PROMO = r.ID_PROMO AND o.SEG = r.SEG AND o.RL = r.RL AND o.RN = r.RN + 1)
                    INSERT INTO #ce_ivm (ID_PROMO, SEG, RL, IV, G)
                    SELECT ID_PROMO, SEG, RL, IV, GS FROM r
                    OPTION (MAXRECURSION 0);

                    UPDATE m
                    SET G = t.GP
                    FROM #ce_ivm AS m
                         JOIN (SELECT v.ID_PROMO, v.SEG, v.RL, v.GL
                                    , GP = (SELECT max(m2.G) FROM #ce_ivm AS m2
                                            WHERE m2.ID_PROMO = v.ID_PROMO AND m2.SEG = v.SEG AND m2.RL = v.RL AND m2.G < v.GL)
                                    , NL = (SELECT sum(i.N1) FROM #ce_is0 AS i
                                            WHERE i.ID_PROMO = v.ID_PROMO AND i.SEG = v.SEG AND i.RL = v.RL AND i.IV >= v.GL)
                               FROM (SELECT ID_PROMO, SEG, RL, GL = max(G) FROM #ce_ivm GROUP BY ID_PROMO, SEG, RL) AS v) AS t
                           ON t.ID_PROMO = m.ID_PROMO AND t.SEG = m.SEG AND t.RL = m.RL AND m.G = t.GL
                    WHERE t.NL < @layer_min_cg AND t.GP IS NOT NULL;

                    -- интервалы сравнения: ЦГ, КГ, доля КГ, в сравнении или нет
                    INSERT INTO #ce_is (ID_PROMO, SEG, RL, IV, N0, N1, OK)
                    SELECT m.ID_PROMO, m.SEG, m.RL, m.G, sum(i.N0), sum(i.N1), 0
                    FROM #ce_ivm AS m
                         JOIN #ce_is0 AS i ON i.ID_PROMO = m.ID_PROMO AND i.SEG = m.SEG AND i.RL = m.RL AND i.IV = m.IV
                    GROUP BY m.ID_PROMO, m.SEG, m.RL, m.G;

                    UPDATE i
                    SET OK = 1
                    FROM #ce_is AS i
                         JOIN #ce_lw AS w ON w.ID_PROMO = i.ID_PROMO AND w.SEG = i.SEG AND w.UNIT = i.RL
                    WHERE w.N1 >= @layer_min_cg                                  -- в неделе не меньше 30 КГ
                      AND i.N0 > 0 AND i.N1 >= @layer_min_cg;

                    -- доля недели — минимальная доля КГ среди интервалов в сравнении, не ниже 3 % (доля < 1: в интервале есть ЦГ)
                    INSERT INTO #ce_ws (ID_PROMO, SEG, RL, SHARE)
                    SELECT ID_PROMO, SEG, RL
                         , CASE WHEN min(N1 * 1e0 / (N0 + N1)) < @kg_share_min THEN @kg_share_min ELSE min(N1 * 1e0 / (N0 + N1)) END
                    FROM #ce_is
                    WHERE OK = 1
                    GROUP BY ID_PROMO, SEG, RL;

                    -- сколько оставить: доля выше доли недели — КГ = ⌈доля · ЦГ / (1 − доля)⌉, ЦГ вся; ниже — ЦГ = ⌊КГ · (1 − доля) / доля⌋
                    -- (не меньше 1), КГ вся
                    UPDATE i
                    SET KEEP1 = CASE WHEN i.N1 * 1e0 / (i.N0 + i.N1) > w.SHARE
                                     THEN CASE WHEN CEILING(w.SHARE * i.N0 / (1e0 - w.SHARE) - 1e-9) > i.N1 THEN i.N1
                                               ELSE CAST(CEILING(w.SHARE * i.N0 / (1e0 - w.SHARE) - 1e-9) AS int) END
                                     ELSE i.N1 END
                      , KEEP0 = CASE WHEN i.N1 * 1e0 / (i.N0 + i.N1) < w.SHARE
                                     THEN CASE WHEN FLOOR(i.N1 * (1e0 - w.SHARE) / w.SHARE + 1e-9) >= i.N0 THEN i.N0
                                               WHEN FLOOR(i.N1 * (1e0 - w.SHARE) / w.SHARE + 1e-9) < 1 THEN 1
                                               ELSE CAST(FLOOR(i.N1 * (1e0 - w.SHARE) / w.SHARE + 1e-9) AS int) END
                                     ELSE i.N0 END
                    FROM #ce_is AS i
                         JOIN #ce_ws AS w ON w.ID_PROMO = i.ID_PROMO AND w.SEG = i.SEG AND w.RL = i.RL
                    WHERE i.OK = 1;

                    -- выровненный список: в интервалах в сравнении — KEEP0 ЦГ и KEEP1 КГ; где убирать не нужно — все; кого оставить —
                    -- случайно: порядок по MD5 от номера клиента и номера акции (воспроизводимо, не связан с номером карты)
                    INSERT INTO #ce_kp WITH (TABLOCK) (ID_PROMO, ID_CONTACT, RL)
                    SELECT e.ID_PROMO, e.ID_CONTACT, e.RL
                    FROM #ce_ev AS e
                         JOIN #ce_ivm AS m ON m.ID_PROMO = e.ID_PROMO AND m.SEG = e.SEG AND m.RL = e.RL AND m.IV = e.IV
                         JOIN #ce_is AS i ON i.ID_PROMO = e.ID_PROMO AND i.SEG = e.SEG AND i.RL = e.RL AND i.IV = m.G
                    WHERE i.OK = 1
                      AND ((e.CG = 0 AND i.KEEP0 = i.N0) OR (e.CG = 1 AND i.KEEP1 = i.N1));

                    INSERT INTO #ce_kp WITH (TABLOCK) (ID_PROMO, ID_CONTACT, RL)
                    SELECT x.ID_PROMO, x.ID_CONTACT, x.RL
                    FROM (SELECT e.ID_PROMO, e.ID_CONTACT, e.RL, KEEP = CASE e.CG WHEN 0 THEN i.KEEP0 ELSE i.KEEP1 END
                               , RN = ROW_NUMBER() OVER (PARTITION BY e.ID_PROMO, e.SEG, e.RL, m.G, e.CG
                                                         ORDER BY HASHBYTES('MD5', CONCAT(e.ID_CONTACT, '|', e.ID_PROMO / 10)), e.ID_CONTACT)
                          FROM #ce_ev AS e
                               JOIN #ce_ivm AS m ON m.ID_PROMO = e.ID_PROMO AND m.SEG = e.SEG AND m.RL = e.RL AND m.IV = e.IV
                               JOIN #ce_is AS i ON i.ID_PROMO = e.ID_PROMO AND i.SEG = e.SEG AND i.RL = e.RL AND i.IV = m.G
                          WHERE i.OK = 1
                            AND ((e.CG = 0 AND i.KEEP0 < i.N0) OR (e.CG = 1 AND i.KEEP1 < i.N1))) AS x
                    WHERE x.RN <= x.KEEP;

                    -- неделя: клиентов ЦГ и КГ в оценке до выравнивания, интервалов в сравнении (для витрины и пересчёта на всю ЦГ)
                    INSERT INTO #ce_wn (ID_PROMO, SEG, RL, N0ALL, N1ALL, IVS)
                    SELECT ID_PROMO, SEG, RL, sum(N0), sum(N1), sum(CASE WHEN OK = 1 THEN 1 ELSE 0 END)
                    FROM #ce_is
                    GROUP BY ID_PROMO, SEG, RL;

                    -- Спящие выровненного списка: ТО месяца последней покупки = накопленный ТО до скидки по снимку на конец этого
                    -- месяца − на конец предыдущего (снимки I_CVM_CONTACT_DATA есть с 31.08.2025: раньше — без проверки)
                    -- (снимки датированы концом месяца — EOMONTH совпадает с REFERENCE_DATE)
                    INSERT INTO #ce_spk WITH (TABLOCK) (ID_PROMO, ID_CONTACT, ORG, M1, M0, FIRST_DATA)
                    SELECT c.ID_PROMO, c.ID_CONTACT, v.BASIS, EOMONTH(s.LAST_DATA), EOMONTH(s.LAST_DATA, -1), s.FIRST_DATA
                    FROM #ce_kp AS k
                         JOIN #ce_pc   AS c ON c.ID_PROMO = k.ID_PROMO AND c.ID_CONTACT = k.ID_CONTACT AND c.SEG = 6
                         JOIN #ce_vp   AS v ON v.ID_PROMO = c.ID_PROMO
                         JOIN #ce_pr   AS p ON p.ID_PROMO = v.REAL_ID
                         JOIN #ce_snap AS s ON s.REF_DATE = CASE v.BASIS WHEN 1 THEN p.REF1 ELSE p.REF3 END AND s.ORG = v.BASIS
                                           AND s.ID_CONTACT = c.ID_CONTACT
                    WHERE s.LAST_DATA IS NOT NULL;

                    IF EXISTS (SELECT 1 FROM #ce_spk)
                    BEGIN
                        -- снимки читаются по одной дате и каналу (константа даты — чтение только нужного снимка)
                        INSERT INTO #ce_srd (RN, RD, ORG)
                        SELECT ROW_NUMBER() OVER (ORDER BY x.RD, x.ORG), x.RD, x.ORG
                        FROM (SELECT RD = M1, ORG FROM #ce_spk UNION SELECT M0, ORG FROM #ce_spk) AS x;
                        SELECT @n = count(*) FROM #ce_srd;
                        SET @i = 1;
                        WHILE @i <= @n
                        BEGIN
                            SELECT @ref = RD, @org = ORG FROM #ce_srd WHERE RN = @i;
                            INSERT INTO #ce_sx WITH (TABLOCK) (ID_CONTACT, ORG, RD, XA)
                            SELECT d.ID_CONTACT, @org, @ref
                                 , max(CAST(d.LTV AS float)
                                       * (1e0 + isnull(CAST(d.CARD_SALE_ALL AS float)
                                                       / nullif(1e0 - isnull(CAST(d.REAL_SALE_ALL AS float), 0e0), 0e0), 0e0)))
                            FROM dbo.I_CVM_CONTACT_DATA AS d (nolock)
                            WHERE d.ID_COMPANY = @idc
                              AND d.ID_ORGANIZATION = @org
                              AND d.REFERENCE_DATE = @ref
                              AND EXISTS (SELECT 1 FROM #ce_spk AS k
                                          WHERE k.ID_CONTACT = d.ID_CONTACT AND k.ORG = @org AND (k.M1 = @ref OR k.M0 = @ref))
                            GROUP BY d.ID_CONTACT
                            OPTION (RECOMPILE, MAX_GRANT_PERCENT = 10);
                            SET @i += 1;
                        END

                        -- первая покупка в месяце последней — снимка на конец прошлого месяца нет, накопленный ТО там 0
                        INSERT INTO #ce_sp WITH (TABLOCK) (ID_PROMO, ID_CONTACT, P)
                        SELECT k.ID_PROMO, k.ID_CONTACT, a.XA - CASE WHEN k.FIRST_DATA > k.M0 THEN 0e0 ELSE b.XA END
                        FROM #ce_spk AS k
                             JOIN #ce_sx AS a ON a.ID_CONTACT = k.ID_CONTACT AND a.ORG = k.ORG AND a.RD = k.M1
                             LEFT JOIN #ce_sx AS b ON b.ID_CONTACT = k.ID_CONTACT AND b.ORG = k.ORG AND b.RD = k.M0
                        WHERE (k.FIRST_DATA > k.M0 OR b.XA IS NOT NULL)
                          AND a.XA >= isnull(b.XA, 0e0);
                    END

                    -- клиенты выровненного списка со слоем-неделей; без «после» его ТО и покупки — нули (итого = в акцию)
                    INSERT INTO #ce_cl WITH (TABLOCK) (ID_PROMO, ID_CONTACT, SEG, LAYER, CG, AON, X, Y1, Y3, A1, A3
                                                     , B_IN, B_IN1, B_IN3, B_AFT, B_AFT1, B_AFT3, CY, KY, CA, KA, P)
                    SELECT c.ID_PROMO, c.ID_CONTACT, c.SEG, k.RL, c.CG, p.AFTER_ON, c.X, c.Y1, c.Y3
                         , c.A1 * p.AFTER_ON, c.A3 * p.AFTER_ON
                         , c.B_IN, c.B_IN1, c.B_IN3, c.B_AFT * p.AFTER_ON, c.B_AFT1 * p.AFTER_ON, c.B_AFT3 * p.AFTER_ON
                         , c.CY, c.KY, c.CA * p.AFTER_ON, c.KA * p.AFTER_ON
                         , CASE WHEN c.SEG = 6 THEN sp.P WHEN c.P <= c.X THEN c.P END
                    FROM #ce_pc AS c
                         JOIN #ce_vp AS p ON p.ID_PROMO = c.ID_PROMO
                         JOIN #ce_kp AS k ON k.ID_PROMO = c.ID_PROMO AND k.ID_CONTACT = c.ID_CONTACT
                         LEFT JOIN #ce_sp AS sp ON sp.ID_PROMO = c.ID_PROMO AND sp.ID_CONTACT = c.ID_CONTACT;

                    -- сегмент входит в итог, если в оценке не меньше 2 клиентов КГ (иначе ни отношения, ни ошибки)
                    INSERT INTO #ce_segok (ID_PROMO, SEG, N0, N1, OK)
                    SELECT ID_PROMO, SEG
                         , sum(CASE WHEN CG = 0 THEN 1 ELSE 0 END)
                         , sum(CASE WHEN CG = 1 THEN 1 ELSE 0 END)
                         , CASE WHEN sum(CASE WHEN CG = 1 THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END
                    FROM #ce_cl
                    GROUP BY ID_PROMO, SEG;

                    -- ---------- 9. Оценки по слоям: акция × сегмент × слой × период (1 в акцию, 2 после, 3 итого) × канал
                    --            — по акции на базе канала (ID = номер · 10 + канал): X — «до» канала базы, Y — ТО периода канала базы ----------
                    --   R = ΣY_КГ / ΣX_КГ; Доп ТО = ΣY_ЦГ − R · ΣX_ЦГ; ожидаемый ТО ЦГ без акции = R · ΣX_ЦГ;
                    --   ошибка: e = Y − R·X; SE² = n_ЦГ · s²_ЦГ + (ΣX_ЦГ / ΣX_КГ)² · n_КГ · s²_КГ, где s²_ЦГ = Σ(e − ē_ЦГ)² / (n_ЦГ − 1),
                    --   s²_КГ = Σe² / (n_КГ − 1) (в КГ Σe = 0); регрессия по КГ: b = Σ(X − X̄)(Y − Ȳ) / Σ(X − X̄)², a = Ȳ − b·X̄,
                    --   Доп ТО = ΣY_ЦГ − n_ЦГ·a − b·ΣX_ЦГ. «После» — только если считается.
                    INSERT INTO #ce_e1 (ID_PROMO, SEG, LAYER, PER, CH, CG, N, SX, SY, SB, SC, SK)
                    SELECT c.ID_PROMO, c.SEG, c.LAYER, m.PER, m.CH, c.CG
                         , count(*), sum(c.X), sum(m.Y), sum(m.B), sum(m.C), sum(m.K)
                    FROM #ce_cl AS c
                         -- акция на базе канала: ТО периода канала базы — в колонках офлайна (CH = 1), остальные каналы не нужны
                         CROSS APPLY (VALUES (cast(1 AS tinyint), cast(1 AS tinyint), c.Y1, cast(c.B_IN1 AS int), c.CY, c.KY)
                                           , (2, 1, c.A1,                      c.B_AFT1, c.CA,        c.KA)
                                           , (3, 1, c.Y1 + c.A1,               NULL,     c.CY + c.CA, c.KY + c.KA)) AS m (PER, CH, Y, B, C, K)
                    WHERE m.PER <> 2 OR c.AON = 1
                    GROUP BY c.ID_PROMO, c.SEG, c.LAYER, m.PER, m.CH, c.CG;

                    INSERT INTO #ce_u (ID_PROMO, SEG, LAYER, PER, CH, N0, SX0, SY0, SB0, N1, SX1, SY1, SB1, R, ADD_A, EBAR0, XBAR1, YBAR1
                                     , SC0, SK0, SC1, SK1)
                    SELECT k.ID_PROMO, k.SEG, k.LAYER, k.PER, k.CH
                         , isnull(a.N, 0), isnull(a.SX, 0e0), isnull(a.SY, 0e0), CASE WHEN k.PER < 3 THEN isnull(a.SB, 0) END
                         , isnull(b.N, 0), isnull(b.SX, 0e0), isnull(b.SY, 0e0), CASE WHEN k.PER < 3 THEN isnull(b.SB, 0) END
                         , r.R
                         , isnull(a.SY, 0e0) - r.R * isnull(a.SX, 0e0)
                         , (isnull(a.SY, 0e0) - r.R * isnull(a.SX, 0e0)) / nullif(a.N, 0)
                         , b.SX / nullif(b.N, 0)
                         , b.SY / nullif(b.N, 0)
                         , isnull(a.SC, 0e0), isnull(a.SK, 0e0), isnull(b.SC, 0e0), isnull(b.SK, 0e0)
                    FROM (SELECT DISTINCT ID_PROMO, SEG, LAYER, PER, CH FROM #ce_e1) AS k
                         LEFT JOIN #ce_e1 AS a ON a.ID_PROMO = k.ID_PROMO AND a.SEG = k.SEG AND a.LAYER = k.LAYER
                                              AND a.PER = k.PER AND a.CH = k.CH AND a.CG = 0
                         LEFT JOIN #ce_e1 AS b ON b.ID_PROMO = k.ID_PROMO AND b.SEG = k.SEG AND b.LAYER = k.LAYER
                                              AND b.PER = k.PER AND b.CH = k.CH AND b.CG = 1
                         CROSS APPLY (SELECT R = b.SY / nullif(b.SX, 0)) AS r;

                    -- второй проход: отклонения от R·X и центрированные суммы для регрессии
                    INSERT INTO #ce_e2 (ID_PROMO, SEG, LAYER, PER, CH, CG, SS, SXY, SXX)
                    SELECT c.ID_PROMO, c.SEG, c.LAYER, m.PER, m.CH, c.CG
                         , sum(square(m.Y - u.R * c.X - CASE WHEN c.CG = 0 THEN u.EBAR0 ELSE 0e0 END))
                         , sum((c.X - u.XBAR1) * (m.Y - u.YBAR1))
                         , sum(square(c.X - u.XBAR1))
                    FROM #ce_cl AS c
                         CROSS APPLY (VALUES (cast(1 AS tinyint), cast(1 AS tinyint), c.Y1)
                                           , (2, 1, c.A1)
                                           , (3, 1, c.Y1 + c.A1)) AS m (PER, CH, Y)
                         JOIN #ce_u AS u ON u.ID_PROMO = c.ID_PROMO AND u.SEG = c.SEG AND u.LAYER = c.LAYER
                                        AND u.PER = m.PER AND u.CH = m.CH
                    GROUP BY c.ID_PROMO, c.SEG, c.LAYER, m.PER, m.CH, c.CG;

                    INSERT INTO #ce_res (ID_PROMO, SEG, LAYER, PER, CH, N0, N1, SX0, SX1, SY0, SY1, SB0, SB1
                                       , EXPV, ADD_A, SE2, ADD_REG, INC, IN_TOTAL, SC0, SC1, SK0, SK1, EXPC, EXPK, EXPB)
                    SELECT u.ID_PROMO, u.SEG, u.LAYER, u.PER, u.CH, u.N0, u.N1, u.SX0, u.SX1, u.SY0, u.SY1, u.SB0, u.SB1
                         , EXPV    = u.R * u.SX0
                         , ADD_A   = u.ADD_A
                         -- в слое с одним клиентом ЦГ его разброс не оценить — часть ошибки от ЦГ 0 (в тесте ошибка была пустой)
                         , SE2     = CASE WHEN u.N1 > 1 AND u.SX1 > 0
                                          THEN CASE WHEN u.N0 > 1 THEN u.N0 * e0.SS / (u.N0 - 1) ELSE 0e0 END
                                             + square(u.SX0 / u.SX1) * u.N1 * e1.SS / (u.N1 - 1) END
                         , ADD_REG = CASE WHEN e1.SXX > 0
                                          THEN u.SY0 - u.N0 * (u.YBAR1 - e1.SXY / e1.SXX * u.XBAR1) - e1.SXY / e1.SXX * u.SX0 END
                         -- инкрементальные покупатели слоя: (доля купивших ЦГ − доля КГ) · n_ЦГ
                         , INC     = CASE WHEN u.PER < 3 AND u.N1 > 0
                                          THEN CASE WHEN u.N0 > 0 THEN (cast(u.SB0 AS float) / u.N0 - cast(u.SB1 AS float) / u.N1) * u.N0
                                                    ELSE 0e0 END END
                         , IN_TOTAL = isnull(g.OK, 0)
                         , u.SC0, u.SC1, u.SK0, u.SK1
                         -- та же привязка к «до», что у ожидаемого ТО (R · ΣX_ЦГ = ΣY_КГ · ΣX_ЦГ / ΣX_КГ)
                         , EXPC    = CASE WHEN u.R IS NOT NULL THEN u.SC1 * u.SX0 / u.SX1 END
                         , EXPK    = CASE WHEN u.R IS NOT NULL THEN u.SK1 * u.SX0 / u.SX1 END
                         , EXPB    = CASE WHEN u.R IS NOT NULL AND u.PER < 3 THEN u.SB1 * u.SX0 / u.SX1 END
                    FROM #ce_u AS u
                         LEFT JOIN #ce_e2 AS e0 ON e0.ID_PROMO = u.ID_PROMO AND e0.SEG = u.SEG AND e0.LAYER = u.LAYER
                                               AND e0.PER = u.PER AND e0.CH = u.CH AND e0.CG = 0
                         LEFT JOIN #ce_e2 AS e1 ON e1.ID_PROMO = u.ID_PROMO AND e1.SEG = u.SEG AND e1.LAYER = u.LAYER
                                               AND e1.PER = u.PER AND e1.CH = u.CH AND e1.CG = 1
                         LEFT JOIN #ce_segok AS g ON g.ID_PROMO = u.ID_PROMO AND g.SEG = u.SEG;

                    -- ---------- 9а. Проверка «до старта» (решения 19.10): та же формула на выровненном списке недели, «период» P —
                    --            последний отрезок «до» (неделя последней покупки; у Спящих — месяц по снимкам), «до» Q = X − P.
                    --            Псевдоэффект = ΣP_ЦГ − ΣQ_ЦГ · ΣP_КГ / ΣQ_КГ по неделям сегментов в итоге; ошибка — как у Доп ТО.
                    --            Дальше @z_pseudo (3,29) ошибки — «КГ несопоставима»: акция не входит в общий эффект месяца ----------
                    INSERT INTO #ce_q1 (ID_PROMO, SEG, LAYER, CG, N, SQ, SP)
                    SELECT ID_PROMO, SEG, LAYER, CG, count(*), sum(X - P), sum(P)
                    FROM #ce_cl
                    WHERE P IS NOT NULL
                    GROUP BY ID_PROMO, SEG, LAYER, CG;

                    INSERT INTO #ce_qu (ID_PROMO, SEG, LAYER, N0, SQ0, SP0, N1, SQ1, SP1, R, EBAR0)
                    SELECT a.ID_PROMO, a.SEG, a.LAYER, a.N, a.SQ, a.SP, b.N, b.SQ, b.SP
                         , r.R, (a.SP - r.R * a.SQ) / nullif(a.N, 0)
                    FROM #ce_q1 AS a
                         JOIN #ce_q1 AS b ON b.ID_PROMO = a.ID_PROMO AND b.SEG = a.SEG AND b.LAYER = a.LAYER AND b.CG = 1
                         CROSS APPLY (SELECT R = b.SP / nullif(b.SQ, 0)) AS r
                    WHERE a.CG = 0;

                    INSERT INTO #ce_q2 (ID_PROMO, SEG, LAYER, CG, SS)
                    SELECT c.ID_PROMO, c.SEG, c.LAYER, c.CG
                         , sum(square(c.P - u.R * (c.X - c.P) - CASE WHEN c.CG = 0 THEN u.EBAR0 ELSE 0e0 END))
                    FROM #ce_cl AS c
                         JOIN #ce_qu AS u ON u.ID_PROMO = c.ID_PROMO AND u.SEG = c.SEG AND u.LAYER = c.LAYER
                    WHERE c.P IS NOT NULL AND u.R IS NOT NULL
                    GROUP BY c.ID_PROMO, c.SEG, c.LAYER, c.CG;

                    INSERT INTO #ce_qt (ID_PROMO, CH, PS, PSE2)
                    SELECT u.ID_PROMO / 10, cast(u.ID_PROMO % 10 AS tinyint)
                         , sum(u.SP0 - u.R * u.SQ0)
                         , sum(CASE WHEN u.N0 > 1 THEN u.N0 * isnull(e0.SS, 0e0) / (u.N0 - 1) ELSE 0e0 END
                               + CASE WHEN u.N1 > 1 AND u.SQ1 > 0 THEN square(u.SQ0 / u.SQ1) * u.N1 * e1.SS / (u.N1 - 1) END)
                    FROM #ce_qu AS u
                         JOIN #ce_segok AS g ON g.ID_PROMO = u.ID_PROMO AND g.SEG = u.SEG AND g.OK = 1
                         LEFT JOIN #ce_q2 AS e0 ON e0.ID_PROMO = u.ID_PROMO AND e0.SEG = u.SEG AND e0.LAYER = u.LAYER AND e0.CG = 0
                         JOIN #ce_q2 AS e1 ON e1.ID_PROMO = u.ID_PROMO AND e1.SEG = u.SEG AND e1.LAYER = u.LAYER AND e1.CG = 1
                    WHERE u.R IS NOT NULL AND u.N1 > 1 AND u.SQ1 > 0
                    GROUP BY u.ID_PROMO;

                    INSERT INTO #ce_qt (ID_PROMO, CH, PS, PSE2)
                    SELECT ID_PROMO, 0, sum(PS), sum(PSE2)
                    FROM #ce_qt
                    WHERE CH IN (1, 3)
                    GROUP BY ID_PROMO;

                    -- Доп ТО на всю ЦГ (решение 19.10: «эффект на одного клиента выводим для всех клиентов ЦГ»): у недели с оценкой —
                    -- Доп ТО на клиента недели × все клиенты ЦГ недели в оценке (до выравнивания); у недели без сравнения — Доп ТО
                    -- на клиента сегмента × её клиенты ЦГ; сегменты вне итога не входят
                    INSERT INTO #ce_all (ID_PROMO, PER, ADDALL)
                    SELECT q.ID_PROMO, q.PER, sum(q.V)
                    FROM (SELECT r.ID_PROMO, r.PER, V = r.ADD_A * wn.N0ALL / cast(r.N0 AS float)
                          FROM #ce_res AS r
                               JOIN #ce_wn AS wn ON wn.ID_PROMO = r.ID_PROMO AND wn.SEG = r.SEG AND wn.RL = r.LAYER
                          WHERE r.CH = 1 AND r.IN_TOTAL = 1 AND r.EXPV IS NOT NULL AND r.N0 > 0
                          UNION ALL
                          SELECT wn.ID_PROMO, sg.PER, wn.N0ALL * sg.APC
                          FROM #ce_wn AS wn
                               JOIN (SELECT ID_PROMO, SEG, PER, APC = sum(ADD_A) / nullif(sum(cast(N0 AS float)), 0)
                                     FROM #ce_res
                                     WHERE CH = 1 AND IN_TOTAL = 1 AND EXPV IS NOT NULL AND N0 > 0
                                     GROUP BY ID_PROMO, SEG, PER) AS sg ON sg.ID_PROMO = wn.ID_PROMO AND sg.SEG = wn.SEG
                          WHERE NOT EXISTS (SELECT 1 FROM #ce_res AS r
                                            WHERE r.ID_PROMO = wn.ID_PROMO AND r.SEG = wn.SEG AND r.LAYER = wn.RL AND r.PER = sg.PER
                                              AND r.CH = 1 AND r.IN_TOTAL = 1 AND r.EXPV IS NOT NULL AND r.N0 > 0)) AS q
                    GROUP BY q.ID_PROMO, q.PER;

                    -- купившие в обоих каналах — по тем же неделям, что купившие в итоге (IN_TOTAL = 1, ожидание посчитано):
                    -- каждый канал — в своей базе; «после» в #ce_cl уже с учётом AFTER_ON
                    INSERT INTO #ce_bb (ID_PROMO, CG, D1, D2)
                    SELECT x.RP, x.CG
                         , sum(CASE WHEN x.I1 = 1 AND x.I3 = 1 THEN 1 ELSE 0 END)
                         , sum(CASE WHEN x.A1 = 1 AND x.A3 = 1 THEN 1 ELSE 0 END)
                    FROM (SELECT RP = c.ID_PROMO / 10, c.ID_CONTACT, c.CG
                               , I1 = max(CASE WHEN c.ID_PROMO % 10 = 1 AND r1.ID_PROMO IS NOT NULL THEN c.B_IN1  ELSE 0 END)
                               , I3 = max(CASE WHEN c.ID_PROMO % 10 = 3 AND r1.ID_PROMO IS NOT NULL THEN c.B_IN1  ELSE 0 END)
                               , A1 = max(CASE WHEN c.ID_PROMO % 10 = 1 AND r2.ID_PROMO IS NOT NULL THEN c.B_AFT1 ELSE 0 END)
                               , A3 = max(CASE WHEN c.ID_PROMO % 10 = 3 AND r2.ID_PROMO IS NOT NULL THEN c.B_AFT1 ELSE 0 END)
                          FROM #ce_cl AS c
                               LEFT JOIN #ce_res AS r1 ON r1.ID_PROMO = c.ID_PROMO AND r1.SEG = c.SEG AND r1.LAYER = c.LAYER
                                                      AND r1.PER = 1 AND r1.CH = 1 AND r1.IN_TOTAL = 1 AND r1.EXPV IS NOT NULL
                               LEFT JOIN #ce_res AS r2 ON r2.ID_PROMO = c.ID_PROMO AND r2.SEG = c.SEG AND r2.LAYER = c.LAYER
                                                      AND r2.PER = 2 AND r2.CH = 1 AND r2.IN_TOTAL = 1 AND r2.EXPV IS NOT NULL
                          WHERE c.B_IN1 = 1 OR c.B_AFT1 = 1                  -- в счёт идут только купившие
                          GROUP BY c.ID_PROMO / 10, c.ID_CONTACT, c.CG) AS x
                    GROUP BY x.RP, x.CG;

                    -- итог акции по каналу продаж (0 все, 1 офлайн, 3 онлайн): суммы слоёв оцениваемых сегментов; ошибки — через сумму SE²
                    INSERT INTO #ce_tot (ID_PROMO, CH, LAYERS, ADD1, SE21, REG1, INC1, SB01, N01, ADD2, SE22, INC2, SB02, N02, ADD3, SE23, REG3
                                       , NE1, SY01, EY1, SC01, EC1, SK01, EK1, NE2, SY02, EY2, SC02, EC2, SK02, EK2
                                       , NB1, EB1, KB1, NB2, EB2, KB2, ALL1, ALL2, ALL3)
                    SELECT r.ID_PROMO / 10, cast(r.ID_PROMO % 10 AS tinyint)
                         , sum(CASE WHEN r.PER = 1 THEN 1 ELSE 0 END)
                         , sum(CASE WHEN r.PER = 1 THEN r.ADD_A END)
                         , sum(CASE WHEN r.PER = 1 THEN r.SE2 END)
                         , CASE WHEN sum(CASE WHEN r.PER = 1 AND r.ADD_REG IS NULL THEN 1 ELSE 0 END) = 0
                                THEN sum(CASE WHEN r.PER = 1 THEN r.ADD_REG END) END
                         , sum(CASE WHEN r.PER = 1 THEN r.INC END)
                         , sum(CASE WHEN r.PER = 1 THEN cast(r.SB0 AS float) END)
                         , sum(CASE WHEN r.PER = 1 THEN cast(r.N0 AS float) END)
                         , sum(CASE WHEN r.PER = 2 THEN r.ADD_A END)
                         , sum(CASE WHEN r.PER = 2 THEN r.SE2 END)
                         , sum(CASE WHEN r.PER = 2 THEN r.INC END)
                         , sum(CASE WHEN r.PER = 2 THEN cast(r.SB0 AS float) END)
                         , sum(CASE WHEN r.PER = 2 THEN cast(r.N0 AS float) END)
                         , sum(CASE WHEN r.PER = 3 THEN r.ADD_A END)
                         , sum(CASE WHEN r.PER = 3 THEN r.SE2 END)
                         , CASE WHEN sum(CASE WHEN r.PER = 3 AND r.ADD_REG IS NULL THEN 1 ELSE 0 END) = 0
                                THEN sum(CASE WHEN r.PER = 3 THEN r.ADD_REG END) END
                         -- шесть показателей: слои с ожиданием (как Доп ТО)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN cast(r.N0 AS float) END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.SY0 END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.EXPV END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.SC0 END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.EXPC END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.SK0 END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.EXPK END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN cast(r.N0 AS float) END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.SY0 END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.EXPV END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.SC0 END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.EXPC END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.SK0 END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.EXPK END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN cast(r.SB0 AS float) END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN r.EXPB END)
                         , sum(CASE WHEN r.PER = 1 AND r.EXPV IS NOT NULL THEN cast(r.SB1 AS float) END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN cast(r.SB0 AS float) END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN r.EXPB END)
                         , sum(CASE WHEN r.PER = 2 AND r.EXPV IS NOT NULL THEN cast(r.SB1 AS float) END)
                         , max(a1.ADDALL), max(a2.ADDALL), max(a3.ADDALL)
                    FROM #ce_res AS r
                         LEFT JOIN #ce_all AS a1 ON a1.ID_PROMO = r.ID_PROMO AND a1.PER = 1
                         LEFT JOIN #ce_all AS a2 ON a2.ID_PROMO = r.ID_PROMO AND a2.PER = 2
                         LEFT JOIN #ce_all AS a3 ON a3.ID_PROMO = r.ID_PROMO AND a3.PER = 3
                    WHERE r.CH = 1 AND r.IN_TOTAL = 1                -- у акции на базе канала ТО периода — в колонке канала 1
                    GROUP BY r.ID_PROMO;

                    -- «Все каналы» = офлайн + онлайн (оценки и квадраты ошибок складываются); доли купивших — по базе канала акции
                    INSERT INTO #ce_tot (ID_PROMO, CH, LAYERS, ADD1, SE21, REG1, INC1, SB01, N01, ADD2, SE22, INC2, SB02, N02, ADD3, SE23, REG3
                                       , NE1, SY01, EY1, SC01, EC1, SK01, EK1, NE2, SY02, EY2, SC02, EC2, SK02, EK2
                                       , NB1, EB1, KB1, NB2, EB2, KB2, ALL1, ALL2, ALL3)
                    SELECT t.ID_PROMO, 0, sum(t.LAYERS)
                         , sum(t.ADD1), sum(t.SE21), CASE WHEN count(t.REG1) = count(*) THEN sum(t.REG1) END
                         , max(CASE WHEN t.CH = p.ORG THEN t.INC1 END), max(CASE WHEN t.CH = p.ORG THEN t.SB01 END)
                         , max(CASE WHEN t.CH = p.ORG THEN t.N01 END)
                         , sum(t.ADD2), sum(t.SE22)
                         , max(CASE WHEN t.CH = p.ORG THEN t.INC2 END), max(CASE WHEN t.CH = p.ORG THEN t.SB02 END)
                         , max(CASE WHEN t.CH = p.ORG THEN t.N02 END)
                         , sum(t.ADD3), sum(t.SE23), CASE WHEN count(t.REG3) = count(*) THEN sum(t.REG3) END
                         -- ТО, чеки, SKU — офлайн + онлайн; клиентов ЦГ — база канала акции (как численность строки)
                         , max(CASE WHEN t.CH = p.ORG THEN t.NE1 END)
                         , sum(t.SY01)
                         , sum(t.EY1)
                         , sum(t.SC01)
                         , sum(t.EC1)
                         , sum(t.SK01)
                         , sum(t.EK1)
                         , max(CASE WHEN t.CH = p.ORG THEN t.NE2 END)
                         , sum(t.SY02)
                         , sum(t.EY2)
                         , sum(t.SC02)
                         , sum(t.EC2)
                         , sum(t.SK02)
                         , sum(t.EK2)
                         -- купившие: офлайн + онлайн без купивших в обоих каналах; у КГ — по её доле купивших в обоих
                         , sum(t.NB1) - isnull(max(b0.D1), 0)
                         , sum(t.EB1) * (1e0 - isnull(max(b1.D1), 0) / nullif(sum(t.KB1), 0))
                         , sum(t.KB1) - isnull(max(b1.D1), 0)
                         , sum(t.NB2) - isnull(max(b0.D2), 0)
                         , sum(t.EB2) * (1e0 - isnull(max(b1.D2), 0) / nullif(sum(t.KB2), 0))
                         , sum(t.KB2) - isnull(max(b1.D2), 0)
                         , sum(t.ALL1), sum(t.ALL2), sum(t.ALL3)
                    FROM #ce_tot AS t
                         JOIN #ce_pr AS p ON p.ID_PROMO = t.ID_PROMO
                         LEFT JOIN #ce_bb AS b0 ON b0.ID_PROMO = t.ID_PROMO AND b0.CG = 0
                         LEFT JOIN #ce_bb AS b1 ON b1.ID_PROMO = t.ID_PROMO AND b1.CG = 1
                    WHERE t.CH IN (1, 3)
                    GROUP BY t.ID_PROMO;

                    -- численность: весь список = фрод + без «до» + выбросы + не с кем сравнить + убрано при выравнивании (ЦГ и КГ)
                    -- + сегменты без оценки + в оценке
                    INSERT INTO #ce_cnt (ID_PROMO, CH, L0, L1, F0, F1, D0, D1, O0, O1, U0, U1, I0, I1, U_SEGS, P0, P1, R0, R1)
                    SELECT v.REAL_ID, v.BASIS
                         , isnull(l.L0, 0), isnull(l.L1, 0), isnull(l.F0, 0), isnull(l.F1, 0)
                         , isnull(c.D0, 0), isnull(c.D1, 0), isnull(c.O0, 0), isnull(c.O1, 0)
                         , isnull(g.U0, 0), isnull(g.U1, 0), isnull(g.I0, 0), isnull(g.I1, 0), g.U_SEGS
                         , isnull(np.P0, 0), isnull(np.P1, 0), isnull(np.R0, 0), isnull(np.R1, 0)
                    FROM #ce_vp AS v
                         JOIN #ce_pr AS p ON p.ID_PROMO = v.REAL_ID
                         LEFT JOIN (SELECT ID_PROMO
                                         , L0 = sum(CASE WHEN CG = 0 THEN 1 ELSE 0 END)
                                         , L1 = sum(CASE WHEN CG = 1 THEN 1 ELSE 0 END)
                                         , F0 = sum(CASE WHEN CG = 0 AND IS_FRAUD = 1 THEN 1 ELSE 0 END)
                                         , F1 = sum(CASE WHEN CG = 1 AND IS_FRAUD = 1 THEN 1 ELSE 0 END)
                                    FROM #ce_list
                                    GROUP BY ID_PROMO) AS l ON l.ID_PROMO = v.REAL_ID
                         LEFT JOIN (SELECT x.ID_PROMO
                                         , D0 = sum(CASE WHEN x.CG = 0 AND (x.SEG = 0 OR x.X <= 0) THEN 1 ELSE 0 END)
                                         , D1 = sum(CASE WHEN x.CG = 1 AND (x.SEG = 0 OR x.X <= 0) THEN 1 ELSE 0 END)
                                         , O0 = sum(CASE WHEN x.CG = 0 AND x.SEG > 0 AND x.X > 0
                                                          AND (xp.ID_CONTACT IS NOT NULL OR xd.ID_CONTACT IS NOT NULL) THEN 1 ELSE 0 END)
                                         , O1 = sum(CASE WHEN x.CG = 1 AND x.SEG > 0 AND x.X > 0
                                                          AND (xp.ID_CONTACT IS NOT NULL OR xd.ID_CONTACT IS NOT NULL) THEN 1 ELSE 0 END)
                                    FROM #ce_pc AS x
                                         LEFT JOIN #ce_xpre  AS xp ON xp.ID_PROMO = x.ID_PROMO AND xp.ID_CONTACT = x.ID_CONTACT
                                         LEFT JOIN #ce_xdrop AS xd ON xd.ID_PROMO = x.ID_PROMO AND xd.ID_CONTACT = x.ID_CONTACT
                                    GROUP BY x.ID_PROMO) AS c ON c.ID_PROMO = v.ID_PROMO
                         LEFT JOIN (SELECT ID_PROMO
                                         , U0 = sum(CASE WHEN OK = 0 THEN N0 ELSE 0 END)
                                         , U1 = sum(CASE WHEN OK = 0 THEN N1 ELSE 0 END)
                                         , I0 = sum(CASE WHEN OK = 1 THEN N0 ELSE 0 END)
                                         , I1 = sum(CASE WHEN OK = 1 THEN N1 ELSE 0 END)
                                         , U_SEGS = left(STRING_AGG(CASE WHEN OK = 0
                                                                         THEN CAST(concat(CASE SEG WHEN 2 THEN N'Активные' WHEN 1 THEN N'Новые'
                                                                                                   WHEN 5 THEN N'Отток' ELSE N'Спящие' END
                                                                                        , N' — ЦГ ', N0, N', КГ ', N1) AS nvarchar(max)) END, N'; '), 400)
                                    FROM #ce_segok
                                    GROUP BY ID_PROMO) AS g ON g.ID_PROMO = v.ID_PROMO
                         LEFT JOIN (SELECT e.ID_PROMO
                                         , P0 = sum(CASE WHEN e.CG = 0 AND i.OK = 0 THEN 1 ELSE 0 END)
                                         , P1 = sum(CASE WHEN e.CG = 1 AND i.OK = 0 THEN 1 ELSE 0 END)
                                         , R0 = sum(CASE WHEN e.CG = 0 AND i.OK = 1 AND k.ID_CONTACT IS NULL THEN 1 ELSE 0 END)
                                         , R1 = sum(CASE WHEN e.CG = 1 AND i.OK = 1 AND k.ID_CONTACT IS NULL THEN 1 ELSE 0 END)
                                    FROM #ce_ev AS e
                                         JOIN #ce_ivm AS m ON m.ID_PROMO = e.ID_PROMO AND m.SEG = e.SEG AND m.RL = e.RL AND m.IV = e.IV
                                         JOIN #ce_is AS i ON i.ID_PROMO = e.ID_PROMO AND i.SEG = e.SEG AND i.RL = e.RL AND i.IV = m.G
                                         LEFT JOIN #ce_kp AS k ON k.ID_PROMO = e.ID_PROMO AND k.ID_CONTACT = e.ID_CONTACT
                                    GROUP BY e.ID_PROMO) AS np ON np.ID_PROMO = v.ID_PROMO;

                    -- строка «Все каналы» — численность по базе канала акции
                    INSERT INTO #ce_cnt (ID_PROMO, CH, L0, L1, F0, F1, D0, D1, O0, O1, U0, U1, I0, I1, U_SEGS, P0, P1, R0, R1)
                    SELECT n.ID_PROMO, 0, n.L0, n.L1, n.F0, n.F1, n.D0, n.D1, n.O0, n.O1, n.U0, n.U1, n.I0, n.I1, n.U_SEGS, n.P0, n.P1, n.R0, n.R1
                    FROM #ce_cnt AS n
                         JOIN #ce_pr AS p ON p.ID_PROMO = n.ID_PROMO AND n.CH = p.ORG;

                    -- ---------- 10. Скидка: витрина скидок, кампании акции, все клиенты списка (ЦГ и КГ, с фродом) ----------
                    INSERT INTO #ce_link (ID_PROMO, ID_CAMPAIGN)
                    SELECT DISTINCT l.ID_PROMO, l.ID_CAMPAIGN
                    FROM dbo.I_PROMO_CAMPAIGN_LINK AS l (nolock)
                    WHERE l.ID_COMPANY = @idc
                      AND l.IS_USED = 1
                      AND EXISTS (SELECT 1 FROM #ce_pr AS p WHERE p.ID_PROMO = l.ID_PROMO);

                    SELECT @d_from = min(START_DATE), @d_to = max(AFT_TO) FROM #ce_pr;
                    IF @d_to > @yesterday SET @d_to = @yesterday;

                    IF EXISTS (SELECT 1 FROM #ce_link)
                        INSERT INTO #ce_dsc WITH (TABLOCK) (ID_PROMO, ID_CONTACT, DATA, ORG, VALUE_TYPE, VAL)
                        SELECT k.ID_PROMO, d.ID_CONTACT, d.DATA, d.ID_ORGANIZATION, d.VALUE_TYPE, d.[VALUE]
                        FROM dbo.I_DISCOUNT_CHECK AS d (nolock)
                             JOIN #ce_link AS k ON k.ID_CAMPAIGN = d.ID_CAMPAIGN
                             JOIN #ce_pr   AS p ON p.ID_PROMO = k.ID_PROMO AND d.DATA >= p.START_DATE AND d.DATA <= p.AFT_TO
                        WHERE d.ID_COMPANY = @idc
                          AND d.DATA >= @d_from AND d.DATA <= @d_to
                        OPTION (RECOMPILE);

                    -- по каналу продаж чека: 0 все каналы, 1 офлайн, 3 онлайн; пометка о типах значения — по всей акции
                    INSERT INTO #ce_dagg (ID_PROMO, CH, D0, D1, DT, DIN, MIXED, NO_CAMP)
                    SELECT p.ID_PROMO, c.CH
                         , isnull(x.D0, 0), isnull(x.D1, 0), isnull(x.D0, 0) + isnull(x.D1, 0), isnull(x.DIN, 0)
                         , isnull(m.MIXED, 0)
                         , CASE WHEN EXISTS (SELECT 1 FROM #ce_link AS k WHERE k.ID_PROMO = p.ID_PROMO) THEN 0 ELSE 1 END
                    FROM #ce_pr AS p
                         CROSS JOIN (VALUES (cast(0 AS tinyint)), (cast(1 AS tinyint)), (cast(3 AS tinyint))) AS c (CH)
                         LEFT JOIN (SELECT s.ID_PROMO, CH = cast(isnull(s.ORG, 0) AS tinyint)
                                         , D0  = sum(CASE WHEN l.CG = 0 THEN abs(s.VAL) ELSE 0 END)
                                         , D1  = sum(CASE WHEN l.CG = 1 THEN abs(s.VAL) ELSE 0 END)
                                         , DIN = sum(CASE WHEN s.DATA <= pp.FINISH_DATE THEN abs(s.VAL) ELSE 0 END)
                                    FROM #ce_dsc AS s
                                         JOIN #ce_list AS l  ON l.ID_PROMO = s.ID_PROMO AND l.ID_CONTACT = s.ID_CONTACT
                                         JOIN #ce_pr   AS pp ON pp.ID_PROMO = s.ID_PROMO
                                    GROUP BY GROUPING SETS ((s.ID_PROMO), (s.ID_PROMO, s.ORG))) AS x
                                ON x.ID_PROMO = p.ID_PROMO AND x.CH = c.CH
                         LEFT JOIN (SELECT s.ID_PROMO
                                         , MIXED = CASE WHEN count(DISTINCT isnull(s.VALUE_TYPE, N'')) > 1
                                                          OR (min(s.VAL) < 0 AND max(s.VAL) > 0) THEN 1 ELSE 0 END
                                    FROM #ce_dsc AS s
                                         JOIN #ce_list AS l ON l.ID_PROMO = s.ID_PROMO AND l.ID_CONTACT = s.ID_CONTACT
                                    GROUP BY s.ID_PROMO) AS m ON m.ID_PROMO = p.ID_PROMO;

                    -- ---------- 11. Доставка: отчёт по доставкам, Push, группа CVM (формулы — как в CVM_DELIVERY_REPORT) ----------
                    IF @dlv_ok = 1
                        INSERT INTO #ce_dlv (ID_PROMO, MSGS, SENT, DELIV, CLICKED, ERRORS)
                        SELECT a.ID_PROMO
                             , count(DISTINCT a.ID_ACTION)
                             , sum(IIF(s.STATUS_CLASS IN ('SENT', 'DELIV', 'OPEN', 'CLICK', 'UNSUB', 'SPAM'), cast(v.CLIENTS AS bigint), 0))
                             , sum(IIF(s.STATUS_CLASS IN ('DELIV', 'OPEN', 'CLICK', 'UNSUB', 'SPAM'),         cast(v.CLIENTS AS bigint), 0))
                             , sum(IIF(s.STATUS_CLASS = 'CLICK',                                              cast(v.CLIENTS AS bigint), 0))
                             , sum(IIF(s.STATUS_CLASS = 'ERROR',                                              cast(v.CLIENTS AS bigint), 0))
                        FROM dbo.I_DELIVERY_ACTION AS a (nolock)
                             JOIN dbo.I_DELIVERY_DAY AS v (nolock) ON v.ID_ACTION = a.ID_ACTION
                             LEFT JOIN dbo.I_DELIVERY_STATUS AS s (nolock) ON s.ID_CHANNEL = v.ID_CHANNEL AND s.STATUS = v.STATUS
                        WHERE a.ACTION_TYPE = 'CVM'
                          AND EXISTS (SELECT 1 FROM #ce_pr AS p WHERE p.ID_PROMO = a.ID_PROMO)
                          AND v.ID_COMPANY = @idc
                          AND v.ID_CHANNEL = 1
                        GROUP BY a.ID_PROMO;

                    SET @msg = concat(N'CVM_EFFECT_TODAY: порция ', @part, N' — сегменты, выбросы, слои, оценки, скидка: '
                                    , datediff(second, @t, sysdatetime()), N' с.');
                    RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

                    -- ---------- 12. Запись: по акции — DELETE + INSERT в обе витрины одной транзакцией ----------
                    SET @ord = 0;
                    WHILE 1 = 1
                    BEGIN
                        SELECT TOP (1) @pid = ID_PROMO, @ord = ORD FROM #ce_pr WHERE ORD > @ord ORDER BY ORD;
                        IF @@ROWCOUNT = 0 BREAK;
                        SET @now = sysdatetime();

                        BEGIN TRANSACTION;

                        DELETE FROM dbo.I_CVM_EFFECT    WHERE ID_COMPANY = @idc AND [ID] = @pid;
                        DELETE FROM dbo.I_CVM_EFFECT_CG WHERE ID_COMPANY = @idc AND ID_PROMO = @pid;

                        INSERT INTO dbo.I_CVM_EFFECT (
                              [Дата расчета], ID_COMPANY, [ID], [Название], [Год], [Месяц], [Дата старта], [Дата окончания]
                            , [После с], [После по], [Сегмент], [Механика], ID_MECHANICS, [Канал], ID_ORGANIZATION, [Канал акции]
                            , CHANNEL_ID, [Канал продаж], [Источник КГ], [Доля КГ в списке, %]
                            , [Клиентов ЦГ], [Клиентов КГ], [Фрод ЦГ], [Фрод КГ], [Без покупок до ЦГ], [Без покупок до КГ]
                            , [Исключено выбросами ЦГ], [Исключено выбросами КГ], [Не с кем сравнить ЦГ], [Не с кем сравнить КГ]
                            , [Убрано при выравнивании ЦГ], [Убрано при выравнивании КГ], [Сегменты без оценки ЦГ], [Сегменты без оценки КГ]
                            , [В оценке ЦГ], [В оценке КГ], [Слоёв]
                            , [Доп ТО в акцию, ₽], [Доп ТО в акцию ДИ от, ₽], [Доп ТО в акцию ДИ до, ₽], [Доп ТО в акцию мин. различимый эффект, ₽], [Значимость в акцию]
                            , [Доп ТО после, ₽], [Доп ТО после ДИ от, ₽], [Доп ТО после ДИ до, ₽], [Доп ТО после мин. различимый эффект, ₽], [Значимость после]
                            , [Доп ТО итого, ₽], [Доп ТО итого ДИ от, ₽], [Доп ТО итого ДИ до, ₽], [Доп ТО итого мин. различимый эффект, ₽], [Значимость итого]
                            , [Доп ТО в акцию регрессия, ₽], [Доп ТО итого регрессия, ₽]
                            , [Псевдоэффект до старта, ₽], [Псевдоэффект ДИ от, ₽], [Псевдоэффект ДИ до, ₽], [КГ несопоставима]
                            , [Доп ТО в акцию на клиента, ₽], [Доп ТО после на клиента, ₽], [Доп ТО итого на клиента, ₽]
                            , [Доп ТО в акцию на всю ЦГ, ₽], [Доп ТО после на всю ЦГ, ₽], [Доп ТО итого на всю ЦГ, ₽]
                            , [Купили в акцию ЦГ %], [Купили в акцию КГ при составе ЦГ %], [Отклик в акцию п.п.], [Инкр. покупателей в акцию]
                            , [Купили после ЦГ %], [Купили после КГ при составе ЦГ %], [Отклик после п.п.], [Инкр. покупателей после]
                            , [Клиентов ЦГ в акцию], [ARPU в акцию ЦГ, ₽], [ARPU в акцию КГ, ₽], [ARPU в акцию инкр., ₽]
                            , [ARPU в акцию инкр., %], [Ср. чек в акцию ЦГ, ₽], [Ср. чек в акцию КГ, ₽], [Ср. чек в акцию инкр., ₽]
                            , [Ср. чек в акцию инкр., %], [Чеков на клиента в акцию ЦГ], [Чеков на клиента в акцию КГ], [Чеков на клиента в акцию инкр.]
                            , [Чеков на клиента в акцию инкр., %], [SKU в чеке в акцию ЦГ], [SKU в чеке в акцию КГ], [SKU в чеке в акцию инкр.]
                            , [SKU в чеке в акцию инкр., %], [Цена SKU в акцию ЦГ, ₽], [Цена SKU в акцию КГ, ₽], [Цена SKU в акцию инкр., ₽]
                            , [Цена SKU в акцию инкр., %], [Клиентов ЦГ после], [ARPU после ЦГ, ₽], [ARPU после КГ, ₽]
                            , [ARPU после инкр., ₽], [ARPU после инкр., %], [Ср. чек после ЦГ, ₽], [Ср. чек после КГ, ₽]
                            , [Ср. чек после инкр., ₽], [Ср. чек после инкр., %], [Чеков на клиента после ЦГ], [Чеков на клиента после КГ]
                            , [Чеков на клиента после инкр.], [Чеков на клиента после инкр., %], [SKU в чеке после ЦГ], [SKU в чеке после КГ]
                            , [SKU в чеке после инкр.], [SKU в чеке после инкр., %], [Цена SKU после ЦГ, ₽], [Цена SKU после КГ, ₽]
                            , [Цена SKU после инкр., ₽], [Цена SKU после инкр., %]
                            , [Скидка ЦГ, ₽], [Скидка КГ, ₽], [Скидка всего, ₽], [Скидка в дни акции, ₽], [Скидка в PL в акцию]
                            , [PL в акцию, ₽], [PL в акцию ДИ от, ₽], [PL в акцию ДИ до, ₽], [PL итого, ₽], [PL итого ДИ от, ₽], [PL итого ДИ до, ₽]
                            , [PL итого на всю ЦГ, ₽]
                            , [Рассылок], [Отправлено сообщений], [Отправлено на 100 клиентов ЦГ], [Доставлено %], [Переходы %], [Ошибки %]
                            , [Каппа выбросов], [Предварительная], [После не считается], [Нет КГ], [Кампании не найдены], [Пометки])
                        SELECT @now, @idc, p.ID_PROMO, pm.PROMO_NAME, year(p.START_DATE), month(p.START_DATE), p.START_DATE, p.FINISH_DATE
                             , CASE WHEN p.AFTER_ON = 1 THEN p.AFT_FROM END, CASE WHEN p.AFTER_ON = 1 THEN p.AFT_TO END
                             , pm.SEGMENT, pm.MECHANICS, pm.ID_MECHANICS, pm.CHANNEL, p.ORG, CASE p.ORG WHEN 3 THEN N'онлайн' ELSE N'офлайн' END
                             , c.CH, c.CH_NAME
                             , CASE WHEN p.CTRL = 1 THEN concat(N'ГКГ: ', p.CTRL_SRC) ELSE N'список' END
                             , cast(round(100e0 * p.KG_SHARE, 3) AS decimal(9, 3))
                             , n.L0, n.L1, n.F0, n.F1, n.D0, n.D1, n.O0, n.O1, n.P0, n.P1, n.R0, n.R1, n.U0, n.U1, n.I0, n.I1, t.LAYERS
                             -- в акцию
                             , cast(round(t.ADD1, 2) AS decimal(19, 2))
                             , cast(round(t.ADD1 - 1.96 * e.SE1, 2) AS decimal(19, 2))
                             , cast(round(t.ADD1 + 1.96 * e.SE1, 2) AS decimal(19, 2))
                             , cast(round(2.8 * e.SE1, 2) AS decimal(19, 2))
                             , sg.S1
                             -- после
                             , cast(round(t.ADD2, 2) AS decimal(19, 2))
                             , cast(round(t.ADD2 - 1.96 * e.SE2, 2) AS decimal(19, 2))
                             , cast(round(t.ADD2 + 1.96 * e.SE2, 2) AS decimal(19, 2))
                             , cast(round(2.8 * e.SE2, 2) AS decimal(19, 2))
                             , sg.S2
                             -- итого
                             , cast(round(t.ADD3, 2) AS decimal(19, 2))
                             , cast(round(t.ADD3 - 1.96 * e.SE3, 2) AS decimal(19, 2))
                             , cast(round(t.ADD3 + 1.96 * e.SE3, 2) AS decimal(19, 2))
                             , cast(round(2.8 * e.SE3, 2) AS decimal(19, 2))
                             , sg.S3
                             , cast(round(t.REG1, 2) AS decimal(19, 2)), cast(round(t.REG3, 2) AS decimal(19, 2))
                             -- проверка «до старта»; признак — по строке «Все каналы», одинаковый у всех строк акции
                             , cast(round(q.PS, 2) AS decimal(19, 2))
                             , cast(round(q.PS - 1.96 * sqrt(q.PSE2), 2) AS decimal(19, 2))
                             , cast(round(q.PS + 1.96 * sqrt(q.PSE2), 2) AS decimal(19, 2))
                             , CASE WHEN q0.PSE2 > 0 AND abs(q0.PS) / sqrt(q0.PSE2) > @z_pseudo THEN 1
                                    WHEN q0.PSE2 > 0 THEN 0 END
                             -- на клиента ЦГ выровненного списка; на всю ЦГ в оценке
                             , cast(round(t.ADD1 / nullif(t.NE1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.ADD2 / nullif(t.NE2, 0), 2) AS decimal(19, 2))
                             , cast(round(t.ADD3 / nullif(t.NE1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.ALL1, 2) AS decimal(19, 2)), cast(round(t.ALL2, 2) AS decimal(19, 2)), cast(round(t.ALL3, 2) AS decimal(19, 2))
                             -- отклик: доли купивших по сумме слоёв; КГ — при составе ЦГ (ЦГ − инкрементальные)
                             , cast(round(100.0 * t.SB01 / nullif(t.N01, 0), 2) AS decimal(9, 2))
                             , cast(round(100.0 * (t.SB01 - t.INC1) / nullif(t.N01, 0), 2) AS decimal(9, 2))
                             , cast(round(100.0 * t.INC1 / nullif(t.N01, 0), 2) AS decimal(9, 2))
                             , cast(round(t.INC1, 1) AS decimal(19, 1))
                             , cast(round(100.0 * t.SB02 / nullif(t.N02, 0), 2) AS decimal(9, 2))
                             , cast(round(100.0 * (t.SB02 - t.INC2) / nullif(t.N02, 0), 2) AS decimal(9, 2))
                             , cast(round(100.0 * t.INC2 / nullif(t.N02, 0), 2) AS decimal(9, 2))
                             , cast(round(t.INC2, 1) AS decimal(19, 1))
                             -- шесть показателей в акцию: ЦГ, КГ (ожидание ЦГ без акции), инкремент
                             , cast(round(t.NE1, 0) AS int)
                             , cast(round(t.SY01 / nullif(t.NB1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY1 / nullif(t.EB1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY01 / nullif(t.NB1, 0) - t.EY1 / nullif(t.EB1, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY01 / nullif(t.NB1, 0) / nullif(t.EY1 / nullif(t.EB1, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY01 / nullif(t.NB1, 0) / nullif(t.EY1 / nullif(t.EB1, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SY01 / nullif(t.SC01, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY1 / nullif(t.EC1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY01 / nullif(t.SC01, 0) - t.EY1 / nullif(t.EC1, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY01 / nullif(t.SC01, 0) / nullif(t.EY1 / nullif(t.EC1, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY01 / nullif(t.SC01, 0) / nullif(t.EY1 / nullif(t.EC1, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SC01 / nullif(t.NB1, 0), 4) AS decimal(19, 4))
                             , cast(round(t.EC1 / nullif(t.EB1, 0), 4) AS decimal(19, 4))
                             , cast(round(t.SC01 / nullif(t.NB1, 0) - t.EC1 / nullif(t.EB1, 0), 4) AS decimal(19, 4))
                             , cast(round(CASE WHEN abs(100.0 * (t.SC01 / nullif(t.NB1, 0) / nullif(t.EC1 / nullif(t.EB1, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SC01 / nullif(t.NB1, 0) / nullif(t.EC1 / nullif(t.EB1, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SK01 / nullif(t.SC01, 0), 4) AS decimal(19, 4))
                             , cast(round(t.EK1 / nullif(t.EC1, 0), 4) AS decimal(19, 4))
                             , cast(round(t.SK01 / nullif(t.SC01, 0) - t.EK1 / nullif(t.EC1, 0), 4) AS decimal(19, 4))
                             , cast(round(CASE WHEN abs(100.0 * (t.SK01 / nullif(t.SC01, 0) / nullif(t.EK1 / nullif(t.EC1, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SK01 / nullif(t.SC01, 0) / nullif(t.EK1 / nullif(t.EC1, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SY01 / nullif(t.SK01, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY1 / nullif(t.EK1, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY01 / nullif(t.SK01, 0) - t.EY1 / nullif(t.EK1, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY01 / nullif(t.SK01, 0) / nullif(t.EY1 / nullif(t.EK1, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY01 / nullif(t.SK01, 0) / nullif(t.EY1 / nullif(t.EK1, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             -- шесть показателей после: ЦГ, КГ (ожидание ЦГ без акции), инкремент
                             , cast(round(t.NE2, 0) AS int)
                             , cast(round(t.SY02 / nullif(t.NB2, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY2 / nullif(t.EB2, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY02 / nullif(t.NB2, 0) - t.EY2 / nullif(t.EB2, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY02 / nullif(t.NB2, 0) / nullif(t.EY2 / nullif(t.EB2, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY02 / nullif(t.NB2, 0) / nullif(t.EY2 / nullif(t.EB2, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SY02 / nullif(t.SC02, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY2 / nullif(t.EC2, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY02 / nullif(t.SC02, 0) - t.EY2 / nullif(t.EC2, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY02 / nullif(t.SC02, 0) / nullif(t.EY2 / nullif(t.EC2, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY02 / nullif(t.SC02, 0) / nullif(t.EY2 / nullif(t.EC2, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SC02 / nullif(t.NB2, 0), 4) AS decimal(19, 4))
                             , cast(round(t.EC2 / nullif(t.EB2, 0), 4) AS decimal(19, 4))
                             , cast(round(t.SC02 / nullif(t.NB2, 0) - t.EC2 / nullif(t.EB2, 0), 4) AS decimal(19, 4))
                             , cast(round(CASE WHEN abs(100.0 * (t.SC02 / nullif(t.NB2, 0) / nullif(t.EC2 / nullif(t.EB2, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SC02 / nullif(t.NB2, 0) / nullif(t.EC2 / nullif(t.EB2, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SK02 / nullif(t.SC02, 0), 4) AS decimal(19, 4))
                             , cast(round(t.EK2 / nullif(t.EC2, 0), 4) AS decimal(19, 4))
                             , cast(round(t.SK02 / nullif(t.SC02, 0) - t.EK2 / nullif(t.EC2, 0), 4) AS decimal(19, 4))
                             , cast(round(CASE WHEN abs(100.0 * (t.SK02 / nullif(t.SC02, 0) / nullif(t.EK2 / nullif(t.EC2, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SK02 / nullif(t.SC02, 0) / nullif(t.EK2 / nullif(t.EC2, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             , cast(round(t.SY02 / nullif(t.SK02, 0), 2) AS decimal(19, 2))
                             , cast(round(t.EY2 / nullif(t.EK2, 0), 2) AS decimal(19, 2))
                             , cast(round(t.SY02 / nullif(t.SK02, 0) - t.EY2 / nullif(t.EK2, 0), 2) AS decimal(19, 2))
                             , cast(round(CASE WHEN abs(100.0 * (t.SY02 / nullif(t.SK02, 0) / nullif(t.EY2 / nullif(t.EK2, 0), 0) - 1)) < 1e6 THEN 100.0 * (t.SY02 / nullif(t.SK02, 0) / nullif(t.EY2 / nullif(t.EK2, 0), 0) - 1) END, 2) AS decimal(9, 2))
                             -- скидка и PL
                             , dg.D0, dg.D1, dg.DT, dg.DIN
                             , CASE WHEN @pl_in_discount = 'ALL' THEN N'старт … финиш + 4' ELSE N'дни акции' END
                             , cast(round((t.ADD1 * @margin - e.DPL1) / @vat, 2) AS decimal(19, 2))
                             , cast(round(((t.ADD1 - 1.96 * e.SE1) * @margin - e.DPL1) / @vat, 2) AS decimal(19, 2))
                             , cast(round(((t.ADD1 + 1.96 * e.SE1) * @margin - e.DPL1) / @vat, 2) AS decimal(19, 2))
                             , cast(round((t.ADD3 * @margin - dg.DT) / @vat, 2) AS decimal(19, 2))
                             , cast(round(((t.ADD3 - 1.96 * e.SE3) * @margin - dg.DT) / @vat, 2) AS decimal(19, 2))
                             , cast(round(((t.ADD3 + 1.96 * e.SE3) * @margin - dg.DT) / @vat, 2) AS decimal(19, 2))
                             , cast(round((t.ALL3 * @margin - dg.DT) / @vat, 2) AS decimal(19, 2))
                             -- доставка: отправлено = все статусы, кроме ошибок; доставлено = доставлено + переходы
                             , dl.MSGS, dl.SENT
                             , cast(round(100.0 * dl.SENT    / nullif(n.L0, 0), 2)                AS decimal(9, 2))
                             , cast(round(100.0 * dl.DELIV   / nullif(dl.SENT, 0), 2)             AS decimal(9, 2))
                             , cast(round(100.0 * dl.CLICKED / nullif(dl.DELIV, 0), 2)            AS decimal(9, 2))
                             , cast(round(100.0 * dl.ERRORS  / nullif(dl.SENT + dl.ERRORS, 0), 2) AS decimal(9, 2))
                             , @kappa, p.PRELIM, 1 - p.AFTER_ON
                             , CASE WHEN n.I1 + n.U1 = 0 THEN 1 ELSE 0 END
                             , dg.NO_CAMP
                             , left(nullif(stuff(concat(
                                     IIF(p.PRELIM = 1, N'; предварительная: финиш + 4 в окне перезаливки чеков (последние 10 дней) — пересчитается', N'')
                                   , CASE WHEN p.CTRL = 1 AND isnull(p.KG_SHARE, 0) = 0
                                          THEN concat(N'; КГ в списке нет — контроль из ГКГ (', p.CTRL_SRC, N')')
                                          WHEN p.CTRL = 1
                                          THEN concat(N'; КГ списка некорректна (доля ', cast(round(100e0 * p.KG_SHARE, 2) AS decimal(9, 2))
                                                    , N' % при отборе 5–10 %) — контроль из ГКГ (', p.CTRL_SRC, N'); «Клиентов КГ» — клиенты ГКГ')
                                          WHEN p.KG_SHARE < 0.05e0 - @z_kg * sqrt(0.05e0 * 0.95e0 / nullif(n.L0 + n.L1, 0))
                                          THEN concat(N'; доля КГ в списке ', cast(round(100e0 * p.KG_SHARE, 2) AS decimal(9, 2))
                                                    , N' % — ниже 5 %, контроль — КГ списка по слоям')
                                          ELSE N'' END
                                   , CASE WHEN p.AFTER_ON = 1 THEN N''
                                          WHEN pm.THEM = 1 THEN N'; «после» не считается: тематическая рассылка — Доп ТО итого = в акцию'
                                          ELSE N'; «после» не считается: однодневная акция без кешбэка — Доп ТО итого = в акцию' END
                                   , CASE WHEN p.FINISH_DATE > @yesterday
                                          THEN concat(N'; акция не закончилась: данные по ', convert(nchar(10), @yesterday, 104)
                                                    , N', «в акцию» неполное, «после» ещё не началось — пересчитается')
                                          WHEN p.AFTER_ON = 1 AND p.AFT_TO > @yesterday
                                          THEN concat(N'; «после» не закончилось: данные по ', convert(nchar(10), @yesterday, 104)
                                                    , N', «после» и «итого» неполные — пересчитается')
                                          ELSE N'' END
                                   , CASE WHEN n.L0 + n.L1 = 0 THEN N'; список акции в I_PROMO_OFFER пуст'
                                          WHEN n.L1 = 0 THEN N'; нет КГ в списке — Доп ТО и PL не посчитаны'
                                          WHEN n.I1 + n.U1 = 0 THEN N'; нет КГ в оценке — Доп ТО и PL не посчитаны'
                                          WHEN t.ID_PROMO IS NULL THEN N'; ни в одном сегменте нет 2 клиентов КГ в оценке — Доп ТО и PL не посчитаны'
                                          ELSE N'' END
                                   , IIF(n.U0 + n.U1 > 0, concat(N'; не оценены сегменты (меньше 2 клиентов КГ в оценке): ', n.U_SEGS), N'')
                                   , CASE WHEN q0.PSE2 > 0 AND abs(q0.PS) / sqrt(q0.PSE2) > @z_pseudo
                                          THEN N'; КГ несопоставима: псевдоэффект до старта дальше 3,29 ошибки — акция не входит в общий эффект месяца'
                                          WHEN q0.PSE2 IS NULL OR q0.PSE2 = 0 THEN N'; проверка «до старта» не проведена (нет сравнимых недель)'
                                          ELSE N'' END
                                   , IIF(dg.NO_CAMP = 1, N'; кампании Manzana не найдены — скидка 0', N'')
                                   , IIF(dg.MIXED = 1, N'; в начислениях скидки несколько типов значения или плюсы и минусы — скидка как сумма модулей', N'')
                                   , IIF(pm.DATE_ROWS > 1, N'; в I_PROMO несколько строк акции с разными датами — взяты самый ранний старт и самый поздний финиш', N'')
                                   , IIF(p.REF_DATE IS NULL OR p.REF_DATE < dateadd(day, -1, p.HIST_FROM)
                                       , concat(N'; снимок клиентов ', isnull(convert(nchar(10), p.REF_DATE, 104), N'не найден')
                                              , N' — старше окна чеков (', @hist_days, N' дней до старта): у Оттока и Спящих «до» и давность не учитывают покупки между снимком и окном')
                                       , N'')), 1, 2, N''), N''), 2000)
                        FROM #ce_pr AS p
                             JOIN #ce_promo AS pm ON pm.ID_PROMO = p.ID_PROMO
                             CROSS JOIN (VALUES (cast(0 AS tinyint), N'Все каналы'), (cast(1 AS tinyint), N'Офлайн')
                                              , (cast(3 AS tinyint), N'Онлайн')) AS c (CH, CH_NAME)
                             JOIN #ce_cnt   AS n  ON n.ID_PROMO  = p.ID_PROMO AND n.CH = c.CH
                             JOIN #ce_dagg  AS dg ON dg.ID_PROMO = p.ID_PROMO AND dg.CH = c.CH
                             LEFT JOIN #ce_tot AS t  ON t.ID_PROMO  = p.ID_PROMO AND t.CH = c.CH
                             LEFT JOIN #ce_qt  AS q  ON q.ID_PROMO  = p.ID_PROMO AND q.CH = c.CH
                             LEFT JOIN #ce_qt  AS q0 ON q0.ID_PROMO = p.ID_PROMO AND q0.CH = 0
                             -- доставка — рассылка акции, без деления по каналам продаж: только в строке «Все каналы»
                             LEFT JOIN #ce_dlv AS dl ON dl.ID_PROMO = p.ID_PROMO AND c.CH = 0
                             CROSS APPLY (SELECT SE1  = sqrt(t.SE21), SE2 = sqrt(t.SE22), SE3 = sqrt(t.SE23)
                                               , DPL1 = CASE WHEN @pl_in_discount = 'ALL' THEN dg.DT ELSE dg.DIN END) AS e
                             CROSS APPLY (SELECT T1 = t.ADD1 / nullif(e.SE1, 0), T2 = t.ADD2 / nullif(e.SE2, 0)
                                               , T3 = t.ADD3 / nullif(e.SE3, 0)) AS z
                             CROSS APPLY (SELECT S1 = CASE WHEN t.ADD1 IS NULL       THEN NULL
                                                           WHEN z.T1 IS NULL         THEN N'Проверьте данные'
                                                           WHEN abs(z.T1) < 1.65     THEN N'Незначимо (p > 0.10)'
                                                           WHEN abs(z.T1) <= 1.96    THEN N'Слабо значимо (p ≈ 0.05-0.10)'
                                                           WHEN abs(z.T1) <= 2.58    THEN N'Значимо (p < 0.05)'
                                                           WHEN abs(z.T1) <= 3.29    THEN N'Сильно значимо (p < 0.01)'
                                                           ELSE N'Очень сильно значимо (p < 0.001)' END
                                               , S2 = CASE WHEN t.ADD2 IS NULL       THEN NULL
                                                           WHEN z.T2 IS NULL         THEN N'Проверьте данные'
                                                           WHEN abs(z.T2) < 1.65     THEN N'Незначимо (p > 0.10)'
                                                           WHEN abs(z.T2) <= 1.96    THEN N'Слабо значимо (p ≈ 0.05-0.10)'
                                                           WHEN abs(z.T2) <= 2.58    THEN N'Значимо (p < 0.05)'
                                                           WHEN abs(z.T2) <= 3.29    THEN N'Сильно значимо (p < 0.01)'
                                                           ELSE N'Очень сильно значимо (p < 0.001)' END
                                               , S3 = CASE WHEN t.ADD3 IS NULL       THEN NULL
                                                           WHEN z.T3 IS NULL         THEN N'Проверьте данные'
                                                           WHEN abs(z.T3) < 1.65     THEN N'Незначимо (p > 0.10)'
                                                           WHEN abs(z.T3) <= 1.96    THEN N'Слабо значимо (p ≈ 0.05-0.10)'
                                                           WHEN abs(z.T3) <= 2.58    THEN N'Значимо (p < 0.05)'
                                                           WHEN abs(z.T3) <= 3.29    THEN N'Сильно значимо (p < 0.01)'
                                                           ELSE N'Очень сильно значимо (p < 0.001)' END) AS sg
                        WHERE p.ID_PROMO = @pid;

                        INSERT INTO dbo.I_CVM_EFFECT_CG (
                              ID_COMPANY, ID_PROMO, START_DATE, FINISH_DATE, ID_ORGANIZATION, SEGMENT_ID, SEGMENT_NAME, LAYER_NO, LAYER_BASIS
                            , WEEK_FROM, WEEK_TO, X_BASIS, PERIOD_ID, PERIOD_NAME, DATE_FROM, DATE_TO, CHANNEL_ID, CHANNEL_NAME
                            , CLIENTS_TG, CLIENTS_CG, TO_BEFORE_TG, TO_BEFORE_CG, TO_PERIOD_TG, TO_PERIOD_CG, EXPECTED_TG
                            , ADD_TO, ADD_TO_SE, ADD_TO_REG, BUYERS_TG, BUYERS_CG, CHECKS_TG, CHECKS_CG, SKU_TG, SKU_CG
                            , EXP_CHECKS_TG, EXP_SKU_TG, CLIENTS_TG_ALL, CLIENTS_CG_ALL, KG_SHARE, INTERVALS, IN_TOTAL, PRELIM, CALC_DT)
                        SELECT @idc, @pid, p.START_DATE, p.FINISH_DATE, p.ORG, r.SEG
                             , CASE r.SEG WHEN 2 THEN N'Активные' WHEN 1 THEN N'Новые' WHEN 5 THEN N'Отток' ELSE N'Спящие' END
                             , cast(r.LAYER AS smallint)                                                               -- неделя
                             , CASE r.SEG WHEN 1 THEN N'недели стажа' WHEN 6 THEN N'4-недельные блоки давности' ELSE N'недели давности' END
                             , w.WEEK_FROM, w.WEEK_TO
                             , CASE WHEN r.SEG IN (1, 2) THEN N'ТО за 42 дня до старта' ELSE N'ТО до скидки за всю историю до старта' END
                             , r.PER, CASE r.PER WHEN 1 THEN N'в акцию' WHEN 2 THEN N'после' ELSE N'итого' END
                             , CASE r.PER WHEN 2 THEN p.AFT_FROM ELSE p.START_DATE END
                             , CASE r.PER WHEN 1 THEN p.FINISH_DATE WHEN 2 THEN p.AFT_TO
                                          ELSE CASE WHEN p.AFTER_ON = 1 THEN p.AFT_TO ELSE p.FINISH_DATE END END
                             , cast(r.ID_PROMO % 10 AS tinyint), CASE r.ID_PROMO % 10 WHEN 1 THEN N'офлайн' ELSE N'онлайн' END
                             , r.N0, r.N1
                             , cast(round(r.SX0, 2) AS decimal(19, 2)), cast(round(r.SX1, 2) AS decimal(19, 2))
                             , cast(round(r.SY0, 2) AS decimal(19, 2)), cast(round(r.SY1, 2) AS decimal(19, 2))
                             , cast(round(r.EXPV, 2) AS decimal(19, 2)), cast(round(r.ADD_A, 2) AS decimal(19, 2))
                             , cast(round(sqrt(r.SE2), 2) AS decimal(19, 2)), cast(round(r.ADD_REG, 2) AS decimal(19, 2))
                             , r.SB0, r.SB1
                             , cast(round(r.SC0, 0) AS int), cast(round(r.SC1, 0) AS int)
                             , cast(round(r.SK0, 3) AS decimal(19, 3)), cast(round(r.SK1, 3) AS decimal(19, 3))
                             , cast(round(r.EXPC, 3) AS decimal(19, 3)), cast(round(r.EXPK, 3) AS decimal(19, 3))
                             , wn.N0ALL, wn.N1ALL, cast(round(100e0 * ws.SHARE, 3) AS decimal(9, 3)), wn.IVS
                             , r.IN_TOTAL, p.PRELIM, @now
                        FROM #ce_res AS r
                             JOIN #ce_pr   AS p ON p.ID_PROMO = @pid
                             JOIN #ce_lrng AS w ON w.ID_PROMO = r.ID_PROMO AND w.SEG = r.SEG AND w.LAYER = r.LAYER
                             LEFT JOIN #ce_wn AS wn ON wn.ID_PROMO = r.ID_PROMO AND wn.SEG = r.SEG AND wn.RL = r.LAYER
                             LEFT JOIN #ce_ws AS ws ON ws.ID_PROMO = r.ID_PROMO AND ws.SEG = r.SEG AND ws.RL = r.LAYER
                        WHERE r.ID_PROMO IN (@pid * 10 + 1, @pid * 10 + 3)   -- акция на базе офлайна и онлайна
                          AND r.CH = 1;                                      -- ТО периода канала базы

                        COMMIT TRANSACTION;
                        SELECT @done += 1, @msg = concat(N'CVM_EFFECT_TODAY: акция ', @pid, N' записана.');

                        SELECT @msg = concat(N'CVM_EFFECT_TODAY: акция ', e.[ID], N' (', convert(nchar(10), e.[Дата старта], 104), N') записана: в оценке ЦГ '
                                           , e.[В оценке ЦГ], N', КГ ', e.[В оценке КГ]
                                           , N'; Доп ТО итого ', isnull(convert(nvarchar(30), cast(round(e.[Доп ТО итого, ₽], 0) AS decimal(19, 0))), N'—')
                                           , N' р (', isnull(e.[Значимость итого], N'—'), N'); скидка '
                                           , convert(nvarchar(30), cast(round(e.[Скидка всего, ₽], 0) AS decimal(19, 0)))
                                           , N' р; PL итого ', isnull(convert(nvarchar(30), cast(round(e.[PL итого, ₽], 0) AS decimal(19, 0))), N'—'), N' р'
                                           , IIF(e.[Предварительная] = 1, N'; предварительная', N''), N'.')
                        FROM dbo.I_CVM_EFFECT AS e (nolock)
                        WHERE e.ID_COMPANY = @idc AND e.[ID] = @pid AND e.CHANNEL_ID = 0;
                        RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
                    END

                    SET @msg = concat(N'CVM_EFFECT_TODAY: порция ', @part, N' из ', @parts, N' — ', datediff(second, @t_part, sysdatetime()), N' с.');
                    RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
                END TRY
                BEGIN CATCH
                    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
                    SET @errors += 1;
                    SET @msg = concat(N'CVM_EFFECT_TODAY: порция ', @part, N' не досчитана (ошибка ', ERROR_NUMBER(), N', строка ', ERROR_LINE(), N'): '
                                    , left(ERROR_MESSAGE(), 1200), N' — записанные акции порции остаются, остальные посчитает следующий запуск.');
                    RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
                END CATCH

                IF @stopped = 1 BREAK;
            END   -- порции

            IF @stopped = 1
                RAISERROR(N'CVM_EFFECT_TODAY: время остановки — новые акции не начинались.', 0, 1) WITH NOWAIT;
            SET @msg = concat(N'CVM_EFFECT_TODAY: готово — записано акций ', @done, N' из ', (SELECT count(*) FROM #ce_sel WHERE READY = 1)
                            , IIF(@errors > 0, concat(N'; порций с ошибкой ', @errors), N''), N'; '
                            , datediff(second, @t_run, sysdatetime()) / 60, N' мин ', datediff(second, @t_run, sysdatetime()) % 60, N' с.');
            RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
        END   -- витрина скидок есть

        -- сколько акций выборки (ночь — горизонт @night_days, история — период, акция — она) нет в витрине
        SELECT @left = count(*)
        FROM #ce_promo AS p
        WHERE p.BAD = 0
          AND p.START_DATE <= @yesterday
          AND (   (@mode = 'NIGHT' AND p.START_DATE >= dateadd(day, -@night_days, @today))
               OR (@mode = 'RANGE' AND p.START_DATE >= @date_from AND p.START_DATE <= @date_to)
               OR (@mode = 'PROMO' AND p.ID_PROMO = @id_promo))
          AND NOT EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT AS e (nolock)
                          WHERE e.ID_COMPANY = @idc AND e.[ID] = p.ID_PROMO
                            AND (@calc_before IS NULL OR @mode <> 'RANGE' OR e.[Дата расчета] >= @calc_before));
        IF @left > 0
            RAISERROR(N'CVM_EFFECT_TODAY: акций выборки, которых нет в витрине (или не пересчитаны после @calc_before): %d.', 0, 1, @left) WITH NOWAIT;

        EXEC sp_releaseapplock @Resource = N'dbo.CVM_EFFECT_TODAY', @LockOwner = 'Session';
        SET @locked = 0;

        IF @errors > 0
        BEGIN
            SET @msg = concat(N'CVM_EFFECT_TODAY: порций с ошибкой — ', @errors, N' (подробности — в сообщениях выше); остальные акции записаны.');
            THROW 50004, @msg, 1;
        END
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF @locked = 1
            EXEC sp_releaseapplock @Resource = N'dbo.CVM_EFFECT_TODAY', @LockOwner = 'Session';
        THROW;
    END CATCH
END
GO

IF isnull(OBJECT_DEFINITION(OBJECT_ID('dbo.CVM_EFFECT_TODAY')), N'') NOT LIKE N'%CVM[_]EFFECT[_]TODAY: методика 2026-10%'
BEGIN
    RAISERROR(N'СТОП: процедура dbo.CVM_EFFECT_TODAY не создалась (нет метки новой версии) — чистку шага 1 не меняю; напиши мне.', 16, 1);
    SET NOEXEC ON;
END
ELSE
    RAISERROR(N'2. Процедура dbo.CVM_EFFECT_TODAY — новая версия (методика 2026-10) поставлена.', 0, 1) WITH NOWAIT;
GO

-- ============================== 3. ЧИСТКА ШАГА 1: clear_data_update и reload_clear_data ==============================
-- Исключение Елены 13.10.2026 (в остальном интеграционные процедуры не меняются): в ЖИВОМ тексте каждой процедуры
--   убираются строки удаления из I_CVM_EFFECT_MONTH, I_CVM_EFFECT_MONTH_CG, I_PROMO_EEFECT, I_PROMO_EEFECT_CG (в том
--   числе закомментированные), а перед строкой чистки I_PP_EFFECT_MONTH встают комментарий и две строки: удаление из
--   I_CVM_EFFECT ([Дата старта] >= @d_from) и I_CVM_EFFECT_CG (START_DATE >= @d_from) с переменной компании процедуры.
--   Остальной текст не меняется, в том числе строки витрины скидок (CVM_DISCOUNT_MART.sql) — есть они уже или появятся
--   позже. Убираемая строка должна быть одной командой delete с @d_from — иначе СТОП. Исходный текст сохраняется в
--   dbo.I_DISCOUNT_PROC_BACKUP (таблица файла витрины скидок; если её ещё нет — создаётся такая же); обе процедуры —
--   одной транзакцией. Метка CVM_EFFECT_PROC уже в тексте — процедура не трогается. Нет строки I_PP_EFFECT_MONTH,
--   @d_from или переменной компании, текст начинается не с CREATE PROCEDURE, процедура создана не с ANSI_NULLS и
--   QUOTED_IDENTIFIER ON — СТОП, ничего не меняется.
USE mci_model
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('dbo.I_DISCOUNT_PROC_BACKUP') IS NULL
BEGIN
    -- та же таблица, что ставит CVM_DISCOUNT_MART.sql (раздел 4): исходные тексты процедур MCI до правки
    CREATE TABLE dbo.I_DISCOUNT_PROC_BACKUP (
          PROC_NAME  sysname        NOT NULL
        , SAVED_AT   datetime2(0)   NOT NULL
        , DEFINITION nvarchar(max)  NOT NULL
        , CONSTRAINT PK_I_DISCOUNT_PROC_BACKUP PRIMARY KEY CLUSTERED (PROC_NAME, SAVED_AT)
    );
END

DROP TABLE IF EXISTS #ce_patch;
CREATE TABLE #ce_patch (RN int NOT NULL, PROC_NAME sysname NOT NULL, IDC_VAR nvarchar(30) NOT NULL
                      , DEF nvarchar(max) NULL, NEW_DEF nvarchar(max) NULL, REMOVED int NULL
                      , ACT nvarchar(20) COLLATE DATABASE_DEFAULT NULL, WHY nvarchar(1000) COLLATE DATABASE_DEFAULT NULL);
INSERT INTO #ce_patch (RN, PROC_NAME, IDC_VAR) VALUES (1, N'dbo.clear_data_update', N'@idc'), (2, N'dbo.reload_clear_data', N'@ID_COMPANY');

DECLARE @i int = 1, @name sysname, @var nvarchar(30), @def nvarchar(max), @work nvarchar(max), @ansi bit, @qi bit
      , @crlf nchar(2) = NCHAR(13) + NCHAR(10), @tab nchar(1) = NCHAR(9);
DECLARE @pos_create int, @p int, @p1 int, @p2 int, @e int, @len int, @back int, @ls int, @le int
      , @line nvarchar(max), @core nvarchar(max), @ident nvarchar(200), @bad nvarchar(1000), @removed int, @guard int
      , @block nvarchar(max), @new nvarchar(max), @msg nvarchar(2000);

-- 1. Проверки и новый текст обеих процедур — до любых изменений
WHILE @i <= 2
BEGIN
    SELECT @name = PROC_NAME, @var = IDC_VAR FROM #ce_patch WHERE RN = @i;
    SELECT @def = NULL, @ansi = NULL, @qi = NULL, @bad = NULL, @removed = 0, @guard = 0;
    SELECT @def = m.definition, @ansi = m.uses_ansi_nulls, @qi = m.uses_quoted_identifier
    FROM sys.sql_modules AS m (nolock)
    WHERE m.object_id = OBJECT_ID(@name);

    SET @pos_create = PATINDEX(N'%CREATE%', isnull(@def, N''));

    IF @def IS NULL
        UPDATE #ce_patch SET ACT = N'СТОП', WHY = N'процедуры нет или нет прав читать её текст' WHERE RN = @i;
    ELSE IF @def LIKE N'%CVM[_]EFFECT[_]PROC%'
    BEGIN
        IF @def LIKE N'%I[_]CVM[_]EFFECT[_]MONTH%' OR @def LIKE N'%I[_]PROMO[_]EEFECT%'
            UPDATE #ce_patch SET ACT = N'СТОП', WHY = N'строки новой витрины эффекта есть, а строки старых таблиц эффекта остались — текст правили вручную' WHERE RN = @i;
        ELSE
            UPDATE #ce_patch SET ACT = N'уже стоит', WHY = N'строки новой витрины эффекта уже есть' WHERE RN = @i;
    END
    ELSE IF @ansi <> 1 OR @qi <> 1
        UPDATE #ce_patch SET ACT = N'СТОП', WHY = N'процедура создана не с ANSI_NULLS / QUOTED_IDENTIFIER ON' WHERE RN = @i;
    ELSE IF @pos_create = 0
         OR ltrim(rtrim(replace(replace(replace(left(@def, @pos_create - 1), NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N''))) <> N''
         OR substring(@def, @pos_create, 40) NOT LIKE N'CREATE%PROC%'
        UPDATE #ce_patch SET ACT = N'СТОП', WHY = N'текст начинается не с CREATE PROCEDURE' WHERE RN = @i;
    ELSE IF @def NOT LIKE N'%@d[_]from%' OR CHARINDEX(@var, @def) = 0 OR CHARINDEX(N'I_PP_EFFECT_MONTH', @def) = 0
        UPDATE #ce_patch SET ACT = N'СТОП', WHY = concat(N'в тексте нет @d_from, ', @var, N' или строки чистки I_PP_EFFECT_MONTH — текст изменился') WHERE RN = @i;
    ELSE
    BEGIN
        SET @work = @def;

        -- строки старых таблиц эффекта убираются по одной (поиск каждый раз заново)
        WHILE @bad IS NULL AND @guard < 50
        BEGIN
            SET @guard += 1;
            SET @p1 = CHARINDEX(N'I_CVM_EFFECT_MONTH', @work);
            SET @p2 = CHARINDEX(N'I_PROMO_EEFECT', @work);
            SET @p  = CASE WHEN @p1 = 0 THEN @p2 WHEN @p2 = 0 THEN @p1 WHEN @p1 < @p2 THEN @p1 ELSE @p2 END;
            IF @p = 0 BREAK;

            -- имя таблицы целиком (буквы, цифры, «_»)
            SET @len = datalength(@work) / 2;
            SET @e = @p;
            WHILE @e <= @len AND substring(@work, @e, 1) LIKE N'[A-Za-z0-9_]'
                SET @e += 1;
            SET @ident = substring(@work, @p, @e - @p);

            -- границы строки: от символа после предыдущего перевода строки до своего перевода строки включительно
            SET @back = CHARINDEX(NCHAR(10), REVERSE(left(@work, @p - 1)));
            SET @ls = CASE WHEN @back = 0 THEN 1 ELSE @p - @back + 1 END;
            SET @le = CHARINDEX(NCHAR(10), @work, @p);
            IF @le = 0 SET @le = @len;
            SET @line = substring(@work, @ls, @le - @ls + 1);

            -- строка должна быть одной командой: [пробелы][--]delete … <таблица> … @d_from
            SET @core = ltrim(replace(replace(replace(@line, NCHAR(9), N' '), NCHAR(13), N' '), NCHAR(10), N' '));
            IF left(@core, 2) = N'--' SET @core = ltrim(substring(@core, 3, 4000));
            IF @ident NOT IN (N'I_CVM_EFFECT_MONTH', N'I_CVM_EFFECT_MONTH_CG', N'I_PROMO_EEFECT', N'I_PROMO_EEFECT_CG')
               OR @core NOT LIKE N'delete %'
               OR @line NOT LIKE N'%@d[_]from%'
               OR (datalength(@line) - datalength(replace(@line, N'delete', N''))) / 12 <> 1
                SET @bad = left(replace(replace(@line, NCHAR(13), N''), NCHAR(10), N''), 300);
            ELSE
            BEGIN
                SET @work = STUFF(@work, @ls, @le - @ls + 1, N'');
                SET @removed += 1;
            END
        END

        IF @bad IS NOT NULL
            UPDATE #ce_patch SET ACT = N'СТОП', WHY = concat(N'строка со старой таблицей эффекта — не одна команда delete с @d_from: «', ltrim(@bad), N'»') WHERE RN = @i;
        ELSE IF @guard >= 50
            UPDATE #ce_patch SET ACT = N'СТОП', WHY = N'слишком много строк со старыми таблицами эффекта — текст изменился' WHERE RN = @i;
        ELSE
        BEGIN
            -- новые строки — перед строкой чистки I_PP_EFFECT_MONTH (она в обеих процедурах до правок других файлов)
            SET @p = CHARINDEX(N'I_PP_EFFECT_MONTH', @work);
            SET @back = CHARINDEX(NCHAR(10), REVERSE(left(@work, @p - 1)));
            SET @ls = CASE WHEN @back = 0 THEN 1 ELSE @p - @back + 1 END;
            SET @block = concat(
                  @tab, N'-- отчёт эффекта CVM (CVM_EFFECT_PROC, решение 13.10.2026): акции со стартом с первого перезалитого дня пересчитывает ночной шаг CVM_EFFECT_TODAY', @crlf
                , @tab, N'delete from [dbo].I_CVM_EFFECT where [Дата старта]>= @d_from and ID_COMPANY=', @var, @crlf
                , @tab, N'delete from [dbo].I_CVM_EFFECT_CG where  START_DATE>= @d_from and ID_COMPANY=', @var, @crlf);
            SET @new = STUFF(@work, @ls, 0, @block);
            -- CREATE → ALTER (вставки — дальше по тексту, позиция CREATE не сдвинулась, но ищется заново)
            SET @pos_create = PATINDEX(N'%CREATE%', @new);
            SET @new = STUFF(@new, @pos_create, 6, N'ALTER');
            UPDATE #ce_patch SET DEF = @def, NEW_DEF = @new, REMOVED = @removed, ACT = N'правка' WHERE RN = @i;
        END
    END

    SET @i += 1;
END

-- 2. Запись: обе процедуры одной транзакцией, исходный текст — в dbo.I_DISCOUNT_PROC_BACKUP
IF EXISTS (SELECT 1 FROM #ce_patch WHERE ACT = N'СТОП')
BEGIN
    SELECT @msg = left(STRING_AGG(CAST(concat(PROC_NAME, N': ', WHY) AS nvarchar(max)), N'; '), 1500) FROM #ce_patch WHERE ACT = N'СТОП';
    RAISERROR(N'СТОП по чистке шага 1: %s. Процедуры MCI не изменены — новая витрина эффекта не встала в чистку шага 1, старые таблицы эффекта удалять нельзя; напиши мне.', 16, 1, @msg);
END
ELSE IF NOT EXISTS (SELECT 1 FROM #ce_patch WHERE ACT = N'правка')
    RAISERROR(N'3. Чистка шага 1: строки новой витрины эффекта уже есть в обеих процедурах — не меняю.', 10, 1) WITH NOWAIT;
ELSE
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
        SELECT concat(PROC_NAME, N' (до CVM_EFFECT_PROC)'), sysdatetime(), DEF
        FROM #ce_patch
        WHERE ACT = N'правка';

        SET @i = 1;
        WHILE @i <= 2
        BEGIN
            SET @new = NULL;
            SELECT @new = NEW_DEF FROM #ce_patch WHERE RN = @i AND ACT = N'правка';
            IF @new IS NOT NULL
                EXEC sp_executesql @new;
            SET @i += 1;
        END

        -- проверка: новые строки на месте, старых таблиц эффекта в тексте нет, настройки те же
        IF EXISTS (SELECT 1
                   FROM #ce_patch AS p
                        LEFT JOIN sys.sql_modules AS m (nolock) ON m.object_id = OBJECT_ID(p.PROC_NAME)
                   WHERE p.ACT = N'правка'
                     AND (   m.definition IS NULL
                          OR m.definition NOT LIKE N'%CVM[_]EFFECT[_]PROC%'
                          OR m.definition NOT LIKE N'%I[_]CVM[_]EFFECT where%'
                          OR m.definition NOT LIKE N'%I[_]CVM[_]EFFECT[_]CG where%'
                          OR m.definition LIKE N'%I[_]CVM[_]EFFECT[_]MONTH%'
                          OR m.definition LIKE N'%I[_]PROMO[_]EEFECT%'
                          OR m.uses_ansi_nulls <> 1 OR m.uses_quoted_identifier <> 1))
            THROW 50001, N'после правки в тексте процедуры нет новых строк, остались старые или изменились настройки — откат.', 1;

        COMMIT TRANSACTION;
        SELECT @msg = STRING_AGG(CAST(concat(PROC_NAME, N' (убрано строк ', REMOVED, N')') AS nvarchar(max)), N', ') FROM #ce_patch WHERE ACT = N'правка';
        RAISERROR(N'3. Чистка шага 1: в %s строки старых таблиц эффекта заменены чисткой I_CVM_EFFECT / I_CVM_EFFECT_CG (исходный текст — в dbo.I_DISCOUNT_PROC_BACKUP).', 10, 1, @msg) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = concat(N'СТОП по чистке шага 1: ошибка ', ERROR_NUMBER(), N' — ', ERROR_MESSAGE(), N'. Всё откачено, процедуры MCI не изменены.');
        RAISERROR(N'%s', 16, 1, @msg);
    END CATCH
END

-- состояние текстов процедур шага 1 (для глаз)
SELECT [Процедура]                       = p.PROC_NAME
     , [Что сделано]                     = p.ACT
     , [Убрано строк]                    = p.REMOVED
     , [Чистка новой витрины эффекта]    = CASE WHEN m.definition LIKE N'%CVM[_]EFFECT[_]PROC%' THEN N'есть' ELSE N'НЕТ' END
     , [Строки старых таблиц эффекта]    = CASE WHEN m.definition LIKE N'%I[_]CVM[_]EFFECT[_]MONTH%'
                                                  OR m.definition LIKE N'%I[_]PROMO[_]EEFECT%' THEN N'ЕСТЬ' ELSE N'нет' END
     , [Строки витрины скидок]           = CASE WHEN m.definition LIKE N'%CVM[_]DISCOUNT[_]MART%' THEN N'есть' ELSE N'нет' END
FROM #ce_patch AS p
     LEFT JOIN sys.sql_modules AS m (nolock) ON m.object_id = OBJECT_ID(p.PROC_NAME)
ORDER BY p.RN;
GO

-- ОТКАТ ЧИСТКИ (не выполняется; выделить и выполнить вручную): вернуть текст процедур шага 1, сохранённый этим файлом.
--   Откатятся и правки, сделанные после него (например, строки витрины скидок, если CVM_DISCOUNT_MART.sql запускался
--   позже). Только пока старые таблицы эффекта не удалены (CVM_EFFECT_JOB.sql), иначе шаг 1 упадёт на их DELETE.
-- DECLARE @n sysname, @d nvarchar(max), @k int = 1;
-- WHILE @k <= 2
-- BEGIN
--     SET @n = CASE @k WHEN 1 THEN N'dbo.clear_data_update' ELSE N'dbo.reload_clear_data' END;
--     SET @d = NULL;
--     SELECT TOP (1) @d = DEFINITION FROM dbo.I_DISCOUNT_PROC_BACKUP (nolock)
--     WHERE PROC_NAME = concat(@n, N' (до CVM_EFFECT_PROC)') ORDER BY SAVED_AT DESC;
--     IF @d IS NOT NULL
--     BEGIN
--         SET @d = STUFF(@d, PATINDEX(N'%CREATE%', @d), 6, N'ALTER');
--         EXEC sp_executesql @d;
--     END
--     SET @k += 1;
-- END

-- ============================== 4. ИТОГ ==============================
USE mci_model
GO

SELECT [Что]       = N'Процедура dbo.CVM_EFFECT_TODAY'
     , [Состояние] = CASE WHEN isnull(OBJECT_DEFINITION(OBJECT_ID('dbo.CVM_EFFECT_TODAY')), N'') LIKE N'%CVM[_]EFFECT[_]TODAY: методика 2026-10%'
                          THEN N'новая версия (методика 2026-10)' ELSE N'НЕТ новой версии' END
UNION ALL
SELECT N'Витрина dbo.I_CVM_EFFECT (три строки на акцию: все каналы, офлайн, онлайн)'
     , concat(N'акций ', (SELECT count_big(*) FROM dbo.I_CVM_EFFECT (nolock) WHERE CHANNEL_ID = 0))
UNION ALL
SELECT N'Витрина dbo.I_CVM_EFFECT_CG (слои)', concat(N'строк ', (SELECT count_big(*) FROM dbo.I_CVM_EFFECT_CG (nolock)))
UNION ALL
SELECT N'Витрина скидок (I_DISCOUNT_CHECK, I_PROMO_CAMPAIGN_LINK, I_DISCOUNT_STATE, I_DISCOUNT_DAY)'
     , CASE WHEN OBJECT_ID('dbo.I_DISCOUNT_CHECK', 'U') IS NOT NULL AND OBJECT_ID('dbo.I_PROMO_CAMPAIGN_LINK', 'U') IS NOT NULL
             AND OBJECT_ID('dbo.I_DISCOUNT_STATE', 'U') IS NOT NULL AND OBJECT_ID('dbo.I_DISCOUNT_DAY', 'U') IS NOT NULL
            THEN N'есть' ELSE N'НЕТ — акции не считаются, пока её нет' END
UNION ALL
SELECT N'Акций отчёта со стартом с 01.09.2025 по вчера'
     , concat((SELECT count(*)
               FROM (SELECT p.ID_PROMO
                     FROM dbo.I_PROMO AS p (nolock)
                     WHERE p.ID_COMPANY = 1
                       AND p.ID_PROMO <> 1013000
                       AND (   p.ID_PROMO BETWEEN 101000 AND 101999
                            OR p.ID_PROMO BETWEEN 110000 AND 111999
                            OR (p.ID_PROMO BETWEEN 500000 AND 599999 AND p.TYPE_PROMO = N'CVM'))
                     GROUP BY p.ID_PROMO
                     HAVING max(CASE WHEN p.START_DATE >= '20250901' THEN 1 ELSE 0 END) = 1
                        AND min(p.START_DATE) <= dateadd(day, -1, CAST(getdate() AS date))
                        AND max(p.FINISH_DATE) >= min(p.START_DATE)) AS x), N' — посчитает ночь (горизонт 120 дней) и догон истории');
GO

-- РУЧНОЙ ПРОГОН С ЗАМЕРОМ (не выполняется; выделить и выполнить): ход и время — во вкладке «Сообщения»
-- EXEC dbo.CVM_EFFECT_TODAY @id_promo = 101384;                                  -- одна акция (была в тестовом расчёте)
-- EXEC dbo.CVM_EFFECT_TODAY @date_from = '2026-09-01', @date_to = '2026-09-30', @stop_at = '23:30';   -- месяц, от новых к старым
--   историю вручную — только с @stop_at: иначе расчёт уйдёт в ночь и пересечётся с перезаливкой чеков и шагом 25
-- SELECT * FROM dbo.I_CVM_EFFECT (nolock) WHERE [ID] IN (101374, 101384);
-- SELECT * FROM dbo.I_CVM_EFFECT_CG (nolock) WHERE ID_PROMO = 101384 AND CHANNEL_ID = 0 ORDER BY SEGMENT_ID, LAYER_NO, PERIOD_ID;

SET NOEXEC OFF;
GO
