-- ============================================================================================
-- CVM_EFFECT_110_DEL_OFFLINE — онлайн-акции 110000–119999: убрать эффект по офлайну из витрин эффекта (19.10.2026).
-- Решение Елены: «для акций 110 вообще не нужно считать эффект по офлайну — он заведомо некорректный, выборка делалась по
-- онлайн-покупкам»; «сейчас их нужно удалить из базы». Процедура (CVM_EFFECT_PROC.sql) офлайн-базу у 110xxx больше не строит.
-- Что делает файл по объектам:
--   dbo.I_CVM_EFFECT    — у акций 110xxx: строки CHANNEL_ID = 1 (офлайн) удаляются; строка CHANNEL_ID = 0 («Все каналы» = офлайн +
--                         онлайн) заменяется копией строки CHANNEL_ID = 3 (онлайн) с прежними скидкой, доставкой и пометками строки
--                         «Все каналы»; PL пересчитывается по формуле процедуры ((Доп ТО · 0,3 − скидка) / 1,15; «PL в акцию» —
--                         скидка дней акции).
--   dbo.I_CVM_EFFECT_CG — у акций 110xxx: строки CHANNEL_ID = 1 (слои офлайн-базы) удаляются.
--   Другие акции, другие таблицы — не трогаются. Ничего не удаляется целиком. Акции 110xxx — только онлайн (ID_ORGANIZATION = 3)
--   и только те, где строка «Офлайн» ещё есть (повторный запуск ничего не меняет).
-- Запуск: целиком (F5). Всё в одной транзакции: если числа после правки не сходятся с проверкой — откат и сообщение.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @n_off int, @n_all int, @n_on int, @n_cg int, @n_all_new int, @msg nvarchar(500);

SELECT @n_off = sum(CASE WHEN CHANNEL_ID = 1 THEN 1 ELSE 0 END)
     , @n_all = sum(CASE WHEN CHANNEL_ID = 0 THEN 1 ELSE 0 END)
     , @n_on  = sum(CASE WHEN CHANNEL_ID = 3 THEN 1 ELSE 0 END)
FROM dbo.I_CVM_EFFECT WITH (NOLOCK)
WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3;

SELECT @n_cg = count(*)
FROM dbo.I_CVM_EFFECT_CG WITH (NOLOCK)
WHERE ID_COMPANY = 1 AND ID_PROMO BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 1;

SET @msg = concat(N'До правки, акции 110xxx: строк «Офлайн» ', @n_off, N', «Все каналы» ', @n_all, N', «Онлайн» ', @n_on
                , N'; слоёв офлайн-базы ', @n_cg, N'.');
RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

-- «Все каналы» = копия строки «Онлайн»; скидка, доставка, пометки — из прежней строки «Все каналы»
DROP TABLE IF EXISTS #e110;
SELECT e.*
INTO #e110
FROM dbo.I_CVM_EFFECT AS e WITH (NOLOCK)
WHERE e.ID_COMPANY = 1 AND e.[ID] BETWEEN 110000 AND 119999 AND e.ID_ORGANIZATION = 3 AND e.CHANNEL_ID = 3
  AND EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT AS o WITH (NOLOCK)                 -- только акции, где офлайн ещё есть
              WHERE o.ID_COMPANY = e.ID_COMPANY AND o.[ID] = e.[ID] AND o.CHANNEL_ID = 1);

UPDATE n
SET CHANNEL_ID                     = 0
  , [Канал продаж]                 = a.[Канал продаж]
  , [Скидка ЦГ, ₽]                 = a.[Скидка ЦГ, ₽]
  , [Скидка КГ, ₽]                 = a.[Скидка КГ, ₽]
  , [Скидка всего, ₽]              = a.[Скидка всего, ₽]
  , [Скидка в дни акции, ₽]        = a.[Скидка в дни акции, ₽]
  , [Скидка в PL в акцию]          = a.[Скидка в PL в акцию]
  , [PL в акцию, ₽]                = cast(round((n.[Доп ТО в акцию, ₽] * 0.3 - CASE WHEN a.[Скидка в PL в акцию] = N'старт … финиш + 4' THEN a.[Скидка всего, ₽] ELSE a.[Скидка в дни акции, ₽] END) / 1.15, 2) AS decimal(19, 2))
  , [PL в акцию ДИ от, ₽]          = cast(round((n.[Доп ТО в акцию ДИ от, ₽] * 0.3 - CASE WHEN a.[Скидка в PL в акцию] = N'старт … финиш + 4' THEN a.[Скидка всего, ₽] ELSE a.[Скидка в дни акции, ₽] END) / 1.15, 2) AS decimal(19, 2))
  , [PL в акцию ДИ до, ₽]          = cast(round((n.[Доп ТО в акцию ДИ до, ₽] * 0.3 - CASE WHEN a.[Скидка в PL в акцию] = N'старт … финиш + 4' THEN a.[Скидка всего, ₽] ELSE a.[Скидка в дни акции, ₽] END) / 1.15, 2) AS decimal(19, 2))
  , [PL итого, ₽]                  = cast(round((n.[Доп ТО итого, ₽]         * 0.3 - a.[Скидка всего, ₽]) / 1.15, 2) AS decimal(19, 2))
  , [PL итого ДИ от, ₽]            = cast(round((n.[Доп ТО итого ДИ от, ₽]   * 0.3 - a.[Скидка всего, ₽]) / 1.15, 2) AS decimal(19, 2))
  , [PL итого ДИ до, ₽]            = cast(round((n.[Доп ТО итого ДИ до, ₽]   * 0.3 - a.[Скидка всего, ₽]) / 1.15, 2) AS decimal(19, 2))
  , [Рассылок]                     = a.[Рассылок]
  , [Отправлено сообщений]         = a.[Отправлено сообщений]
  , [Отправлено на 100 клиентов ЦГ] = a.[Отправлено на 100 клиентов ЦГ]
  , [Доставлено %]                 = a.[Доставлено %]
  , [Переходы %]                   = a.[Переходы %]
  , [Ошибки %]                     = a.[Ошибки %]
  , [Кампании не найдены]          = a.[Кампании не найдены]
  , [Пометки]                      = left(concat(a.[Пометки], CASE WHEN a.[Пометки] IS NULL THEN N'' ELSE N'; ' END
                                               , N'онлайн-акция: эффект только по онлайну (офлайн убран 19.10.2026)'), 2000)
FROM #e110 AS n
     JOIN dbo.I_CVM_EFFECT AS a WITH (NOLOCK)
       ON a.ID_COMPANY = n.ID_COMPANY AND a.[ID] = n.[ID] AND a.CHANNEL_ID = 0;

-- у онлайн-строки без пары «Все каналы» (не должно быть) — не вставляем
DELETE FROM #e110 WHERE CHANNEL_ID <> 0;

-- строка «Все каналы» с офлайном, но без онлайн-строки для замены — стоп, ничего не меняется
IF EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT AS a WITH (NOLOCK)
           WHERE a.ID_COMPANY = 1 AND a.[ID] BETWEEN 110000 AND 119999 AND a.ID_ORGANIZATION = 3 AND a.CHANNEL_ID = 0
             AND EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT AS o WITH (NOLOCK)
                         WHERE o.ID_COMPANY = a.ID_COMPANY AND o.[ID] = a.[ID] AND o.CHANNEL_ID = 1)
             AND NOT EXISTS (SELECT 1 FROM #e110 AS n WHERE n.[ID] = a.[ID]))
BEGIN
    RAISERROR(N'СТОП: у части акций 110xxx есть «Все каналы» и «Офлайн», но нет строки «Онлайн» для замены — ничего не меняю; напиши мне.', 16, 1);
    RETURN;
END

BEGIN TRANSACTION;

DELETE FROM dbo.I_CVM_EFFECT
WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 1;

DELETE e
FROM dbo.I_CVM_EFFECT AS e
WHERE e.ID_COMPANY = 1 AND e.[ID] BETWEEN 110000 AND 119999 AND e.ID_ORGANIZATION = 3 AND e.CHANNEL_ID = 0
  AND EXISTS (SELECT 1 FROM #e110 AS n WHERE n.ID_COMPANY = e.ID_COMPANY AND n.[ID] = e.[ID]);

INSERT INTO dbo.I_CVM_EFFECT
SELECT * FROM #e110;

DELETE FROM dbo.I_CVM_EFFECT_CG
WHERE ID_COMPANY = 1 AND ID_PROMO BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 1;

-- проверка: офлайна нет, строк «Все каналы» столько же, сколько было
IF EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT WITH (NOLOCK) WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 1)
   OR EXISTS (SELECT 1 FROM dbo.I_CVM_EFFECT_CG WITH (NOLOCK) WHERE ID_COMPANY = 1 AND ID_PROMO BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 1)
   OR (SELECT count(*) FROM dbo.I_CVM_EFFECT WITH (NOLOCK) WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 0) <> @n_all
BEGIN
    ROLLBACK TRANSACTION;
    RAISERROR(N'СТОП: после правки числа не сходятся — всё откатено, витрины как были; напиши мне.', 16, 1);
    RETURN;
END

COMMIT TRANSACTION;

SELECT @n_all_new = count(*) FROM dbo.I_CVM_EFFECT WITH (NOLOCK)
WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3 AND CHANNEL_ID = 0;
SET @msg = concat(N'Готово: удалено строк «Офлайн» ', @n_off, N', слоёв офлайн-базы ', @n_cg
                , N'; строк «Все каналы» пересобрано из онлайна ', @n_all_new, N'.');
RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;

-- итог: акции 110xxx по каналам
SELECT [Канал продаж], [Акций] = count(*), [Доп ТО итого, ₽] = sum([Доп ТО итого, ₽]), [PL итого, ₽] = sum([PL итого, ₽])
FROM dbo.I_CVM_EFFECT WITH (NOLOCK)
WHERE ID_COMPANY = 1 AND [ID] BETWEEN 110000 AND 119999 AND ID_ORGANIZATION = 3
GROUP BY CHANNEL_ID, [Канал продаж]
ORDER BY CHANNEL_ID;
GO
