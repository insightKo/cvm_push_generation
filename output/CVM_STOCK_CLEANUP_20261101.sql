-- CVM · уборка таблиц «стоков»: I_STOCK и I_STOCK_DATA — очистить, I_PRODUCT_SHOP, stock_delta, product_shop — удалить
-- Елена 31.10.2026 (решение «2б»): «они не используются а место занимают», «выложи скрипт на их удаление и очистку памяти».
-- Разбор — output/Наличие_на_полке_2026-10-31/разбор_I_STOCK_и_предложение_модели.txt.
-- Что удаляем (проверено 31.10 чтением каталога и текстов процедур):
--   I_STOCK — 8 347 411 025 строк, 178 ГБ, columnstore: промежутки между соседними продажами товара в магазине, не остатки;
--     пишет только stock_delta, шага в задании нет (удалён 16.10), последний расчёт — 07.2025.
--   I_STOCK_DATA — 94 554 строки за 09.2020–07.2025, почти пустая из-за ошибки в stock_delta.
--   I_PRODUCT_SHOP — 43 190 277 строк, 2,9 ГБ; пишет product_shop, читает for_model_growth — обе никто не вызывает.
-- I_STOCK и I_STOCK_DATA очищаются (TRUNCATE), а не удаляются: их чистят при перезаливке интеграционные процедуры
--   clear_data_update и reload_clear_data, а интеграционные процедуры не меняем. Пустая таблица места не занимает,
--   перезаливка работает как раньше.
-- Кто ещё ссылается (01.11: тексты всех 120 модулей базы, sp_help_jobstep всех 17 заданий): только stock_delta,
--   product_shop, for_model_growth, for_model_churn, clear_data_update, reload_clear_data; for_model_growth и
--   for_model_churn не вызывает ни шаг, ни процедура. for_model_growth при ручном запуске упадёт на I_PRODUCT_SHOP —
--   старая модель драйверов роста, не используется; for_model_churn читает I_STOCK_DATA — таблица остаётся (пустой).
-- Тексты stock_delta и product_shop перед удалением — в dbo.I_DISCOUNT_PROC_BACKUP. Данные таблиц не сохраняются.
-- Место: TRUNCATE и DROP освобождают его внутри файлов базы; у больших таблиц страницы освобождаются фоном (отложенное
--   освобождение), поэтому скрипт ждёт, пока занятое место перестанет уменьшаться (не дольше 15 минут), и только потом
--   DBCC SHRINKFILE … TRUNCATEONLY отдаёт диску свободный хвост файлов данных — страницы не переносит. Журнал не трогаем.
--
-- Скрипт для задания CVM_API_nightly (обменник; раннер — sqlcmd -b, ошибка уровня 16 = неуспех, повтор через 10 минут),
--   идемпотентный: повтор после успеха ничего не меняет, падение посередине — повтор доделывает.
-- Стоп (ошибка 16, без изменений в базе): шаг любого задания упоминает эти объекты; модуль базы, кроме перечисленных выше,
--   на них ссылается; текст удаляемой процедуры не читается.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
SET XACT_ABORT ON
SET LOCK_TIMEOUT 60000   -- ждать блокировку не больше минуты; не дождались — ошибка, раннер повторит через 10 минут
GO

USE mci_model;
GO

-- ============================== 0. СТОПЫ ==============================
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobsteps WITH (NOLOCK)
           WHERE command LIKE N'%I[_]STOCK%'
              OR command LIKE N'%PRODUCT[_]SHOP%'
              OR command LIKE N'%stock[_]delta%'
              OR command LIKE N'%for[_]model[_]growth%'
              OR command LIKE N'%for[_]model[_]churn%')
BEGIN
    RAISERROR(N'СТОП: шаг задания SQL Agent упоминает I_STOCK / I_PRODUCT_SHOP / stock_delta / product_shop / for_model_* — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

IF EXISTS (SELECT 1
           FROM sys.sql_expression_dependencies AS d
           WHERE d.referenced_entity_name IN (N'I_STOCK', N'I_STOCK_DATA', N'I_PRODUCT_SHOP',
                                              N'stock_delta', N'product_shop', N'for_model_growth', N'for_model_churn')
             AND NOT EXISTS (SELECT 1
                             FROM (VALUES (N'I_STOCK', N'clear_data_update'), (N'I_STOCK', N'reload_clear_data'),
                                          (N'I_STOCK', N'stock_delta'), (N'I_STOCK', N'product_shop'),
                                          (N'I_STOCK', N'for_model_growth'),
                                          (N'I_STOCK_DATA', N'clear_data_update'), (N'I_STOCK_DATA', N'reload_clear_data'),
                                          (N'I_STOCK_DATA', N'stock_delta'), (N'I_STOCK_DATA', N'for_model_churn'),
                                          (N'I_PRODUCT_SHOP', N'product_shop'), (N'I_PRODUCT_SHOP', N'for_model_growth')
                                  ) AS ok (tbl, proc_name)
                             WHERE ok.tbl = d.referenced_entity_name
                               AND ok.proc_name = OBJECT_NAME(d.referencing_id)))
BEGIN
    RAISERROR(N'СТОП: таблицы или процедуры стоков читает или вызывает ещё какой-то модуль базы — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

IF (OBJECT_ID(N'dbo.stock_delta', N'P') IS NOT NULL AND OBJECT_DEFINITION(OBJECT_ID(N'dbo.stock_delta')) IS NULL)
   OR (OBJECT_ID(N'dbo.product_shop', N'P') IS NOT NULL AND OBJECT_DEFINITION(OBJECT_ID(N'dbo.product_shop')) IS NULL)
BEGIN
    RAISERROR(N'СТОП: текст stock_delta или product_shop не читается — без бэкапа не удаляю, ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. ДО ==============================
BEGIN TRY
    DECLARE @rows_stock bigint = ISNULL((SELECT SUM(rows) FROM sys.partitions WITH (NOLOCK)
                                         WHERE object_id = OBJECT_ID(N'dbo.I_STOCK') AND index_id IN (0, 1)), 0);
    RAISERROR(N'До: I_STOCK %I64d строк.', 10, 1, @rows_stock) WITH NOWAIT;
END TRY
BEGIN CATCH
    RAISERROR(N'До: число строк I_STOCK не прочитано (блокировка каталога) — продолжаю.', 10, 1) WITH NOWAIT;
END CATCH
DECLARE @f sysname, @size_mb bigint, @used_mb bigint, @free_mb bigint;
DECLARE f CURSOR LOCAL FAST_FORWARD FOR
    SELECT name, CAST(size AS bigint) / 128, CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) / 128
    FROM sys.database_files WHERE type_desc = N'ROWS';
OPEN f;
FETCH NEXT FROM f INTO @f, @size_mb, @used_mb;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @free_mb = @size_mb - @used_mb;
    RAISERROR(N'До: файл %s — %I64d МБ, занято %I64d МБ, свободно %I64d МБ.', 10, 1, @f, @size_mb, @used_mb, @free_mb) WITH NOWAIT;
    FETCH NEXT FROM f INTO @f, @size_mb, @used_mb;
END
CLOSE f;
DEALLOCATE f;
GO

-- ============================== 2. ПРОЦЕДУРЫ: бэкап текста и удаление ==============================
BEGIN TRANSACTION;
IF OBJECT_ID(N'dbo.stock_delta', N'P') IS NOT NULL
BEGIN
    INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    VALUES (N'dbo.stock_delta', sysdatetime(), OBJECT_DEFINITION(OBJECT_ID(N'dbo.stock_delta')));
    DROP PROCEDURE dbo.stock_delta;
    RAISERROR(N'stock_delta удалена (текст — в dbo.I_DISCOUNT_PROC_BACKUP).', 10, 1) WITH NOWAIT;
END
IF OBJECT_ID(N'dbo.product_shop', N'P') IS NOT NULL
BEGIN
    INSERT INTO dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME, SAVED_AT, DEFINITION)
    VALUES (N'dbo.product_shop', sysdatetime(), OBJECT_DEFINITION(OBJECT_ID(N'dbo.product_shop')));
    DROP PROCEDURE dbo.product_shop;
    RAISERROR(N'product_shop удалена (текст — в dbo.I_DISCOUNT_PROC_BACKUP).', 10, 1) WITH NOWAIT;
END
COMMIT;
GO

-- ============================== 3. ТАБЛИЦЫ ==============================
IF OBJECT_ID(N'dbo.I_STOCK', N'U') IS NOT NULL
    TRUNCATE TABLE dbo.I_STOCK;
IF OBJECT_ID(N'dbo.I_STOCK_DATA', N'U') IS NOT NULL
    TRUNCATE TABLE dbo.I_STOCK_DATA;
DROP TABLE IF EXISTS dbo.I_PRODUCT_SHOP;
RAISERROR(N'I_STOCK и I_STOCK_DATA очищены, I_PRODUCT_SHOP удалена.', 10, 1) WITH NOWAIT;
GO

-- ============================== 4. ОЖИДАНИЕ ФОНОВОГО ОСВОБОЖДЕНИЯ ==============================
-- Раз в минуту, не дольше 15 минут: занятое место в файлах данных; три минуты подряд не уменьшается — дальше.
DECLARE @prev bigint = (SELECT SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint)) / 128
                        FROM sys.database_files WHERE type_desc = N'ROWS');
DECLARE @cur bigint, @i int = 0, @flat int = 0;
WHILE @i < 15 AND @flat < 3
BEGIN
    WAITFOR DELAY '00:01:00';
    SET @i += 1;
    SET @cur = (SELECT SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint)) / 128
                FROM sys.database_files WHERE type_desc = N'ROWS');
    SET @flat = CASE WHEN @cur < @prev THEN 0 ELSE @flat + 1 END;
    RAISERROR(N'Минута %d: занято в файлах данных %I64d МБ.', 10, 1, @i, @cur) WITH NOWAIT;
    SET @prev = @cur;
END
GO

-- ============================== 5. СВОБОДНЫЙ ХВОСТ ФАЙЛОВ ДАННЫХ — ДИСКУ ==============================
-- Только TRUNCATEONLY: без переноса страниц. Не вышло (права, блокировка) — предупреждение, остальное уже сделано.
DECLARE @f sysname, @sql nvarchar(400), @e nvarchar(2048);
DECLARE f CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM sys.database_files WHERE type_desc = N'ROWS';
OPEN f;
FETCH NEXT FROM f INTO @f;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'DBCC SHRINKFILE (' + QUOTENAME(@f) + N', TRUNCATEONLY) WITH NO_INFOMSGS;';
    BEGIN TRY
        EXEC sys.sp_executesql @sql;
    END TRY
    BEGIN CATCH
        SET @e = ERROR_MESSAGE();
        RAISERROR(N'Предупреждение: хвост файла %s не отдан диску — %s', 10, 1, @f, @e) WITH NOWAIT;
    END CATCH
    FETCH NEXT FROM f INTO @f;
END
CLOSE f;
DEALLOCATE f;
GO

-- ============================== 6. ПОСЛЕ ==============================
DECLARE @f sysname, @size_mb bigint, @used_mb bigint, @free_mb bigint;
DECLARE f CURSOR LOCAL FAST_FORWARD FOR
    SELECT name, CAST(size AS bigint) / 128, CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) / 128
    FROM sys.database_files WHERE type_desc = N'ROWS';
OPEN f;
FETCH NEXT FROM f INTO @f, @size_mb, @used_mb;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @free_mb = @size_mb - @used_mb;
    RAISERROR(N'После: файл %s — %I64d МБ, занято %I64d МБ, свободно внутри %I64d МБ.', 10, 1, @f, @size_mb, @used_mb, @free_mb) WITH NOWAIT;
    FETCH NEXT FROM f INTO @f, @size_mb, @used_mb;
END
CLOSE f;
DEALLOCATE f;
RAISERROR(N'Готово.', 10, 1) WITH NOWAIT;
GO

SET NOEXEC OFF;
GO
