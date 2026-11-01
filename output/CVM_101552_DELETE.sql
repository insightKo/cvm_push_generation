-- Удаление списка 101552 из I_PROMO_OFFER по слову Елены 01.11.2026 (сегмент ЦИ неправильный, список пересобирается).
-- Шапка I_PROMO остаётся. Повтор безопасен.
SET NOCOUNT ON
SET XACT_ABORT ON
GO
USE mci_model
GO
DELETE FROM I_PROMO_OFFER WHERE ID_PROMO = 101552
SELECT [Удалено строк 101552] = @@ROWCOUNT
GO
