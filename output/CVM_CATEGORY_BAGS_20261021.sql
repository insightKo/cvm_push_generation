-- CVM · справочник категорий: пакеты-майки — в «Служебные ТМЦ и упаковка» (решение Елены 21.10.2026).
-- Код ДИКСИ 64021401 «ПАКЕТЫ ДЛЯ ПОКУПОК» (11 товаров, все — пакеты-майки) целиком: 1602 «Пакеты и сумки для покупок»
-- (группа 18 «Товары для дома») → 1654 «Служебные ТМЦ и упаковка» (группа 20 «Служебное»). Сумки, спанбонд, термопакеты и
-- фирменные пакеты ДИКСИ (коды 64021402, 64021403 и др.) остаются в 1602.
-- Пакеты-майки есть почти в каждом чеке, поэтому выходили первой трафиковой категорией кластеров. Группа 20 в скоры и топы не входит.
-- Товары перепривяжет ночной шаг CVM_CATEGORY (dbo.cvm_category_refresh), скоры кластеров пересчитает шаг CLUSTER_SCORE той же ночи.
-- Повторный прогон ничего не меняет.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
GO

USE mci_model
GO

update dbo.I_CVM_CATEGORY_MAP
set ID_CVM_CATEGORY = 1654
	, DATE_UPDATE = cast(getdate() as date)
	, NOTE = N'21.10.2026 Елена: пакеты-майки — в «Служебные ТМЦ и упаковка» (были в 1602)'
where ID_CATEGORY_5_ext = N'64021401'
	and MAP_METHOD = 1
	and ID_CVM_CATEGORY = 1602

select ID_CATEGORY_5_ext, EXT_NAME, MAP_METHOD, ID_CVM_CATEGORY, DATE_UPDATE, NOTE
from dbo.I_CVM_CATEGORY_MAP (nolock)
where ID_CATEGORY_5_ext = N'64021401'
GO
