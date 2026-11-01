-- Переименование кластера офлайн в mci_model (Елена 01.11.2026): кластер 1 «Продукты по мелочи» → «Перекусы на ходу».
-- ПРИМЕР ДЛЯ СЛЕДУЮЩИХ КЛАСТЕРОВ: поменять только блок параметров (номер кластера уровня 10, старое и новое имя).
--
-- Что меняется (только строки со старым именем, остальное не трогается):
--   dbo.I_CLUSTER_DICT — имя кластера уровня 10 (LEVEL_NO 2, K = 10) модели @ver; и имя его родителя уровня 7 (LEVEL_NO 1),
--     но только если у родителя нет других детей и его имя такое же старое (у «Продукты по мелочи» уровня 7 — один ребёнок, тот же кластер);
--   dbo.I_PROMO.SEGMENT — у акций лайфстайл-сегмента ID_SEGMENT = 200 + номер кластера (отчёт эффекта группирует по SEGMENT).
-- Не трогается: I_CVM_EFFECT* (имя сегмента подтянется при пересчёте эффекта процедурой), I_CLUSTER_CONTACT (там номера, не имена),
--   I_CLUSTER_NAME (старая модель 2024–2025).
-- Повторный прогон ничего не меняет (старого имени уже нет — выход). Одна транзакция: при ошибке не меняется ничего.

SET QUOTED_IDENTIFIER ON		-- раннер — sqlcmd без -I: иначе QUOTED_IDENTIFIER OFF, UPDATE таблицы с фильтр. индексом/индекс. представлением — ошибка 1934
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
SET XACT_ABORT ON
GO

USE mci_model
GO

-- ===== ПАРАМЕТРЫ =====
declare @ver varchar(64) = 'how6-2026-10-09'
declare @org int = 1
declare @cluster int = 1							-- номер кластера уровня 10 (как на экране «Кластеры»)
declare @old nvarchar(200) = N'Продукты по мелочи'
declare @new nvarchar(200) = N'Перекусы на ходу'
-- ======================

set xact_abort on		-- и в этом пакете: при копировании одного пакета (SSMS) транзакция остаётся атомарной

declare @seg int = 200 + @cluster
declare @parent int = (select PARENT_CLUSTER_ID from I_CLUSTER_DICT with (nolock) where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 2 and CLUSTER_ID = @cluster)
declare @cur nvarchar(200) = (select CLUSTER_NAME from I_CLUSTER_DICT with (nolock) where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 2 and CLUSTER_ID = @cluster)
declare @children int = (select count(*) from I_CLUSTER_DICT with (nolock) where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 2 and PARENT_CLUSTER_ID = @parent)
declare @msg nvarchar(400)

if @cur is null
begin
	raiserror(N'Нет кластера уровня 10 с таким номером в I_CLUSTER_DICT — ничего не меняю', 16, 1)
	return
end
if @cur <> @old and @cur <> @new
begin
	raiserror(N'У кластера уровня 10 имя «%s» — ни старое, ни новое: проверь параметры, ничего не меняю', 16, 1, @cur)
	return
end
if exists (select 1 from I_CLUSTER_DICT with (nolock) where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 2 and CLUSTER_NAME = @new and CLUSTER_ID <> @cluster)
begin
	raiserror(N'Новое имя уже занято другим кластером уровня 10 — ничего не меняю', 16, 1)
	return
end
if not exists (select 1 from I_CLUSTER_DICT with (nolock) where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and CLUSTER_NAME = @old)
	and not exists (select 1 from I_PROMO with (nolock) where ID_COMPANY = 1 and ID_SEGMENT = @seg and SEGMENT = @old)
begin
	raiserror(N'Старого имени уже нет ни в I_CLUSTER_DICT, ни в I_PROMO — переименование уже сделано', 10, 1)
	return
end

-- ДО
select N'ДО' as [Когда], LEVEL_NO, K, CLUSTER_ID, CLUSTER_NAME, PARENT_CLUSTER_ID
from I_CLUSTER_DICT with (nolock)
where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and ((LEVEL_NO = 2 and CLUSTER_ID = @cluster) or (LEVEL_NO = 1 and CLUSTER_ID = @parent))
select N'ДО' as [Когда], ID_SEGMENT, SEGMENT, [Акций] = count(*), [С] = min(ID_PROMO), [По] = max(ID_PROMO)
from I_PROMO with (nolock) where ID_COMPANY = 1 and ID_SEGMENT = @seg group by ID_SEGMENT, SEGMENT

begin transaction

update I_CLUSTER_DICT set CLUSTER_NAME = @new
where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 2 and CLUSTER_ID = @cluster and CLUSTER_NAME = @old
set @msg = concat(N'I_CLUSTER_DICT уровень 10: ', @@rowcount)
raiserror(@msg, 0, 1) with nowait

update I_CLUSTER_DICT set CLUSTER_NAME = @new
where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and LEVEL_NO = 1 and CLUSTER_ID = @parent and CLUSTER_NAME = @old
	and @children = 1
set @msg = concat(N'I_CLUSTER_DICT уровень 7 (родитель с единственным ребёнком): ', @@rowcount)
raiserror(@msg, 0, 1) with nowait

update I_PROMO set SEGMENT = @new
where ID_COMPANY = 1 and ID_SEGMENT = @seg and SEGMENT = @old
set @msg = concat(N'I_PROMO ID_SEGMENT ', @seg, N': ', @@rowcount)
raiserror(@msg, 0, 1) with nowait

commit transaction

-- ПОСЛЕ
select N'ПОСЛЕ' as [Когда], LEVEL_NO, K, CLUSTER_ID, CLUSTER_NAME, PARENT_CLUSTER_ID
from I_CLUSTER_DICT with (nolock)
where MODEL_VERSION = @ver and ID_ORGANIZATION = @org and ((LEVEL_NO = 2 and CLUSTER_ID = @cluster) or (LEVEL_NO = 1 and CLUSTER_ID = @parent))
select N'ПОСЛЕ' as [Когда], ID_SEGMENT, SEGMENT, [Акций] = count(*), [С] = min(ID_PROMO), [По] = max(ID_PROMO)
from I_PROMO with (nolock) where ID_COMPANY = 1 and ID_SEGMENT = @seg group by ID_SEGMENT, SEGMENT
GO
