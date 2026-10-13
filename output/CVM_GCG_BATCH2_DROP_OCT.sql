-- Партия 2 (догонка): клиенты с первой покупкой с 01.10.2026 убираются из ГКГ и из партии — у них ещё нет месяца
-- покупок; их разыграет ночная нарезка 01.11 вместе с остальными новыми октября (первая покупка 01.10–31.10).
-- Партия 2 остаётся: первая покупка 02.07–30.09.2026, в отчёте с 14.10.
USE mci_model;
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @cut date = '20261001';

-- партия 2 — догонка с 02.07, и после неё партий ещё нет (иначе убранные клиенты 01.10–12.10 не попадут ни в одну партию)
IF NOT EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock) WHERE BATCH_ID = 2 AND FIRST_FROM = '20260702')
   OR EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock) WHERE BATCH_ID > 2)
    THROW 50040, N'СТОП: партия 2 — не догонка с 02.07 или после неё уже заведена партия; ничего не изменено', 1;

-- сколько убирается
SELECT [Клиентов с первой покупкой с 01.10] = count(*), [из них в ГКГ] = sum(GCG * 1)
FROM dbo.I_GCG_PRE (nolock)
WHERE BATCH_ID = 2 AND FIRST_DATA >= @cut;

BEGIN TRANSACTION;

DELETE g
FROM dbo.I_GLOBAL_CG AS g
     JOIN dbo.I_GCG_PRE AS p WITH (NOLOCK) ON p.ID_CONTACT = g.ID_CONTACT
WHERE p.BATCH_ID = 2 AND p.FIRST_DATA >= @cut AND p.GCG = 1
  AND g.ID_COMPANY = 1 AND g.CONTROL_GROUP = 1;

DELETE FROM dbo.I_GCG_PRE WHERE BATCH_ID = 2 AND FIRST_DATA >= @cut;

UPDATE b
SET FIRST_TO    = dateadd(day, -1, @cut)
  , CLIENTS     = s.CLIENTS
  , GCG_CLIENTS = s.GCG_CLIENTS
  , NOTE        = N'донарезка ГКГ: первая покупка 02.07.2026–30.09.2026 (с 01.10 — в нарезку 01.11)'
FROM dbo.I_GCG_BATCH AS b
     CROSS APPLY (SELECT CLIENTS = count(*), GCG_CLIENTS = sum(GCG * 1) FROM dbo.I_GCG_PRE WITH (NOLOCK) WHERE BATCH_ID = 2) AS s
WHERE b.BATCH_ID = 2;

COMMIT TRANSACTION;

-- итог
SELECT [Партия] = BATCH_ID, [Первая покупка с] = FIRST_FROM, [по] = FIRST_TO, [Клиентов] = CLIENTS, [В ГКГ] = GCG_CLIENTS, [Примечание] = NOTE
FROM dbo.I_GCG_BATCH (nolock) ORDER BY BATCH_ID;
SELECT [Всего в ГКГ] = count(*) FROM dbo.I_GLOBAL_CG (nolock) WHERE ID_COMPANY = 1 AND CONTROL_GROUP = 1;
