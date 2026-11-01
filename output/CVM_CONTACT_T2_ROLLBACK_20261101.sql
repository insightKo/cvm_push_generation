-- ============================================================================================
-- CVM_CONTACT_T2_ROLLBACK_20261101 — откат правки Т2 процедуры dbo.CVM_CONTACT (CVM_CONTACT_T2_20261101.sql).
-- Возвращает прежний текст (версия 11.10.2026) из dbo.I_DISCOUNT_PROC_BACKUP (PROC_NAME = 'CVM_CONTACT'), который
-- скрипт Т2 сохранил перед ALTER. В обменник — только если понадобится откат.
-- Защиты: процедура не в версии Т2 — ничего не делает (уже прежняя — сообщение, другая — стоп); пока идёт
--   «MCI Regular update» или загрузка MCI — ошибка без изменений (обменник повторит через 10 минут); ALTER и сверка
--   хэша — одной транзакцией.
-- ============================================================================================

USE mci_model
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

DECLARE @sha_old varbinary(32) = 0xF1BEAC3B625815FCEAC63A6FDF42FD0937A03CD3762DE1768824B949290EDBAF   -- версия 11.10.2026
      , @sha_new varbinary(32) = 0xDDF8ACB33D464C74DDB9D6F77880DF79018327FD9B7BC7E4368323FF7DC66579   -- версия Т2 (после ревью sql-senior круг 1)
      , @hdr_c nvarchar(60) = N'CREATE PROCEDURE  [dbo].[CVM_CONTACT]'
      , @hdr_a nvarchar(60) = N'ALTER PROCEDURE  [dbo].[CVM_CONTACT]'
      , @cur varbinary(32), @old nvarchar(max), @alter nvarchar(max);

SET @cur = HASHBYTES('SHA2_256', REPLACE(OBJECT_DEFINITION(OBJECT_ID(N'dbo.CVM_CONTACT')) COLLATE Latin1_General_BIN2, @hdr_a, @hdr_c));
IF @cur = @sha_old
BEGIN
    PRINT N'T2 rollback: CVM_CONTACT is already the 11.10.2026 version — nothing to do.';
    RETURN;
END
IF @cur IS NULL OR @cur <> @sha_new
BEGIN
    RAISERROR(N'STOP: CVM_CONTACT is not the T2 version — rollback would overwrite other changes; nothing changed.', 16, 1);
    RETURN;
END
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a (nolock)
                JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
           WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                            N'CVM REBUILD ONCE')
             AND a.session_id = (SELECT max(s.session_id) FROM msdb.dbo.syssessions AS s (nolock))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'Job running: MCI Regular update or MCI load is running; exchange will retry in 10 min.', 16, 1);
    RETURN;
END

SELECT TOP (1) @old = DEFINITION
FROM dbo.I_DISCOUNT_PROC_BACKUP (nolock)
WHERE PROC_NAME = N'CVM_CONTACT'
  AND HASHBYTES('SHA2_256', REPLACE(DEFINITION COLLATE Latin1_General_BIN2, @hdr_a, @hdr_c)) = @sha_old
ORDER BY SAVED_AT DESC;
IF @old IS NULL
BEGIN
    RAISERROR(N'STOP: no 11.10.2026 text of CVM_CONTACT in I_DISCOUNT_PROC_BACKUP — nothing changed.', 16, 1);
    RETURN;
END

SET @alter = STUFF(@old, CHARINDEX(@hdr_c COLLATE Latin1_General_BIN2, @old COLLATE Latin1_General_BIN2), LEN(N'CREATE'), N'ALTER');
BEGIN TRAN;
    EXEC sys.sp_executesql @alter;
    IF HASHBYTES('SHA2_256', REPLACE(OBJECT_DEFINITION(OBJECT_ID(N'dbo.CVM_CONTACT')) COLLATE Latin1_General_BIN2, @hdr_a, @hdr_c)) <> @sha_old
        THROW 50021, N'T2 rollback: text after ALTER does not match the 11.10.2026 version — rolled back.', 1;
COMMIT;
PRINT N'T2 rollback: CVM_CONTACT returned to the 11.10.2026 version.';
GO
