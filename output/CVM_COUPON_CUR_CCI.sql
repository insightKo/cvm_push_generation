/* =====================================================================================
   CVM_COUPON_CUR_CCI.sql — loyalty__coupon_cur: кластерный индекс idx → кластерный колоночный idx (23.10.2026)
   Раздел 1Б скрипта CVM_ML_LIST_CHECK.sql, отдельным файлом для обменника D:\cvm_api\exchange.

   Что делает, по объектам:
   - dbo.loyalty__coupon_cur (≈ 912 млн строк, 260 ГБ): индекс idx перестраивается в кластерный колоночный
     с тем же именем (DROP_EXISTING). Данные не меняются. Остальные таблицы, пакеты integration.packet
     и задания MCI не трогаются.

   Окно (сервер Standard: перестройка только офлайн, в один поток):
   - старт только с 19:00 до 21:00 и только когда не идут «MCI Reload every day» (с 01:00, ~4 ч)
     и «MCI Regular update» (с ~05:00, ~13 ч). 19.10 восемь таблиц перестроились в 22:51–23:04
     (loyalty__list_member_a, 270 млн строк, — 9 мин); здесь ожидается до 1,5 ч. Обрыв раннером через 3 ч
     и откат заканчиваются до 01:00. Если прошлая попытка шла дольше 2 ч (журнал CVM_API_JOB_LOG),
     перестройка больше не запускается — ошибка «СТОП», решение за Еленой.
   - пока идёт перестройка, таблица закрыта для часовой загрузки Manzana и для чтения; загрузка
     догрузит пропущенное следующим часом (инкремент по dt_load).
   - ожидание блокировки — не дольше 5 мин (иначе ошибка и повтор через 10 мин).

   Обменник (прогон каждые 10 мин): вне окна или во время ночного прохода — ошибка 16, скрипт остаётся
   в обменнике и повторяется; после перестройки — пропуск без ошибки, раннер переносит скрипт в done.
   ===================================================================================== */

use mci_model
go

set nocount on;
set quoted_identifier on;
set ansi_nulls on;
set lock_timeout 300000;

declare @t0 datetime2(0) = sysdatetime(),
        @tm time(0)      = cast(getdate() as time(0));

if exists (select 1 from sys.indexes
           where object_id = object_id(N'dbo.loyalty__coupon_cur') and name = N'idx' and type = 5)
    print N'loyalty__coupon_cur: уже кластерный колоночный idx — пропуск';
else if not exists (select 1 from sys.indexes
                    where object_id = object_id(N'dbo.loyalty__coupon_cur') and name = N'idx'
                      and type = 1 and is_primary_key = 0 and is_unique_constraint = 0)
    raiserror(N'СТОП: у loyalty__coupon_cur нет кластерного индекса idx — перестройка не запущена.', 16, 1);
else if exists (select 1 from dbo.CVM_API_JOB_LOG with (nolock)
                where JOB = N'CVM_COUPON_CUR_CCI' and STATUS <> 'ok'
                  and datediff(minute, STARTED, FINISHED) > 120)
    raiserror(N'СТОП: прошлая попытка перестройки loyalty__coupon_cur шла больше 2 ч и не закончилась — повторно не запускаю.', 16, 1);
else if @tm < '19:00' or @tm >= '21:00'
    raiserror(N'Ждём окно 19:00–21:00 — перестройка loyalty__coupon_cur не запущена.', 16, 1);
else if exists (select 1
                from msdb.dbo.sysjobactivity a with (nolock)
                join msdb.dbo.sysjobs j with (nolock) on j.job_id = a.job_id
                where j.name in (N'MCI Regular update', N'MCI Reload every day')
                  and a.session_id = (select max(session_id) from msdb.dbo.syssessions with (nolock))
                  and a.start_execution_date is not null
                  and a.stop_execution_date is null)
    raiserror(N'Ждём: идёт «MCI Reload every day» или «MCI Regular update» — перестройка loyalty__coupon_cur не запущена.', 16, 1);
else
begin
    begin try
        create clustered columnstore index idx on dbo.loyalty__coupon_cur with (drop_existing = on, maxdop = 1);
    end try
    begin catch
        throw;
    end catch;

    declare @msg nvarchar(200) = N'loyalty__coupon_cur: кластерный колоночный idx, '
                               + cast(datediff(minute, @t0, sysdatetime()) as nvarchar(20)) + N' мин';
    print @msg;
end
go
