/* =====================================================================================
   CVM_ML_LIST_CHECK.sql — сверка списков CVM со списками Manzana (19.10.2026)

   Что делает скрипт, по объектам:
   1. 8 таблиц из CDP (crmdata__emission_task, crmdata__coupon_type, crmdata__coupon_emission,
      crmdata__List, loyalty__list_member_b, crmdata__ListMember, loyalty__coupons_application_cur,
      loyalty__list_member_a) — кластерный индекс idx перестраивается в кластерный колоночный
      с тем же именем idx (DROP_EXISTING). Данные не меняются.
   1Б. loyalty__coupon_cur (892 млн строк) — то же, ОТДЕЛЬНЫМ запуском в своё окно, не во время
      MCI Reload / Regular update: перестройка идёт часами, всё это время таблица закрыта
      и для загрузки Манзаны, и для чтения (даже с nolock).
      Пока перестраивается любая таблица, часовая загрузка в неё ждёт. Перестройка — одна
      транзакция на таблицу: журнал и место в файле данных под новый индекс нужны на время перестройки.
   2. dbo.I_ML_LIST_CHECK — новая таблица: результат каждой сверки (история, строка = акция × проверка).
   3. dbo.I_ML_LIST_MISSING — новая таблица: контакты, которых нет в списке Manzana,
      по последней проверке (перезаписывается каждый час), только для «загружен не полностью».
   4. dbo.ml_list_check — новая процедура сверки; письмо при новом расхождении.
   5. Задание SQL Agent «CVM ML list check» — новое, каждый час в :20, один шаг exec dbo.ml_list_check.

   Не удаляется ничего. Пакеты integration.packet, интеграционные процедуры
   и задания MCI (Regular update, Reload, List to ML) не меняются.

   Скрипт выкладывается в обменник D:\cvm_api\exchange и выполняется заданием CVM_API_nightly
   при КАЖДОМ его запуске (раз в 10 мин): все разделы при повторе проходят пропуском, без ошибок.

   Порядок: раздел 1 → разделы 2–5 (процедуру и задание — после раздела 1, иначе процедура
   каждый час читает loyalty__list_member_a построчно целиком) → раздел 1Б в своё окно.

   20.10.2026 (ночь): участники берутся из loyalty__list_member_a (было crmdata__ListMember — там
   наших октябрьских списков нет, все акции выходили «без участников»); справочник списков —
   одна строка на ListId (было по ListId + instance_id — каждая акция писалась 6 раз).

   Что сверяется:
   - акции I_PROMO (ID_COMPANY = 1), которые уже идут или стартуют завтра и закончились
     не раньше 7 дней назад;
   - наш список: I_PROMO_OFFER, CONTROL_GROUP = 0, ID_ORGANIZATION = 1, ID_CONTACT <> 0,
     любой LOAD_TO_ML (ушёл через ListsToML или через SFTP в Campaign);
   - список в Manzana: crmdata__List, имя начинается с «<номер акции>_» или «<номер акции>.»,
     последняя версия списка, не удалённый; участники — loyalty__list_member_a (list_id = ListId,
     contact_id = ID_CONTACT), последняя версия участника, не удалённый. GUID списка — crmdata__List.ListGuid.
     (crmdata__ListMember — участники CRM-списков; наших октябрьских там нет, 19.10 проверено.)
   Статусы: загружен / загружен не полностью / список без участников / нет списка в Manzana /
   не выгружен из MCI (LOAD_TO_ML = 0 у всего списка) / ещё не выгружен (старт завтра, списка нет).
   Письмо (raro@, ekor@manzanagroup.ru — как письмо об ошибке MCI Regular update) уходит,
   когда у акции появился новый статус-расхождение; одно и то же расхождение повторно не шлётся.
   ===================================================================================== */

use mci_model
go

/* ---------- 1. Кластерные колоночные индексы на 8 таблицах из CDP ---------- */
declare @t sysname, @sql nvarchar(max), @t0 datetime2(0);
declare c_tabs cursor local static for
    select v.t from (values
        (1, N'crmdata__emission_task'), (2, N'crmdata__coupon_type'), (3, N'crmdata__coupon_emission'),
        (4, N'crmdata__List'), (5, N'loyalty__list_member_b'), (6, N'crmdata__ListMember'),
        (7, N'loyalty__coupons_application_cur'), (8, N'loyalty__list_member_a')
    ) v(n, t) order by v.n;
open c_tabs;
fetch next from c_tabs into @t;
while @@fetch_status = 0
begin
    if exists (select 1 from sys.indexes i
               where i.object_id = object_id(N'dbo.' + @t) and i.name = N'idx'
                 and i.type = 1 and i.is_primary_key = 0 and i.is_unique_constraint = 0)
    begin
        set @t0 = sysdatetime();
        set @sql = N'create clustered columnstore index idx on dbo.' + quotename(@t)
                 + N' with (drop_existing = on, maxdop = 1);';
        begin try
            exec (@sql);
        end try
        begin catch
            print @t + N': ОШИБКА — ' + error_message();
        end catch
        if exists (select 1 from sys.indexes where object_id = object_id(N'dbo.' + @t) and name = N'idx' and type = 5)
            print @t + N': кластерный колоночный idx, ' + cast(datediff(second, @t0, sysdatetime()) as nvarchar(20)) + N' с';
    end
    else
        print @t + N': пропуск — нет строкового кластерного индекса idx (уже колоночный или другой)';
    fetch next from c_tabs into @t;
end
close c_tabs; deallocate c_tabs;
go

/* ---------- 2. История сверок ---------- */
if object_id(N'dbo.I_ML_LIST_CHECK') is null
begin
    create table dbo.I_ML_LIST_CHECK (
        CHECK_DT       datetime2(0)     not null,  -- время проверки
        ID_COMPANY     int              not null,
        ID_PROMO       int              not null,
        PROMO_NAME_ML  nvarchar(40)     null,
        START_DATE     date             null,
        FINISH_DATE    date             null,
        LOAD_TO_ML     int              null,      -- max по списку
        OUR_CONTACTS   int              not null,  -- наш список без КГ, уникальные контакты
        ML_LISTS       int              not null,  -- сколько списков Manzana с этим номером
        LIST_ID        int              null,      -- последний список Manzana (max ListId)
        LIST_GUID      uniqueidentifier null,
        LIST_NAME      nvarchar(256)    null,
        LIST_DT_LOAD   datetime         null,      -- когда версия списка пришла в mci_model
        ML_CONTACTS    int              not null,  -- участники во всех списках Manzana с этим номером
        MATCHED        int              not null,  -- наши контакты, найденные в Manzana
        MISSING        int              not null,  -- наши контакты, которых нет в Manzana
        EXTRA          int              not null,  -- в Manzana есть, у нас нет
        STATUS         nvarchar(60)     not null,
        IS_PROBLEM     bit              not null,  -- статус-расхождение
        ALERTED        bit              not null,  -- письмо об этом расхождении ушло
        ML_DATA_DT     datetime         null       -- max dt_load loyalty__list_member_a на момент проверки
    );
    create clustered columnstore index cci on dbo.I_ML_LIST_CHECK;
end
go

/* ---------- 3. Недостающие контакты по последней проверке ---------- */
if object_id(N'dbo.I_ML_LIST_MISSING') is null
begin
    create table dbo.I_ML_LIST_MISSING (
        CHECK_DT    datetime2(0)     not null,
        ID_COMPANY  int              not null,
        ID_PROMO    int              not null,
        ID_CONTACT  int              not null,
        CRM_GUID    uniqueidentifier null
    );
    create clustered columnstore index cci on dbo.I_ML_LIST_MISSING;
end
go

/* ---------- 4. Процедура сверки ---------- */
create or alter procedure dbo.ml_list_check
    @days_back  int = 7,   -- акции, закончившиеся не раньше чем столько дней назад
    @days_ahead int = 1    -- и стартующие не позже чем через столько дней
as
begin
    set nocount on;
    set xact_abort on;

    declare @now        datetime2(0)  = sysdatetime(),
            @today      date          = cast(getdate() as date),
            @recipients nvarchar(500) = N'raro@manzanagroup.ru;ekor@manzanagroup.ru;',
            @rc         int;

    declare @ml_data_dt datetime =
        (select max(dt_load) from dbo.loyalty__list_member_a with (nolock) where ID_COMPANY = 1);

    -- 4.1 Акции окна
    select ID_PROMO, PROMO_NAME_ML = max(PROMO_NAME_ML), START_DATE = min(START_DATE), FINISH_DATE = max(FINISH_DATE)
    into #promo
    from dbo.I_PROMO with (nolock)
    where ID_COMPANY = 1
      and START_DATE  <= dateadd(day, @days_ahead, @today)
      and FINISH_DATE >= dateadd(day, -@days_back, @today)
    group by ID_PROMO;

    declare @pmin int = (select min(ID_PROMO) from #promo),
            @pmax int = (select max(ID_PROMO) from #promo);

    -- 4.2 Наш список: получатели без КГ (диапазон номеров — для отсечения групп строк колоночного индекса)
    select o.ID_PROMO, o.ID_CONTACT, o.CRM_GUID, o.LOAD_TO_ML
    into #our
    from dbo.I_PROMO_OFFER o with (nolock)
    where o.ID_PROMO between @pmin and @pmax
      and o.ID_PROMO in (select ID_PROMO from #promo)
      and o.CONTROL_GROUP = 0
      and o.ID_COMPANY = 1
      and o.ID_ORGANIZATION = 1
      and o.ID_CONTACT <> 0;
    create clustered columnstore index cci on #our;

    -- 4.3 Списки Manzana: последняя версия каждого списка, не удалённый, номер акции из начала имени.
    --     Справочник лежит копией на каждом экземпляре (instance_id) — берётся одна строка на ListId.
    select x.ID_PROMO, x.ListId, x.ListName, x.ListGuid, x.dt_load
    into #ml_list
    from (select l.ListId, l.ListName, l.ListGuid, l.dt_load, l.sys_change_operation,
                 ID_PROMO = try_cast(left(l.ListName, patindex(N'%[_.]%', l.ListName + N'_') - 1) as int),
                 rn = row_number() over (partition by l.ListId order by l.last_version desc, l.dt_load desc)
          from dbo.crmdata__List l with (nolock)
          where l.ID_COMPANY = 1) x
    where x.rn = 1
      and isnull(x.sys_change_operation, 'I') <> 'D'
      and x.ID_PROMO in (select ID_PROMO from #promo);

    -- 4.4 Участники списков — loyalty__list_member_a: list_id = ListId справочника, contact_id = ID_CONTACT.
    --     Участники разложены по экземплярам: ключ участника = id + instance_id.
    --     Сначала ключи участников наших списков, затем последняя версия каждого
    --     (строка удаления может прийти без list_id — поэтому версия ищется по ключу).
    select distinct m.id, m.instance_id
    into #lm_key
    from dbo.loyalty__list_member_a m with (nolock)
    where m.ID_COMPANY = 1
      and m.list_id in (select ListId from #ml_list);
    create clustered columnstore index cci on #lm_key;

    select distinct l.ID_PROMO, ID_CONTACT = x.contact_id
    into #ml
    from (select m.id, m.instance_id, m.list_id, m.contact_id, m.sys_change_operation,
                 rn = row_number() over (partition by m.id, m.instance_id order by m.last_version desc, m.dt_load desc)
          from dbo.loyalty__list_member_a m with (nolock)
          join #lm_key k on k.id = m.id and k.instance_id = m.instance_id
          where m.ID_COMPANY = 1) x
    join #ml_list l on l.ListId = x.list_id   -- список — по последней версии участника
    where x.rn = 1
      and isnull(x.sys_change_operation, 'I') <> 'D'
    option (maxdop 2);
    create clustered columnstore index cci on #ml;

    -- 4.5 Итог по акциям и статус (один раз)
    select oc.ID_PROMO, oc.OUR_CONTACTS, oc.LOAD_TO_ML,
           ML_LISTS    = isnull(ls.ML_LISTS, 0),
           ls.LIST_ID,
           ML_CONTACTS = isnull(mc.ML_CONTACTS, 0),
           MATCHED     = isnull(mt.MATCHED, 0),
           STATUS      = cast(null as nvarchar(60)),
           IS_PROBLEM  = cast(0 as bit)
    into #res
    from (select ID_PROMO, OUR_CONTACTS = count(distinct ID_CONTACT), LOAD_TO_ML = max(LOAD_TO_ML)
          from #our group by ID_PROMO) oc
    left join (select ID_PROMO, ML_LISTS = count(*), LIST_ID = max(ListId) from #ml_list group by ID_PROMO) ls
           on ls.ID_PROMO = oc.ID_PROMO
    left join (select ID_PROMO, ML_CONTACTS = count(*) from #ml group by ID_PROMO) mc
           on mc.ID_PROMO = oc.ID_PROMO
    left join (select o.ID_PROMO, MATCHED = count(distinct o.ID_CONTACT)
               from #our o
               join #ml m on m.ID_PROMO = o.ID_PROMO and m.ID_CONTACT = o.ID_CONTACT
               group by o.ID_PROMO) mt
           on mt.ID_PROMO = oc.ID_PROMO;

    update r
    set STATUS = case
                     when r.ML_LISTS = 0 and p.START_DATE > @today then N'ещё не выгружен'
                     when r.ML_LISTS = 0 and r.LOAD_TO_ML = 0      then N'не выгружен из MCI'
                     when r.ML_LISTS = 0                           then N'нет списка в Manzana'
                     when r.ML_CONTACTS = 0                        then N'список без участников'
                     when r.MATCHED < r.OUR_CONTACTS               then N'загружен не полностью'
                     else N'загружен'
                 end
    from #res r
    join #promo p on p.ID_PROMO = r.ID_PROMO;

    update #res
    set IS_PROBLEM = case when STATUS in (N'не выгружен из MCI', N'нет списка в Manzana',
                                          N'список без участников', N'загружен не полностью') then 1 else 0 end;

    declare @prev datetime2(0) =
        (select max(CHECK_DT) from dbo.I_ML_LIST_CHECK with (nolock) where CHECK_DT < @now);

    begin tran;

    -- ALERTED = 1, если о том же расхождении письмо уже ушло в прошлой проверке
    insert into dbo.I_ML_LIST_CHECK
        (CHECK_DT, ID_COMPANY, ID_PROMO, PROMO_NAME_ML, START_DATE, FINISH_DATE, LOAD_TO_ML,
         OUR_CONTACTS, ML_LISTS, LIST_ID, LIST_GUID, LIST_NAME, LIST_DT_LOAD,
         ML_CONTACTS, MATCHED, MISSING, EXTRA, STATUS, IS_PROBLEM, ALERTED, ML_DATA_DT)
    select @now, 1, r.ID_PROMO, p.PROMO_NAME_ML, p.START_DATE, p.FINISH_DATE, r.LOAD_TO_ML,
           r.OUR_CONTACTS, r.ML_LISTS, r.LIST_ID, l.ListGuid, l.ListName, l.dt_load,
           r.ML_CONTACTS, r.MATCHED, r.OUR_CONTACTS - r.MATCHED, r.ML_CONTACTS - r.MATCHED,
           r.STATUS, r.IS_PROBLEM,
           ALERTED = case when r.IS_PROBLEM = 1 and exists (
                              select 1 from dbo.I_ML_LIST_CHECK pr with (nolock)
                              where pr.CHECK_DT = @prev and pr.ID_PROMO = r.ID_PROMO
                                and pr.STATUS = r.STATUS and pr.ALERTED = 1)
                          then 1 else 0 end,
           @ml_data_dt
    from #res r
    join #promo p on p.ID_PROMO = r.ID_PROMO
    left join #ml_list l on l.ListId = r.LIST_ID;

    truncate table dbo.I_ML_LIST_MISSING;

    insert into dbo.I_ML_LIST_MISSING with (tablock) (CHECK_DT, ID_COMPANY, ID_PROMO, ID_CONTACT, CRM_GUID)
    select distinct @now, 1, o.ID_PROMO, o.ID_CONTACT, o.CRM_GUID
    from #our o
    join #res r on r.ID_PROMO = o.ID_PROMO and r.STATUS = N'загружен не полностью'
    where not exists (select 1 from #ml m where m.ID_PROMO = o.ID_PROMO and m.ID_CONTACT = o.ID_CONTACT);

    commit;

    -- 4.6 Письмо: есть расхождение, о котором письмо ещё не уходило; в письме — все текущие расхождения
    if exists (select 1 from dbo.I_ML_LIST_CHECK with (nolock)
               where CHECK_DT = @now and IS_PROBLEM = 1 and ALERTED = 0)
    begin
        declare @body nvarchar(max) =
            N'Сверка списков CVM со списками Manzana на ' + convert(nvarchar(16), @now, 120) + N'.' + char(13) + char(10)
          + N'Участники списков Manzana в mci_model загружены по ' + isnull(convert(nvarchar(16), @ml_data_dt, 120), N'—') + N'.'
          + char(13) + char(10) + char(13) + char(10)
          + (select string_agg(cast(
                   cast(c.ID_PROMO as nvarchar(10)) + N' ' + isnull(c.PROMO_NAME_ML, N'') + N' — ' + c.STATUS
                 + N'. Наш список: ' + cast(c.OUR_CONTACTS as nvarchar(12))
                 + N', в Manzana: ' + cast(c.MATCHED as nvarchar(12))
                 + N', нет в Manzana: ' + cast(c.MISSING as nvarchar(12))
                 + isnull(N'. Список ' + cast(c.LIST_ID as nvarchar(12)) + N' ' + c.LIST_NAME
                          + N', GUID ' + cast(c.LIST_GUID as nvarchar(36)), N'')
                 as nvarchar(max)), char(13) + char(10)) within group (order by c.ID_PROMO)
             from dbo.I_ML_LIST_CHECK c with (nolock)
             where c.CHECK_DT = @now and c.IS_PROBLEM = 1);

        exec @rc = msdb.dbo.sp_send_dbmail
            @profile_name = N'Mail',
            @recipients   = @recipients,
            @subject      = N'Сверка списков CVM с Manzana: есть расхождения',
            @body         = @body;

        if @rc = 0
            update dbo.I_ML_LIST_CHECK
            set ALERTED = 1
            where CHECK_DT = @now and IS_PROBLEM = 1;
        else
            throw 50001, N'ml_list_check: письмо не поставлено в очередь', 1;
    end
end
go

/* ---------- 5. Задание «CVM ML list check» — каждый час в :20 ---------- */
use msdb
go
if exists (select 1 from msdb.dbo.sysjobs with (nolock) where name = N'CVM ML list check')
    print N'Задание «CVM ML list check» уже есть — пропуск';
else
begin
    declare @job_id uniqueidentifier;
    exec msdb.dbo.sp_add_job
        @job_name = N'CVM ML list check', @enabled = 1, @owner_login_name = N'sa',
        @description = N'Сверка списков CVM (I_PROMO_OFFER) со списками Manzana из CDP: dbo.ml_list_check, каждый час',
        @job_id = @job_id output;
    exec msdb.dbo.sp_add_jobstep
        @job_id = @job_id, @step_name = N'ml_list_check', @subsystem = N'TSQL',
        @database_name = N'mci_model', @command = N'exec dbo.ml_list_check;',
        @on_success_action = 1, @on_fail_action = 2;
    exec msdb.dbo.sp_add_jobschedule
        @job_id = @job_id, @name = N'every hour at :20', @enabled = 1,
        @freq_type = 4, @freq_interval = 1, @freq_subday_type = 8, @freq_subday_interval = 1,
        @active_start_time = 2000;
    exec msdb.dbo.sp_add_jobserver @job_id = @job_id, @server_name = N'(local)';
    print N'Задание «CVM ML list check» создано: каждый час в :20';
end
go

/* ---------- 1Б. loyalty__coupon_cur — отдельным запуском в своё окно ----------
use mci_model
declare @t0 datetime2(0) = sysdatetime();
if exists (select 1 from sys.indexes i
           where i.object_id = object_id(N'dbo.loyalty__coupon_cur') and i.name = N'idx'
             and i.type = 1 and i.is_primary_key = 0 and i.is_unique_constraint = 0)
    create clustered columnstore index idx on dbo.loyalty__coupon_cur with (drop_existing = on, maxdop = 1);
if exists (select 1 from sys.indexes where object_id = object_id(N'dbo.loyalty__coupon_cur') and name = N'idx' and type = 5)
    print N'loyalty__coupon_cur: кластерный колоночный idx, ' + cast(datediff(minute, @t0, sysdatetime()) as nvarchar(20)) + N' мин';
*/

/* ---------- Проверка после запуска ----------
use mci_model
exec dbo.ml_list_check;
select ID_PROMO, PROMO_NAME_ML, STATUS, OUR_CONTACTS, MATCHED, MISSING, EXTRA, LIST_ID, LIST_GUID, ALERTED, ML_DATA_DT
from dbo.I_ML_LIST_CHECK with (nolock)
where CHECK_DT = (select max(CHECK_DT) from dbo.I_ML_LIST_CHECK with (nolock))
order by IS_PROBLEM desc, STATUS, ID_PROMO;
*/

/* ---------- Откат (не запускать без решения) ----------
use msdb
exec msdb.dbo.sp_delete_job @job_name = N'CVM ML list check';
use mci_model
drop procedure if exists dbo.ml_list_check;
-- таблицы I_ML_LIST_CHECK / I_ML_LIST_MISSING — удалять только по решению
-- индекс обратно в строковый (пример для одной таблицы, ключ как был):
-- create clustered index idx on dbo.crmdata__ListMember (ListMemberId, instance_id, ID_COMPANY, last_version, dt_load)
--     with (drop_existing = on);
*/
