-- 9. Ежедневная процедура: новые коды ДИКСИ → автопривязка; каждый товар I_PRODUCT → наша категория
create or alter procedure dbo.cvm_category_refresh
as
begin
    set nocount on;
    set xact_abort on;

    declare @today date = cast(getdate() as date), @unmapped int, @new_codes int, @upd int, @ins int,
            @auto_hist int, @auto_carry int, @auto_sib int;

    select @unmapped = c.ID_CVM_CATEGORY
    from   dbo.I_CVM_CATEGORY as c (nolock)
    where  c.NODE_TYPE = 2 and c.NAME = N'Несопоставленные' and c.STATUS = 1;
    if @unmapped is null
    begin
        raiserror(N'СТОП: в справочнике нет категории «Несопоставленные» — ничего не изменено.', 16, 1);
        return;
    end;

    -- товары: одна строка на товар; название — верхний регистр, Ё = Е, пробел в начале для поиска «с начала слова»
    drop table if exists #p;
    select ID_PRODUCT        = cast(p.ID_PRODUCT as bigint)
         , ID_CATEGORY_5_ext = max(cast(p.ID_CATEGORY_5_ext as nvarchar(50)))
         , NNAME             = N' ' + replace(upper(max(cast(p.PRODUCT_NAME as nvarchar(500)))), N'Ё', N'Е')
    into   #p
    from   dbo.I_PRODUCT as p (nolock)
    where  p.ID_COMPANY = 1 and p.ID_PRODUCT is not null
    group by cast(p.ID_PRODUCT as bigint);
    create clustered index IX_p on #p (ID_CATEGORY_5_ext, ID_PRODUCT);

    begin transaction;

    -- новые коды ДИКСИ — «несопоставлен»
    insert into dbo.I_CVM_CATEGORY_MAP (ID_CATEGORY_5_ext, EXT_NAME, MAP_METHOD, ID_CVM_CATEGORY, STATUS, DATE_CREATE, NOTE)
    select x.ID_CATEGORY_5_ext
         , (select max(e.I_CATEGORY_5_ext) from dbo.I_CATEGORY_EXT as e (nolock)
            where e.ID_COMPANY = 1 and e.ID_CATEGORY_5_ext = x.ID_CATEGORY_5_ext)
         , null, null, 2, @today, N'новый код ДИКСИ'
    from   (select distinct ID_CATEGORY_5_ext from #p where ID_CATEGORY_5_ext is not null) as x
    where  not exists (select 1 from dbo.I_CVM_CATEGORY_MAP as m where m.ID_CATEGORY_5_ext = x.ID_CATEGORY_5_ext);
    set @new_codes = @@rowcount;

    -- автопривязка несопоставленных кодов (каждый день, сразу после загрузки I_PRODUCT):
    -- а) перекодировка ДИКСИ: товары кода уже были в справочнике под прежним кодом (RECODED = 1) —
    --    все в одной категории → код целиком туда; в разных → каждый товар сохраняет свою категорию (поштучно).
    --    Товары, привязанные к этому же коду вручную (без перекодировки), код целиком не тянут, но блокируют шаг б.
    drop table if exists #hist;
    select p.ID_CATEGORY_5_ext, p.ID_PRODUCT, t.ID_CVM_CATEGORY
         , RECODED = case when isnull(t.ID_CATEGORY_5_ext, N'') <> p.ID_CATEGORY_5_ext then 1 else 0 end
    into   #hist
    from   #p as p
           join dbo.I_CVM_CATEGORY_MAP as m (nolock) on m.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext and m.STATUS = 2
           join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_PRODUCT = p.ID_PRODUCT and t.HOW <> 0
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = t.ID_CVM_CATEGORY and c.STATUS = 1
                                                and c.ID_CVM_CATEGORY <> @unmapped;

    update m
    set    m.MAP_METHOD = 1, m.ID_CVM_CATEGORY = h.ID_CVM_CATEGORY, m.STATUS = 1, m.DATE_UPDATE = @today,
           m.NOTE = N'авто ' + convert(nchar(10), @today, 104) + N': товары кода уже были в этой категории (перекодировка ДИКСИ)'
    from   dbo.I_CVM_CATEGORY_MAP as m
           join (select ID_CATEGORY_5_ext, ID_CVM_CATEGORY = min(ID_CVM_CATEGORY)
                 from #hist where RECODED = 1 group by ID_CATEGORY_5_ext
                 having min(ID_CVM_CATEGORY) = max(ID_CVM_CATEGORY)) as h on h.ID_CATEGORY_5_ext = m.ID_CATEGORY_5_ext
    where  m.STATUS = 2;
    set @auto_hist = @@rowcount;

    insert into dbo.I_CVM_PRODUCT_CATEGORY_MANUAL (ID_PRODUCT, ID_CVM_CATEGORY, SOURCE, DATE_UPDATE, NOTE)
    select h.ID_PRODUCT, h.ID_CVM_CATEGORY, 1, @today, N'авто: категория до перекодировки ДИКСИ'
    from   #hist as h
           join dbo.I_CVM_CATEGORY_MAP as m (nolock) on m.ID_CATEGORY_5_ext = h.ID_CATEGORY_5_ext and m.STATUS = 2
    where  h.RECODED = 1
      and  not exists (select 1 from dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as x where x.ID_PRODUCT = h.ID_PRODUCT);
    set @auto_carry = @@rowcount;

    -- б) новый код без истории товаров: соседние коды того же родителя (первые 6 знаков) — не меньше двух,
    --    все привязаны целиком к одной категории → код туда же (проверка на маппинге 19.10: верно в 94 % случаев)
    update m
    set    m.MAP_METHOD = 1, m.ID_CVM_CATEGORY = s.ID_CVM_CATEGORY, m.STATUS = 1, m.DATE_UPDATE = @today,
           m.NOTE = N'авто ' + convert(nchar(10), @today, 104) + N': по соседним кодам ' + left(m.ID_CATEGORY_5_ext, 6) + N'…'
    from   dbo.I_CVM_CATEGORY_MAP as m
           join (select PARENT = left(o.ID_CATEGORY_5_ext, 6), ID_CVM_CATEGORY = min(o.ID_CVM_CATEGORY)
                 from   dbo.I_CVM_CATEGORY_MAP as o (nolock)
                 where  len(o.ID_CATEGORY_5_ext) = 8 and o.STATUS <> 0
                 group by left(o.ID_CATEGORY_5_ext, 6)
                 having count(*) - sum(case when o.STATUS = 2 then 1 else 0 end) >= 2         -- привязанных соседей ≥ 2
                    and sum(case when o.STATUS = 1 and o.MAP_METHOD = 1 then 0 else 1 end)
                        = sum(case when o.STATUS = 2 then 1 else 0 end)                         -- все привязанные — целиком
                    and min(o.ID_CVM_CATEGORY) = max(o.ID_CVM_CATEGORY)) as s                   -- и в одну категорию
                on s.PARENT = left(m.ID_CATEGORY_5_ext, 6)
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = s.ID_CVM_CATEGORY and c.STATUS = 1
                                                and c.ID_CVM_CATEGORY <> @unmapped
    where  m.STATUS = 2 and len(m.ID_CATEGORY_5_ext) = 8
      and  not exists (select 1 from #hist as h where h.ID_CATEGORY_5_ext = m.ID_CATEGORY_5_ext);
    set @auto_sib = @@rowcount;

    commit transaction;

    -- слова правил по одному в строке: A — «любое из», N — «ни одного из»
    drop table if exists #w;
    select r.ID_CATEGORY_5_ext, r.RULE_ORDER, KIND = 'A', WORD = replace(upper(ltrim(rtrim(s.value))), N'Ё', N'Е')
    into   #w
    from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
           cross apply string_split(r.WORDS_ANY, N';') as s
    where  ltrim(rtrim(s.value)) <> N''
    union all
    select r.ID_CATEGORY_5_ext, r.RULE_ORDER, 'N', replace(upper(ltrim(rtrim(s.value))), N'Ё', N'Е')
    from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
           cross apply string_split(r.WORDS_NONE, N';') as s
    where  ltrim(rtrim(s.value)) <> N'';

    -- совпадения слов (только товары кодов «по словам»)
    drop table if exists #hit;
    select distinct p.ID_PRODUCT, w.RULE_ORDER, w.KIND
    into   #hit
    from   #p as p
           join #w as w on w.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
    where  p.NNAME collate Cyrillic_General_BIN2 like N'%[^А-ЯA-Z0-9]' + w.WORD + N'%';
    create clustered index IX_hit on #hit (ID_PRODUCT, RULE_ORDER, KIND);

    -- итог: поштучно (3) → код целиком (1) → первое подходящее правило (2) → «Несопоставленные» (0)
    drop table if exists #r;
    select p.ID_PRODUCT, p.ID_CATEGORY_5_ext
         , ID_CVM_CATEGORY = coalesce(man.ID_CVM_CATEGORY, case when m.MAP_METHOD = 1 then m.ID_CVM_CATEGORY end,
                                      rl.ID_CVM_CATEGORY, @unmapped)
         , HOW = case when coalesce(man.ID_CVM_CATEGORY, case when m.MAP_METHOD = 1 then m.ID_CVM_CATEGORY end,
                                    rl.ID_CVM_CATEGORY, @unmapped) = @unmapped then 0
                      when man.ID_CVM_CATEGORY is not null then 3
                      when m.MAP_METHOD = 1 then 1
                      else 2 end
    into   #r
    from   #p as p
           left join dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as man (nolock) on man.ID_PRODUCT = p.ID_PRODUCT
           left join dbo.I_CVM_CATEGORY_MAP as m (nolock) on m.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
           outer apply (select top (1) r.ID_CVM_CATEGORY
                        from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
                        where  m.MAP_METHOD = 2
                          and  r.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
                          and  (r.WORDS_ANY is null
                                or exists (select 1 from #hit as h where h.ID_PRODUCT = p.ID_PRODUCT and h.RULE_ORDER = r.RULE_ORDER and h.KIND = 'A'))
                          and  not exists (select 1 from #hit as h where h.ID_PRODUCT = p.ID_PRODUCT and h.RULE_ORDER = r.RULE_ORDER and h.KIND = 'N')
                        order by r.RULE_ORDER) as rl;

    begin transaction;

    -- привязка товаров: меняются только изменившиеся строки; товары, ушедшие из I_PRODUCT, остаются
    -- (история чеков читается по текущему справочнику)
    update t
    set    t.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY, t.ID_CVM_GROUP = c.ID_PARENT,
           t.ID_CATEGORY_5_ext = r.ID_CATEGORY_5_ext, t.HOW = r.HOW, t.DATE_UPDATE = @today
    from   dbo.I_CVM_PRODUCT_CATEGORY as t
           join #r as r on r.ID_PRODUCT = t.ID_PRODUCT
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY
    where  t.ID_CVM_CATEGORY <> r.ID_CVM_CATEGORY or t.ID_CVM_GROUP <> c.ID_PARENT or t.HOW <> r.HOW
       or  isnull(t.ID_CATEGORY_5_ext, N'') <> isnull(r.ID_CATEGORY_5_ext, N'');
    set @upd = @@rowcount;

    insert into dbo.I_CVM_PRODUCT_CATEGORY (ID_PRODUCT, ID_CVM_CATEGORY, ID_CVM_GROUP, ID_CATEGORY_5_ext, HOW, DATE_UPDATE)
    select r.ID_PRODUCT, r.ID_CVM_CATEGORY, c.ID_PARENT, r.ID_CATEGORY_5_ext, r.HOW, @today
    from   #r as r
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY
    where  not exists (select 1 from dbo.I_CVM_PRODUCT_CATEGORY as t where t.ID_PRODUCT = r.ID_PRODUCT);
    set @ins = @@rowcount;

    commit transaction;

    declare @un int = (select count(*) from #r where HOW = 0);
    raiserror(N'cvm_category_refresh: новых кодов %d, авто по истории %d (+%d товаров поштучно), по соседям %d; товаров добавлено %d, изменено %d, несопоставлено %d.',
              10, 1, @new_codes, @auto_hist, @auto_carry, @auto_sib, @ins, @upd, @un) with nowait;
end;
go
