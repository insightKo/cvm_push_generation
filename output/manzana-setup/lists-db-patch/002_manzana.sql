-- База списков (cvm_lists): робот «Заведение в Manzana» (cvm-manzana-setup) берёт списки отсюда, а не с SFTP-обмена,
-- и пишет сюда итог заведения — список, PUSH-кампании, инфокупон (решение Елены 20.10.2026: первоисточник — наш Postgres,
-- в Google — только уведомления). Таблицы promo, offer_chunk, transfer и всё, что читает сервер MCI, не меняются.
--
-- Роль lists_manzana: читает только номер, старт, имя кампании и число CRM_GUID акции и CRM_GUID участников (без ID_CONTACT);
-- пишет только в lists.manzana. Пароль ставит set-secrets.sh робота, вход — строка в pg_hba.conf (setup.sh, из сети compose).
--
-- Кладётся в db/lists/ портала рядом с 001_lists.sql; применяет db/lists/migrate.sh. Повторный прогон безопасен.

begin;

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'lists_manzana') then
    create role lists_manzana login;               -- робот cvm-manzana-setup; без пароля войти не может
  end if;
end $$;

grant connect on database cvm_lists to lists_manzana;
grant usage on schema lists to lists_manzana;

set local role lists_owner;

-- Итог заведения: одна строка на объект. Статусов два: done — заведено, error — не заведено (текст в detail).
-- kind: list — список в Campaign; push — кампания одного сообщения (item = «<msg>:<дата отправки>»);
-- coupon — инфокупон тестового показа (item пусто) и перевод в бой (item = «бой»).
-- Номер не ссылается на lists.promo: здесь и акции из скриптов выборок (101xxx), которых в lists.promo нет.
create table if not exists lists.manzana (
  id_promo     integer not null,
  start_date   date not null,
  kind         text not null check (kind in ('list', 'push', 'coupon')),
  item         text not null default '',
  name         text not null default '',                     -- имя МС / кампании / купона в Manzana
  external_id  text not null default '',                     -- ID импорта списка / SID кампании / ID правила купона
  guid         uuid,                                         -- GUID инфокупона (не клиента); у списка и PUSH пусто
  rows         integer,                                      -- список: загружено контактов (LoadedMembersCamp)
  status       text not null check (status in ('done', 'error')),
  detail       text not null default '',
  at           timestamptz not null default now(),
  primary key (id_promo, start_date, kind, item)
);

reset role;

revoke all on lists.manzana from public;
grant select (id_promo, start_date, campaign_name, guids_send) on lists.promo to lists_manzana;
grant select (id_promo, control_group, chunk_no, crm_guid) on lists.offer_chunk to lists_manzana;
grant select, insert, update on lists.manzana to lists_manzana;
grant select on lists.manzana to lists_api;                   -- портал покажет «заведено / не заведено»

insert into lists.migration (filename) values ('002_manzana.sql') on conflict do nothing;

commit;
