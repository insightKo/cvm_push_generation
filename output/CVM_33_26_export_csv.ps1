# CVM неделя 33 - выгрузка списков на отправку в CSV
# Каждый список сохраняется отдельным файлом: <номер>_<название>.csv (одна колонка CRM_GUID, без заголовка).
#
# КАК ЗАПУСТИТЬ (на той же машине, где работаешь в SSMS):
# 1. Скопируй этот файл на ту машину (например в D:\).
# 2. Пуск -> набери PowerShell -> Enter, и в синем окне выполни:
#    powershell -ExecutionPolicy Bypass -File "D:\CVM_33_26_export_csv.ps1"
#    (путь поправь на тот, куда положила файл)
# Аутентификация: по умолчанию Windows (-T). Если заходишь в SSMS под SQL-логином,
# в строке bcp замени -T на: -U твой_логин -P твой_пароль

$Server = "SQL01"                              # имя сервера из окна подключения SSMS
$OutDir = "D:\All documents\CVM неделя 33"   # папка для файлов

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

$lists = @(
    @{ Id = 101346; Name = "101346_Активируй 20 на арбузы и дыни" },
    @{ Id = 101347; Name = "101347_Активируй 20 кешбэка на сезонные овощи" },
    @{ Id = 101358; Name = "101358_Вернем 100 монет за любую покупку в период акции" },
    @{ Id = 101359; Name = "101359_Скидка 20 на туалетную бумагу и бумажные полотенца" }
)

foreach ($l in $lists) {
    $f = Join-Path $OutDir ($l.Name + ".csv")
    $q = "select CRM_GUID from mci_model.dbo.I_PROMO_OFFER (nolock) where ID_PROMO = $($l.Id) and CONTROL_GROUP = 0"
    bcp $q queryout $f -S $Server -T -c -C 65001
    if ($LASTEXITCODE -eq 0) { Write-Host "OK: $f" } else { Write-Host "ОШИБКА: $($l.Name)" -ForegroundColor Red }
}
