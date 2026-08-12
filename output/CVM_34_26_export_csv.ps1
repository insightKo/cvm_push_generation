# CVM неделя 34 - выгрузка списков на отправку в CSV
# Каждый список сохраняется отдельным файлом: <номер>_<название>.csv (одна колонка CRM_GUID, без заголовка).
#
# КАК ЗАПУСТИТЬ (на той же машине, где работаешь в SSMS):
# 1. Скопируй этот файл на ту машину (например в D:\).
# 2. Пуск -> набери PowerShell -> Enter, и в синем окне выполни:
#    powershell -ExecutionPolicy Bypass -File "D:\CVM_34_26_export_csv.ps1"
#    (путь поправь на тот, куда положила файл)
# Аутентификация: по умолчанию Windows (-T). Если заходишь в SSMS под SQL-логином,
# в строке bcp замени -T на: -U твой_логин -P твой_пароль

$Server = "SQL01"                              # имя сервера из окна подключения SSMS
$OutDir = "D:\All documents\CVM неделя 34"   # папка для файлов

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

$lists = @(
    @{ Id = 101349; Name = "101349_Активируй 20 на яблоки и груши" },
    @{ Id = 101350; Name = "101350_Активируй 100р на чек от 1000р" },
    @{ Id = 101352; Name = "101352_Активируй 200р на чек от 2000р" },
    @{ Id = 101353; Name = "101353_Активируй 300р на чек от 3000р" },
    @{ Id = 101351; Name = "101351_Активируй 150р на чек от 1500р" },
    @{ Id = 101354; Name = "101354_Скидка 20 на шампуни и уход за волосами" }
)

foreach ($l in $lists) {
    $f = Join-Path $OutDir ($l.Name + ".csv")
    $q = "select CRM_GUID from mci_model.dbo.I_PROMO_OFFER (nolock) where ID_PROMO = $($l.Id) and CONTROL_GROUP = 0"
    bcp $q queryout $f -S $Server -T -c -C 65001
    if ($LASTEXITCODE -eq 0) { Write-Host "OK: $f" } else { Write-Host "ОШИБКА: $($l.Name)" -ForegroundColor Red }
}
