# CVM неделя 35 - выгрузка списков на отправку в CSV
# Каждый список сохраняется отдельным файлом: <номер>_<название>.csv (одна колонка CRM_GUID, без заголовка).
#
# КАК ЗАПУСТИТЬ (на той же машине, где работаешь в SSMS):
# 1. Скопируй этот файл на ту машину (например в D:\).
# 2. Пуск -> набери PowerShell -> Enter, и в синем окне выполни:
#    powershell -ExecutionPolicy Bypass -File "D:\CVM_35_26_export_csv.ps1"
#    (путь поправь на тот, куда положила файл)
# Аутентификация: по умолчанию Windows (-T). Если заходишь в SSMS под SQL-логином,
# в строке bcp замени -T на: -U твой_логин -P твой_пароль

$Server = "SQL01"                              # имя сервера из окна подключения SSMS
$OutDir = "D:\All documents\CVM неделя 35"   # папка для файлов

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

$lists = @(
    @{ Id = 101355; Name = "101355_Активируй 20 на конфеты в коробках" },
    @{ Id = 101356; Name = "101356_Баланс баллов" },
    @{ Id = 101357; Name = "101357_Скидка 20 на средства для ухода за полостью рта" }
)

foreach ($l in $lists) {
    $f = Join-Path $OutDir ($l.Name + ".csv")
    $q = "select CRM_GUID from mci_model.dbo.I_PROMO_OFFER (nolock) where ID_PROMO = $($l.Id) and CONTROL_GROUP = 0"
    bcp $q queryout $f -S $Server -T -c -C 65001
    if ($LASTEXITCODE -eq 0) { Write-Host "OK: $f" } else { Write-Host "ОШИБКА: $($l.Name)" -ForegroundColor Red }
}
