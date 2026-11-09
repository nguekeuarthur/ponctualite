# Lance la CLI Fabric sur le tenant perso, avec le profil Azure isolé partagé avec le projet « Le Système ».
# Exemple : .\ponctualite\fab.ps1 ls "Ponctualite.Workspace"
$base = Join-Path $PSScriptRoot '..\le-systeme'
$env:AZURE_CONFIG_DIR = Join-Path $base '.az-perso'
& (Join-Path $base '.venv-fabric\Scripts\fab.exe') @args
