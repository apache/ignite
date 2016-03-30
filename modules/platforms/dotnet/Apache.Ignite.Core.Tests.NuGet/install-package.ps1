$ng = (Get-Item .).FullName + '\nuget.exe'

if (!(Test-Path $ng)) {
    $ng = 'nuget'
}

rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

& $ng pack ..\Apache.Ignite.Core\Apache.Ignite.Core.csproj -Prop Configuration=Release -OutputDirectory nupkg
& $ng pack ..\Apache.Ignite.Linq\Apache.Ignite.Linq.csproj -Prop Configuration=Release -OutputDirectory nupkg

$ver = (Get-ChildItem nupkg\Apache.Ignite.Linq*)[0].Name -replace '\D+([\d.]+)\.\D+','$1'

# Replace versions in project files
(Get-Content packages.config) `
    -replace 'id="Apache.Ignite(.*?)" version=".*?"', ('id="Apache.Ignite$1" version="' + $ver + '"') `
    | Out-File packages.config -Encoding utf8

(Get-Content Apache.Ignite.Core.Tests.NuGet.csproj) `
    -replace '<HintPath>packages\\Apache.Ignite(.*?)\.[\d.]+\\', ('<HintPath>packages\Apache.Ignite$1.' + "$ver\") `
    | Out-File Apache.Ignite.Core.Tests.NuGet.csproj  -Encoding utf8