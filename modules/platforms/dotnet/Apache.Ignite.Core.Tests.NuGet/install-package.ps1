$ng = (Get-Item .).FullName + '\nuget.exe'

if (!(Test-Path $ng)) {
    $ng = 'nuget'
}

$cfg = 'Release'
$ver = (gi ..\Apache.Ignite.Core\bin\$cfg\Apache.Ignite.Core.dll).VersionInfo.ProductVersion

rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

# Find all nuspec files and run 'nuget pack' on corresponding csproj files
ls ..\*.nuspec -Recurse  `
    | % { If (Test-Path [io.path]::ChangeExtension($_.FullName, ".csproj")){[io.path]::ChangeExtension($_.FullName, ".csproj")} Else {$_.FullName}  } `
    | % { & $ng pack $_ -Prop Configuration=$cfg -Version $ver -Prop Platform=AnyCPU -OutputDirectory nupkg }

# Replace versions in project files
(Get-Content packages.config) `
    -replace 'id="Apache.Ignite(.*?)" version=".*?"', ('id="Apache.Ignite$1" version="' + $ver + '"') `
    | Out-File packages.config -Encoding utf8

(Get-Content Apache.Ignite.Core.Tests.NuGet.csproj) `
    -replace 'packages\\Apache.Ignite(.*?)\.\d.*?\\', ('packages\Apache.Ignite$1.' + "$ver\") `
    | Out-File Apache.Ignite.Core.Tests.NuGet.csproj  -Encoding utf8