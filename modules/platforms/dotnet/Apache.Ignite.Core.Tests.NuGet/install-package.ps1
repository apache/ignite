$ver = (gi ..\Apache.Ignite.Core\bin\$cfg\Apache.Ignite.Core.dll).VersionInfo.ProductVersion

rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

# Replace versions in project files
(Get-Content packages.config) `
    -replace 'id="Apache.Ignite(.*?)" version=".*?"', ('id="Apache.Ignite$1" version="' + $ver + '"') `
    | Out-File packages.config -Encoding utf8

(Get-Content Apache.Ignite.Core.Tests.NuGet.csproj) `
    -replace 'packages\\Apache.Ignite(.*?)\.\d.*?\\', ('packages\Apache.Ignite$1.' + "$ver\") `
    | Out-File Apache.Ignite.Core.Tests.NuGet.csproj  -Encoding utf8

# restore packages
& $ng restore

# refresh content files
ls packages\*\content | % {copy ($_.FullName + "\*.*") .\ }