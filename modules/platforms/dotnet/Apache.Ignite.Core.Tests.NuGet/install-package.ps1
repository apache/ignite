$ver = (gi ..\Apache.Ignite.Core\bin\Release\Apache.Ignite.Core.dll).VersionInfo.ProductVersion

rmdir packages -Force -Recurse

# Replace versions in project files
(Get-Content packages.config) `
    -replace 'id="Apache.Ignite(.*?)" version=".*?"', ('id="Apache.Ignite$1" version="' + $ver + '"') `
    | Out-File packages.config -Encoding utf8

(Get-Content Apache.Ignite.Core.Tests.NuGet.csproj) `
    -replace 'packages\\Apache.Ignite(.*?)\.\d.*?\\', ('packages\Apache.Ignite$1.' + "$ver\") `
    | Out-File Apache.Ignite.Core.Tests.NuGet.csproj  -Encoding utf8
	
# Detect NuGet
$ng = "nuget"
if ((Get-Command $ng -ErrorAction SilentlyContinue) -eq $null) { 
    $ng = ".\nuget.exe"

    if (-not (Test-Path $ng)) {
        echo "Downloading NuGet..."
        (New-Object System.Net.WebClient).DownloadFile("https://dist.nuget.org/win-x86-commandline/v3.3.0/nuget.exe", "nuget.exe");    
    }
}

# restore packages
& $ng restore

# refresh content files
ls packages\*\content | % {copy ($_.FullName + "\*.*") .\ }