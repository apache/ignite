param([string]$dir = ".\", [string]$msbuild = "C:\Program Files (x86)\MSBuild\14.0\Bin\amd64\MSBuild.exe", [string]$configuration="Debug")

$x64 = [System.Environment]::Is64BitOperatingSystem

# Fisrt, check if JAVA_HOME is set
if (Test-Path Env:\JAVA_HOME) {
    if ($x64) {
        $env:JAVA_HOME64 = $env:JAVA_HOME
    }
    else {
        $env:JAVA_HOME32 = $env:JAVA_HOME
    }
}

# Next, check registry
Function GetJavaHome([string]$path) {
    if (Test-Path $path) {
        $key = Get-Item $path
        $subKeys = $key.GetSubKeyNames()
        $curVer = $key.GetValue("CurrentVersion")

        if ($subKeys.Length -eq 0) {
            return $null
        }

        If ($curVer -eq $null -or !$subKeys.Contains($curVer)) {
            $curVer = $subKeys[0]
        }
            
        return $key.OpenSubKey($curVer).GetValue("JavaHome")
    }
}

if ($x64) {
    $env:JAVA_HOME64 = GetJavaHome 'HKLM:\Software\JavaSoft\Java Development Kit'
    $env:JAVA_HOME32 = GetJavaHome 'HKLM:\Software\Wow6432Node\JavaSoft\Java Development Kit'
} else {
    $env:JAVA_HOME32 = GetJavaHome 'HKLM:\Software\JavaSoft\Java Development Kit'
}

echo "JAVA_HOME64: $env:JAVA_HOME64"
echo "JAVA_HOME32: $env:JAVA_HOME32"

$env:JAVA_HOME = $null

# build common project
if ($env:JAVA_HOME64) {
    $env:JAVA_HOME = $env:JAVA_HOME64

    & $msbuild "$dir..\..\cpp\common\project\vs\common.vcxproj" /p:Platform=x64 /p:Configuration=$Configuration /t:Rebuild
}

if ($env:JAVA_HOME32) {
    $env:JAVA_HOME = $env:JAVA_HOME32

    & $msbuild "$dir..\..\cpp\common\project\vs\common.vcxproj" /p:Platform=Win32 /p:Configuration=$Configuration /t:Rebuild
}