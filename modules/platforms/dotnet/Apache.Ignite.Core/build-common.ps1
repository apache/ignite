param([string]$configuration="Debug", [string]$msbuildexe = "MSBuild.exe")

$x64 = [System.Environment]::Is64BitOperatingSystem
$jdkRegKey = 'Software\JavaSoft\Java Development Kit'

# Fisrt, check if JAVA_HOME env vars are set
if (Test-Path Env:\JAVA_HOME) {
    if ($x64 -and !$env:JAVA_HOME64) {
        $env:JAVA_HOME64 = $env:JAVA_HOME
    }
    elseif (!$env:JAVA_HOME32) {
        $env:JAVA_HOME32 = $env:JAVA_HOME
    }
}

# Next, check registry
Function GetJavaHome([string]$path, [Microsoft.Win32.RegistryView] $mode) {
    $key = [Microsoft.Win32.RegistryKey]::OpenBaseKey([Microsoft.Win32.RegistryHive]::LocalMachine, $mode).OpenSubKey($path)

    if ($key -eq $null) {
        return $null
    }

    $subKeys = $key.GetSubKeyNames()
    $curVer = $key.GetValue("CurrentVersion")

    if ($subKeys.Length -eq 0) {
        return $null
    }

    if ($curVer -eq $null -or !$subKeys.Contains($curVer)) {
        $curVer = $subKeys[0]
    }
            
    return $key.OpenSubKey($curVer).GetValue("JavaHome")
}

# do not read registry if env vars are already set
if (!$env:JAVA_HOME32) {
    $env:JAVA_HOME32 = GetJavaHome $jdkRegKey Registry32
}


if ($x64 -and !$env:JAVA_HOME64) {
    $env:JAVA_HOME64 = GetJavaHome $jdkRegKey Registry64
}

echo "JAVA_HOME64: $env:JAVA_HOME64"
echo "JAVA_HOME32: $env:JAVA_HOME32"
echo "msbuildexe: $msbuildexe"
echo "x64: $x64"

# build common project
if ($env:JAVA_HOME64) {
    $env:JAVA_HOME = $env:JAVA_HOME64

    & $msbuildexe "..\..\cpp\common\project\vs\common.vcxproj" /p:Platform=x64 /p:Configuration=$Configuration /t:Rebuild
}

if ($env:JAVA_HOME32) {
    $env:JAVA_HOME = $env:JAVA_HOME32

    & $msbuildexe "..\..\cpp\common\project\vs\common.vcxproj" /p:Platform=Win32 /p:Configuration=$Configuration /t:Rebuild
}

# build jni project
if ($env:JAVA_HOME64) {
    $env:JAVA_HOME = $env:JAVA_HOME64

    & $msbuildexe "..\..\cpp\jni\project\vs\jni.vcxproj" /p:Platform=x64 /p:Configuration=$Configuration /t:Rebuild
}

if ($env:JAVA_HOME32) {
    $env:JAVA_HOME = $env:JAVA_HOME32

    & $msbuildexe "..\..\cpp\jni\project\vs\jni.vcxproj" /p:Platform=Win32 /p:Configuration=$Configuration /t:Rebuild
}