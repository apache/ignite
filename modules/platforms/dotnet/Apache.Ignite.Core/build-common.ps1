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
    $env:JAVA_HOME64 = GetJavaHome 'HKLM:\Software\JavaSoft\Java Runtime Environment'
    $env:JAVA_HOME32 = GetJavaHome 'HKLM:\Software\Wow6432Node\JavaSoft\Java Runtime Environment'
} else {
    $env:JAVA_HOME32 = GetJavaHome 'HKLM:\Software\JavaSoft\Java Runtime Environment'
}

echo "JAVA_HOME64: $env:JAVA_HOME64"
echo "JAVA_HOME32: $env:JAVA_HOME32"