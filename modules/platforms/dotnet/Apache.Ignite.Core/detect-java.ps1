$x64 = [System.Environment]::Is64BitOperatingSystem

# Fisrt, check if JAVA_HOME is set
If (Test-Path Env:\JAVA_HOME) {
    If ($x64) {
        $env:JAVA_HOME64 = $env:JAVA_HOME
    }
    Else {
        $env:JAVA_HOME32 = $env:JAVA_HOME
    }
}

# Next, check registry

if ($x64) {
    If (Test-Path 'HKLM:\Software\JavaSoft\Java Runtime Environment') {
        $key = Get-Item 'HKLM:\Software\JavaSoft\Java Runtime Environment'
        $subKeys = $key.GetSubKeyNames()
        $curVer = $key.GetValue("CurrentVersion")

        If ($curVer -eq $null -or !$subKeys.Contains($curVer)) {
            $curVer = $subKeys[0]
        }
            
        $env:JAVA_HOME64 = $key.OpenSubKey($curVer).GetValue("JavaHome")
    }
}

echo "JAVA_HOME64: $env:JAVA_HOME64"
echo "JAVA_HOME32: $env:JAVA_HOME32"