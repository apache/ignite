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
    If (Test-Path 'Software\JavaSoft\Java Runtime Environment') {
        
    }
}