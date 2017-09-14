# TODO: Integrate this script into build.ps1 with -asmDirs

# 1. Copy .NET binaries to corresponding bin\Release folders
$bins = "c:\w\ignite-2.2\ignite.dotnet.bin"

ls $bins | % `
{
    $projName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name);

    if ($projName.StartsWith("Apache.Ignite")) {
        $target = "$projName\bin\Release"
        mkdir -Force $target

        xcopy /s /y $_.FullName $target
    }
}

# 2. Build packages
# .\build.bat -skipJava -skipDotNet -jarDirs c:\w\ignite-2.2\apache-ignite-fabric-2.1.0-bin\apache-ignite-fabric-2.1.0-bin\libs