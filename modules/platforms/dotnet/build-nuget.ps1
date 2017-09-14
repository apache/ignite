# TODO: Integrate this script into build.ps1 with -asmDirs

# 1. Copy .NET binaries to corresponding bin\Release folders
$bins = "c:\w\ignite-2.2\ignite.dotnet.bin"

ls $bins | % `
{
    if (($_.Extension -eq ".exe") -or ($_.Extension -eq ".dll")) {
        $projName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name);

        if ($projName.StartsWith("Apache.Ignite")) {
            $target = "$projName\bin\Release"
            mkdir -Force $target

            xcopy /s /y $_.FullName $target
        }
    }
}