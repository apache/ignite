#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# Apache Ignite.NET build script

# Requirements:
# * PowerShell 3
# * NuGet in PATH
# * Apache Maven in PATH
# * JDK 7+

# Examples:
# 'powershell -file build.ps1 -clean': Full rebuild of Java, .NET and NuGet packages.
# 'powershell -file build.ps1 -skipJava -skipCodeAnalysis -skipNuGet -configuration Debug -platform x64': Quick build of .NET code only.

param (
    [switch]$skipJava,
    [switch]$skipNuGet,
    [switch]$skipCodeAnalysis,  
    [switch]$clean,
    [string]$platform="Any CPU",
    [string]$configuration="Release"
 )

# 1) Build Java (Maven)
if (!$skipJava)
{
    # change to home directory
    cd $PSScriptRoot\..

    while (!((Test-Path bin) -and (Test-Path examples) -and ((Test-Path modules) -or (Test-Path platforms))))
    { cd .. }

    echo "Ignite home detected at '$pwd'."

    # run Maven
    if ($clean)
    {
        echo "Executing Maven cleanup..."
        cmd /c "mvn clean"        
    }

    echo "Starting Java (Maven) build..."
    cmd /c "mvn package -DskipTests -U -P-lgpl,-scala,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true"

    # restore directory
    cd $PSScriptRoot
}
else
{
    echo "Java (Maven) build skipped."
}

# 2) Build .NET

# Download and install Invoke-MsBuild module
if (!(Test-Path Invoke-MsBuild))
{
    echo "Installing MsBuild module..."
    Save-Module -Name Invoke-MsBuild -Path .
}

Import-Module .\Invoke-MsBuild

# Build
echo "Starting MsBuild..."
$targets = if ($clean) {"Clean;Rebuild"} else {"Build"}
$codeAnalysis = if ($skipCodeAnalysis) {"/p:RunCodeAnalysis=false"} else {""}
Invoke-MsBuild Apache.Ignite.sln -Params "/target:$targets /p:Configuration=$configuration /p:Platform=`"$platform`" $codeAnalysis /p:UseSharedCompilation=false" -ShowBuildOutputInCurrentWindow

# 3) Pack NuGet
if (!$skipNuGet)
{
    # Check parameters
    if (($platform -ne "Any CPU") -or ($configuration -ne "Release"))
    {
        echo "NuGet can only package 'Release' 'Any CPU' builds; you have specified '$configuration' '$platform'."
        exit -1
    }

    # Prepare paths
    $ng = (Get-Item .).FullName + '\nuget.exe'
    if (!(Test-Path $ng)) { $ng = 'nuget' }

    rmdir nupkg -Force -Recurse
    mkdir nupkg

    # Detect version
    $ver = (gi Apache.Ignite.Core\bin\Release\Apache.Ignite.Core.dll).VersionInfo.ProductVersion

    # Find all nuspec files and run 'nuget pack' either directly, or on corresponding csproj files (if present).
    ls *.nuspec -Recurse  `
        | % { If (Test-Path ([io.path]::ChangeExtension($_.FullName, ".csproj"))){[io.path]::ChangeExtension($_.FullName, ".csproj")} Else {$_.FullName}  } `
        | % { & $ng pack $_ -Prop Configuration=Release -Prop Platform=AnyCPU -Version $ver -OutputDirectory nupkg }

    echo "NuGet packages created in $pwd\nupkg"
}