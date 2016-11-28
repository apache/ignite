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
# Requires PowerShell 3
# TODO: Describe arguments

param (
    [switch]$skipJava,
    [switch]$skipNuGet,
    [switch]$skipDocs,
    [string]$platform="Any CPU",
    [string]$configuration="Release"
 )

# 1) Build Java (Maven)
if (!$skipJava)
{
    echo "Starting Java (Maven) build..."

    # change to home directory
    cd $PSScriptRoot\..

    while (!((Test-Path bin) -and (Test-Path examples) -and ((Test-Path modules) -or (Test-Path platforms))))
    { cd .. }

    echo "Ignite home detected at $pwd"

    # run Maven
    cmd /c "mvn clean package -DskipTests -U -P-lgpl,-scala,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true"

    # restore directory
    cd $PSScriptRoot
}
else
{
    echo "Java (Maven) build skipped."
}

# 2) Build .NET

# Download and install Invoke-MsBuild module
echo "Installing MsBuild module..."
Save-Module -Name Invoke-MsBuild -Path .
Import-Module .\Invoke-MsBuild

# Build
echo "Starting MsBuild..."
Invoke-MsBuild Apache.Ignite.sln -Params "/p:Configuration=$configuration /p:Platform=$platform" -ShowBuildOutputInCurrentWindow

# Remove module dir
Remove-Item -Force -Recurse -ErrorAction SilentlyContinue "Invoke-MsBuild"

# TODO:
# build java (skippable)
# build AnyCPU binaries (debug/release switchable)
# pack NuGet (skippable)
# doxygen (?)
# copy results to a folder