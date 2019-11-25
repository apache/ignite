<#
Copyright 2019 GridGain Systems, Inc. and Contributors.

Licensed under the GridGain Community Edition License (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#>

 <#

.SYNOPSIS
Verifies release NuGet packages.

.DESCRIPTION
Creates a project with .NET Core CLI, installs all packages, runs simple code to start Ignite node, verifies exit code.

Requirements:
* .NET Core 2.0+
* JDK 8+

.PARAMETER packageDir
Directory with nupkg files to verify, defaults to ..\nupkg.

#>

param (
    [string]$packageDir="..\nupkg"
)


# Find NuGet packages (*.nupkg)
$dir = If ([System.IO.Path]::IsPathRooted($packageDir)) { $packageDir } Else { Join-Path $PSScriptRoot $packageDir }
if (!(Test-Path $dir)) {
    throw "Path does not exist: '$packageDir' (resolved to '$dir')"
}

$packages = ls $dir *.nupkg
if ($packages.Length -eq 0) {
    throw "nupkg files not found in '$dir'"
}


echo "Verifying $($packages.Length) packages from '$dir'..."  


# Create test dir
$testDir = Join-Path $PSScriptRoot "test-proj"
mkdir -Force $testDir
del -Force $testDir\*.*
cd $testDir


# Create project, install packages, copy test code, run
dotnet new console
if ($LastExitCode -ne 0) {
    throw "Failed to create .NET Core project"
}


$packages | % { 
    $packageId = $_.Name -replace '(.*?)\.\d\.\d\.\d\.nupkg', '$1'
    dotnet add package $packageId -s $dir

    if ($LastExitCode -ne 0) {
        throw "Failed to install package $packageId"
    }
}


$programFile = Join-Path $PSScriptRoot "Program.cs"
Copy-Item $programFile $testDir -force


dotnet run
if ($LastExitCode -ne 0) {
    throw "Failed to run the test program"
}
