<#

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

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

.EXAMPLE
./verify-nuget.ps1 -packageDir /foo/packages

#>

param (
    [string]$packageDir="..\nupkg"
)


# Find NuGet packages (*.nupkg)
$dir = If ([System.IO.Path]::IsPathRooted($packageDir)) { $packageDir } Else { Join-Path $PSScriptRoot $packageDir }
if (!(Test-Path $dir)) {
    throw "Path does not exist: '$packageDir' (resolved to '$dir')"
}

$packages = Get-ChildItem $dir *.nupkg
if ($packages.Length -eq 0) {
    throw "nupkg files not found in '$dir'"
}


echo "Verifying $($packages.Length) packages from '$dir'..."


# Clear package cache
dotnet nuget locals all --clear


# Create test dir
$testDir = Join-Path $PSScriptRoot "test-proj"
New-Item -Path $testDir -ItemType "directory" -Force
Remove-Item -Force $testDir\*.*
cd $testDir

# Create project, install packages, copy test code, run
dotnet new console
if ($LastExitCode -ne 0) {
    throw "Failed to create .NET Core project"
}


$packages | % {
    $packageId = $_.Name -replace '(.*?)\.\d+\.\d+\.\d+\.nupkg', '$1'
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
