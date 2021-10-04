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
Apache Ignite.NET build script.

.DESCRIPTION
Builds all parts of Apache Ignite.NET: Java, .NET, NuGet. Copies results to 'bin' and 'nupkg' folders.

Requirements:
* PowerShell 3
* JDK 8 or JDK 11
* MAVEN_HOME environment variable or mvn.bat in PATH

.PARAMETER skipJava
Skip Java build.

.PARAMETER skipDotNet
Skip .NET build.

.PARAMETER skipDotNetCore
Skip .NET Core build.

.PARAMETER skipNuGet
Skip NuGet packaging.

.PARAMETER skipCodeAnalysis
Skip code analysis.

.PARAMETER skipExamples
Skip examples build.

.PARAMETER clean
Perform a clean rebuild.

.PARAMETER platform
Build platform ("Any CPU", "x86", "x64").

.PARAMETER configuration
Build configuration ("Release", "Debug").

.PARAMETER mavenOpts
Custom Maven options, default is "-U -P-lgpl,-scala,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true".

.PARAMETER jarDirs
Java jar files source folders, default is "modules\indexing\target,modules\core\target,modules\spring\target"

.PARAMETER asmDirs
.NET assembly directories to copy pre-build binaries from. Default is "", which means no copy.

.PARAMETER nugetPath
Path to nuget.exe.

.PARAMETER version
NuGet version override (normally inferred from assembly version).

.EXAMPLE
.\build.ps1 -clean
# Full rebuild of Java, .NET and NuGet packages.

.EXAMPLE
.\build.ps1 -skipJava -skipCodeAnalysis -skipNuGet -configuration Debug -platform x64
# Quick build of .NET code only.

#>

param (
    [switch]$skipJava,
	[switch]$skipDotNet,
    [switch]$skipDotNetCore,
    [switch]$skipNuGet,
    [switch]$skipCodeAnalysis,
    [switch]$skipExamples,
    [switch]$clean,
    [ValidateSet("Any CPU", "x64", "x86")]
    [string]$platform="Any CPU",
    [ValidateSet("Release", "Debug")]
    [string]$configuration="Release",
    [string]$mavenOpts="-U -P-lgpl,-scala,-all-scala,-spark-2.4,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true",
	[string]$jarDirs="modules\indexing\target,modules\core\target,modules\spring\target",
    [string]$asmDirs="",
    [string]$nugetPath="",
	[string]$version=""
 )

# 0) Functions
function Make-Dir([string]$dirPath) {
    New-Item -Path $dirPath -ItemType "directory" -Force
    Remove-Item -Force -Recurse $dirPath\*.*
}

function Exec([string]$command) {
    try {
        iex "& $command"
    } catch {
        echo "Command failed: $command"
        throw
        exit -1
    }
}

# 1) Build Java (Maven)
# Detect Ignite root directory
cd $PSScriptRoot\..

while (!((Test-Path bin) -and (Test-Path examples) -and ((Test-Path modules) -or (Test-Path platforms)))) {
	cd ..
	if ((Get-Location).Drive.Root -eq (Get-Location).Path) {
		break
	}
}

echo "Ignite home detected at '$pwd'."

if (!$skipJava) {
    # Detect Maven
    $mv = "mvn"
    if ((Get-Command $mv -ErrorAction SilentlyContinue) -eq $null) {
        $mvHome = ($env:MAVEN_HOME, $env:M2_HOME, $env:M3_HOME, $env:MVN_HOME -ne $null)[0]

        if ($mvHome -eq $null) {
            echo "Maven not found. Make sure to update PATH variable or set MAVEN_HOME, M2_HOME, M3_HOME, or MVN_HOME."
            exit -1
        }

        $mv = "`"" + (join-path $mvHome "bin\mvn.bat") + "`""
        echo "Maven detected at $mv."
    }

    # Install Maven Wrapper
    Exec "$mv --% -N io.takari:maven:wrapper -Dmaven=3.5.2"
    $mv = If ($IsLinux) { "./mvnw" } else { ".\mvnw.cmd" }

    # Run Maven
    echo "Starting Java (Maven) build..."

    $mvnTargets = if ($clean)  { "clean package" } else { "package" }
    Exec "$mv --% $mvnTargets -DskipTests $mavenOpts"
}
else {
    echo "Java (Maven) build skipped."
}

# Copy (relevant) jars
$libsDir = "$PSScriptRoot\bin\libs"
Make-Dir($libsDir)

Get-ChildItem $jarDirs.Split(',') *.jar -recurse `
   -include "ignite-core*","ignite-indexing*","ignite-shmem*","ignite-spring*","lucene*","h2*","cache-api*","commons-*","spring*" `
   -exclude "*-sources*","*-javadoc*","*-tests*" `
   | ? { $_.FullName -inotmatch '[\\/]optional[\\/]' } `
   | % { Copy-Item -Force $_ $libsDir }

# Restore directory
cd $PSScriptRoot


# 2) Build .NET
if (!$skipDotNet) {
    $targetSolution =  ".\Apache.Ignite.sln"
    if ($clean) {
        $cleanCommand = "dotnet clean $targetSolution -c $configuration"
        echo "Starting dotnet clean: '$cleanCommand'"
        Exec $cleanCommand
    }

    $buildCommand = "dotnet build $targetSolution -c $configuration"
    echo "Starting dotnet build: '$buildCommand'"
    Exec $buildCommand
}

if(!$skipDotNetCore) {
    $targetSolution =  ".\Apache.Ignite\Apache.Ignite.DotNetCore.csproj"
    if ($clean) {
        $cleanCommand = "dotnet clean $targetSolution -c $configuration"
        echo "Starting dotnet clean: '$cleanCommand'"
        Exec $cleanCommand
    }

    $publishCommand = "dotnet publish $targetSolution -c $configuration"
	echo "Starting dotnet publish: '$publishCommand'"
	Exec $publishCommand
}

if ($asmDirs) {
    Get-ChildItem $asmDirs.Split(',') | % `
    {
        $projName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)

        if ($_.Name.EndsWith(".exe.config")) {
            $projName = [System.IO.Path]::GetFileNameWithoutExtension($projName)
        }

        if ($projName.StartsWith("Apache.Ignite")) {
            $target = [IO.Path]::Combine($projName, "bin", $configuration)
            New-Item -Path $target -ItemType "directory" -Force

            echo "Copying '$_' to '$target'"
            Copy-Item -Force $_.FullName $target
        }
    }
}

# Copy binaries
Make-Dir("bin")

Get-ChildItem *.csproj -Recurse | where Name -NotLike "*Examples*" `
                     | where Name -NotLike "*Tests*" `
                     | where Name -NotLike "*DotNetCore*" `
                     | where Name -NotLike "*Benchmark*" | % {
    $projDir = split-path -parent $_.FullName
    $dir = [IO.Path]::Combine($projDir, "bin", $configuration)

    if ([IO.Directory]::Exists($dir)) {
        echo "Copying files to bin from '$dir'"

        $dirAll = [IO.Path]::Combine($dir, "*")
        Copy-Item -Force -Recurse $dirAll bin
    }
}


# 3) Pack NuGet
if (!$skipNuGet) {
    # Check parameters
    if (($platform -ne "Any CPU") -or ($configuration -ne "Release")) {
        echo "NuGet can only package 'Release' 'Any CPU' builds; you have specified '$configuration' '$platform'."
        exit -1
    }

    $nupkgDir = "nupkg"
    Make-Dir($nupkgDir)

    # Detect version
    $ver = if ($version) { $version } else { (gi Apache.Ignite.Core\bin\Release\Apache.Ignite.Core.dll).VersionInfo.ProductVersion }

    # Find all nuspec files and run 'nuget pack' either directly, or on corresponding csproj files (if present)
    Get-ChildItem *.nuspec -Recurse  `
        | % {
            Exec "$ng pack $_ -Prop Configuration=Release -Prop Platform=AnyCPU -Version $ver -OutputDirectory $nupkgDir"
        }

    echo "NuGet packages created in '$pwd\$nupkgDir'."
    
    # Examples template
    if (!$skipExamples) {
        # Copy csproj to current dir temporarily: dotnet-new templates can't be packed with parent dir content.
        Copy-Item .\templates\public\Apache.Ignite.Examples\Apache.Ignite.Examples.csproj $pwd

        Exec "dotnet pack Apache.Ignite.Examples.csproj --output $nupkgDir -p:PackageVersion=$ver"

        Remove-Item Apache.Ignite.Examples.csproj

        echo "Examples template NuGet package created in '$pwd\$nupkgDir'."
    }
}

# 4) Build Examples
if ((!$skipDotNetCore) -and (!$skipExamples)) {
    Exec "dotnet build .\examples\Apache.Ignite.Examples.sln"
}

