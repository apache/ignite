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
* JDK 8+
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
    [switch]$clean,
    [ValidateSet("Any CPU", "x64", "x86")]
    [string]$platform="Any CPU",
    [ValidateSet("Release", "Debug")]
    [string]$configuration="Release",
    [string]$mavenOpts="-U -P-lgpl,-scala,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true",
	[string]$jarDirs="modules\indexing\target,modules\core\target,modules\spring\target",
    [string]$asmDirs="",
    [string]$nugetPath="",
	[string]$version=""
 )

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
    cmd /c "$mv -N io.takari:maven:wrapper -Dmaven=3.5.2"
    $mv = "mvnw.cmd"

    # Run Maven
    echo "Starting Java (Maven) build..."
    
    $mvnTargets = if ($clean)  { "clean package" } else { "package" }
    cmd /c "$mv $mvnTargets -DskipTests $mavenOpts"

    # Check result
    if ($LastExitCode -ne 0) {
        echo "Java (Maven) build failed."; exit -1
    }
}
else {
    echo "Java (Maven) build skipped."
}

# Copy (relevant) jars
$libsDir = "$PSScriptRoot\bin\libs"
mkdir -Force $libsDir; del -Force $libsDir\*.*

ls $jarDirs.Split(',') *.jar -recurse `
   -include "ignite-core*","ignite-indexing*","ignite-shmem*","ignite-spring*","lucene*","h2*","cache-api*","commons-*","spring*" `
   -exclude "*-sources*","*-javadoc*","*-tests*","*optional*" `
   | % { copy -Force $_ $libsDir }
   
# Restore directory
cd $PSScriptRoot


# 2) Build .NET

# Detect NuGet
$ng = if ($nugetPath) { $nugetPath } else { "nuget" }

if ((Get-Command $ng -ErrorAction SilentlyContinue) -eq $null) { 
	$ng = ".\nuget.exe"

	if (-not (Test-Path $ng)) {
		echo "Downloading NuGet..."
		(New-Object System.Net.WebClient).DownloadFile("https://dist.nuget.org/win-x86-commandline/v3.3.0/nuget.exe", "nuget.exe");    
	}
}

echo "Using NuGet from: $ng"

if (!$skipDotNet) {
	# Detect MSBuild 4.0+
	for ($i=20; $i -ge 4; $i--) {
		$regKey = "HKLM:\software\Microsoft\MSBuild\ToolsVersions\$i.0"
		if (Test-Path $regKey) { break }
	}

	if (!(Test-Path $regKey)) {
		echo "Failed to detect MSBuild path, exiting."
		exit -1
	}

	$msbuildExe = (join-path -path (Get-ItemProperty $regKey)."MSBuildToolsPath" -childpath "msbuild.exe")
	echo "MSBuild detected at '$msbuildExe'."

	# Restore NuGet packages
	echo "Restoring NuGet..."
	& $ng restore Apache.Ignite.sln

	# Build
	$targets = if ($clean) {"Clean;Rebuild"} else {"Build"}
	$codeAnalysis = if ($skipCodeAnalysis) {"/p:RunCodeAnalysis=false"} else {""}
	$msBuildCommand = "`"$msBuildExe`" Apache.Ignite.sln /target:$targets /p:Configuration=$configuration /p:Platform=`"$platform`" $codeAnalysis /p:UseSharedCompilation=false"
	echo "Starting MsBuild: '$msBuildCommand'"
	cmd /c $msBuildCommand   

	# Check result
	if ($LastExitCode -ne 0) {
		echo ".NET build failed."
		exit -1
	}
}

if(!$skipDotNetCore) {
    
    # Build core
    $targetSolution =  ".\Apache.Ignite\Apache.Ignite.DotNetCore.csproj"
    if ($clean) {
        $cleanCommand = "dotnet clean $targetSolution -c $configuration"
        echo "Starting dotnet clean: '$cleanCommand'"
        cmd /c $cleanCommand
    }

    $publishCommand = "dotnet publish $targetSolution -c $configuration"
	echo "Starting dotnet publish: '$publishCommand'"
	cmd /c $publishCommand    

    # Check result
    if ($LastExitCode -ne 0) {
        echo ".NET Core build failed."
        exit -1
    }
}

if ($asmDirs) {
    ls $asmDirs.Split(',') | % `
    {
        $projName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name);

        if ($_.Name.EndsWith(".exe.config")) {
            $projName = [System.IO.Path]::GetFileNameWithoutExtension($projName);
        }

        if ($projName.StartsWith("Apache.Ignite")) {
            $target = "$projName\bin\Release"
            mkdir -Force $target

            xcopy /s /y $_.FullName $target
        }
    }    
}

# Copy binaries
mkdir -Force bin; del -Force -Recurse bin\*.*

ls *.csproj -Recurse | where Name -NotLike "*Examples*" `
                     | where Name -NotLike "*Tests*" `
                     | where Name -NotLike "*Benchmarks*" | % {
    $binDir = if (($configuration -eq "Any CPU") -or ($_.Name -ne "Apache.Ignite.Core.csproj")) `
                {"bin\$configuration"} else {"bin\$platform\$configuration"}
    $dir = join-path (split-path -parent $_) $binDir    
    xcopy /s /y $dir\*.* bin
}


# 3) Pack NuGet
if (!$skipNuGet) {
    # Check parameters
    if (($platform -ne "Any CPU") -or ($configuration -ne "Release")) {
        echo "NuGet can only package 'Release' 'Any CPU' builds; you have specified '$configuration' '$platform'."
        exit -1
    }

    $nupkgDir = "nupkg"
    mkdir -Force $nupkgDir; del -Force $nupkgDir\*.*

    # Detect version
    $ver = if ($version) { $version } else { (gi Apache.Ignite.Core\bin\Release\Apache.Ignite.Core.dll).VersionInfo.ProductVersion }

    # Find all nuspec files and run 'nuget pack' either directly, or on corresponding csproj files (if present)
    ls *.nuspec -Recurse  `
        | % { 
            & $ng pack $_ -Prop Configuration=Release -Prop Platform=AnyCPU -Version $ver -OutputDirectory $nupkgDir

            # check result
            if ($LastExitCode -ne 0)
            {
                echo "NuGet pack failed."; exit -1
            }
        }

    echo "NuGet packages created in '$pwd\$nupkgDir'."
}
