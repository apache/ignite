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
* JDK 7+
* MAVEN_HOME environment variable or mvn.bat in PATH

.PARAMETER skipJava
Skip Java build.

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

.EXAMPLE
.\build.ps1 -clean  
# Full rebuild of Java, .NET and NuGet packages.

.EXAMPLE
.\build.ps1 -skipJava -skipCodeAnalysis -skipNuGet -configuration Debug -platform x64
# Quick build of .NET code only.

#>

param (
    [switch]$skipJava,
    [switch]$skipNuGet,
    [switch]$skipCodeAnalysis,  
    [switch]$clean,
    [ValidateSet("Any CPU", "x64", "x86")]
    [string]$platform="Any CPU",
    [ValidateSet("Release", "Debug")]
    [string]$configuration="Release",
    [string]$mavenOpts="-U -P-lgpl,-scala,-examples,-test,-benchmarks -Dmaven.javadoc.skip=true"
 )

# 1) Build Java (Maven)
if (!$skipJava) {
    # Detect Ignite root directory
    cd $PSScriptRoot\..

    while (!((Test-Path bin) -and (Test-Path examples) -and ((Test-Path modules) -or (Test-Path platforms))))
    { cd .. }

    echo "Ignite home detected at '$pwd'."

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

    # Run Maven
    echo "Starting Java (Maven) build..."
    
    $mvnTargets = if ($clean)  { "clean package" } else { "package" }
    cmd /c "$mv $mvnTargets -DskipTests $mavenOpts"

    # Check result
    if ($LastExitCode -ne 0) {
        echo "Java (Maven) build failed."; exit -1
    }

    # Copy (relevant) jars
    $libsDir = "$PSScriptRoot\bin\Libs"
    mkdir -Force $libsDir; del -Force $libsDir\*.*
    
    copy -Force target\release-package\libs\*.jar $libsDir
    copy -Force target\release-package\libs\ignite-spring\*.jar $libsDir
    copy -Force target\release-package\libs\ignite-indexing\*.jar $libsDir
    copy -Force target\release-package\libs\licenses\*.jar $libsDir

    # Restore directory
    cd $PSScriptRoot
}
else {
    echo "Java (Maven) build skipped."
}


# 2) Build .NET
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

# Detect NuGet
$ng = "nuget"
if ((Get-Command $ng -ErrorAction SilentlyContinue) -eq $null) { 
    echo "Downloading NuGet..."
    (New-Object System.Net.WebClient).DownloadFile("https://dist.nuget.org/win-x86-commandline/v3.3.0/nuget.exe", "nuget.exe");    
    $ng = ".\nuget.exe"
}

# Restore NuGet packages
echo "Restoring NuGet..."
& $ng restore

# Build
echo "Starting MsBuild..."
$targets = if ($clean) {"Clean;Rebuild"} else {"Build"}
$codeAnalysis = if ($skipCodeAnalysis) {"/p:RunCodeAnalysis=false"} else {""}
& $msbuildExe Apache.Ignite.sln /target:$targets /p:Configuration=$configuration /p:Platform=`"$platform`" $codeAnalysis /p:UseSharedCompilation=false

# Check result
if ($LastExitCode -ne 0) {
    echo ".NET build failed."
    exit -1
}

# Copy binaries
mkdir -Force bin; del -Force bin\*.*

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
    $ver = (gi Apache.Ignite.Core\bin\Release\Apache.Ignite.Core.dll).VersionInfo.ProductVersion

    # Find all nuspec files and run 'nuget pack' either directly, or on corresponding csproj files (if present)
    ls *.nuspec -Recurse  `
        | % { 
            If (Test-Path ([io.path]::ChangeExtension($_.FullName, ".csproj"))){
                [io.path]::ChangeExtension($_.FullName, ".csproj")
            } Else { $_.FullName }
        } | % { 
            & $ng pack $_ -Prop Configuration=Release -Prop Platform=AnyCPU -Version $ver -OutputDirectory $nupkgDir

            # check result
            if ($LastExitCode -ne 0)
            {
                echo "NuGet pack failed."; exit -1
            }
        }

    echo "NuGet packages created in '$pwd\$nupkgDir'."
}