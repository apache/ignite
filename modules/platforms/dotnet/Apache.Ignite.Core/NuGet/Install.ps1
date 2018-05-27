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

param($installPath, $toolsPath, $package, $project)

Write-Host "Updating project properties..."

. (Join-Path $toolsPath "PostBuild.ps1")

# Get the current Post Build Event cmd
$currentPostBuildCmd = $project.Properties.Item("PostBuildEvent").Value

# Append our post build command if it's not already there
if (!$currentPostBuildCmd.Contains($IgnitePostBuildCmd)) {
    $project.Properties.Item("PostBuildEvent").Value += $IgnitePostBuildCmd
}

# Save
$project.Save()

# Remove bin\Libs folders with old jars
$project.ConfigurationManager | % { 
    $projPath = $project.Properties.Item("FullPath").Value
    $binDir = ($_.Properties | Where Name -match OutputPath).Value

    $binPath = Join-Path $projPath $binDir
    $libsPath = Join-Path $binPath "libs"

    Remove-Item -Force -Recurse -ErrorAction SilentlyContinue $libsPath
}

Write-Host "Welcome to Apache Ignite.NET!"