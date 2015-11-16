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

Function SetProperties
{
    param ($projItem)
    
    Write-Host $projItem.Name 

    $projItem.Properties.Item("CopyToOutputDirectory").Value = 2  # copy if newer
}

# Copy default config to output dir for user conveniece
SetProperties $project.ProjectItems.Item("Config").ProjectItems.Item("default-config.xml")

# ForEach ($item in $project.ProjectItems.Item("Libs").ProjectItems) 
# {
#    SetProperties $item
# }

. (Join-Path $toolsPath "GetSqlCEPostBuildCmd.ps1")

# Get the current Post Build Event cmd
$currentPostBuildCmd = $project.Properties.Item("PostBuildEvent").Value

# Append our post build command if it's not already there
if (!$currentPostBuildCmd.Contains($IgnitePostBuildCmd)) {
    $project.Properties.Item("PostBuildEvent").Value += $IgnitePostBuildCmd
}

Write-Host "Welcome to Apache Ignite.NET!"