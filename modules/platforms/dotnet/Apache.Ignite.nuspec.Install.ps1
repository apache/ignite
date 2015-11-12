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

Write-Host "Welcome to Apache Ignite .NET!"
    
SetProperties $project.ProjectItems.Item["Content"].Item["default-config.xml"]

ForEach ($item in $project.ProjectItems.Item["Libs"].Items) 
    SetProperties $item

Function SetProperties
{
	param ($projItem)
    
    Write-Host $projItem.Name 

    $projItem.Properties.Item("BuildAction") = 2  # content
    $projItem.Properties.Item("CopyToOutputDirectory").Value = 2  # copy if newer
}