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

ForEach ($item in $project.ProjectItems) 
{ 
    # Copy Always = 1
    # Copy if Newer = 2  
    # $item.Properties.Item("CopyToOutputDirectory").Value = 2
    Write-Host $item.Name 

    #$buildAction = $item.Properties.Item("BuildAction")
    #$buildAction.Value = 2
}
