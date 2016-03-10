rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

nuget pack ..\Apache.Ignite.Core\Apache.Ignite.Core.csproj -Prop Configuration=Debug -Prop Platform=x64 -OutputDirectory nupkg
nuget pack ..\Apache.Ignite.Linq\Apache.Ignite.Linq.csproj -Prop Configuration=Debug -Prop Platform=x64 -OutputDirectory nupkg

$ver = (Get-ChildItem nupkg\Apache.Ignite.Linq*)[0].Name -replace '\D+([\d.]+)\.\D+','$1'
$nugetSrc = (Get-Location).Path + "\nupkg"

nuget install Apache.Ignite.Linq -Version $ver -OutputDirectory pkg -Source $nugetSrc

move ".\pkg\Apache.Ignite.$ver" ".\pkg\Apache.Ignite"
move ".\pkg\Apache.Ignite.Linq.$ver" ".\pkg\Apache.Ignite.Linq"