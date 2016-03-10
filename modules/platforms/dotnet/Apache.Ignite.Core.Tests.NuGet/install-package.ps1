rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

nuget pack ..\Apache.Ignite.Core\Apache.Ignite.Core.csproj -Prop Configuration=Debug -Prop Platform=x64
nuget pack ..\Apache.Ignite.Linq\Apache.Ignite.Linq.csproj -Prop Configuration=Debug -Prop Platform=x64

copy ..\Apache.Ignite.Core\*.nupkg nupkg
copy ..\Apache.Ignite.Linq\*.nupkg nupkg

$ver = (Get-ChildItem nupkg\Apache.Ignite.Linq*)[0].Name -replace '\D+([\d.]+)\.\D+','$1'

nuget install Apache.Ignite.Linq -Version $ver -OutputDirectory pkg

move ".\pkg\Apache.Ignite.$ver" ".\pkg\Apache.Ignite"
move ".\pkg\Apache.Ignite.Linq.$ver" ".\pkg\Apache.Ignite.Linq"