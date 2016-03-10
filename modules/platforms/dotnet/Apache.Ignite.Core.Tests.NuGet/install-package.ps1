rmdir nupkg -Force -Recurse
rmdir pkg -Force -Recurse

mkdir nupkg
mkdir pkg

& nuget pack ..\Apache.Ignite.Core\Apache.Ignite.Core.csproj -Prop Configuration=Release -Prop Platform=x64 -OutputDirectory nupkg
& nuget pack ..\Apache.Ignite.Linq\Apache.Ignite.Linq.csproj -Prop Configuration=Release -Prop Platform=x64 -OutputDirectory nupkg

$ver = (Get-ChildItem nupkg\Apache.Ignite.Linq*)[0].Name -replace '\D+([\d.]+)\.\D+','$1'

# Replace versions in project files