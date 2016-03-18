rem Install NuGet package to a 'pkg' folder no matter what version it is

rmdir pkg /S /Q
rmdir tmp1 /S /Q
mkdir tmp1
cd tmp1
nuget install Apache.Ignite
cd ..
move tmp1\Apache.Ignite.* pkg
rmdir tmp1 /S /Q