::  Licensed under the Apache License, Version 2.0 (the "License");
::  you may not use this file except in compliance with the License.
::  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
::  Unless required by applicable law or agreed to in writing, software
::  distributed under the License is distributed on an "AS IS" BASIS,
::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::  See the License for the specific language governing permissions and
::  limitations under the License.

:: Apache Ignite.NET build script

:: Requirements:
:: * PowerShell 3
:: * NuGet in PATH
:: * Apache Maven in PATH
:: * JDK 7+

:: Examples:
:: 'build -clean': Full rebuild of Java, .NET and NuGet packages.
:: 'build -skipJava -skipCodeAnalysis -skipNuGet -configuration Debug -platform x64': Quick build of .NET code only.

powershell -executionpolicy remotesigned -file build.ps1 %*