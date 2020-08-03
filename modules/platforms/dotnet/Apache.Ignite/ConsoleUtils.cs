/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite
{
    using System;

    /// <summary>
    /// Console utilities.
    /// </summary>
    internal static class ConsoleUtils
    {
        /// <summary>
        /// Prints help.
        /// </summary>
        /// <param name="entryPoint">Application entry point.</param>
        /// <param name="useServices">Indicates whether include service info in help or not.</param>
        public static void PrintHelp(string entryPoint, bool useServices)
        {
            if (useServices)
            {
                Console.WriteLine("Usage: {0} [/install] [/uninstall] [-options]", entryPoint);
                Console.WriteLine("");
                Console.WriteLine("\t/install [-options]    installs Ignite Windows service with provided options.");
                Console.WriteLine("\t/uninstall             uninstalls Ignite Windows service.");
            }
            else
            {
                Console.WriteLine("Usage: {0}  [-options]", entryPoint);
            }
            Console.WriteLine("");
            Console.WriteLine("Options:");
            Console.WriteLine("\t-IgniteHome            path to Ignite installation directory (if not provided IGNITE_HOME environment variable is used).");
            Console.WriteLine("\t-ConfigSectionName     name of the IgniteConfigurationSection in app.config to use.");
            Console.WriteLine("\t-ConfigFileName        path to the app.config file (if not provided Apache.Ignite.exe.config is used).");
            Console.WriteLine("\t-springConfigUrl       path to Spring configuration file.");
            Console.WriteLine("\t-jvmDll                path to JVM library jvm.dll (if not provided JAVA_HOME environment variable is used).");
            Console.WriteLine("\t-jvmClasspath          classpath passed to JVM (enlist additional jar files here).");
            Console.WriteLine("\t-suppressWarnings      whether to print warnings.");
            Console.WriteLine("\t-J<javaOption>         JVM options passed to created JVM.");
            Console.WriteLine("\t-assembly=userLib.dll  additional .NET assemblies to be loaded.");
            Console.WriteLine("\t-jvmInitialMemoryMB    Initial Java heap size, in megabytes. Maps to -Xms Java parameter. Defaults to 512.");
            Console.WriteLine("\t-jvmMaxMemoryMB        Maximum Java heap size, in megabytes. Maps to -Xmx Java parameter. Defaults to 1024.");
            Console.WriteLine("");
            Console.WriteLine("Examples:");
            Console.WriteLine("\t{0} -J-Xms1024m -J-Xmx1024m -springConfigUrl=C:/woer/gg-test/my-test-gg-confignative.xml", entryPoint);
            Console.WriteLine("\t{0} -IgniteHome=c:/apache-ignite -jvmClasspath=libs/myLib1.jar;libs/myLib2.jar", entryPoint);
            Console.WriteLine("\t{0} -assembly=c:/myProject/libs/lib1.dll -assembly=c:/myProject/libs/lib2.dll", entryPoint);
            Console.WriteLine("\t{0} -jvmInitialMemoryMB=1024 -jvmMaxMemoryMB=4096", entryPoint);
            Console.WriteLine("");
            Console.WriteLine("Note:");
            Console.WriteLine("Command line settings have priority over Apache.Ignite.exe.config settings. JVM options and assemblies are concatenated; data from config file comes first, then data from command line.");
        }
    }
}
