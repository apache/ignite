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
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.ServiceProcess;
    using Apache.Ignite.Config;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Service;

    /// <summary>
    /// Runner class.
    /// </summary>
    public class IgniteRunner
    {
        /** Help commands. */
        private static readonly IList<string> Help = new List<string> { "/help", "-help", "--help" };

        /** Argument meaning that this is service call. */
        internal static readonly string Svc = "/service";

        /** Service install command. */
        internal static readonly string SvcInstall = "/install";

        /** Service uninstall command. */
        internal static readonly string SvcUninstall = "/uninstall";

        /// <summary>
        /// Application entry point.
        /// </summary>
        internal static void Main(string[] args)
        {
            bool svc = false;
            bool install = false;

            try
            {
                // Check for special cases.
                if (args.Length > 0)
                {
                    string first = args[0].ToLowerInvariant();

                    if (Help.Contains(first))
                    {
                        PrintHelp();

                        return;
                    }
                    
                    if (Svc.Equals(first))
                    {
                        args = RemoveFirstArg(args);

                        svc = true;
                    }

                    else if (SvcInstall.Equals(first))
                    {
                        args = RemoveFirstArg(args);

                        install = true;
                    }
                    else if (SvcUninstall.Equals(first))
                    {
                        IgniteService.Uninstall();

                        return;
                    }
                }

                if (!svc)
                {
                    // Pick application configuration first, command line arguments second.
                    var allArgs = AppSettingsConfigurator.GetArgs(ConfigurationManager.AppSettings)
                        .Concat(ArgsConfigurator.GetArgs(args)).ToArray();

                    if (install)
                        IgniteService.DoInstall(allArgs);
                    else
                    {
                        Ignition.Start(Configurator.GetConfiguration(allArgs));

                        IgniteManager.DestroyJvm();
                    }

                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e);

                Environment.Exit(-1);
            }

            // If we are here, then this is a service call.
            // Use only arguments, not app.config.
            var cfg = Configurator.GetConfiguration(ArgsConfigurator.GetArgs(args).ToArray());

            ServiceBase.Run(new IgniteService(cfg));
        }

        /// <summary>
        /// Prints help.
        /// </summary>
        private static void PrintHelp()
        {
            Console.WriteLine("Usage: Apache.Ignite.exe [/install] [/uninstall] [-options]");
            Console.WriteLine("");
            Console.WriteLine("\t/install [-options]    installs Ignite Windows service with provided options.");
            Console.WriteLine("\t/uninstall             uninstalls Ignite Windows service.");
            Console.WriteLine("");
            Console.WriteLine("Options:");
            Console.WriteLine("\t-IgniteHome            path to Ignite installation directory (if not provided IGNITE_HOME environment variable is used).");
            Console.WriteLine("\t-ConfigSectionName     name of the IgniteConfigurationSection in app.config to use.");
            Console.WriteLine("\t-ConfigFileName        path to the app.config file (if not provided Apache.Ignite.exe.config is used).");
            Console.WriteLine("\t-springConfigUrl       path to Spring configuration file.");
            Console.WriteLine("\t-jvmDllPath            path to JVM library jvm.dll (if not provided JAVA_HOME environment variable is used).");
            Console.WriteLine("\t-jvmClasspath          classpath passed to JVM (enlist additional jar files here).");
            Console.WriteLine("\t-suppressWarnings      whether to print warnings.");
            Console.WriteLine("\t-J<javaOption>         JVM options passed to created JVM.");
            Console.WriteLine("\t-assembly=userLib.dll  additional .NET assemblies to be loaded.");
            Console.WriteLine("\t-jvmInitialMemoryMB    Initial Java heap size, in megabytes. Maps to -Xms Java parameter. Defaults to 512.");
            Console.WriteLine("\t-jvmMaxMemoryMB        Maximum Java heap size, in megabytes. Maps to -Xmx Java parameter. Defaults to 1024.");
            Console.WriteLine("");
            Console.WriteLine("Examples:");
            Console.WriteLine("\tApache.Ignite.exe -J-Xms1024m -J-Xmx1024m -springConfigUrl=C:/woer/gg-test/my-test-gg-confignative.xml");
            Console.WriteLine("\tApache.Ignite.exe -IgniteHome=c:/apache-ignite -jvmClasspath=libs/myLib1.jar;libs/myLib2.jar");
            Console.WriteLine("\tApache.Ignite.exe -assembly=c:/myProject/libs/lib1.dll -assembly=c:/myProject/libs/lib2.dll");
            Console.WriteLine("\tApache.Ignite.exe -jvmInitialMemoryMB=1024 -jvmMaxMemoryMB=4096");
            Console.WriteLine("");
            Console.WriteLine("Note:");
            Console.WriteLine("Command line settings have priority over Apache.Ignite.exe.config settings. JVM options and assemblies are concatenated; data from config file comes first, then data from command line.");
        }

        /// <summary>
        /// Remove the first argument.
        /// </summary>
        /// <param name="args">Arguments.</param>
        /// <returns>New arguments.</returns>
        private static string[] RemoveFirstArg(string[] args)
        {
            return args.Skip(1).ToArray();
        }
    }
}
