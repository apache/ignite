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
    using System.Threading;
    using Apache.Ignite.Config;
    using Apache.Ignite.Core;
    using Apache.Ignite.Service;

    /// <summary>
    /// Runner class.
    /// </summary>
    public static class IgniteRunner
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
                        ConsoleUtils.PrintHelp("Apache.Ignite.exe", true);

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
                        .Concat(ArgsConfigurator.GetArgs(args))
                        .ToArray();

                    if (install)
                        IgniteService.DoInstall(allArgs);
                    else
                    {
                        // Load assemblies before instantiating IgniteConfiguration,
                        // it can reference types from those assemblies.
                        allArgs = allArgs.LoadAssemblies().ToArray();

                        using (var ignite = Ignition.Start(Configurator.GetConfiguration(allArgs)))
                        {
                            // Wait until stopped.
                            var evt = new ManualResetEventSlim(false);
                            ignite.Stopped += (s, a) => evt.Set();
                            Console.CancelKeyPress += (s, a) => evt.Set();
                            evt.Wait();
                        }
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
            IgniteService.Run(cfg);
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
