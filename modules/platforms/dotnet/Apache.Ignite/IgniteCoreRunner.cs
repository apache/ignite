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

    /// <summary>
    /// Runner class.
    /// </summary>
    public static class IgniteCoreRunner
    {
        /** Help commands. */
        private static readonly IList<string> Help = new List<string> { "/help", "-help", "--help" };

        /// <summary>
        /// Application entry point.
        /// </summary>
        internal static void Main(string[] args)
        {
            try
            {
                // Check for special cases.
                if (args.Length > 0)
                {
                    string first = args[0].ToLowerInvariant();

                    if (Help.Contains(first))
                    {
                        ConsoleUtils.PrintHelp("Apache.Ignite.dll", false);

                        return;
                    }
                }

                // Pick application configuration first, command line arguments second.
                var allArgs = AppSettingsConfigurator.GetArgs(ConfigurationManager.AppSettings)
                    .Concat(ArgsConfigurator.GetArgs(args)).ToArray();

                var ignite = Ignition.Start(Configurator.GetConfiguration(allArgs));

                // Wait until stopped.
                var evt = new ManualResetEventSlim(false);
                ignite.Stopped += (s, a) => evt.Set();
                evt.Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e);

                Environment.Exit(-1);
            }
        }
    }
}
