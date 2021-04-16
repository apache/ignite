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

namespace Apache.Ignite.Service
{
    using System;
    using System.Collections.Generic;
    using System.Configuration.Install;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.ServiceProcess;
    using System.Text;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Lifecycle;

    /// <summary>
    /// Ignite windows service.
    /// </summary>
    internal class IgniteService : ServiceBase, ILifecycleHandler
    {
        /** Service name. */
        public const string SvcName = "Apache Ignite.NET";

        /** Service display name. */
        public static readonly string SvcDisplayName = SvcName + " " +
            Assembly.GetExecutingAssembly().GetName().Version.ToString(4);

        /** Service description. */
        public const string SvcDesc = "Apache Ignite.NET Service";

        /** Current executable name. */
        private static readonly string ExeName =
            new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).FullName;

        /** Ignite configuration to start with. */
        private readonly IgniteConfiguration _cfg;

        /** Stopping recurse check flag. */
        private bool _isStopping;

        /// <summary>
        /// Constructor.
        /// </summary>
        public IgniteService(IgniteConfiguration cfg)
        {
            AutoLog = true;
            CanStop = true;
            ServiceName = SvcName;

            _cfg = cfg;

            // Subscribe to lifecycle events
            var beans = _cfg.LifecycleHandlers ?? new List<ILifecycleHandler>();

            beans.Add(this);

            _cfg.LifecycleHandlers = beans;
        }

        /** <inheritDoc /> */
        protected override void OnStart(string[] args)
        {
            Ignition.Start(_cfg);
        }

        /** <inheritDoc /> */
        protected override void OnStop()
        {
            if (!_isStopping)
                Ignition.StopAll(true);
        }

        /// <summary>
        /// Install service programmatically.
        /// </summary>
        /// <param name="args">The arguments.</param>
        internal static void DoInstall(Tuple<string, string>[] args)
        {
            // 1. Check if already defined.
            if (ServiceController.GetServices().Any(svc => SvcName.Equals(svc.ServiceName)))
            {
                throw new IgniteException("Ignite service is already installed (uninstall it using \"" +
                                          ExeName + " " + IgniteRunner.SvcUninstall + "\" first)");
            }

            // 2. Create startup arguments.
            if (args.Length > 0)
            {
                Console.WriteLine("Installing \"" + SvcName + "\" service with the following startup " +
                    "arguments:");

                foreach (var arg in args)
                    Console.WriteLine("\t" + arg);
            }
            else
                Console.WriteLine("Installing \"" + SvcName + "\" service ...");

            // 3. Actual installation.
            Install0(args);

            Console.WriteLine("\"" + SvcName + "\" service installed successfully.");
        }

        /// <summary>
        /// Uninstall service programmatically.
        /// </summary>
        internal static void Uninstall()
        {
            var svc = ServiceController.GetServices().FirstOrDefault(x => SvcName == x.ServiceName);

            if (svc == null)
            {
                Console.WriteLine("\"" + SvcName + "\" service is not installed.");
            }
            else if (svc.Status != ServiceControllerStatus.Stopped)
            {
                throw new IgniteException("Ignite service is running, please stop it first.");
            }
            else
            {
                Console.WriteLine("Uninstalling \"" + SvcName + "\" service ...");

                Uninstall0();

                Console.WriteLine("\"" + SvcName + "\" service uninstalled successfully.");
            }
        }

        /// <summary>
        /// Runs the service.
        /// </summary>
        internal static void Run(IgniteConfiguration cfg)
        {
            ServiceBase.Run(new IgniteService(cfg));
        }

        /// <summary>
        /// Native service installation.
        /// </summary>
        /// <param name="args">Arguments.</param>
        private static void Install0(Tuple<string, string>[] args)
        {
            // 1. Prepare arguments.
            var argString = new StringBuilder(IgniteRunner.Svc);

            foreach (var arg in args)
            {
                var val = arg.Item2;

                if (val.Contains(' '))
                {
                    val = '"' + val + '"';
                }

                argString.Append(" ").AppendFormat("-{0}={1}", arg.Item1, val);
            }

            IgniteServiceInstaller.Args = argString.ToString();

            // 2. Install service.
            ManagedInstallerClass.InstallHelper(new[] { ExeName });
        }

        /// <summary>
        /// Native service uninstallation.
        /// </summary>
        private static void Uninstall0()
        {
            ManagedInstallerClass.InstallHelper(new[] { ExeName, "/u" });
        }

        /** <inheritdoc /> */
        public void OnLifecycleEvent(LifecycleEventType evt)
        {
            if (evt == LifecycleEventType.AfterNodeStop)
            {
                _isStopping = true;

                Stop();
            }
        }
    }
}
