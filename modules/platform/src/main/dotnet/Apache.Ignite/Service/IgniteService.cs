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
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.ServiceProcess;
    using System.Text;
    using Apache.Ignite.Config;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Ignite windows service.
    /// </summary>
    internal class IgniteService : ServiceBase
    {
        /** Service name. */
        internal static readonly string SvcName = "Apache Ignite";

        /** Service display name. */
        internal static readonly string SvcDisplayName = "Apache Ignite .NET " + 
            Assembly.GetExecutingAssembly().GetName().Version.ToString(4);

        /** Service description. */
        internal static readonly string SvcDesc = "Apache Ignite .Net Service.";

        /** Current executable name. */
        internal static readonly string ExeName =
            new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).FullName;

        /** Current executable fully qualified name. */
        internal static readonly string FullExeName = Path.GetFileName(FullExeName);

        /** Ignite configuration to start with. */
        private readonly IgniteConfiguration _cfg;

        /// <summary>
        /// Constructor.
        /// </summary>
        public IgniteService(IgniteConfiguration cfg)
        {
            AutoLog = true;
            CanStop = true;
            ServiceName = SvcName;

            _cfg = cfg;
        }

        /** <inheritDoc /> */
        protected override void OnStart(string[] args)
        {
            Ignition.Start(_cfg);
        }

        /** <inheritDoc /> */
        protected override void OnStop()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Install service programmatically.
        /// </summary>
        /// <param name="cfg">Ignite configuration.</param>
        internal static void DoInstall(IgniteConfiguration cfg)
        {
            // 1. Check if already defined.
            if (ServiceController.GetServices().Any(svc => SvcName.Equals(svc.ServiceName)))
            {
                throw new IgniteException("Ignite service is already installed (uninstall it using \"" +
                                          ExeName + " " + IgniteRunner.SvcUninstall + "\" first)");
            }

            // 2. Create startup arguments.
            var args = ArgsConfigurator.ToArgs(cfg);

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
        /// Native service installation.
        /// </summary>
        /// <param name="args">Arguments.</param>
        private static void Install0(string[] args)
        {
            // 1. Prepare arguments.
            var binPath = new StringBuilder(FullExeName).Append(" ").Append(IgniteRunner.Svc);

            foreach (var arg in args)
                binPath.Append(" ").Append(arg);

            // 2. Get SC manager.
            var scMgr = OpenServiceControlManager();

            // 3. Create service.
            var svc = NativeMethods.CreateService(
                scMgr,
                SvcName,
                SvcDisplayName,
                983551, // Access constant. 
                0x10,   // Service type SERVICE_WIN32_OWN_PROCESS.
                0x2,    // Start type SERVICE_AUTO_START.
                0x2,    // Error control SERVICE_ERROR_SEVERE.
                binPath.ToString(),
                null,
                IntPtr.Zero,
                null,
                null,   // Use priviliged LocalSystem account.
                null
            );

            if (svc == IntPtr.Zero)
                throw new IgniteException("Failed to create the service.", new Win32Exception());

            // 4. Set description.
            var desc = new ServiceDescription {desc = Marshal.StringToHGlobalUni(SvcDesc)};


            try 
            {
                if (!NativeMethods.ChangeServiceConfig2(svc, 1u, ref desc)) 
                    throw new IgniteException("Failed to set service description.", new Win32Exception());
            }
            finally 
            {
                Marshal.FreeHGlobal(desc.desc);
            }
        }

        /// <summary>
        /// Native service uninstallation.
        /// </summary>
        private static void Uninstall0()
        {
            var scMgr = OpenServiceControlManager();

            var svc = NativeMethods.OpenService(scMgr, SvcName, 65536);

            if (svc == IntPtr.Zero)
                throw new IgniteException("Failed to uninstall the service.", new Win32Exception());

            NativeMethods.DeleteService(svc);
        }

        /// <summary>
        /// Opens SC manager.
        /// </summary>
        /// <returns>SC manager pointer.</returns>
        private static IntPtr OpenServiceControlManager()
        {
            var ptr = NativeMethods.OpenSCManager(null, null, 983103);

            if (ptr == IntPtr.Zero)
                throw new IgniteException("Failed to initialize Service Control manager " +
                                          "(did you run the command as administrator?)", new Win32Exception());

            return ptr;
        }
    }
}
