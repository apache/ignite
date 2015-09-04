/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Runner.Service
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.ServiceProcess;
    using System.Text;

    using GridGain.Common;
    using GridGain.Impl.Runner.Config;

    /// <summary>
    /// GridGain service.
    /// </summary>
    internal class GridService : ServiceBase
    {
        /** Service name. */
        internal static readonly string SVC_NAME = "GridGain";

        /** Service display name. */
        internal static readonly string SVC_DISPLAY_NAME = "GridGain In-Memory Data Fabric For .NET " + 
            Assembly.GetExecutingAssembly().GetName().Version.ToString(4);

        /** Service description. */
        internal static readonly string SVC_DESC = "Middleware for in-memory processing of big data in" + 
            " a distributed environment.";

        /** Current executable name. */
        internal static readonly string EXE_NAME = Process.GetCurrentProcess().ProcessName;

        /** Current executable fully qualified name. */
        internal static readonly string FULL_EXE_NAME = Assembly.GetExecutingAssembly().CodeBase;

        /** Grid configuration to start with. */
        private readonly GridConfiguration cfg;

        /// <summary>
        /// Static initilizer.
        /// </summary>
        static GridService()
        {
            FULL_EXE_NAME = new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).FullName;

            EXE_NAME = Path.GetFileName(FULL_EXE_NAME);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        public GridService(GridConfiguration cfg)
        {
            this.AutoLog = true;
            this.CanStop = true;
            this.ServiceName = SVC_NAME;

            this.cfg = cfg;
        }

        /** <inheritDoc /> */
        protected override void OnStart(string[] args)
        {
            GridFactory.Start(cfg);
        }

        /** <inheritDoc /> */
        protected override void OnStop()
        {
            GridFactory.StopAll(true);
        }

        /// <summary>
        /// Install service programmatically.
        /// </summary>
        /// <param name="cfg">Grid configuration.</param>
        internal static void DoInstall(GridConfiguration cfg)
        {
            // 1. Check if already defined.
            foreach (ServiceController svc in ServiceController.GetServices())
            {
                if (GridService.SVC_NAME.Equals(svc.ServiceName))
                    throw new GridException("GridGain service is already installed (uninstall it using \"" +
                        GridService.EXE_NAME + " " + GridRunner.SVC_UNINSTALL + "\" first)");
            }

            // 2. Create startup arguments.
            string[] args = GridArgsConfigurator.ToArgs(cfg);

            if (args.Length > 0)
            {
                Console.WriteLine("Installing \"" + GridService.SVC_NAME + "\" service with the following startup " +
                    "arguments:");

                foreach (string arg in args)
                    Console.WriteLine("\t" + arg);
            }
            else
                Console.WriteLine("Installing \"" + GridService.SVC_NAME + "\" service ...");

            // 3. Actual installation.
            Install0(args);

            Console.WriteLine("\"" + GridService.SVC_NAME + "\" service installed successfully.");
        }

        /// <summary>
        /// Uninstall service programmatically.
        /// </summary>
        internal static void Uninstall()
        {
            bool found = false;

            foreach (ServiceController svc in ServiceController.GetServices())
            {
                if (GridService.SVC_NAME.Equals(svc.ServiceName))
                {
                    if (svc.Status.Equals(ServiceControllerStatus.Stopped))
                    {
                        found = true;

                        break;
                    }
                    else
                        // Do not stop existing service automatically.
                        throw new GridException("GridGain service is running, please stop it first.");
                }
            }

            if (found)
            {
                Console.WriteLine("Uninstalling \"" + GridService.SVC_NAME + "\" service ...");

                Uninstall0();

                Console.WriteLine("\"" + GridService.SVC_NAME + "\" service uninstalled successfully.");
            }
            else
                Console.WriteLine("\"" + GridService.SVC_NAME + "\" service is not installed.");
        }

        /// <summary>
        /// Native service installation.
        /// </summary>
        /// <param name="args">Arguments.</param>
        private static void Install0(string[] args)
        {
            // 1. Prepare arguments.
            StringBuilder binPath = new StringBuilder(GridService.FULL_EXE_NAME).Append(" ").Append(GridRunner.SVC);

            foreach (string arg in args)
                binPath.Append(" ").Append(arg);

            // 2. Get SC manager.
            IntPtr scMgr = OpenSCManager();

            // 3. Create service.
            IntPtr svc = CreateService(
                scMgr,
                GridService.SVC_NAME,
                GridService.SVC_DISPLAY_NAME,
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
                throw new GridException("Failed to create the service.", new Win32Exception());

            // 4. Set description.
            SERVICE_DESCRIPTION desc = new SERVICE_DESCRIPTION();

            desc.desc = Marshal.StringToHGlobalUni(GridService.SVC_DESC);

            try 
            {
                if (!ChangeServiceConfig2(svc, 1u, ref desc)) 
                    throw new GridException("Failed to set service description.", new Win32Exception());
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
            IntPtr scMgr = OpenSCManager();

            IntPtr svc = OpenService(scMgr, GridService.SVC_NAME, 65536);

            if (svc == IntPtr.Zero)
                throw new GridException("Failed to uninstall the service.", new Win32Exception());

            DeleteService(svc);
        }

        /// <summary>
        /// Opens SC manager.
        /// </summary>
        /// <returns>SC manager pointer.</returns>
        private static IntPtr OpenSCManager()
        {
            IntPtr ptr = OpenSCManager(null, null, 983103);

            if (ptr == IntPtr.Zero)
                throw new GridException("Failed to initialize SC manager (did you run the command as administrator?)", new Win32Exception());

            return ptr;
        }

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr OpenSCManager(
            string machineName,
            string dbName,
            int access
        );

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateService(
            IntPtr db,
            string svcName,
            string displayName,
            int access,
            int svcType,
            int startType,
            int errControl,
            string binPath,
            string loadOrderGrp,
            IntPtr pTagId,
            string dependencies,
            string servicesStartName,
            string pwd
        );

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr OpenService(
            IntPtr db,
            string svcName,
            int access
        );

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool DeleteService(IntPtr svc);

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool ChangeServiceConfig2(
            IntPtr svc, 
            uint infoLevel, 
            ref SERVICE_DESCRIPTION desc
        );

        /// <summary>
        /// Service description structure.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        public struct SERVICE_DESCRIPTION
        {
            /** Pointer to description. */
            public IntPtr desc;
        }
    }
}
