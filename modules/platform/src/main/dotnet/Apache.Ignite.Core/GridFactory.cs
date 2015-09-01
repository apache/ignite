/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain 
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Common;
    using GridGain.Impl;
    using GridGain.Impl.Java;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;
    using GridGain.Lifecycle;

    using U = GridGain.Impl.GridUtils;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using PU = GridGain.Impl.Portable.PortableUtils;
    
    /// <summary>
    /// This class defines a factory for the main GridGain API.
    /// <p/>
    /// Use <see cref="GridFactory.Start()"/> method to start grid with default configuration.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// <example>
    /// You can also use <see cref="GridConfiguration"/> to override some default configuration.
    /// Below is an example on how to start grid with custom configuration for portable types and
    /// provide path to Spring XML configuration file:
    /// <code>
    /// GridConfiguration cfg = new GridConfiguration();
    ///
    /// // Create portable type configuration.
    /// PortableConfiguration portableCfg = new PortableConfiguration();
    ///
    /// cfg.SpringConfigUrl = "examples\\config\\example-cache.xml";
    ///
    /// portableCfg.TypeConfigurations = new List&lt;PortableTypeConfiguration&gt; 
    /// {
    ///     new PortableTypeConfiguration(typeof(Address)),
    ///     new PortableTypeConfiguration(typeof(Organization))
    /// };
    ///
    /// cfg.PortableConfiguration = portableCfg;
    ///
    /// // Start grid node with grid configuration.
    /// IGrid grid = GridGainFactory.Start(cfg);
    /// </code>
    /// </example>
    /// </summary>
    public static class GridFactory
    {
        /** */
        private const string DFLT_CFG = "config/default-config.xml";

        /** */
        private static readonly object MUX = new object();

        /** GC warning flag. */
        private static int GC_WARN;

        /** */
        private static readonly IDictionary<GridKey, GridImpl> grids = new Dictionary<GridKey, GridImpl>();
        
        /** Current DLL name. */
        private static readonly string GRIDGAIN_DLL_NAME = Path.GetFileName(Assembly.GetExecutingAssembly().Location);

        /** Startup info. */
        [ThreadStatic]
        private static Startup STARTUP;

        /** Client mode flag. */
        [ThreadStatic]
        private static bool CLIENT_MODE;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static GridFactory()
        {
            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
        }

        /// <summary>
        ///
        /// </summary>
        internal class GridKey
        {
            /** */
            private readonly string name;

            /// <summary>
            ///
            /// </summary>
            /// <param name="name"></param>
            internal GridKey(string name)
            {
                this.name = name;
            }

            /// <summary>
            ///
            /// </summary>
            /// <returns></returns>
            public string Name()
            {
                return name;
            }

            public override bool Equals(object obj)
            {
                GridKey other = obj as GridKey;

                if (other == null)
                    return false;

                if (name == null)
                    return other.name == null;

                return name.Equals(other.name);
            }

            public override int GetHashCode()
            {
                return name == null ? 0 : name.GetHashCode();
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether grids should be started in client mode.
        /// Client nodes cannot hold data in caches.
        /// </summary>
        public static bool ClientMode
        {
            get { return CLIENT_MODE; }
            set { CLIENT_MODE = value; }
        }

        /// <summary>
        /// Starts grid with default configuration. By default this method will
        /// use grid configuration defined in <code>GRIDGAIN_HOME/config/default-config.xml</code>
        /// configuration file. If such file is not found, then all system defaults will be used.
        /// </summary>
        /// <returns>Started grid.</returns>
        public static IGrid Start()
        {
            return Start(new GridConfiguration());
        }

        /// <summary>
        /// Starts all grids specified within given Spring XML configuration file. If grid with given name
        /// is already started, then exception is thrown. In this case all instances that may
        /// have been started so far will be stopped too.
        /// </summary>
        /// <param name="springCfgPath">Spring XML configuration file path or URL. Note, that the path can be
        /// absolute or relative to GRIDGAIN_HOME.</param>
        /// <returns>Started grid. If Spring configuration contains multiple grid instances, then the 1st
        /// found instance is returned.</returns>
        public static IGrid Start(string springCfgPath)
        {
            return Start(new GridConfiguration {SpringConfigUrl = springCfgPath});
        }

        /// <summary>
        /// Starts grid with given configuration.
        /// </summary>
        /// <returns>Started grid.</returns>
        public unsafe static IGrid Start(GridConfiguration cfg)
        {
            A.NotNull(cfg, "cfg");

            // Copy configuration to avoid changes to user-provided instance.
            GridConfigurationEx cfgEx = cfg as GridConfigurationEx;

            cfg = cfgEx == null ? new GridConfiguration(cfg) : new GridConfigurationEx(cfgEx);

            // Set default Spring config if needed.
            if (cfg.SpringConfigUrl == null)
                cfg.SpringConfigUrl = DFLT_CFG;

            lock (MUX)
            {
                // 1. Check GC settings.
                CheckServerGC(cfg);

                // 2. Create context.
                GridUtils.LoadDlls(cfg.JvmDllPath);

                var cbs = new UnmanagedCallbacks();

                void* ctx = GridManager.GetContext(cfg, cbs);

                sbyte* cfgPath0 = GridUtils.StringToUtf8Unmanaged(cfg.SpringConfigUrl ?? DFLT_CFG);

                string gridName = cfgEx != null ? cfgEx.GridName : null;
                sbyte* gridName0 = GridUtils.StringToUtf8Unmanaged(gridName);

                // 3. Create startup object which will guide us through the rest of the process.
                STARTUP = new Startup(cfg) { Context = ctx };

                IUnmanagedTarget interopProc = null;

                try
                {
                    // 4. Initiate Grid start.
                    interopProc = UU.IgnitionStart(cbs.Context, cfg.SpringConfigUrl ?? DFLT_CFG, 
                        cfgEx != null ? cfgEx.GridName : null, ClientMode);

                    // 5. At this point start routine is finished. We expect STARTUP object to have all necessary data.
                    GridImpl node = new GridImpl(cfg, STARTUP.Name, interopProc, STARTUP.Marshaller, 
                        STARTUP.LifecycleBeans, cbs);

                    // 6. On-start callback (notify lifecycle components).
                    node.OnStart();

                    grids[new GridKey(STARTUP.Name)] = node;

                    return node;
                }
                catch (Exception)
                {
                    // 1. Perform keys cleanup.
                    string name = STARTUP.Name;

                    if (name != null)
                    {
                        GridKey key = new GridKey(name);

                        if (grids.ContainsKey(key))
                            grids.Remove(key);
                    }

                    // 2. Stop Grid node if it was started.
                    if (interopProc != null)
                        UU.IgnitionStop(interopProc.Context, gridName, true);

                    // 3. Throw error further (use startup error if exists because it is more precise).
                    if (STARTUP.Error != null)
                        throw STARTUP.Error;

                    throw;
                }
                finally
                {
                    STARTUP = null;

                    Marshal.FreeHGlobal((IntPtr)cfgPath0);

                    if ((IntPtr)gridName0 != IntPtr.Zero)
                        Marshal.FreeHGlobal((IntPtr)gridName0);

                    if (interopProc != null)
                        UU.ProcessorReleaseStart(interopProc);
                }
            }
        }

        /// <summary>
        /// Check whether GC is set to server mode.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        private static void CheckServerGC(GridConfiguration cfg)
        {
            if (!cfg.SuppressWarnings && !GCSettings.IsServerGC && Interlocked.CompareExchange(ref GC_WARN, 1, 0) == 0)
                Console.WriteLine("GC server mode is not enabled, this could lead to less " +
                    "than optimal performance on multi-core machines (to enable see " +
                    "http://msdn.microsoft.com/en-us/library/ms229357(v=vs.110).aspx).");
        }

        /// <summary>
        /// Prepare callback invoked from Java.
        /// </summary>
        /// <param name="inStream">Intput stream with data.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="handleRegistry">Handle registry.</param>
        internal static void OnPrepare(PlatformMemoryStream inStream, PlatformMemoryStream outStream, 
            HandleRegistry handleRegistry)
        {
            try
            {
                PortableReaderImpl reader = PU.Marshaller.StartUnmarshal(inStream);

                PrepareConfiguration(reader.ReadObject<InteropDotNetConfiguration>());

                PrepareLifecycleBeans(reader, outStream, handleRegistry);
            }
            catch (Exception e)
            {
                STARTUP.Error = e;

                throw;
            }
        }

        /// <summary>
        /// Preapare configuration.
        /// </summary>
        /// <param name="dotNetCfg">Dot net configuration.</param>
        private static void PrepareConfiguration(InteropDotNetConfiguration dotNetCfg)
        {
            // 1. Load assemblies.
            GridConfiguration cfg = STARTUP.Configuration;

            LoadAssemblies(cfg.Assemblies);

            if (dotNetCfg != null)
                LoadAssemblies(dotNetCfg.Assemblies);

            // 2. Create marshaller only after assemblies are loaded.
            if (cfg.PortableConfiguration == null && dotNetCfg != null && dotNetCfg.PortableCfg != null)
                cfg.PortableConfiguration = dotNetCfg.PortableCfg.ToPortableConfiguration();

            STARTUP.Marshaller = new PortableMarshaller(cfg.PortableConfiguration);
        }

        /// <summary>
        /// Prepare lifecycle beans.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="handleRegistry">Handle registry.</param>
        private static void PrepareLifecycleBeans(PortableReaderImpl reader, PlatformMemoryStream outStream, 
            HandleRegistry handleRegistry)
        {
            IList<LifecycleBeanHolder> beans = new List<LifecycleBeanHolder>();

            // 1. Read beans defined in Java.
            int cnt = reader.ReadInt();

            for (int i = 0; i < cnt; i++)
                beans.Add(new LifecycleBeanHolder(CreateLifecycleBean(reader)));

            // 2. Append beans definied in local configuration.
            ICollection<ILifecycleBean> nativeBeans = STARTUP.Configuration.LifecycleBeans;

            if (nativeBeans != null)
            {
                foreach (ILifecycleBean nativeBean in nativeBeans)
                    beans.Add(new LifecycleBeanHolder(nativeBean));
            }

            // 3. Write bean pointers to Java stream.
            outStream.WriteInt(beans.Count);

            foreach (LifecycleBeanHolder bean in beans)
                outStream.WriteLong(handleRegistry.AllocateCritical(bean));

            outStream.SynchronizeOutput();

            // 4. Set beans to STARTUP object.
            STARTUP.LifecycleBeans = beans;
        }

        /// <summary>
        /// Create lifecycle bean.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Lifecycle bean.</returns>
        internal static ILifecycleBean CreateLifecycleBean(PortableReaderImpl reader)
        {
            // 1. Instantiate.
            string assemblyName = reader.ReadString();
            string clsName = reader.ReadString();

            object bean = U.CreateInstance(assemblyName, clsName);

            // 2. Set properties.
            IDictionary<string, object> props = reader.ReadGenericDictionary<string, object>();

            U.SetProperties(bean, props);

            return bean as ILifecycleBean;
        }

        /// <summary>
        /// Kernal start callback.
        /// </summary>
        /// <param name="stream">Stream.</param>
        internal static void OnStart(IPortableStream stream)
        {
            try
            {
                // 1. Read data and leave critical state ASAP.
                PortableReaderImpl reader = PU.Marshaller.StartUnmarshal(stream);
                
                // ReSharper disable once PossibleInvalidOperationException
                var name = reader.ReadString();
                
                // 2. Set ID and name so that Start() method can use them later.
                STARTUP.Name = name;

                if (grids.ContainsKey(new GridKey(name)))
                    throw new IgniteException("Grid with the same name already started: " + name);

            }
            catch (Exception e)
            {
                // 5. Preserve exception to throw it later in the "Start" method and throw it further
                //    to abort startup in Java.
                STARTUP.Error = e;

                throw;
            }
        }

        /// <summary>
        /// Value object to pass data between .Net methods during startup bypassing Java.
        /// </summary>
        private unsafe class Startup
        {
            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="cfg">Configuration.</param>
            internal Startup(GridConfiguration cfg)
            {
                Configuration = cfg;
            }

            /// <summary>
            /// Configuration.
            /// </summary>
            internal GridConfiguration Configuration
            {
                get;
                private set;
            }

            /// <summary>
            /// Lifecycle beans.
            /// </summary>
            internal IList<LifecycleBeanHolder> LifecycleBeans
            {
                get;
                set;
            }

            /// <summary>
            /// Node name.
            /// </summary>
            internal string Name
            {
                get;
                set;
            }

            /// <summary>
            /// Marshaller.
            /// </summary>
            internal PortableMarshaller Marshaller
            {
                get;
                set;
            }

            /// <summary>
            /// Start error.
            /// </summary>
            internal Exception Error
            {
                get;
                set;
            }

            internal void* Context
            {
                get; 
                set;
            }
        }

        /// <summary>
        /// Load assemblies.
        /// </summary>
        /// <param name="assemblies">Assemblies.</param>
        private static void LoadAssemblies(IEnumerable<string> assemblies)
        {
            if (assemblies != null)
            {
                foreach (string s in assemblies)
                {
                    // 1. Try loading as directory.
                    if (Directory.Exists(s))
                    {
                        string[] files = Directory.GetFiles(s, "*.dll");

#pragma warning disable 0168

                        foreach (string dllPath in files)
                        {
                            if (!SelfAssembly(dllPath))
                            {
                                try
                                {
                                    Assembly.LoadFile(dllPath);
                                }

                                catch (BadImageFormatException)
                                {
                                    // No-op.
                                }
                            }
                        }

#pragma warning restore 0168

                        continue;
                    }

                    // 2. Try loading using full-name.
                    try
                    {
                        Assembly assembly = Assembly.Load(s);

                        if (assembly != null)
                            continue;
                    }
                    catch (Exception e)
                    {
                        if (!(e is FileNotFoundException || e is FileLoadException))
                            throw new IgniteException("Failed to load assembly: " + s, e);
                    }

                    // 3. Try loading using file path.
                    try
                    {
                        Assembly assembly = Assembly.LoadFrom(s);

                        if (assembly != null)
                            continue;
                    }
                    catch (Exception e)
                    {
                        if (!(e is FileNotFoundException || e is FileLoadException))
                            throw new IgniteException("Failed to load assembly: " + s, e);
                    }

                    // 4. Not found, exception.
                    throw new IgniteException("Failed to load assembly: " + s);
                }
            }
        }

        /// <summary>
        /// Whether assembly points to GridGain binary.
        /// </summary>
        /// <param name="assembly">Assembly to check..</param>
        /// <returns><c>True</c> if this is one of GG assemblies.</returns>
        private static bool SelfAssembly(string assembly)
        {
            return assembly.EndsWith(GRIDGAIN_DLL_NAME, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets an named grid instance. If grid name is {@code null} or empty string,
        /// then default no-name grid will be returned. Note that caller of this method
        /// should not assume that it will return the same instance every time.
        /// <p/>
        /// Note that single process can run multiple grid instances and every grid instance (and its
        /// node) can belong to a different grid. Grid name defines what grid a particular grid
        /// instance (and correspondingly its node) belongs to.
        /// </summary>
        /// <param name="name">Grid name to which requested grid instance belongs to. If <code>null</code>,
        /// then grid instance belonging to a default no-name grid will be returned.
        /// </param>
        /// <returns>An instance of named grid.</returns>
        public static IGrid Grid(string name)
        {
            lock (MUX)
            {
                GridKey key = new GridKey(name);

                if (!grids.ContainsKey(key))
                    throw new IgniteException("Grid instance was not properly started or was already stopped: " + name);

                return grids[key].Grid;
            }
        }

        /// <summary>
        /// Gets an instance of default no-name grid. Note that
        /// caller of this method should not assume that it will return the same
        /// instance every time.
        /// </summary>
        /// <returns>An instance of default no-name grid.</returns>
        public static IGrid Grid()
        {
            return Grid(null);
        }

        /// <summary>
        /// Stops named grid. If <code>cancel</code> flag is set to <code>true</code> then
        /// all jobs currently executing on local node will be interrupted. If
        /// grid name is <code>null</code>, then default no-name grid will be stopped.
        /// </summary>
        /// <param name="name">Grid name. If <code>null</code>, then default no-name grid will be stopped.</param>
        /// <param name="cancel">If <code>true</code> then all jobs currently executing will be cancelled
        /// by calling <code>ComputeJob.cancel</code>method.</param>
        /// <returns><code>true</code> if named grid instance was indeed found and stopped, <code>false</code>
        /// othwerwise (the instance with given <code>name</code> was not found).</returns>
        public static bool Stop(string name, bool cancel)
        {
            lock (MUX)
            {
                GridKey key = new GridKey(name);

                GridImpl node;

                if (!grids.TryGetValue(key, out node))
                    return false;

                node.Stop(cancel);

                grids.Remove(key);
                
                GC.Collect();

                return true;
            }
        }

        /// <summary>
        /// Stops <b>all</b> started grids. If <code>cancel</code> flag is set to <code>true</code> then
        /// all jobs currently executing on local node will be interrupted.
        /// </summary>
        /// <param name="cancel">If <code>true</code> then all jobs currently executing will be cancelled
        /// by calling <code>ComputeJob.cancel</code>method.</param>
        public static void StopAll(bool cancel)
        {
            lock (MUX)
            {
                while (grids.Count > 0)
                {
                    var entry = grids.First();
                    
                    entry.Value.Stop(cancel);

                    grids.Remove(entry.Key);
                }
            }

            GC.Collect();
        }
        
        /// <summary>
        /// Handles the AssemblyResolve event of the CurrentDomain control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The <see cref="ResolveEventArgs"/> instance containing the event data.</param>
        /// <returns>Manually resolved assembly, or null.</returns>
        private static Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            return LoadedAssembliesResolver.Instance.GetAssembly(args.Name);
        }
    }
}
