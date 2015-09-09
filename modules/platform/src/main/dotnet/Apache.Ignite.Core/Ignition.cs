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

namespace Apache.Ignite.Core 
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Interop;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Lifecycle;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;
    using PU = Apache.Ignite.Core.Impl.Portable.PortableUtils;
    
    /// <summary>
    /// This class defines a factory for the main Ignite API.
    /// <p/>
    /// Use <see cref="Ignition.Start()"/> method to start Ignite with default configuration.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// <example>
    /// You can also use <see cref="IgniteConfiguration"/> to override some default configuration.
    /// Below is an example on how to start Ignite with custom configuration for portable types and
    /// provide path to Spring XML configuration file:
    /// <code>
    /// IgniteConfiguration cfg = new IgniteConfiguration();
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
    /// // Start Ignite node with Ignite configuration.
    /// var ignite = Ignition.Start(cfg);
    /// </code>
    /// </example>
    /// </summary>
    public static class Ignition
    {
        /** */
        private const string DefaultCfg = "config/default-config.xml";

        /** */
        private static readonly object SyncRoot = new object();

        /** GC warning flag. */
        private static int _gcWarn;

        /** */
        private static readonly IDictionary<NodeKey, Ignite> Nodes = new Dictionary<NodeKey, Ignite>();
        
        /** Current DLL name. */
        private static readonly string IgniteDllName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);

        /** Startup info. */
        [ThreadStatic]
        private static Startup _startup;

        /** Client mode flag. */
        [ThreadStatic]
        private static bool _clientMode;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static Ignition()
        {
            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
        }

        /// <summary>
        /// Gets or sets a value indicating whether Ignite should be started in client mode.
        /// Client nodes cannot hold data in caches.
        /// </summary>
        public static bool ClientMode
        {
            get { return _clientMode; }
            set { _clientMode = value; }
        }

        /// <summary>
        /// Starts Ignite with default configuration. By default this method will
        /// use Ignite configuration defined in <code>IGNITE/config/default-config.xml</code>
        /// configuration file. If such file is not found, then all system defaults will be used.
        /// </summary>
        /// <returns>Started Ignite.</returns>
        public static IIgnite Start()
        {
            return Start(new IgniteConfiguration());
        }

        /// <summary>
        /// Starts all grids specified within given Spring XML configuration file. If Ignite with given name
        /// is already started, then exception is thrown. In this case all instances that may
        /// have been started so far will be stopped too.
        /// </summary>
        /// <param name="springCfgPath">Spring XML configuration file path or URL. Note, that the path can be
        /// absolute or relative to IGNITE_HOME.</param>
        /// <returns>Started Ignite. If Spring configuration contains multiple Ignite instances, then the 1st
        /// found instance is returned.</returns>
        public static IIgnite Start(string springCfgPath)
        {
            return Start(new IgniteConfiguration {SpringConfigUrl = springCfgPath});
        }

        /// <summary>
        /// Starts Ignite with given configuration.
        /// </summary>
        /// <returns>Started Ignite.</returns>
        public unsafe static IIgnite Start(IgniteConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            // Copy configuration to avoid changes to user-provided instance.
            IgniteConfigurationEx cfgEx = cfg as IgniteConfigurationEx;

            cfg = cfgEx == null ? new IgniteConfiguration(cfg) : new IgniteConfigurationEx(cfgEx);

            // Set default Spring config if needed.
            if (cfg.SpringConfigUrl == null)
                cfg.SpringConfigUrl = DefaultCfg;

            lock (SyncRoot)
            {
                // 1. Check GC settings.
                CheckServerGc(cfg);

                // 2. Create context.
                IgniteUtils.LoadDlls(cfg.JvmDllPath);

                var cbs = new UnmanagedCallbacks();

                void* ctx = IgniteManager.GetContext(cfg, cbs);

                sbyte* cfgPath0 = IgniteUtils.StringToUtf8Unmanaged(cfg.SpringConfigUrl ?? DefaultCfg);

                string gridName = cfgEx != null ? cfgEx.GridName : null;
                sbyte* gridName0 = IgniteUtils.StringToUtf8Unmanaged(gridName);

                // 3. Create startup object which will guide us through the rest of the process.
                _startup = new Startup(cfg, cbs) { Context = ctx };

                IUnmanagedTarget interopProc = null;

                try
                {
                    // 4. Initiate Ignite start.
                    UU.IgnitionStart(cbs.Context, cfg.SpringConfigUrl ?? DefaultCfg,
                        cfgEx != null ? cfgEx.GridName : null, ClientMode);

                    // 5. At this point start routine is finished. We expect STARTUP object to have all necessary data.
                    var node = _startup.Ignite;
                    interopProc = node.InteropProcessor;

                    // 6. On-start callback (notify lifecycle components).
                    node.OnStart();

                    Nodes[new NodeKey(_startup.Name)] = node;

                    return node;
                }
                catch (Exception)
                {
                    // 1. Perform keys cleanup.
                    string name = _startup.Name;

                    if (name != null)
                    {
                        NodeKey key = new NodeKey(name);

                        if (Nodes.ContainsKey(key))
                            Nodes.Remove(key);
                    }

                    // 2. Stop Ignite node if it was started.
                    if (interopProc != null)
                        UU.IgnitionStop(interopProc.Context, gridName, true);

                    // 3. Throw error further (use startup error if exists because it is more precise).
                    if (_startup.Error != null)
                        throw _startup.Error;

                    throw;
                }
                finally
                {
                    _startup = null;

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
        private static void CheckServerGc(IgniteConfiguration cfg)
        {
            if (!cfg.SuppressWarnings && !GCSettings.IsServerGC && Interlocked.CompareExchange(ref _gcWarn, 1, 0) == 0)
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
                _startup.Error = e;

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
            IgniteConfiguration cfg = _startup.Configuration;

            LoadAssemblies(cfg.Assemblies);

            if (dotNetCfg != null)
                LoadAssemblies(dotNetCfg.Assemblies);

            // 2. Create marshaller only after assemblies are loaded.
            if (cfg.PortableConfiguration == null && dotNetCfg != null && dotNetCfg.PortableCfg != null)
                cfg.PortableConfiguration = dotNetCfg.PortableCfg.ToPortableConfiguration();

            _startup.Marshaller = new PortableMarshaller(cfg.PortableConfiguration);
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
            ICollection<ILifecycleBean> nativeBeans = _startup.Configuration.LifecycleBeans;

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
            _startup.LifecycleBeans = beans;
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

            object bean = IgniteUtils.CreateInstance(assemblyName, clsName);

            // 2. Set properties.
            IDictionary<string, object> props = reader.ReadGenericDictionary<string, object>();

            IgniteUtils.SetProperties(bean, props);

            return bean as ILifecycleBean;
        }

        /// <summary>
        /// Kernal start callback.
        /// </summary>
        /// <param name="interopProc">Interop processor.</param>
        /// <param name="stream">Stream.</param>
        internal static void OnStart(IUnmanagedTarget interopProc, IPortableStream stream)
        {
            try
            {
                // 1. Read data and leave critical state ASAP.
                PortableReaderImpl reader = PU.Marshaller.StartUnmarshal(stream);
                
                // ReSharper disable once PossibleInvalidOperationException
                var name = reader.ReadString();
                
                // 2. Set ID and name so that Start() method can use them later.
                _startup.Name = name;

                if (Nodes.ContainsKey(new NodeKey(name)))
                    throw new IgniteException("Ignite with the same name already started: " + name);

                _startup.Ignite = new Ignite(_startup.Configuration, _startup.Name, interopProc, _startup.Marshaller, 
                    _startup.LifecycleBeans, _startup.Callbacks);
            }
            catch (Exception e)
            {
                // 5. Preserve exception to throw it later in the "Start" method and throw it further
                //    to abort startup in Java.
                _startup.Error = e;

                throw;
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
        /// Whether assembly points to Ignite binary.
        /// </summary>
        /// <param name="assembly">Assembly to check..</param>
        /// <returns><c>True</c> if this is one of GG assemblies.</returns>
        private static bool SelfAssembly(string assembly)
        {
            return assembly.EndsWith(IgniteDllName, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets a named Ignite instance. If Ignite name is {@code null} or empty string,
        /// then default no-name Ignite will be returned. Note that caller of this method
        /// should not assume that it will return the same instance every time.
        /// <p/>
        /// Note that single process can run multiple Ignite instances and every Ignite instance (and its
        /// node) can belong to a different grid. Ignite name defines what grid a particular Ignite
        /// instance (and correspondingly its node) belongs to.
        /// </summary>
        /// <param name="name">Ignite name to which requested Ignite instance belongs. If <code>null</code>,
        /// then Ignite instance belonging to a default no-name Ignite will be returned.
        /// </param>
        /// <returns>An instance of named grid.</returns>
        public static IIgnite GetIgnite(string name)
        {
            lock (SyncRoot)
            {
                Ignite result;

                if (!Nodes.TryGetValue(new NodeKey(name), out result))
                    throw new IgniteException("Ignite instance was not properly started or was already stopped: " + name);

                return result;
            }
        }

        /// <summary>
        /// Gets an instance of default no-name grid. Note that
        /// caller of this method should not assume that it will return the same
        /// instance every time.
        /// </summary>
        /// <returns>An instance of default no-name grid.</returns>
        public static IIgnite GetIgnite()
        {
            return GetIgnite(null);
        }

        /// <summary>
        /// Stops named grid. If <code>cancel</code> flag is set to <code>true</code> then
        /// all jobs currently executing on local node will be interrupted. If
        /// grid name is <code>null</code>, then default no-name Ignite will be stopped.
        /// </summary>
        /// <param name="name">Grid name. If <code>null</code>, then default no-name Ignite will be stopped.</param>
        /// <param name="cancel">If <code>true</code> then all jobs currently executing will be cancelled
        /// by calling <code>ComputeJob.cancel</code>method.</param>
        /// <returns><code>true</code> if named Ignite instance was indeed found and stopped, <code>false</code>
        /// othwerwise (the instance with given <code>name</code> was not found).</returns>
        public static bool Stop(string name, bool cancel)
        {
            lock (SyncRoot)
            {
                NodeKey key = new NodeKey(name);

                Ignite node;

                if (!Nodes.TryGetValue(key, out node))
                    return false;

                node.Stop(cancel);

                Nodes.Remove(key);
                
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
            lock (SyncRoot)
            {
                while (Nodes.Count > 0)
                {
                    var entry = Nodes.First();
                    
                    entry.Value.Stop(cancel);

                    Nodes.Remove(entry.Key);
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

        /// <summary>
        /// Grid key.
        /// </summary>
        private class NodeKey
        {
            /** */
            private readonly string _name;

            /// <summary>
            /// Initializes a new instance of the <see cref="NodeKey"/> class.
            /// </summary>
            /// <param name="name">The name.</param>
            internal NodeKey(string name)
            {
                _name = name;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                var other = obj as NodeKey;

                return other != null && Equals(_name, other._name);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return _name == null ? 0 : _name.GetHashCode();
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
            /// <param name="cbs"></param>
            internal Startup(IgniteConfiguration cfg, UnmanagedCallbacks cbs)
            {
                Configuration = cfg;
                Callbacks = cbs;
            }
            /// <summary>
            /// Configuration.
            /// </summary>
            internal IgniteConfiguration Configuration { get; private set; }

            /// <summary>
            /// Gets unmanaged callbacks.
            /// </summary>
            internal UnmanagedCallbacks Callbacks { get; private set; }

            /// <summary>
            /// Lifecycle beans.
            /// </summary>
            internal IList<LifecycleBeanHolder> LifecycleBeans { get; set; }

            /// <summary>
            /// Node name.
            /// </summary>
            internal string Name { get; set; }

            /// <summary>
            /// Marshaller.
            /// </summary>
            internal PortableMarshaller Marshaller { get; set; }

            /// <summary>
            /// Start error.
            /// </summary>
            internal Exception Error { get; set; }

            /// <summary>
            /// Gets or sets the context.
            /// </summary>
            internal void* Context { get; set; }

            /// <summary>
            /// Gets or sets the ignite.
            /// </summary>
            internal Ignite Ignite { get; set; }
        }
    }
}
