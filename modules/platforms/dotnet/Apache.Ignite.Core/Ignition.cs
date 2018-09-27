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
    using System.Configuration;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Log;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Resource;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// This class defines a factory for the main Ignite API.
    /// <p/>
    /// Use <see cref="Start()"/> method to start Ignite with default configuration.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public static class Ignition
    {
        /// <summary>
        /// Default configuration section name.
        /// </summary>
        public const string ConfigurationSectionName = "igniteConfiguration";

        /// <summary>
        /// Default configuration section name.
        /// </summary>
        public const string ClientConfigurationSectionName = "igniteClientConfiguration";

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
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static Ignition()
        {
            AppDomain.CurrentDomain.DomainUnload += CurrentDomain_DomainUnload;
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
        /// use Ignite configuration defined in <c>{IGNITE_HOME}/config/default-config.xml</c>
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
        /// Reads <see cref="IgniteConfiguration"/> from application configuration
        /// <see cref="IgniteConfigurationSection"/> with <see cref="ConfigurationSectionName"/>
        /// name and starts Ignite.
        /// </summary>
        /// <returns>Started Ignite.</returns>
        public static IIgnite StartFromApplicationConfiguration()
        {
            // ReSharper disable once IntroduceOptionalParameters.Global
            return StartFromApplicationConfiguration(ConfigurationSectionName);
        }

        /// <summary>
        /// Reads <see cref="IgniteConfiguration"/> from application configuration 
        /// <see cref="IgniteConfigurationSection"/> with specified name and starts Ignite.
        /// </summary>
        /// <param name="sectionName">Name of the section.</param>
        /// <returns>Started Ignite.</returns>
        public static IIgnite StartFromApplicationConfiguration(string sectionName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(sectionName, "sectionName");

            var section = ConfigurationManager.GetSection(sectionName) as IgniteConfigurationSection;

            if (section == null)
                throw new ConfigurationErrorsException(string.Format("Could not find {0} with name '{1}'",
                    typeof(IgniteConfigurationSection).Name, sectionName));

            if (section.IgniteConfiguration == null)
                throw new ConfigurationErrorsException(
                    string.Format("{0} with name '{1}' is defined in <configSections>, " +
                                  "but not present in configuration.",
                        typeof(IgniteConfigurationSection).Name, sectionName));

            return Start(section.IgniteConfiguration);
        }

        /// <summary>
        /// Reads <see cref="IgniteConfiguration" /> from application configuration
        /// <see cref="IgniteConfigurationSection" /> with specified name and starts Ignite.
        /// </summary>
        /// <param name="sectionName">Name of the section.</param>
        /// <param name="configPath">Path to the configuration file.</param>
        /// <returns>Started Ignite.</returns>
        public static IIgnite StartFromApplicationConfiguration(string sectionName, string configPath)
        {
            var section = GetConfigurationSection<IgniteConfigurationSection>(sectionName, configPath);

            if (section.IgniteConfiguration == null)
            {
                throw new ConfigurationErrorsException(
                    string.Format("{0} with name '{1}' in file '{2}' is defined in <configSections>, " +
                                  "but not present in configuration.",
                        typeof(IgniteConfigurationSection).Name, sectionName, configPath));
            }

            return Start(section.IgniteConfiguration);
        }

        /// <summary>
        /// Gets the configuration section.
        /// </summary>
        private static T GetConfigurationSection<T>(string sectionName, string configPath)
            where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(sectionName, "sectionName");
            IgniteArgumentCheck.NotNullOrEmpty(configPath, "configPath");

            var fileMap = GetConfigMap(configPath);
            var config = ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None);

            var section = config.GetSection(sectionName) as T;

            if (section == null)
            {
                throw new ConfigurationErrorsException(
                    string.Format("Could not find {0} with name '{1}' in file '{2}'",
                        typeof(T).Name, sectionName, configPath));
            }

            return section;
        }

        /// <summary>
        /// Gets the configuration file map.
        /// </summary>
        private static ExeConfigurationFileMap GetConfigMap(string fileName)
        {
            var fullFileName = Path.GetFullPath(fileName);

            if (!File.Exists(fullFileName))
                throw new ConfigurationErrorsException("Specified config file does not exist: " + fileName);

            return new ExeConfigurationFileMap { ExeConfigFilename = fullFileName };
        }

        /// <summary>
        /// Starts Ignite with given configuration.
        /// </summary>
        /// <returns>Started Ignite.</returns>
        public static IIgnite Start(IgniteConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            cfg = new IgniteConfiguration(cfg);  // Create a copy so that config can be modified and reused.

            lock (SyncRoot)
            {
                // 0. Init logger
                var log = cfg.Logger ?? new JavaLogger();

                log.Debug("Starting Ignite.NET " + Assembly.GetExecutingAssembly().GetName().Version);

                // 1. Check GC settings.
                CheckServerGc(cfg, log);

                // 2. Create context.
                JvmDll.Load(cfg.JvmDllPath, log);

                var cbs = IgniteManager.CreateJvmContext(cfg, log);
                var env = cbs.Jvm.AttachCurrentThread();
                log.Debug("JVM started.");

                var gridName = cfg.IgniteInstanceName;

                if (cfg.AutoGenerateIgniteInstanceName)
                {
                    gridName = (gridName ?? "ignite-instance-") + Guid.NewGuid();
                }

                // 3. Create startup object which will guide us through the rest of the process.
                _startup = new Startup(cfg, cbs);

                PlatformJniTarget interopProc = null;

                try
                {
                    // 4. Initiate Ignite start.
                    UU.IgnitionStart(env, cfg.SpringConfigUrl, gridName, ClientMode, cfg.Logger != null, cbs.IgniteId,
                        cfg.RedirectJavaConsoleOutput);

                    // 5. At this point start routine is finished. We expect STARTUP object to have all necessary data.
                    var node = _startup.Ignite;
                    interopProc = (PlatformJniTarget)node.InteropProcessor;

                    var javaLogger = log as JavaLogger;
                    if (javaLogger != null)
                    {
                        javaLogger.SetIgnite(node);
                    }

                    // 6. On-start callback (notify lifecycle components).
                    node.OnStart();

                    Nodes[new NodeKey(_startup.Name)] = node;

                    return node;
                }
                catch (Exception ex)
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
                        UU.IgnitionStop(gridName, true);

                    // 3. Throw error further (use startup error if exists because it is more precise).
                    if (_startup.Error != null)
                    {
                        // Wrap in a new exception to preserve original stack trace.
                        throw new IgniteException("Failed to start Ignite.NET, check inner exception for details",
                            _startup.Error);
                    }

                    var jex = ex as JavaException;

                    if (jex == null)
                    {
                        throw;
                    }

                    throw ExceptionUtils.GetException(null, jex);
                }
                finally
                {
                    var ignite = _startup.Ignite;

                    _startup = null;

                    if (ignite != null)
                    {
                        ignite.ProcessorReleaseStart();
                    }
                }
            }
        }

        /// <summary>
        /// Check whether GC is set to server mode.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="log">Log.</param>
        private static void CheckServerGc(IgniteConfiguration cfg, ILogger log)
        {
            if (!cfg.SuppressWarnings && !GCSettings.IsServerGC && Interlocked.CompareExchange(ref _gcWarn, 1, 0) == 0)
                log.Warn("GC server mode is not enabled, this could lead to less " +
                    "than optimal performance on multi-core machines (to enable see " +
                    "http://msdn.microsoft.com/en-us/library/ms229357(v=vs.110).aspx).");
        }

        /// <summary>
        /// Prepare callback invoked from Java.
        /// </summary>
        /// <param name="inStream">Input stream with data.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="handleRegistry">Handle registry.</param>
        /// <param name="log">Log.</param>
        internal static void OnPrepare(PlatformMemoryStream inStream, PlatformMemoryStream outStream,
            HandleRegistry handleRegistry, ILogger log)
        {
            try
            {
                BinaryReader reader = BinaryUtils.Marshaller.StartUnmarshal(inStream);

                PrepareConfiguration(reader, outStream, log);

                PrepareLifecycleHandlers(reader, outStream, handleRegistry);

                PrepareAffinityFunctions(reader, outStream);

                outStream.SynchronizeOutput();
            }
            catch (Exception e)
            {
                _startup.Error = e;

                throw;
            }
        }

        /// <summary>
        /// Prepare configuration.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="outStream">Response stream.</param>
        /// <param name="log">Log.</param>
        private static void PrepareConfiguration(BinaryReader reader, PlatformMemoryStream outStream, ILogger log)
        {
            // 1. Load assemblies.
            IgniteConfiguration cfg = _startup.Configuration;

            LoadAssemblies(cfg.Assemblies);

            ICollection<string> cfgAssembllies;
            BinaryConfiguration binaryCfg;

            BinaryUtils.ReadConfiguration(reader, out cfgAssembllies, out binaryCfg);

            LoadAssemblies(cfgAssembllies);

            // 2. Create marshaller only after assemblies are loaded.
            if (cfg.BinaryConfiguration == null)
                cfg.BinaryConfiguration = binaryCfg;

            _startup.Marshaller = new Marshaller(cfg.BinaryConfiguration, log);

            // 3. Send configuration details to Java
            cfg.Validate(log);
            // Use system marshaller.
            cfg.Write(BinaryUtils.Marshaller.StartMarshal(outStream), ClientSocket.CurrentProtocolVersion);
        }

        /// <summary>
        /// Prepare lifecycle handlers.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="handleRegistry">Handle registry.</param>
        private static void PrepareLifecycleHandlers(IBinaryRawReader reader, IBinaryStream outStream,
            HandleRegistry handleRegistry)
        {
            IList<LifecycleHandlerHolder> beans = new List<LifecycleHandlerHolder>
            {
                new LifecycleHandlerHolder(new InternalLifecycleHandler())   // add internal bean for events
            };

            // 1. Read beans defined in Java.
            int cnt = reader.ReadInt();

            for (int i = 0; i < cnt; i++)
                beans.Add(new LifecycleHandlerHolder(CreateObject<ILifecycleHandler>(reader)));

            // 2. Append beans defined in local configuration.
            ICollection<ILifecycleHandler> nativeBeans = _startup.Configuration.LifecycleHandlers;

            if (nativeBeans != null)
            {
                foreach (ILifecycleHandler nativeBean in nativeBeans)
                    beans.Add(new LifecycleHandlerHolder(nativeBean));
            }

            // 3. Write bean pointers to Java stream.
            outStream.WriteInt(beans.Count);

            foreach (LifecycleHandlerHolder bean in beans)
                outStream.WriteLong(handleRegistry.AllocateCritical(bean));

            // 4. Set beans to STARTUP object.
            _startup.LifecycleHandlers = beans;
        }

        /// <summary>
        /// Prepares the affinity functions.
        /// </summary>
        private static void PrepareAffinityFunctions(BinaryReader reader, PlatformMemoryStream outStream)
        {
            var cnt = reader.ReadInt();

            var writer = reader.Marshaller.StartMarshal(outStream);

            for (var i = 0; i < cnt; i++)
            {
                var objHolder = new ObjectInfoHolder(reader);
                AffinityFunctionSerializer.Write(writer, objHolder.CreateInstance<IAffinityFunction>(), objHolder);
            }
        }

        /// <summary>
        /// Creates an object and sets the properties.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Resulting object.</returns>
        private static T CreateObject<T>(IBinaryRawReader reader)
        {
            return IgniteUtils.CreateInstance<T>(reader.ReadString(),
                reader.ReadDictionaryAsGeneric<string, object>());
        }

        /// <summary>
        /// Kernal start callback.
        /// </summary>
        /// <param name="interopProc">Interop processor.</param>
        /// <param name="stream">Stream.</param>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "PlatformJniTarget is passed further")]
        internal static void OnStart(GlobalRef interopProc, IBinaryStream stream)
        {
            try
            {
                // 1. Read data and leave critical state ASAP.
                BinaryReader reader = BinaryUtils.Marshaller.StartUnmarshal(stream);
                
                // ReSharper disable once PossibleInvalidOperationException
                var name = reader.ReadString();
                
                // 2. Set ID and name so that Start() method can use them later.
                _startup.Name = name;

                if (Nodes.ContainsKey(new NodeKey(name)))
                    throw new IgniteException("Ignite with the same name already started: " + name);

                _startup.Ignite = new Ignite(_startup.Configuration, _startup.Name,
                    new PlatformJniTarget(interopProc, _startup.Marshaller), _startup.Marshaller,
                    _startup.LifecycleHandlers, _startup.Callbacks);
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

                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
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
        /// Gets a named Ignite instance. If Ignite name is <c>null</c> or empty string,
        /// then default no-name Ignite will be returned. Note that caller of this method
        /// should not assume that it will return the same instance every time.
        /// <p />
        /// Note that single process can run multiple Ignite instances and every Ignite instance (and its
        /// node) can belong to a different grid. Ignite name defines what grid a particular Ignite
        /// instance (and correspondingly its node) belongs to.
        /// </summary>
        /// <param name="name">Ignite name to which requested Ignite instance belongs. If <c>null</c>,
        /// then Ignite instance belonging to a default no-name Ignite will be returned.</param>
        /// <returns>
        /// An instance of named grid.
        /// </returns>
        /// <exception cref="IgniteException">When there is no Ignite instance with specified name.</exception>
        public static IIgnite GetIgnite(string name)
        {
            var ignite = TryGetIgnite(name);

            if (ignite == null)
                throw new IgniteException("Ignite instance was not properly started or was already stopped: " + name);

            return ignite;
        }

        /// <summary>
        /// Gets the default Ignite instance with null name, or an instance with any name when there is only one.
        /// <para />
        /// Note that caller of this method should not assume that it will return the same instance every time.
        /// </summary>
        /// <returns>Default Ignite instance.</returns>
        /// <exception cref="IgniteException">When there is no matching Ignite instance.</exception>
        public static IIgnite GetIgnite()
        {
            lock (SyncRoot)
            {
                if (Nodes.Count == 0)
                {
                    throw new IgniteException("Failed to get default Ignite instance: " +
                                              "there are no instances started.");
                }

                if (Nodes.Count == 1)
                {
                    return Nodes.Single().Value;
                }

                Ignite result;

                if (Nodes.TryGetValue(new NodeKey(null), out result))
                {
                    return result;
                }

                throw new IgniteException(string.Format("Failed to get default Ignite instance: " +
                    "there are {0} instances started, and none of them has null name.", Nodes.Count));
            }
        }

        /// <summary>
        /// Gets all started Ignite instances.
        /// </summary>
        /// <returns>All Ignite instances.</returns>
        public static ICollection<IIgnite> GetAll()
        {
            lock (SyncRoot)
            {
                return Nodes.Values.ToArray();
            }
        }

        /// <summary>
        /// Gets a named Ignite instance, or <c>null</c> if none found. If Ignite name is <c>null</c> or empty string,
        /// then default no-name Ignite will be returned. Note that caller of this method
        /// should not assume that it will return the same instance every time.
        /// <p/>
        /// Note that single process can run multiple Ignite instances and every Ignite instance (and its
        /// node) can belong to a different grid. Ignite name defines what grid a particular Ignite
        /// instance (and correspondingly its node) belongs to.
        /// </summary>
        /// <param name="name">Ignite name to which requested Ignite instance belongs. If <c>null</c>,
        /// then Ignite instance belonging to a default no-name Ignite will be returned.
        /// </param>
        /// <returns>An instance of named grid, or null.</returns>
        public static IIgnite TryGetIgnite(string name)
        {
            lock (SyncRoot)
            {
                Ignite result;

                return !Nodes.TryGetValue(new NodeKey(name), out result) ? null : result;
            }
        }

        /// <summary>
        /// Gets the default Ignite instance with null name, or an instance with any name when there is only one.
        /// Returns null when there are no Ignite instances started, or when there are more than one,
        /// and none of them has null name.
        /// </summary>
        /// <returns>An instance of default no-name grid, or null.</returns>
        public static IIgnite TryGetIgnite()
        {
            lock (SyncRoot)
            {
                if (Nodes.Count == 1)
                {
                    return Nodes.Single().Value;
                }

                return TryGetIgnite(null);
            }
        }

        /// <summary>
        /// Stops named grid. If <c>cancel</c> flag is set to <c>true</c> then
        /// all jobs currently executing on local node will be interrupted. If
        /// grid name is <c>null</c>, then default no-name Ignite will be stopped.
        /// </summary>
        /// <param name="name">Grid name. If <c>null</c>, then default no-name Ignite will be stopped.</param>
        /// <param name="cancel">If <c>true</c> then all jobs currently executing will be cancelled
        /// by calling <c>ComputeJob.cancel</c>method.</param>
        /// <returns><c>true</c> if named Ignite instance was indeed found and stopped, <c>false</c>
        /// othwerwise (the instance with given <c>name</c> was not found).</returns>
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
        /// Stops <b>all</b> started grids. If <c>cancel</c> flag is set to <c>true</c> then
        /// all jobs currently executing on local node will be interrupted.
        /// </summary>
        /// <param name="cancel">If <c>true</c> then all jobs currently executing will be cancelled
        /// by calling <c>ComputeJob.Cancel()</c> method.</param>
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
        /// Connects Ignite lightweight (thin) client to an Ignite node.
        /// <para />
        /// Thin client connects to an existing Ignite node with a socket and does not start JVM in process.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        /// <returns>Ignite client instance.</returns>
        public static IIgniteClient StartClient(IgniteClientConfiguration clientConfiguration)
        {
            IgniteArgumentCheck.NotNull(clientConfiguration, "clientConfiguration");

            return new IgniteClient(clientConfiguration);
        }

        /// <summary>
        /// Reads <see cref="IgniteClientConfiguration"/> from application configuration
        /// <see cref="IgniteClientConfigurationSection"/> with <see cref="ClientConfigurationSectionName"/>
        /// name and connects Ignite lightweight (thin) client to an Ignite node.
        /// <para />
        /// Thin client connects to an existing Ignite node with a socket and does not start JVM in process.
        /// </summary>
        /// <returns>Ignite client instance.</returns>
        public static IIgniteClient StartClient()
        {
            // ReSharper disable once IntroduceOptionalParameters.Global
            return StartClient(ClientConfigurationSectionName);
        }

        /// <summary>
        /// Reads <see cref="IgniteClientConfiguration" /> from application configuration
        /// <see cref="IgniteClientConfigurationSection" /> with specified name and connects
        /// Ignite lightweight (thin) client to an Ignite node.
        /// <para />
        /// Thin client connects to an existing Ignite node with a socket and does not start JVM in process.
        /// </summary>
        /// <param name="sectionName">Name of the configuration section.</param>
        /// <returns>Ignite client instance.</returns>
        public static IIgniteClient StartClient(string sectionName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(sectionName, "sectionName");

            var section = ConfigurationManager.GetSection(sectionName) as IgniteClientConfigurationSection;

            if (section == null)
            {
                throw new ConfigurationErrorsException(string.Format("Could not find {0} with name '{1}'.",
                    typeof(IgniteClientConfigurationSection).Name, sectionName));
            }

            if (section.IgniteClientConfiguration == null)
            {
                throw new ConfigurationErrorsException(
                    string.Format("{0} with name '{1}' is defined in <configSections>, " +
                                  "but not present in configuration.",
                        typeof(IgniteClientConfigurationSection).Name, sectionName));
            }

            return StartClient(section.IgniteClientConfiguration);
        }

        /// <summary>
        /// Reads <see cref="IgniteConfiguration" /> from application configuration
        /// <see cref="IgniteConfigurationSection" /> with specified name and starts Ignite.
        /// </summary>
        /// <param name="sectionName">Name of the section.</param>
        /// <param name="configPath">Path to the configuration file.</param>
        /// <returns>Started Ignite.</returns>
        public static IIgniteClient StartClient(string sectionName, string configPath)
        {
            var section = GetConfigurationSection<IgniteClientConfigurationSection>(sectionName, configPath);

            if (section.IgniteClientConfiguration == null)
            {
                throw new ConfigurationErrorsException(
                    string.Format("{0} with name '{1}' in file '{2}' is defined in <configSections>, " +
                                  "but not present in configuration.",
                        typeof(IgniteClientConfigurationSection).Name, sectionName, configPath));
            }

            return StartClient(section.IgniteClientConfiguration);
        }

        /// <summary>
        /// Handles the DomainUnload event of the CurrentDomain control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="EventArgs"/> instance containing the event data.</param>
        private static void CurrentDomain_DomainUnload(object sender, EventArgs e)
        {
            // If we don't stop Ignite.NET on domain unload,
            // we end up with broken instances in Java (invalid callbacks, etc).
            // IIS, in particular, is known to unload and reload domains within the same process.
            StopAll(true);
        }

        /// <summary>
        /// Grid key. Workaround for non-null key requirement in Dictionary.
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
        private class Startup
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
            /// Lifecycle handlers.
            /// </summary>
            internal IList<LifecycleHandlerHolder> LifecycleHandlers { get; set; }

            /// <summary>
            /// Node name.
            /// </summary>
            internal string Name { get; set; }

            /// <summary>
            /// Marshaller.
            /// </summary>
            internal Marshaller Marshaller { get; set; }

            /// <summary>
            /// Start error.
            /// </summary>
            internal Exception Error { get; set; }

            /// <summary>
            /// Gets or sets the ignite.
            /// </summary>
            internal Ignite Ignite { get; set; }
        }

        /// <summary>
        /// Internal handler for event notification.
        /// </summary>
        private class InternalLifecycleHandler : ILifecycleHandler
        {
            /** */
            #pragma warning disable 649   // unused field
            [InstanceResource] private readonly IIgnite _ignite;

            /** <inheritdoc /> */
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                if (evt == LifecycleEventType.BeforeNodeStop && _ignite != null)
                    ((Ignite) _ignite).BeforeNodeStop();
            }
        }
    }
}
