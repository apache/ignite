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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Log;
    using Microsoft.Win32;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;

    /// <summary>
    /// Native utility methods.
    /// </summary>
    internal static class IgniteUtils
    {
        /** Environment variable: JAVA_HOME. */
        private const string EnvJavaHome = "JAVA_HOME";

        /** Lookup paths. */
        private static readonly string[] JvmDllLookupPaths = Os.IsWindows
            ? new[]
            {
                // JRE paths
                @"bin\server",
                @"bin\client",

                // JDK paths
                @"jre\bin\server",
                @"jre\bin\client",
                @"jre\bin\default"
            }
            : new[]
            {
                // JRE paths
                "lib/amd64/server",
                "lib/amd64/client",

                // JDK paths
                "jre/lib/amd64/server",
                "jre/lib/amd64/client"
            };

        /** Registry lookup paths. */
        private static readonly string[] JreRegistryKeys =
        {
            @"Software\JavaSoft\Java Runtime Environment",
            @"Software\Wow6432Node\JavaSoft\Java Runtime Environment"
        };

        /** Jvm dll file name. */
        internal static readonly string FileJvmDll = Os.IsWindows ? "jvm.dll" : "libjvm.so";

        /** Prefix for temp directory names. */
        private const string DirIgniteTmp = "Ignite_";
        
        /** Loaded. */
        private static bool _loaded;        

        /** Thread-local random. */
        [ThreadStatic]
        private static Random _rnd;

        /// <summary>
        /// Gets thread local random.
        /// </summary>
        /// <value>Thread local random.</value>
        public static Random ThreadLocalRandom
        {
            get { return _rnd ?? (_rnd = new Random()); }
        }

        /// <summary>
        /// Returns shuffled list copy.
        /// </summary>
        /// <returns>Shuffled list copy.</returns>
        public static IList<T> Shuffle<T>(IList<T> list)
        {
            int cnt = list.Count;

            if (cnt > 1) {
                List<T> res = new List<T>(list);

                Random rnd = ThreadLocalRandom;

                while (cnt > 1)
                {
                    cnt--;
                    
                    int idx = rnd.Next(cnt + 1);

                    T val = res[idx];
                    res[idx] = res[cnt];
                    res[cnt] = val;
                }

                return res;
            }
            return list;
        }

        /// <summary>
        /// Load JVM DLL if needed.
        /// </summary>
        /// <param name="configJvmDllPath">JVM DLL path from config.</param>
        /// <param name="log">Log.</param>
        public static void LoadDlls(string configJvmDllPath, ILogger log)
        {
            Console.WriteLine("|| LoadDlls: " + _loaded);

            if (_loaded)
            {
                log.Debug("JNI dll is already loaded.");
                return;
            }

            // 1. Load JNI dll.
            LoadJvmDll(configJvmDllPath, log);

            _loaded = true;
        }

        /// <summary>
        /// Create new instance of specified class.
        /// </summary>
        /// <param name="typeName">Class name</param>
        /// <param name="props">Properties to set.</param>
        /// <returns>New Instance.</returns>
        public static T CreateInstance<T>(string typeName, IEnumerable<KeyValuePair<string, object>> props = null)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var type = new TypeResolver().ResolveType(typeName);

            if (type == null)
                throw new IgniteException("Failed to create class instance [className=" + typeName + ']');

            var res =  (T) Activator.CreateInstance(type);

            if (props != null)
                SetProperties(res, props);

            return res;
        }

        /// <summary>
        /// Set properties on the object.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="props">Properties.</param>
        private static void SetProperties(object target, IEnumerable<KeyValuePair<string, object>> props)
        {
            if (props == null)
                return;

            IgniteArgumentCheck.NotNull(target, "target");

            Type typ = target.GetType();

            foreach (KeyValuePair<string, object> prop in props)
            {
                PropertyInfo prop0 = typ.GetProperty(prop.Key, 
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

                if (prop0 == null)
                    throw new IgniteException("Property is not found [type=" + typ.Name + 
                        ", property=" + prop.Key + ']');

                prop0.SetValue(target, prop.Value, null);
            }
        }

        /// <summary>
        /// Loads the JVM DLL.
        /// </summary>
        private static void LoadJvmDll(string configJvmDllPath, ILogger log)
        {
            Console.WriteLine("|| LoadJvmDll");  // TODO

            var messages = new List<string>();
            foreach (var dllPath in GetJvmDllPaths(configJvmDllPath))
            {
                log.Debug("Trying to load JVM dll from [option={0}, path={1}]...", dllPath.Key, dllPath.Value);

                var errInfo = LoadDll(dllPath.Value, FileJvmDll);
                if (errInfo == null)
                {
                    log.Debug("jvm.dll successfully loaded from [option={0}, path={1}]", dllPath.Key, dllPath.Value);
                    return;
                }

                var message = string.Format(CultureInfo.InvariantCulture, "[option={0}, path={1}, error={2}]",
                                                  dllPath.Key, dllPath.Value, errInfo);
                messages.Add(message);

                log.Debug("Failed to load jvm.dll: " + message);

                if (dllPath.Value == configJvmDllPath)
                    break;  // if configJvmDllPath is specified and is invalid - do not try other options
            }

            if (!messages.Any())  // not loaded and no messages - everything was null
                messages.Add(string.Format(CultureInfo.InvariantCulture, 
                    "Please specify IgniteConfiguration.JvmDllPath or {0}.", EnvJavaHome));

            if (messages.Count == 1)
                throw new IgniteException(string.Format(CultureInfo.InvariantCulture, "Failed to load {0} ({1})", 
                    FileJvmDll, messages[0]));

            var combinedMessage =
                messages.Aggregate((x, y) => string.Format(CultureInfo.InvariantCulture, "{0}\n{1}", x, y));

            throw new IgniteException(string.Format(CultureInfo.InvariantCulture, "Failed to load {0}:\n{1}", 
                FileJvmDll, combinedMessage));
        }

        /// <summary>
        /// Try loading DLLs first using file path, then using it's simple name.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="simpleName"></param>
        /// <returns>Null in case of success, error info in case of failure.</returns>
        private static string LoadDll(string filePath, string simpleName)
        {
            Console.WriteLine("LoadDll: " + filePath);  // TODO

            string res = null;

            if (filePath != null)
            {
                res = DllLoader.Load(filePath);

                if (res == null)
                {
                    return null;  // Success.
                }

                Console.WriteLine("LoadDll fail: " + res);  // TODO
            }

            // Failed to load using file path, fallback to simple name.
            var res2 = DllLoader.Load(simpleName);

            if (res2 == null)
            {
                return null;  // Success.
            }

            return res;
        }

        /// <summary>
        /// Gets the JVM DLL paths in order of lookup priority.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPaths(string configJvmDllPath)
        {
            if (!string.IsNullOrEmpty(configJvmDllPath))
            {
                yield return new KeyValuePair<string, string>("IgniteConfiguration.JvmDllPath", configJvmDllPath);
            }

            var javaHomeDir = Environment.GetEnvironmentVariable(EnvJavaHome);

            if (!string.IsNullOrEmpty(javaHomeDir))
            {
                foreach (var path in JvmDllLookupPaths)
                {
                    yield return
                        new KeyValuePair<string, string>(EnvJavaHome, Path.Combine(javaHomeDir, path, FileJvmDll));
                }
            }

            foreach (var keyValuePair in GetJvmDllPathsFromRegistry().Concat(GetJvmDllPathsFromSymlink()))
            {
                yield return keyValuePair;
            }
        }

        /// <summary>
        /// Gets Jvm dll paths from Windows registry.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPathsFromRegistry()
        {
            if (!Os.IsWindows)
            {
                yield break;
            }

            foreach (var regPath in JreRegistryKeys)
            {
                using (var jSubKey = Registry.LocalMachine.OpenSubKey(regPath))
                {
                    if (jSubKey == null)
                        continue;

                    var curVer = jSubKey.GetValue("CurrentVersion") as string;

                    // Current version comes first
                    var versions = new[] {curVer}.Concat(jSubKey.GetSubKeyNames().Where(x => x != curVer));

                    foreach (var ver in versions.Where(v => !string.IsNullOrEmpty(v)))
                    {
                        using (var verKey = jSubKey.OpenSubKey(ver))
                        {
                            var dllPath = verKey == null ? null : verKey.GetValue("RuntimeLib") as string;

                            if (dllPath != null)
                                yield return new KeyValuePair<string, string>(verKey.Name, dllPath);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the Jvm dll paths from symlink.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPathsFromSymlink()
        {
            if (Os.IsWindows)
            {
                yield break;
            }

            const string javaExec = "/usr/bin/java";
            if (!File.Exists(javaExec))
            {
                yield break;
            }

            var file = Shell.BashExecute("readlink -f /usr/bin/java");
            // /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

            var dir = Path.GetDirectoryName(file);
            // /usr/lib/jvm/java-8-openjdk-amd64/jre/bin

            if (dir == null)
            {
                yield break;
            }

            var libFolder = Path.GetFullPath(Path.Combine(dir, "../lib/"));
            if (!Directory.Exists(libFolder))
            {
                yield break;
            }

            // Predefined path: /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so
            yield return new KeyValuePair<string, string>(javaExec,
                Path.Combine(libFolder, "amd64", "server", FileJvmDll));

            // Last resort - custom paths:
            foreach (var f in Directory.GetFiles(libFolder, FileJvmDll, SearchOption.AllDirectories))
            {
                yield return new KeyValuePair<string, string>(javaExec, f);
            }
        }

        /// <summary>
        /// Creates a uniquely named, empty temporary directory on disk and returns the full path of that directory.
        /// </summary>
        /// <returns>The full path of the temporary directory.</returns>
        internal static string GetTempDirectoryName()
        {
            var baseDir = Path.Combine(Path.GetTempPath(), DirIgniteTmp);

            while (true)
            {
                try
                {
                    return Directory.CreateDirectory(baseDir + Path.GetRandomFileName()).FullName;
                }
                catch (IOException)
                {
                    // Expected
                }
                catch (UnauthorizedAccessException)
                {
                    // Expected
                }
            }
        }

        /// <summary>
        /// Convert unmanaged char array to string.
        /// </summary>
        /// <param name="chars">Char array.</param>
        /// <param name="charsLen">Char array length.</param>
        /// <returns></returns>
        public static unsafe string Utf8UnmanagedToString(sbyte* chars, int charsLen)
        {
            IntPtr ptr = new IntPtr(chars);

            if (ptr == IntPtr.Zero)
                return null;

            byte[] arr = new byte[charsLen];

            Marshal.Copy(ptr, arr, 0, arr.Length);

            return Encoding.UTF8.GetString(arr);
        }

        /// <summary>
        /// Convert string to unmanaged byte array.
        /// </summary>
        /// <param name="str">String.</param>
        /// <returns>Unmanaged byte array.</returns>
        public static unsafe sbyte* StringToUtf8Unmanaged(string str)
        {
            var ptr = IntPtr.Zero;

            if (str != null)
            {
                byte[] strBytes = Encoding.UTF8.GetBytes(str);

                ptr = Marshal.AllocHGlobal(strBytes.Length + 1);

                Marshal.Copy(strBytes, 0, ptr, strBytes.Length);

                *((byte*)ptr.ToPointer() + strBytes.Length) = 0; // NULL-terminator.
            }
            
            return (sbyte*)ptr.ToPointer();
        }

        /// <summary>
        /// Reads node collection from stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="pred">The predicate.</param>
        /// <returns> Nodes list or null. </returns>
        public static List<IClusterNode> ReadNodes(BinaryReader reader, Func<ClusterNodeImpl, bool> pred = null)
        {
            var cnt = reader.ReadInt();

            if (cnt < 0)
                return null;

            var res = new List<IClusterNode>(cnt);

            var ignite = reader.Marshaller.Ignite;

            if (pred == null)
            {
                for (var i = 0; i < cnt; i++)
                    res.Add(ignite.GetNode(reader.ReadGuid()));
            }
            else
            {
                for (var i = 0; i < cnt; i++)
                {
                    var node = ignite.GetNode(reader.ReadGuid());
                    
                    if (pred(node))
                        res.Add(node);
                }
            }

            return res;
        }

        /// <summary>
        /// Encodes the peek modes into a single int value.
        /// </summary>
        public static int EncodePeekModes(CachePeekMode[] modes)
        {
            var res = 0;

            if (modes == null)
            {
                return res;
            }

            foreach (var mode in modes)
            {
                res |= (int)mode;
            }

            return res;
        }
    }
}
