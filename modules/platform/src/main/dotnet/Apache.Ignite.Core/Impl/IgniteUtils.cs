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
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Native utility methods.
    /// </summary>
    internal static class IgniteUtils
    {
        /** Environment variable: JAVA_HOME. */
        private const string EnvJavaHome = "JAVA_HOME";

        /** Directory: jre. */
        private const string DirJre = "jre";

        /** Directory: bin. */
        private const string DirBin = "bin";

        /** Directory: server. */
        private const string DirServer = "server";

        /** File: jvm.dll. */
        private const string FileJvmDll = "jvm.dll";

        /** File: Ignite.Common.dll. */
        internal const string FileIgniteJniDll = "ignite.common.dll";
        
        /** Prefix for temp directory names. */
        private const string DirIgniteTmp = "Ignite_";
        
        /** Loaded. */
        private static bool _loaded;        

        /** Thread-local random. */
        [ThreadStatic]
        private static Random _rnd;

        /// <summary>
        /// Initializes the <see cref="IgniteUtils"/> class.
        /// </summary>
        static IgniteUtils()
        {
            TryCleanTempDirectories();
        }

        /// <summary>
        /// Gets thread local random.
        /// </summary>
        /// <returns>Thread local random.</returns>
        public static Random ThreadLocalRandom()
        {
            if (_rnd == null)
                _rnd = new Random();

            return _rnd;
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

                Random rnd = ThreadLocalRandom();

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
        public static void LoadDlls(string configJvmDllPath)
        {
            if (_loaded) return;

            // 1. Load JNI dll.
            LoadJvmDll(configJvmDllPath);

            // 2. Load GG JNI dll.
            UnmanagedUtils.Initialize();

            _loaded = true;
        }

        /// <summary>
        /// Create new instance of specified class.
        /// </summary>
        /// <param name="assemblyName">Assembly name.</param>
        /// <param name="clsName">Class name</param>
        /// <returns>New Instance.</returns>
        public static object CreateInstance(string assemblyName, string clsName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(clsName, "clsName");

            var type = new TypeResolver().ResolveType(clsName, assemblyName);

            if (type == null)
                throw new IgniteException("Failed to create class instance [assemblyName=" + assemblyName +
                    ", className=" + clsName + ']');

            return Activator.CreateInstance(type);
        }

        /// <summary>
        /// Set properties on the object.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="props">Properties.</param>
        public static void SetProperties(object target, IEnumerable<KeyValuePair<string, object>> props)
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
        private static void LoadJvmDll(string configJvmDllPath)
        {
            var messages = new List<string>();
            foreach (var dllPath in GetJvmDllPaths(configJvmDllPath))
            {
                var errCode = LoadDll(dllPath.Value, FileJvmDll);
                if (errCode == 0)
                    return;

                messages.Add(string.Format("[option={0}, path={1}, errorCode={2}]", 
                    dllPath.Key, dllPath.Value, errCode));

                if (dllPath.Value == configJvmDllPath)
                    break;  // if configJvmDllPath is specified and is invalid - do not try other options
            }

            if (!messages.Any())  // not loaded and no messages - everything was null
                messages.Add(string.Format("Please specify IgniteConfiguration.JvmDllPath or {0}.", EnvJavaHome));

            if (messages.Count == 1)
                throw new IgniteException(string.Format("Failed to load {0} ({1})", FileJvmDll, messages[0]));

            var combinedMessage = messages.Aggregate((x, y) => string.Format("{0}\n{1}", x, y));
            throw new IgniteException(string.Format("Failed to load {0}:\n{1}", FileJvmDll, combinedMessage));
        }

        /// <summary>
        /// Try loading DLLs first using file path, then using it's simple name.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="simpleName"></param>
        /// <returns>Zero in case of success, error code in case of failure.</returns>
        private static int LoadDll(string filePath, string simpleName)
        {
            int res = 0;

            IntPtr ptr;

            if (filePath != null)
            {
                ptr = NativeMethods.LoadLibrary(filePath);

                if (ptr == IntPtr.Zero)
                    res = Marshal.GetLastWin32Error();
                else
                    return res;
            }

            // Failed to load using file path, fallback to simple name.
            ptr = NativeMethods.LoadLibrary(simpleName);

            if (ptr == IntPtr.Zero)
            {
                // Preserve the first error code, if any.
                if (res == 0)
                    res = Marshal.GetLastWin32Error();
            }
            else
                res = 0;

            return res;
        }

        /// <summary>
        /// Gets the JVM DLL paths in order of lookup priority.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPaths(string configJvmDllPath)
        {
            if (!string.IsNullOrEmpty(configJvmDllPath))
                yield return new KeyValuePair<string, string>("IgniteConfiguration.JvmDllPath", configJvmDllPath);

            var javaHomeDir = Environment.GetEnvironmentVariable(EnvJavaHome);

            if (!string.IsNullOrEmpty(javaHomeDir))
                yield return
                    new KeyValuePair<string, string>(EnvJavaHome, GetJvmDllPath(Path.Combine(javaHomeDir, DirJre)));
        }

        /// <summary>
        /// Gets the JVM DLL path from JRE dir.
        /// </summary>
        private static string GetJvmDllPath(string jreDir)
        {
            return Path.Combine(jreDir, DirBin, DirServer, FileJvmDll);
        }

        /// <summary>
        /// Unpacks an embedded resource into a temporary folder and returns the full path of resulting file.
        /// </summary>
        /// <param name="resourceName">Resource name.</param>
        /// <returns>Path to a temp file with an unpacked resource.</returns>
        public static string UnpackEmbeddedResource(string resourceName)
        {
            var dllRes = Assembly.GetExecutingAssembly().GetManifestResourceNames()
                .Single(x => x.EndsWith(resourceName, StringComparison.OrdinalIgnoreCase));

            return WriteResourceToTempFile(dllRes, resourceName);
        }

        /// <summary>
        /// Writes the resource to temporary file.
        /// </summary>
        /// <param name="resource">The resource.</param>
        /// <param name="name">File name prefix</param>
        /// <returns>Path to the resulting temp file.</returns>
        private static string WriteResourceToTempFile(string resource, string name)
        {
            // Dll file name should not be changed, so we create a temp folder with random name instead.
            var file = Path.Combine(GetTempDirectoryName(), name);

            using (var src = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource))
            using (var dest = File.OpenWrite(file))
            {
                // ReSharper disable once PossibleNullReferenceException
                src.CopyTo(dest);

                return file;
            }
        }

        /// <summary>
        /// Tries to clean temporary directories created with <see cref="GetTempDirectoryName"/>.
        /// </summary>
        private static void TryCleanTempDirectories()
        {
            foreach (var dir in Directory.GetDirectories(Path.GetTempPath(), DirIgniteTmp + "*"))
            {
                try
                {
                    Directory.Delete(dir, true);
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
        /// Creates a uniquely named, empty temporary directory on disk and returns the full path of that directory.
        /// </summary>
        /// <returns>The full path of the temporary directory.</returns>
        private static string GetTempDirectoryName()
        {
            while (true)
            {
                var dir = Path.Combine(Path.GetTempPath(), DirIgniteTmp + Path.GetRandomFileName());

                try
                {
                    return Directory.CreateDirectory(dir).FullName;
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
        public static List<IClusterNode> ReadNodes(IPortableRawReader reader, Func<ClusterNodeImpl, bool> pred = null)
        {
            var cnt = reader.ReadInt();

            if (cnt < 0)
                return null;

            var res = new List<IClusterNode>(cnt);

            var ignite = ((PortableReaderImpl)reader).Marshaller.Ignite;

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
        /// Gets the asynchronous mode disabled exception.
        /// </summary>
        /// <returns>Asynchronous mode disabled exception.</returns>
        public static InvalidOperationException GetAsyncModeDisabledException()
        {
            return new InvalidOperationException("Asynchronous mode is disabled");
        }
    }
}
