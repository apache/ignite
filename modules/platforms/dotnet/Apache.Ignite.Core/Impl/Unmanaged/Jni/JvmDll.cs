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

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;
    using Microsoft.Win32;

    /// <summary>
    /// Jvm.dll loader (libjvm.so on Linux, libjvm.dylib on macOs).
    /// </summary>
    internal class JvmDll
    {
        /** Cached instance. */
        private static JvmDll _instance;

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
                "lib/server",
                "lib/client",
                "lib/amd64/server",
                "lib/amd64/client",

                // JDK paths
                "jre/lib/server",
                "jre/lib/client",
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
        internal static readonly string FileJvmDll = Os.IsWindows
            ? "jvm.dll"
            : Os.IsMacOs
                ? "libjvm.dylib"
                : "libjvm.so";

        /** */
        private unsafe delegate JniResult CreateJvmDel(out IntPtr pvm, out IntPtr penv, JvmInitArgs* args);

        /** */
        private unsafe delegate JniResult GetDefaultArgsDel(JvmInitArgs* args);

        /** */
        private delegate JniResult GetCreatedJvmsDel(out IntPtr pvm, int size, out int size2);

        /** */
        private readonly CreateJvmDel _createJvm;

        /** */
        private readonly GetCreatedJvmsDel _getCreatedJvms;

        /** */
        private readonly GetDefaultArgsDel _getDefaultArgs;

        /// <summary>
        /// Initializes a new instance of the <see cref="JvmDll"/> class.
        /// </summary>
        private unsafe JvmDll(IntPtr ptr)
        {
            if (Os.IsMacOs)
            {
                if (ptr == IntPtr.Zero)
                {
                    // Retrieve already loaded dll by name.
                    // This happens in default AppDomain when Ignite starts in another domain.
                    var res = DllLoader.Load(FileJvmDll);
                    ptr = res.Key;

                    if (res.Key == IntPtr.Zero)
                    {
                        throw new IgniteException(
                            string.Format("{0} has not been loaded: {1}", FileJvmDll, res.Value));
                    }
                }

                // dlopen + DllImport combo does not work on macOs, so we have to call dlsym manually.
                var createJvmPtr = DllLoader.NativeMethodsMacOs.dlsym(ptr, "JNI_CreateJavaVM");
                _createJvm = (CreateJvmDel) Marshal.GetDelegateForFunctionPointer(createJvmPtr, typeof(CreateJvmDel));

                var getJvmsPtr = DllLoader.NativeMethodsMacOs.dlsym(ptr, "JNI_GetCreatedJavaVMs");
                _getCreatedJvms = (GetCreatedJvmsDel) Marshal.GetDelegateForFunctionPointer(getJvmsPtr,
                    typeof(GetCreatedJvmsDel));

                var getArgsPtr = DllLoader.NativeMethodsMacOs.dlsym(ptr, "JNI_GetDefaultJavaVMInitArgs");
                _getDefaultArgs = (GetDefaultArgsDel) Marshal.GetDelegateForFunctionPointer(getArgsPtr,
                    typeof(GetDefaultArgsDel));
            }
            else if (Os.IsWindows)
            {
                _createJvm = JniNativeMethodsWindows.JNI_CreateJavaVM;
                _getCreatedJvms = JniNativeMethodsWindows.JNI_GetCreatedJavaVMs;
                _getDefaultArgs = JniNativeMethodsWindows.JNI_GetDefaultJavaVMInitArgs;
            }
            else
            {
                _createJvm = JniNativeMethodsLinux.JNI_CreateJavaVM;
                _getCreatedJvms = JniNativeMethodsLinux.JNI_GetCreatedJavaVMs;
                _getDefaultArgs = JniNativeMethodsLinux.JNI_GetDefaultJavaVMInitArgs;
            }
        }

        /// <summary>
        /// Gets the instance.
        /// </summary>
        public static JvmDll Instance
        {
            get { return _instance ?? (_instance = new JvmDll(IntPtr.Zero)); }
        }

        /// <summary>
        /// Creates the JVM.
        /// </summary>
        public unsafe JniResult CreateJvm(out IntPtr pvm, out IntPtr penv, JvmInitArgs* args)
        {
            return _createJvm(out pvm, out penv, args);
        }

        /// <summary>
        /// Gets the created JVMS.
        /// </summary>
        public JniResult GetCreatedJvms(out IntPtr pvm, int size, out int size2)
        {
            return _getCreatedJvms(out pvm, size, out size2);
        }

        /// <summary>
        /// Gets the default JVM init args.
        /// Before calling this function, native code must set the vm_args->version field to the JNI version
        /// it expects the VM to support. After this function returns, vm_args->version will be set
        /// to the actual JNI version the VM supports.
        /// </summary>
        public unsafe JniResult GetDefaultJvmInitArgs(JvmInitArgs* args)
        {
            return _getDefaultArgs(args);
        }

        /// <summary>
        /// Loads the JVM DLL into process memory.
        /// </summary>
        public static void Load(string configJvmDllPath, ILogger log)
        {
            // Load only once.
            // Locking is performed by the caller three, omit here.
            if (_instance != null)
            {
                log.Debug("JNI dll is already loaded.");
                return;
            }

            var messages = new List<string>();
            foreach (var dllPath in GetJvmDllPaths(configJvmDllPath))
            {
                log.Debug("Trying to load {0} from [option={1}, path={2}]...", FileJvmDll, dllPath.Key, dllPath.Value);

                var res = LoadDll(dllPath.Value, FileJvmDll);
                if (res.Key != IntPtr.Zero)
                {
                    log.Debug("{0} successfully loaded from [option={1}, path={2}]",
                        FileJvmDll, dllPath.Key, dllPath.Value);

                    _instance = new JvmDll(res.Key);

                    return;
                }

                var message = string.Format(CultureInfo.InvariantCulture, "[option={0}, path={1}, error={2}]",
                    dllPath.Key, dllPath.Value, res.Value);
                messages.Add(message);

                log.Debug("Failed to load {0}:  {1}", FileJvmDll, message);

                if (dllPath.Value == configJvmDllPath)
                    break; // if configJvmDllPath is specified and is invalid - do not try other options
            }

            if (!messages.Any()) // not loaded and no messages - everything was null
            {
                messages.Add(string.Format(CultureInfo.InvariantCulture,
                    "Please specify IgniteConfiguration.JvmDllPath or {0}.", EnvJavaHome));
            }

            if (messages.Count == 1)
            {
                throw new IgniteException(string.Format(CultureInfo.InvariantCulture, "Failed to load {0} ({1})",
                    FileJvmDll, messages[0]));
            }

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
        private static KeyValuePair<IntPtr, string> LoadDll(string filePath, string simpleName)
        {
            var res = new KeyValuePair<IntPtr, string>();

            if (filePath != null)
            {
                res = DllLoader.Load(filePath);

                if (res.Key != IntPtr.Zero)
                {
                    return res; // Success.
                }
            }

            // Failed to load using file path, fallback to simple name.
            var res2 = DllLoader.Load(simpleName);

            if (res2.Key != IntPtr.Zero)
            {
                return res2; // Success.
            }

            return res.Value != null ? res : res2;
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

            foreach (var keyValuePair in
                GetJvmDllPathsWindows()
                    .Concat(GetJvmDllPathsLinux())
                    .Concat(GetJvmDllPathsMacOs()))
            {
                yield return keyValuePair;
            }
        }

        /// <summary>
        /// Gets Jvm dll paths from Windows registry.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPathsWindows()
        {
#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0
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
#else
            yield break;
#endif
        }

        /// <summary>
        /// Gets the Jvm dll paths from /usr/bin/java symlink.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPathsLinux()
        {
            if (Os.IsWindows || Os.IsMacOs)
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
        /// Gets the JVM DLL paths on macOs.
        /// </summary>
        private static IEnumerable<KeyValuePair<string, string>> GetJvmDllPathsMacOs()
        {
            const string jvmDir = "/Library/Java/JavaVirtualMachines";

            if (!Directory.Exists(jvmDir))
            {
                yield break;
            }

            const string subDir = "Contents/Home";

            foreach (var dir in Directory.GetDirectories(jvmDir))
            {
                foreach (var path in JvmDllLookupPaths)
                {
                    yield return
                        new KeyValuePair<string, string>(dir, Path.Combine(dir, subDir, path, FileJvmDll));
                }
            }
        }

        /// <summary>
        /// DLL imports.
        /// </summary>
        private static unsafe class JniNativeMethodsWindows
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("jvm.dll", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_CreateJavaVM(out IntPtr pvm, out IntPtr penv, JvmInitArgs* args);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("jvm.dll", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_GetCreatedJavaVMs(out IntPtr pvm, int size,
                [Out] out int size2);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("jvm.dll", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_GetDefaultJavaVMInitArgs(JvmInitArgs* args);
        }

        /// <summary>
        /// DLL imports.
        /// </summary>
        private static unsafe class JniNativeMethodsLinux
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("libjvm.so", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_CreateJavaVM(out IntPtr pvm, out IntPtr penv, JvmInitArgs* args);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("libjvm.so", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_GetCreatedJavaVMs(out IntPtr pvm, int size,
                [Out] out int size2);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            [DllImport("libjvm.so", CallingConvention = CallingConvention.StdCall)]
            internal static extern JniResult JNI_GetDefaultJavaVMInitArgs(JvmInitArgs* args);
        }
    }
}
