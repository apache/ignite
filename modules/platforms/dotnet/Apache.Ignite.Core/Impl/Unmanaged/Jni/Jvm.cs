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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Threading;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// JVM holder. Should exist once per domain.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal sealed unsafe class Jvm
    {
        /** */
        // ReSharper disable once InconsistentNaming
        private const int JNI_VERSION_1_8 = 0x00010008;

        /** */
        // ReSharper disable once InconsistentNaming
        private const int JNI_VERSION_9 = 0x00090000;

        /** Options to enable startup on Java 9. */
        private static readonly string[] Java9Options =
        {
            "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED",
            "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
            "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
            "--illegal-access=permit"
        };

        /** */
        private readonly IntPtr _jvmPtr;

        /** */
        private readonly JvmDelegates.AttachCurrentThread _attachCurrentThread;

        /** */
        private readonly MethodId _methodId;

        /** Callbacks. */
        private readonly Callbacks _callbacks;

        /** Thread exit callback id. */
        private readonly int _threadExitCallbackId;

        /** Static instance */
        private static volatile Jvm _instance;

        /** Sync. */
        private static readonly object SyncRoot = new object();

        /** Console writer. */
        private static readonly ConsoleWriter ConsoleWriter = new ConsoleWriter();

        /** Env for current thread. */
        [ThreadStatic] private static Env _env;

        /** Console writer flag. */
        private int _isConsoleWriterEnabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="_instance"/> class.
        /// </summary>
        private Jvm(IntPtr jvmPtr)
        {
            Debug.Assert(jvmPtr != IntPtr.Zero);

            _jvmPtr = jvmPtr;

            var funcPtr = (JvmInterface**)jvmPtr;
            var func = **funcPtr;
            GetDelegate(func.AttachCurrentThread, out _attachCurrentThread);

            // JVM is a singleton, so this is one-time subscription.
            // This is a shortcut - we pass DetachCurrentThread pointer directly as a thread exit callback,
            // because signatures happen to match exactly.
            _threadExitCallbackId = UnmanagedThread.SetThreadExitCallback(func.DetachCurrentThread);

            var env = AttachCurrentThread();

            _methodId = new MethodId(env);

            // Keep AppDomain check here to avoid JITting GetCallbacksFromDefaultDomain method on .NET Core
            // (which fails due to _AppDomain usage).
            _callbacks = AppDomain.CurrentDomain.IsDefaultAppDomain()
                ? new Callbacks(env, this)
                : GetCallbacksFromDefaultDomain();
        }

        /// <summary>
        /// Gets the callbacks.
        /// </summary>
        private static Callbacks GetCallbacksFromDefaultDomain()
        {
#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0
            // JVM exists once per process, and JVM callbacks exist once per process.
            // We should register callbacks ONLY from the default AppDomain (which can't be unloaded).
            // Non-default appDomains should delegate this logic to the default one.
            var defDomain = AppDomains.GetDefaultAppDomain();

            // In some cases default AppDomain is not able to locate Apache.Ignite.Core assembly.
            // First, use CreateInstanceFrom to set up the AssemblyResolve handler.
            var resHelpType = typeof(AssemblyResolver);
            var resHelp = (AssemblyResolver)defDomain.CreateInstanceFrom(resHelpType.Assembly.Location, resHelpType.FullName)
                .Unwrap();
            resHelp.TrackResolve(resHelpType.Assembly.FullName, resHelpType.Assembly.Location);

            // Now use CreateInstance to get the domain helper of a properly loaded class.
            var type = typeof(CallbackAccessor);
            var helper = (CallbackAccessor)defDomain.CreateInstance(type.Assembly.FullName, type.FullName).Unwrap();

            return helper.GetCallbacks();
#else
            throw new IgniteException("Multiple domains are not supported on .NET Core.");
#endif
        }

        /// <summary>
        /// Gets or creates the JVM.
        /// </summary>
        /// <param name="options">JVM options.</param>
        public static Jvm GetOrCreate(IList<string> options)
        {
            lock (SyncRoot)
            {
                return _instance ?? (_instance = new Jvm(GetJvmPtr(options)));
            }
        }

        /// <summary>
        /// Gets the JVM.
        /// </summary>
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Global
        public static Jvm Get(bool ignoreMissing = false)
        {
            var res = _instance;

            if (res == null && !ignoreMissing)
            {
                throw new IgniteException("JVM has not been created.");
            }

            return res;
        }

        /// <summary>
        /// Gets the method IDs.
        /// </summary>
        public MethodId MethodId
        {
            get { return _methodId; }
        }

        /// <summary>
        /// Attaches current thread to the JVM and returns JNIEnv.
        /// </summary>
        public Env AttachCurrentThread()
        {
            if (_env == null)
            {
                IntPtr envPtr;
                var res = _attachCurrentThread(_jvmPtr, out envPtr, IntPtr.Zero);

                if (res != JniResult.Success)
                {
                    throw new IgniteException("AttachCurrentThread failed: " + res);
                }

                _env = new Env(envPtr, this);
                UnmanagedThread.EnableCurrentThreadExitEvent(_threadExitCallbackId, _jvmPtr);
            }

            return _env;
        }

        /// <summary>
        /// Attaches current thread to the JVM using known envPtr and returns JNIEnv.
        /// </summary>
        public Env AttachCurrentThread(IntPtr envPtr)
        {
            if (_env == null || _env.EnvPtr != envPtr)
            {
                _env = new Env(envPtr, this);
            }

            return _env;
        }

        /// <summary>
        /// Registers the callbacks.
        /// </summary>
        public void RegisterCallbacks(UnmanagedCallbacks cbs)
        {
            var id = _callbacks.RegisterHandlers(cbs);
            cbs.SetContext(id);
        }

        /// <summary>
        /// Releases the callbacks.
        /// </summary>
        public void ReleaseCallbacks(long igniteId)
        {
            _callbacks.ReleaseHandlers(igniteId);
        }

        /// <summary>
        /// Enables the Java console output propagation.
        /// </summary>
        public void EnableJavaConsoleWriter()
        {
            if (Interlocked.CompareExchange(ref _isConsoleWriterEnabled, 1, 0) == 0)
            {
                var writerId = _callbacks.RegisterConsoleWriter(ConsoleWriter);
                AppDomain.CurrentDomain.DomainUnload += (s, a) => _callbacks.ReleaseConsoleWriter(writerId);
            }
        }

        /// <summary>
        /// Gets the JVM pointer.
        /// </summary>
        private static IntPtr GetJvmPtr(IList<string> options)
        {
            IntPtr jvm;
            int existingJvmCount;

            // Use existing JVM if present.
            var res = JvmDll.Instance.GetCreatedJvms(out jvm, 1, out existingJvmCount);
            if (res != JniResult.Success)
            {
                throw new IgniteException("JNI_GetCreatedJavaVMs failed: " + res);
            }

            if (existingJvmCount > 0)
            {
                return jvm;
            }

            return CreateJvm(options);
        }

        /// <summary>
        /// Determines whether we are on Java 9.
        /// </summary>
        private static bool IsJava9()
        {
            var args = new JvmInitArgs
            {
                version = JNI_VERSION_9
            };

            // Returns error on Java 8 and lower.
            var res = JvmDll.Instance.GetDefaultJvmInitArgs(&args);
            return res == JniResult.Success;
        }

        /// <summary>
        /// Creates the JVM.
        /// </summary>
        private static IntPtr CreateJvm(IList<string> options)
        {
            if (IsJava9())
            {
                options = options == null
                    ? Java9Options.ToList()
                    : new List<string>(options.Concat(Java9Options));
            }

            var args = new JvmInitArgs
            {
                version = JNI_VERSION_1_8,
                nOptions = options.Count
            };

            var opts = GetJvmOptions(options);

            try
            {
                JniResult res;
                IntPtr jvm;

                fixed (JvmOption* optPtr = &opts[0])
                {
                    args.options = optPtr;
                    IntPtr env;
                    res = JvmDll.Instance.CreateJvm(out jvm, out env, &args);
                }

                if (res != JniResult.Success)
                {
                    throw new IgniteException("JNI_CreateJavaVM failed: " + res);
                }

                return jvm;
            }
            finally
            {
                foreach (var opt in opts)
                {
                    Marshal.FreeHGlobal(opt.optionString);
                }
            }
        }

        /// <summary>
        /// Gets the JVM options.
        /// </summary>
        private static JvmOption[] GetJvmOptions(IList<string> options)
        {
            var opt = new JvmOption[options.Count];

            for (var i = 0; i < options.Count; i++)
            {
                opt[i].optionString = Marshal.StringToHGlobalAnsi(options[i]);
            }

            return opt;
        }

        /// <summary>
        /// Gets the delegate.
        /// </summary>
        private static void GetDelegate<T>(IntPtr ptr, out T del)
        {
            del = (T) (object) Marshal.GetDelegateForFunctionPointer(ptr, typeof(T));
        }


        /// <summary>
        /// Provides access to <see cref="Callbacks"/> instance in the default AppDomain.
        /// </summary>
        private class CallbackAccessor : MarshalByRefObject
        {
            /// <summary>
            /// Gets the callbacks.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic",
                Justification = "Only instance methods can be called across AppDomain boundaries.")]
            public Callbacks GetCallbacks()
            {
                return GetOrCreate(null)._callbacks;
            }
        }

        /// <summary>
        /// Resolves Apache.Ignite.Core assembly in the default AppDomain when needed.
        /// </summary>
        private class AssemblyResolver : MarshalByRefObject
        {
            /// <summary>
            /// Tracks the AssemblyResolve event.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic",
                Justification = "Only instance methods can be called across AppDomain boundaries.")]
            public void TrackResolve(string name, string path)
            {
                AppDomain.CurrentDomain.AssemblyResolve += (sender, args) =>
                {
                    if (args.Name == name)
                    {
                        return Assembly.LoadFrom(path);
                    }

                    return null;
                };
            }
        }
    }
}
