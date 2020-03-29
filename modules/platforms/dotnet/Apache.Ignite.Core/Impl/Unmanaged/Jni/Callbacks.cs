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

using System;

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Java -> .NET callback dispatcher.
    /// Instance of this class should only exist once per process, in the default AppDomain.
    /// </summary>
    internal sealed class Callbacks : MarshalByRefObject
    {
        /** Holds delegates so that GC does not collect them. */
        // ReSharper disable once CollectionNeverQueried.Local
        private readonly List<Delegate> _delegates = new List<Delegate>();

        /** Holds Ignite instance-specific callbacks. */
        private readonly HandleRegistry _callbackRegistry = new HandleRegistry(100);

        /** Console writers. */
        private readonly ConcurrentDictionary<long, ConsoleWriter> _consoleWriters
            = new ConcurrentDictionary<long, ConsoleWriter>();

        /** Gets the JVM. */
        private readonly Jvm _jvm;

        /** Console writer id generator. */
        private long _consoleWriterId;

        /// <summary>
        /// Initializes a new instance of the <see cref="Callbacks"/> class.
        /// </summary>
        public Callbacks(Env env, Jvm jvm)
        {
            Debug.Assert(env != null);
            Debug.Assert(jvm != null);

            _jvm = jvm;
            RegisterNatives(env);
        }

        /** <inheritdoc /> */
        public override object InitializeLifetimeService()
        {
            // Ensure that cross-AppDomain reference lives forever.
            return null;
        }

        /// <summary>
        /// Registers callback handlers.
        /// </summary>
        public long RegisterHandlers(UnmanagedCallbacks cbs)
        {
            Debug.Assert(cbs != null);

            return _callbackRegistry.AllocateCritical(cbs);
        }

        /// <summary>
        /// Releases callback handlers.
        /// </summary>
        public void ReleaseHandlers(long igniteId)
        {
            _callbackRegistry.Release(igniteId);
        }

        /// <summary>
        /// Registers the console writer.
        /// </summary>
        public long RegisterConsoleWriter(ConsoleWriter writer)
        {
            Debug.Assert(writer != null);

            var id = Interlocked.Increment(ref _consoleWriterId);

            var res = _consoleWriters.TryAdd(id, writer);
            Debug.Assert(res);

            return id;
        }

        /// <summary>
        /// Registers the console writer.
        /// </summary>
        public void ReleaseConsoleWriter(long id)
        {
            ConsoleWriter writer;
            var res = _consoleWriters.TryRemove(id, out writer);
            Debug.Assert(res);
        }

        /// <summary>
        /// Registers native callbacks.
        /// </summary>
        private void RegisterNatives(Env env)
        {
            // Native callbacks are per-jvm.
            // Every callback (except ConsoleWrite) includes envPtr (third arg) to identify Ignite instance.

            using (var callbackUtils = env.FindClass(
                "org/apache/ignite/internal/processors/platform/callback/PlatformCallbackUtils"))
            {
                // Any signature works, but wrong one will cause segfault eventually.
                var methods = new[]
                {
                    GetNativeMethod("loggerLog", "(JILjava/lang/String;Ljava/lang/String;Ljava/lang/String;J)V",
                        (CallbackDelegates.LoggerLog) LoggerLog),

                    GetNativeMethod("loggerIsLevelEnabled", "(JI)Z",
                        (CallbackDelegates.LoggerIsLevelEnabled) LoggerIsLevelEnabled),

                    GetNativeMethod("consoleWrite", "(Ljava/lang/String;Z)V",
                        (CallbackDelegates.ConsoleWrite) ConsoleWrite),

                    GetNativeMethod("inLongOutLong", "(JIJ)J", (CallbackDelegates.InLongOutLong) InLongOutLong),

                    GetNativeMethod("inLongLongLongObjectOutLong", "(JIJJJLjava/lang/Object;)J",
                        (CallbackDelegates.InLongLongLongObjectOutLong) InLongLongLongObjectOutLong)
                };

                try
                {
                    env.RegisterNatives(callbackUtils, methods);
                }
                finally
                {
                    foreach (var nativeMethod in methods)
                    {
                        Marshal.FreeHGlobal(nativeMethod.Name);
                        Marshal.FreeHGlobal(nativeMethod.Signature);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the native method.
        /// </summary>
        private unsafe NativeMethod GetNativeMethod(string name, string sig, Delegate d)
        {
            _delegates.Add(d);

            return new NativeMethod
            {
                Name = (IntPtr)IgniteUtils.StringToUtf8Unmanaged(name),
                Signature = (IntPtr)IgniteUtils.StringToUtf8Unmanaged(sig),
                FuncPtr = Marshal.GetFunctionPointerForDelegate(d)
            };
        }

        /// <summary>
        /// <see cref="ILogger.Log"/> callback.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void LoggerLog(IntPtr envPtr, IntPtr clazz, long igniteId, int level, IntPtr message, IntPtr category,
            IntPtr errorInfo, long memPtr)
        {
            try
            {
                var cbs = _callbackRegistry.Get<UnmanagedCallbacks>(igniteId, true);
                var env = _jvm.AttachCurrentThread(envPtr);

                var message0 = env.JStringToString(message);
                var category0 = env.JStringToString(category);
                var errorInfo0 = env.JStringToString(errorInfo);

                cbs.LoggerLog(level, message0, category0, errorInfo0, memPtr);
            }
            catch (Exception e)
            {
                _jvm.AttachCurrentThread(envPtr).ThrowToJava(e);
            }
        }

        /// <summary>
        /// <see cref="ILogger.IsEnabled"/> callback.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private bool LoggerIsLevelEnabled(IntPtr env, IntPtr clazz, long igniteId, int level)
        {
            try
            {
                var cbs = _callbackRegistry.Get<UnmanagedCallbacks>(igniteId, true);

                return cbs.LoggerIsLevelEnabled(level);
            }
            catch (Exception e)
            {
                _jvm.AttachCurrentThread(env).ThrowToJava(e);
                return false;
            }
        }

        /// <summary>
        /// 3 longs + object -> long.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private long InLongLongLongObjectOutLong(IntPtr env, IntPtr clazz, long igniteId,
            int op, long arg1, long arg2, long arg3, IntPtr arg)
        {
            try
            {
                var cbs = _callbackRegistry.Get<UnmanagedCallbacks>(igniteId, true);

                _jvm.AttachCurrentThread(env);

                return cbs.InLongLongLongObjectOutLong(op, arg1, arg2, arg3, arg);
            }
            catch (Exception e)
            {
                _jvm.AttachCurrentThread(env).ThrowToJava(e);
                return 0;
            }
        }

        /// <summary>
        /// long -> long.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private long InLongOutLong(IntPtr env, IntPtr clazz, long igniteId,
            int op, long arg)
        {
            try
            {
                var cbs = _callbackRegistry.Get<UnmanagedCallbacks>(igniteId, true);

                _jvm.AttachCurrentThread(env);

                return cbs.InLongOutLong(op, arg);
            }
            catch (Exception e)
            {
                _jvm.AttachCurrentThread(env).ThrowToJava(e);

                return 0;
            }
        }

        /// <summary>
        /// System.out.println -> Console.Write.
        /// <para />
        /// Java uses system output which can not be accessed with native .NET APIs.
        /// For example, unit test runners won't show Java console output, and so on.
        /// To fix this we delegate console output from Java to .NET APIs.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void ConsoleWrite(IntPtr envPtr, IntPtr clazz, IntPtr message, bool isError)
        {
            try
            {
                if (message != IntPtr.Zero)
                {
                    // Each domain registers it's own writer.
                    var writer = _consoleWriters.Select(x => x.Value).FirstOrDefault();

                    if (writer != null)
                    {
                        var env = _jvm.AttachCurrentThread(envPtr);
                        var msg = env.JStringToString(message);

                        writer.Write(msg, isError);
                    }
                }
            }
            catch (Exception e)
            {
                _jvm.AttachCurrentThread(envPtr).ThrowToJava(e);
            }
        }
    }
}
