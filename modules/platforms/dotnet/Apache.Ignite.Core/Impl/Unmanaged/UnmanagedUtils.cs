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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;

    /// <summary>
    /// Unmanaged utility classes.
    /// </summary>
    internal static unsafe class UnmanagedUtils
    {
        /** Interop factory ID for .Net. */
        private const int InteropFactoryId = 1;

        #region NATIVE METHODS: PROCESSOR

        internal static void IgnitionStart(Env env, string cfgPath, string gridName,
            bool clientMode, bool userLogger, long igniteId, bool redirectConsole)
        {
            using (var mem = IgniteManager.Memory.Allocate().GetStream())
            using (var cfgPath0 = env.NewStringUtf(cfgPath))
            using (var gridName0 = env.NewStringUtf(gridName))
            {
                mem.WriteBool(clientMode);
                mem.WriteBool(userLogger);
                mem.WriteBool(redirectConsole);

                long* args = stackalloc long[5];
                args[0] = cfgPath == null ? 0 : cfgPath0.Target.ToInt64();
                args[1] = gridName == null ? 0 : gridName0.Target.ToInt64();
                args[2] = InteropFactoryId;
                args[3] = igniteId;
                args[4] = mem.SynchronizeOutput();

                // OnStart receives InteropProcessor reference and stores it.
                var methodId = env.Jvm.MethodId;
                env.CallStaticVoidMethod(methodId.PlatformIgnition, methodId.PlatformIgnitionStart, args);
            }
        }

        internal static bool IgnitionStop(string gridName, bool cancel)
        {
            var env = Jvm.Get().AttachCurrentThread();
            var methodId = env.Jvm.MethodId;

            using (var gridName1 = env.NewStringUtf(gridName))
            {
                long* args = stackalloc long[2];
                args[0] = gridName == null ? 0 : gridName1.Target.ToInt64();
                args[1] = cancel ? 1 : 0;

                return env.CallStaticBoolMethod(methodId.PlatformIgnition, methodId.PlatformIgnitionStop, args);
            }
        }

        #endregion

        #region NATIVE METHODS: TARGET

        internal static long TargetInLongOutLong(GlobalRef target, int opType, long memPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[2];
            args[0] = opType;
            args[1] = memPtr;

            return jvm.AttachCurrentThread().CallLongMethod(target, jvm.MethodId.TargetInLongOutLong, args);
        }

        internal static long TargetInStreamOutLong(GlobalRef target, int opType, long memPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[2];
            args[0] = opType;
            args[1] = memPtr;

            return jvm.AttachCurrentThread().CallLongMethod(target, jvm.MethodId.TargetInStreamOutLong, args);
        }

        internal static void TargetInStreamOutStream(GlobalRef target, int opType, long inMemPtr,
            long outMemPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[3];
            args[0] = opType;
            args[1] = inMemPtr;
            args[2] = outMemPtr;

            jvm.AttachCurrentThread().CallVoidMethod(target, jvm.MethodId.TargetInStreamOutStream, args);
        }

        internal static GlobalRef TargetInStreamOutObject(GlobalRef target, int opType, long inMemPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[2];
            args[0] = opType;
            args[1] = inMemPtr;

            return jvm.AttachCurrentThread().CallObjectMethod(
                target, jvm.MethodId.TargetInStreamOutObject, args);
        }

        internal static GlobalRef TargetInObjectStreamOutObjectStream(GlobalRef target, int opType, 
            GlobalRef arg, long inMemPtr, long outMemPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[4];
            args[0] = opType;
            args[1] = (long) arg.Target;
            args[2] = inMemPtr;
            args[3] = outMemPtr;

            return jvm.AttachCurrentThread().CallObjectMethod(
                target, jvm.MethodId.TargetInObjectStreamOutObjectStream, args);
        }

        internal static void TargetOutStream(GlobalRef target, int opType, long memPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[4];
            args[0] = opType;
            args[1] = memPtr;

            jvm.AttachCurrentThread().CallVoidMethod(target, jvm.MethodId.TargetOutStream, args);
        }

        internal static GlobalRef TargetOutObject(GlobalRef target, int opType)
        {
            var jvm = target.Jvm;

            long opType0 = opType;

            return jvm.AttachCurrentThread().CallObjectMethod(
                target, jvm.MethodId.TargetOutObject, &opType0);
        }

        internal static void TargetInStreamAsync(GlobalRef target, int opType, long memPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[4];
            args[0] = opType;
            args[1] = memPtr;

            jvm.AttachCurrentThread().CallVoidMethod(target, jvm.MethodId.TargetInStreamAsync, args);
        }

        internal static GlobalRef TargetInStreamOutObjectAsync(GlobalRef target, int opType, long memPtr)
        {
            var jvm = target.Jvm;

            long* args = stackalloc long[4];
            args[0] = opType;
            args[1] = memPtr;

            return jvm.AttachCurrentThread().CallObjectMethod(
                target, jvm.MethodId.TargetInStreamOutObjectAsync, args);
        }

        #endregion

        #region NATIVE METHODS: MISCELANNEOUS

        internal static void Reallocate(long memPtr, int cap)
        {
            var jvm = Jvm.Get();
            var methodId = jvm.MethodId;

            long* args = stackalloc long[4];
            args[0] = memPtr;
            args[1] = cap;

            jvm.AttachCurrentThread().CallStaticVoidMethod(methodId.PlatformUtils, methodId.PlatformUtilsReallocate,
                args);
        }

        #endregion
    }
}
