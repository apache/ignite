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
    using System.Runtime.InteropServices;
    using System.Security;

    /// <summary>
    /// Ignite JNI native methods.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal static unsafe class IgniteJniNativeMethods
    {
        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteReallocate")]
        public static extern int Reallocate(long memPtr, int cap);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStart")]
        public static extern void* IgnitionStart(void* ctx, sbyte* cfgPath, sbyte* gridName, int factoryId, 
            long dataPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStop")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool IgnitionStop(void* ctx, sbyte* gridName, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStopAll")]
        public static extern void IgnitionStopAll(void* ctx, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorReleaseStart")]
        public static extern void ProcessorReleaseStart(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorProjection")]
        public static extern void* ProcessorProjection(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCache")]
        public static extern void* ProcessorCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCache")]
        public static extern void* ProcessorCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCacheFromConfig")]
        public static extern void* ProcessorCreateCacheFromConfig(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCache")]
        public static extern void* ProcessorGetOrCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCacheFromConfig")]
        public static extern void* ProcessorGetOrCreateCacheFromConfig(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateNearCache")]
        public static extern void* ProcessorCreateNearCache(void* ctx, void* obj, sbyte* name, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateNearCache")]
        public static extern void* ProcessorGetOrCreateNearCache(void* ctx, void* obj, sbyte* name, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDestroyCache")]
        public static extern void ProcessorDestroyCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAffinity")]
        public static extern void* ProcessorAffinity(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDataStreamer")]
        public static extern void* ProcessorDataStreamer(void* ctx, void* obj, sbyte* name, 
            [MarshalAs(UnmanagedType.U1)] bool keepBinary);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorTransactions")]
        public static extern void* ProcessorTransactions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCompute")]
        public static extern void* ProcessorCompute(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorMessage")]
        public static extern void* ProcessorMessage(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorEvents")]
        public static extern void* ProcessorEvents(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorServices")]
        public static extern void* ProcessorServices(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorExtensions")]
        public static extern void* ProcessorExtensions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicLong")]
        public static extern void* ProcessorAtomicLong(void* ctx, void* obj, sbyte* name, long initVal,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicSequence")]
        public static extern void* ProcessorAtomicSequence(void* ctx, void* obj, sbyte* name, long initVal,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicReference")]
        public static extern void* ProcessorAtomicReference(void* ctx, void* obj, sbyte* name, long memPtr,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetIgniteConfiguration")]
        public static extern void ProcessorGetIgniteConfiguration(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetCacheNames")]
        public static extern void ProcessorGetCacheNames(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInLongOutLong")]
        public static extern long TargetInLongOutLong(void* ctx, void* target, int opType, long val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorLoggerIsLevelEnabled")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool ProcessorLoggerIsLevelEnabled(void* ctx, void* obj, int level);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorLoggerLog")]
        public static extern void ProcessorLoggerLog(void* ctx, void* obj, int level, sbyte* messsage, sbyte* category, sbyte* errorInfo);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorBinaryProcessor")]
        public static extern void* ProcessorBinaryProcessor(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutLong")]
        public static extern long TargetInStreamOutLong(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutStream")]
        public static extern void TargetInStreamOutStream(void* ctx, void* target, int opType, long inMemPtr,
            long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutObject")]
        public static extern void* TargetInStreamOutObject(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInObjectStreamOutObjectStream")]
        public static extern void* TargetInObjectStreamOutObjectStream(void* ctx, void* target, int opType,
            void* arg, long inMemPtr, long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutStream")]
        public static extern void TargetOutStream(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutObject")]
        public static extern void* TargetOutObject(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAcquire")]
        public static extern void* Acquire(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRelease")]
        public static extern void Release(void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteThrowToJava")]
        public static extern void ThrowToJava(void* ctx, char* msg);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteHandlersSize")]
        public static extern int HandlersSize();

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCreateContext")]
        public static extern void* CreateContext(void* opts, int optsLen, void* cbs);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDeleteContext")]
        public static extern void DeleteContext(void* ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDestroyJvm")]
        public static extern void DestroyJvm(void* ctx);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteSetConsoleHandler")]
        public static extern void SetConsoleHandler(void* consoleHandler);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRemoveConsoleHandler")]
        public static extern int RemoveConsoleHandler(void* consoleHandler);
    }
}