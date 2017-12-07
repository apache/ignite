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
    using System.Runtime.InteropServices;

    /// <summary>
    /// Delegates for JNI Env entity.
    /// </summary>
    internal static unsafe class EnvDelegates
    {
        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        public delegate JniResult CallStaticVoidMethod(
            IntPtr env, IntPtr clazz, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate byte CallStaticBooleanMethod(
            IntPtr env, IntPtr clazz, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr NewGlobalRef(IntPtr env, IntPtr lobj);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void DeleteLocalRef(IntPtr env, IntPtr lref);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void DeleteGlobalRef(IntPtr env, IntPtr gref);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr FindClass(IntPtr env, [MarshalAs(UnmanagedType.LPStr)] string name);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr GetStaticMethodId(IntPtr env, IntPtr clazz,
            [MarshalAs(UnmanagedType.LPStr)] string name, [MarshalAs(UnmanagedType.LPStr)] string sig);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr GetMethodId(IntPtr env, IntPtr clazz, [MarshalAs(UnmanagedType.LPStr)] string name,
            [MarshalAs(UnmanagedType.LPStr)] string sig);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr NewStringUtf(IntPtr env, IntPtr utf);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr ExceptionOccurred(IntPtr env);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void ExceptionClear(IntPtr env);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate byte ExceptionCheck(IntPtr env);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr GetObjectClass(IntPtr env, IntPtr obj);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr CallObjectMethod(
            IntPtr env, IntPtr obj, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate long CallLongMethod(
            IntPtr env, IntPtr obj, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void CallVoidMethod(
            IntPtr env, IntPtr obj, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr CallStaticObjectMethod(
            IntPtr env, IntPtr clazz, IntPtr methodId, long* argsPtr);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr GetStringChars(IntPtr env, IntPtr jstring, byte* isCopy);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void ReleaseStringChars(IntPtr env, IntPtr jstring, IntPtr chars);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate IntPtr GetStringUtfChars(IntPtr env, IntPtr jstring, byte* isCopy);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate void ReleaseStringUtfChars(IntPtr env, IntPtr jstring, IntPtr chars);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate int GetStringUtfLength(IntPtr env, IntPtr jstring);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate JniResult RegisterNatives(IntPtr env, IntPtr clazz,
            NativeMethod* methods, int nMethods);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        internal delegate JniResult ThrowNew(IntPtr env, IntPtr clazz, IntPtr msg);
    }
}