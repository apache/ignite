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
    using System.Diagnostics;

    /// <summary>
    /// Cached JNI method ids: initialized once on JVM start.
    /// </summary>
    internal sealed class MethodId
    {
        /// <summary>
        /// Class.getName().
        /// </summary>
        public IntPtr ClassGetName { get; private set; }

        /// <summary>
        /// Throwable.getMessage().
        /// </summary>
        public IntPtr ThrowableGetMessage { get; private set; }

        /// <summary>
        /// PlatformIgnition.
        /// </summary>
        public GlobalRef PlatformIgnition { get; private set; }

        /// <summary>
        /// PlatformIgnition.start().
        /// </summary>
        public IntPtr PlatformIgnitionStart { get; private set; }

        /// <summary>
        /// PlatformIgnition.stop().
        /// </summary>
        public IntPtr PlatformIgnitionStop { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inStreamOutObjectAsync().
        /// </summary>
        public IntPtr TargetInStreamOutObjectAsync { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inStreamAsync().
        /// </summary>
        public IntPtr TargetInStreamAsync { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.outObject ().
        /// </summary>
        public IntPtr TargetOutObject { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.outStream().
        /// </summary>
        public IntPtr TargetOutStream { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inStreamOutStream().
        /// </summary>
        public IntPtr TargetInStreamOutStream { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inObjectStreamOutObjectStream().
        /// </summary>
        public IntPtr TargetInObjectStreamOutObjectStream { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inStreamOutObject().
        /// </summary>
        public IntPtr TargetInStreamOutObject { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inStreamOutLong().
        /// </summary>
        public IntPtr TargetInStreamOutLong { get; private set; }

        /// <summary>
        /// PlatformTargetProxy.inLongOutLong().
        /// </summary>
        public IntPtr TargetInLongOutLong { get; private set; }

        /// <summary>
        /// PlatformUtils class.
        /// </summary>
        public GlobalRef PlatformUtils { get; private set; }

        /// <summary>
        /// PlatformUtils.getFullStackTrace.
        /// </summary>
        public IntPtr PlatformUtilsGetStackTrace { get; private set; }

        /// <summary>
        /// PlatformUtils.reallocate.
        /// </summary>
        public IntPtr PlatformUtilsReallocate { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="MethodId"/> class.
        /// </summary>
        public MethodId(Env env)
        {
            Debug.Assert(env != null);

            using (var classCls = env.FindClass("java/lang/Class"))
            {
                ClassGetName = env.GetMethodId(classCls, "getName", "()Ljava/lang/String;");
            }

            using (var throwableCls = env.FindClass("java/lang/Throwable"))
            {
                ThrowableGetMessage = env.GetMethodId(throwableCls, "getMessage", "()Ljava/lang/String;");
            }

            PlatformIgnition = env.FindClass(
                "org/apache/ignite/internal/processors/platform/PlatformIgnition");
            PlatformIgnitionStart = env.GetStaticMethodId(PlatformIgnition,
                "start", "(Ljava/lang/String;Ljava/lang/String;IJJ)V");
            PlatformIgnitionStop = env.GetStaticMethodId(PlatformIgnition, "stop", "(Ljava/lang/String;Z)Z");

            using (var target = env.FindClass("org/apache/ignite/internal/processors/platform/PlatformTargetProxy"))
            {
                TargetInLongOutLong = env.GetMethodId(target, "inLongOutLong", "(IJ)J");
                TargetInStreamOutLong = env.GetMethodId(target, "inStreamOutLong", "(IJ)J");
                TargetInStreamOutObject = env.GetMethodId(target, "inStreamOutObject", "(IJ)Ljava/lang/Object;");
                TargetInStreamOutStream = env.GetMethodId(target, "inStreamOutStream", "(IJJ)V");
                TargetInObjectStreamOutObjectStream = env.GetMethodId(target, "inObjectStreamOutObjectStream",
                    "(ILjava/lang/Object;JJ)Ljava/lang/Object;");
                TargetOutStream = env.GetMethodId(target, "outStream", "(IJ)V");
                TargetOutObject = env.GetMethodId(target, "outObject", "(I)Ljava/lang/Object;");
                TargetInStreamAsync = env.GetMethodId(target, "inStreamAsync", "(IJ)V");
                TargetInStreamOutObjectAsync =
                    env.GetMethodId(target, "inStreamOutObjectAsync", "(IJ)Ljava/lang/Object;");
            }

            PlatformUtils = env.FindClass("org/apache/ignite/internal/processors/platform/utils/PlatformUtils");
            PlatformUtilsGetStackTrace = env.GetStaticMethodId(PlatformUtils, "getFullStackTrace",
                "(Ljava/lang/Throwable;)Ljava/lang/String;");
            PlatformUtilsReallocate = env.GetStaticMethodId(PlatformUtils, "reallocate", "(JI)V");
        }
    }
}
