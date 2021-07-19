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

namespace Apache.Ignite.Core.Tests
{
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;

    /// <summary>
    /// Test utils: JNI calls.
    /// </summary>
    public static class TestUtilsJni
    {
        /** */
        private const string ClassPlatformProcessUtils = "org/apache/ignite/platform/PlatformProcessUtils";

        /** */
        private const string ClassPlatformThreadUtils = "org/apache/ignite/platform/PlatformThreadUtils";

        /** */
        private const string ClassPlatformStartIgniteUtils = "org/apache/ignite/platform/PlatformStartIgniteUtils";

        /// <summary>
        /// Suspend Ignite threads for the given grid.
        /// </summary>
        public static void SuspendThreads(string gridName)
        {
            CallStringMethod(ClassPlatformThreadUtils, "suspend", "(Ljava/lang/String;)V", gridName);
        }

        /// <summary>
        /// Resume Ignite threads for the given grid.
        /// </summary>
        public static void ResumeThreads(string gridName)
        {
            CallStringMethod(ClassPlatformThreadUtils, "resume", "(Ljava/lang/String;)V", gridName);
        }

        /// <summary>
        /// Starts a new process.
        /// </summary>
        /// <param name="file">Executable name.</param>
        /// <param name="arg1">Argument.</param>
        /// <param name="arg2">Argument.</param>
        /// <param name="workDir">Work directory.</param>
        /// <param name="waitForOutput">A string to look for in the output.</param>
        public static unsafe void StartProcess(
            string file, string arg1, string arg2, string workDir, string waitForOutput)
        {
            Debug.Assert(file != null);
            Debug.Assert(arg1 != null);
            Debug.Assert(arg2 != null);
            Debug.Assert(workDir != null);

            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(ClassPlatformProcessUtils))
            {
                var methodId = env.GetStaticMethodId(cls, "startProcess",
                    "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

                using (var fileRef = env.NewStringUtf(file))
                using (var arg1Ref = env.NewStringUtf(arg1))
                using (var arg2Ref = env.NewStringUtf(arg2))
                using (var workDirRef = env.NewStringUtf(workDir))
                using (var waitForOutputRef = env.NewStringUtf(waitForOutput))
                {
                    var methodArgs = stackalloc long[5];
                    methodArgs[0] = fileRef.Target.ToInt64();
                    methodArgs[1] = arg1Ref.Target.ToInt64();
                    methodArgs[2] = arg2Ref.Target.ToInt64();
                    methodArgs[3] = workDirRef.Target.ToInt64();
                    methodArgs[4] = waitForOutputRef == null ? 0 : waitForOutputRef.Target.ToInt64();

                    env.CallStaticVoidMethod(cls, methodId, methodArgs);
                }
            }
        }

        /// <summary>
        /// Kills the process previously started with <see cref="StartProcess"/>.
        /// </summary>
        public static void DestroyProcess()
        {
            CallVoidMethod(ClassPlatformProcessUtils, "destroyProcess", "()V");
        }

        /// <summary>
        /// Gets the Java thread name.
        /// </summary>
        /// <returns></returns>
        public static string GetJavaThreadName()
        {
            return CallStringMethod(ClassPlatformThreadUtils, "getThreadName", "()Ljava/lang/String;");
        }

        public static void StartIgnite(string name)
        {
            CallStringMethod(ClassPlatformStartIgniteUtils, "startWithSecurity", "(Ljava/lang/String;)V", name);
        }

        public static void StopIgnite(string name)
        {
            CallStringMethod(ClassPlatformStartIgniteUtils, "stop", "(Ljava/lang/String;)V", name);
        }

        /** */
        private static unsafe void CallStringMethod(string className, string methodName, string methodSig, string arg)
        {
            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(className))
            {
                var methodId = env.GetStaticMethodId(cls, methodName, methodSig);
                using (var gridNameRef = env.NewStringUtf(arg))
                {
                    var args = stackalloc long[1];
                    args[0] = gridNameRef.Target.ToInt64();

                    env.CallStaticVoidMethod(cls, methodId, args);
                }
            }
        }

        /** */
        private static unsafe void CallVoidMethod(string className, string methodName, string methodSig)
        {
            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(className))
            {
                var methodId = env.GetStaticMethodId(cls, methodName, methodSig);
                env.CallStaticVoidMethod(cls, methodId);
            }
        }

        /** */
        private static unsafe string CallStringMethod(string className, string methodName, string methodSig)
        {
            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(className))
            {
                var methodId = env.GetStaticMethodId(cls, methodName, methodSig);
                var res = env.CallStaticObjectMethod(cls, methodId);
                return env.JStringToString(res.Target);
            }
        }
    }
}
