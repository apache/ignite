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
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;

    /// <summary>
    /// Test utils: JNI calls.
    /// </summary>
    public class TestUtilsJni
    {
        /// <summary>
        /// Suspend Ignite threads for the given grid.
        /// </summary>
        public static void SuspendThreads(string gridName)
        {
            CallStringMethod(gridName, "org/apache/ignite/platform/PlatformThreadUtils", "suspend",
                "(Ljava/lang/String;)V");
        }

        /// <summary>
        /// Resume Ignite threads for the given grid.
        /// </summary>
        public static void ResumeThreads(string gridName)
        {
            CallStringMethod(gridName, "org/apache/ignite/platform/PlatformThreadUtils", "resume",
                "(Ljava/lang/String;)V");
        }

        /** */
        private static unsafe void CallStringMethod(string gridName, string className, string methodName, string methodSig)
        {
            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(className))
            {
                var methodId = env.GetStaticMethodId(cls, methodName, methodSig);
                using (var gridNameRef = env.NewStringUtf(gridName))
                {
                    var args = stackalloc long[1];
                    args[0] = gridNameRef.Target.ToInt64();

                    env.CallStaticVoidMethod(cls, methodId, args);
                }
            }
        }
    }
}
