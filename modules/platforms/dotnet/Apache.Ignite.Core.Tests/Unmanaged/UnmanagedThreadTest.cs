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

namespace Apache.Ignite.Core.Tests.Unmanaged
{
    using System;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="UnmanagedThread"/>.
    /// </summary>
    public class UnmanagedThreadTest
    {
        /// <summary>
        /// Tests that ThreadExit event fires when enabled
        /// with <see cref="UnmanagedThread.EnableCurrentThreadExitEvent"/>.
        /// </summary>
        [Test]
        public void TestThreadExitFiresWhenEnabled([Values(true, false)] bool enableThreadExitCallback)
        {
            var evt = new ManualResetEventSlim();
            var threadLocalVal = new IntPtr(42);
            var resultThreadLocalVal = IntPtr.Zero;

            UnmanagedThread.ThreadExitCallback callback = val =>
            {
                evt.Set();
                resultThreadLocalVal = val;
            };

            GC.KeepAlive(callback);
            var callbackId = UnmanagedThread.SetThreadExitCallback(Marshal.GetFunctionPointerForDelegate(callback));

            try
            {
                ParameterizedThreadStart threadStart = _ =>
                {
                    if (enableThreadExitCallback)
                        UnmanagedThread.EnableCurrentThreadExitEvent(callbackId, threadLocalVal);
                };

                var t = new Thread(threadStart);

                t.Start();
                t.Join();

                var threadExitCallbackCalled = evt.Wait(TimeSpan.FromSeconds(1));

                Assert.AreEqual(enableThreadExitCallback, threadExitCallbackCalled);
                Assert.AreEqual(enableThreadExitCallback ? threadLocalVal : IntPtr.Zero, resultThreadLocalVal);
            }
            finally
            {
                UnmanagedThread.RemoveThreadExitCallback(callbackId);
            }
        }

        /// <summary>
        /// Tests that invalid callback id causes and exception.
        /// </summary>
        [Test]
        public void TestInvalidCallbackIdThrowsException()
        {
            Assert.Throws<InvalidOperationException>(() =>
                UnmanagedThread.EnableCurrentThreadExitEvent(int.MaxValue, new IntPtr(1)));

            Assert.Throws<InvalidOperationException>(() =>
                UnmanagedThread.RemoveThreadExitCallback(int.MaxValue));
        }
    }
}
