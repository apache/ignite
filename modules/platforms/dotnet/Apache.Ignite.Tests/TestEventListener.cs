/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests
{
    using System.Diagnostics.Tracing;
    using System.Threading;

    /// <summary>
    /// Diagnostic event listener for tests.
    /// </summary>
    internal class TestEventListener : EventListener
    {
        private int _buffersRented;

        private int _buffersReturned;

        public int BuffersRented => Interlocked.CompareExchange(ref _buffersRented, -1, -1);

        public int BuffersReturned => Interlocked.CompareExchange(ref _buffersRented, -1, -1);

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            base.OnEventSourceCreated(eventSource);

            if (eventSource.Name == "System.Buffers.ArrayPoolEventSource")
            {
                EnableEvents(eventSource, EventLevel.Verbose);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            switch (eventData.EventName)
            {
                case "BufferRented":
                    Interlocked.Increment(ref _buffersRented);
                    break;

                case "BufferReturned":
                    Interlocked.Increment(ref _buffersReturned);
                    break;
            }

            base.OnEventWritten(eventData);
        }
    }
}
