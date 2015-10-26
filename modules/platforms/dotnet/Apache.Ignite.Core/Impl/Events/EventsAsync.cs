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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async Ignite events.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class EventsAsync : Events
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Events"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        public EventsAsync(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup)
            : base(target, marsh, clusterGroup, true)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public Task<ICollection<T>> RemoteQueryAsyncEx<T>(IEventFilter<T> filter, TimeSpan? timeout = null,
            params int[] types) where T: IEvent
        {
            RemoteQuery(filter, timeout, types);

            return GetFuture((futId, futTyp) => UU.TargetListenFutureForOperation(Target, futId, futTyp,
                (int) Op.RemoteQuery), convertFunc: ReadEvents<T>).ToTask();
        }


        /** <inheritdoc /> */
        public Task<T> WaitForLocalAsyncEx<T>(IEventFilter<T> filter, params int[] types) where T : IEvent
        {
            long hnd = 0;

            try
            {
                WaitForLocal0(filter, ref hnd, types);

                var fut = GetFuture((futId, futTyp) => UU.TargetListenFutureForOperation(Target, futId, futTyp, 
                    (int) Op.WaitForLocal), convertFunc: reader => (T)EventReader.Read<IEvent>(reader));

                if (filter != null)
                {
                    // Dispose handle as soon as future ends.
                    fut.Listen(() => Ignite.HandleRegistry.Release(hnd));
                }

                return fut.ToTask();
            }
            catch (Exception)
            {
                Ignite.HandleRegistry.Release(hnd);
                throw;
            }
        }
    }
}