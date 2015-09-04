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

namespace Apache.Ignite.Core.Impl.Services
{
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Services;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async services implementation.
    /// </summary>
    internal class ServicesAsync : Services
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServicesAsync" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <param name="keepPortable">Portable flag.</param>
        /// <param name="srvKeepPortable">Server portable flag.</param>
        public ServicesAsync(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup,
            bool keepPortable, bool srvKeepPortable)
            : base(target, marsh, clusterGroup, keepPortable, srvKeepPortable)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public override bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public override IServices WithKeepPortable()
        {
            if (KeepPortable)
                return this;

            return new ServicesAsync(Target, Marshaller, ClusterGroup, true, SrvKeepPortable);
        }

        /** <inheritDoc /> */
        public override IServices WithServerKeepPortable()
        {
            if (SrvKeepPortable)
                return this;

            return new ServicesAsync(Target, Marshaller, ClusterGroup, KeepPortable, true);
        }

        /** <inheritDoc /> */
        public override IServices WithAsync()
        {
            return this;
        }

        /** <inheritDoc /> */
        public override IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritDoc /> */
        public override IFuture<T> GetFuture<T>()
        {
            return GetFuture<T>((futId, futTyp) => UU.TargetListenFuture(Target, futId, futTyp));
        }
    }
}