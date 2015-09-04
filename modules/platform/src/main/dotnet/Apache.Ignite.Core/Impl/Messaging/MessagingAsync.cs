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

namespace Apache.Ignite.Core.Impl.Messaging
{
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async messaging implementation.
    /// </summary>
    internal class MessagingAsync : Messaging
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MessagingAsync" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="prj">Cluster group.</param>
        public MessagingAsync(IUnmanagedTarget target, PortableMarshaller marsh, 
            IClusterGroup prj) : base(target, marsh, prj)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override IMessaging WithAsync()
        {
            return this;
        }

        /** <inheritdoc /> */
        public override bool IsAsync
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public override IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritdoc /> */
        public override IFuture<T> GetFuture<T>()
        {
            return GetFuture<T>((futId, futTyp) => UU.TargetListenFuture(Target, futId, futTyp));
        }
    }
}