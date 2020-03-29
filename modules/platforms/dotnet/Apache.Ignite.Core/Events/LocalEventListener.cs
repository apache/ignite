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

namespace Apache.Ignite.Core.Events
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Abstract local event listener holder for <see cref="IgniteConfiguration.LocalEventListeners"/>.
    /// Use <see cref="LocalEventListener{T}"/> derived class.
    /// </summary>
    public abstract class LocalEventListener
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LocalEventListener"/> class.
        /// </summary>
        protected internal LocalEventListener()
        {
            // No-op.
        }

        /// <summary>
        /// Gets or sets the event types.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<int> EventTypes { get; set; }

        /// <summary>
        /// Gets the original user listener object.
        /// </summary>
        internal abstract object ListenerObject { get; }

        /// <summary>
        /// Invokes the specified reader.
        /// </summary>
        internal abstract bool Invoke(BinaryReader reader);
    }

    /// <summary>
    /// Generic local event listener holder, see <see cref="IgniteConfiguration.LocalEventListeners"/>.
    /// </summary>
    public class LocalEventListener<T> : LocalEventListener where T : IEvent
    {
        /// <summary>
        /// Gets or sets the listener.
        /// </summary>
        public IEventListener<T> Listener { get; set; }

        /** <inheritdoc /> */
        internal override object ListenerObject
        {
            get { return Listener; }
        }

        /** <inheritdoc /> */
        internal override bool Invoke(BinaryReader reader)
        {
            var evt = EventReader.Read<T>(reader);

            return Listener.Invoke(evt);
        }
    }
}
