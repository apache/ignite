/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Runtime.Serialization;
    using NUnit.Framework;

    /// <summary>
    /// Object with no fields.
    /// </summary>
    [Serializable]
    public class EmptyObject : ISerializable
    {
        /// <summary>
        /// Initializes a new instance of the EmptyObject class.
        /// </summary>
        public EmptyObject()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the EmptyObject class.
        /// </summary>
        private EmptyObject(SerializationInfo info, StreamingContext context)
        {
            Assert.AreEqual(StreamingContextStates.All, context.State);
            Assert.IsNull(context.Context);
        }

        /** <inheritdoc /> */
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            Assert.AreEqual(StreamingContextStates.All, context.State);
            Assert.IsNull(context.Context);
        }
    }
}
