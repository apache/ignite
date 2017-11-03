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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.IO;
    using System.Web;
    using System.Web.SessionState;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Ignite <see cref="SessionStateStoreData"/> implementation.
    /// </summary>
    internal class IgniteSessionStateStoreData : SessionStateStoreData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public IgniteSessionStateStoreData(IBinaryRawReader reader) : base(
            new IgniteSessionStateItemCollection(reader),
            DeserializeStaticObjects(reader.ReadByteArray()), reader.ReadInt())
        {
            LockNodeId = reader.ReadGuid();
            LockId = reader.ReadLong();
            LockTime = reader.ReadTimestamp();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="staticObjects">The static objects.</param>
        /// <param name="timeout">The timeout.</param>
        public IgniteSessionStateStoreData(HttpStaticObjectsCollection staticObjects, int timeout)
            : base(new IgniteSessionStateItemCollection(), staticObjects, timeout)
        {
            // No-op.
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        public void WriteBinary(IBinaryRawWriter writer, bool changesOnly)
        {
            ((IgniteSessionStateItemCollection)Items).WriteBinary(writer, changesOnly);
            writer.WriteByteArray(SerializeStaticObjects());
            writer.WriteInt(Timeout);

            writer.WriteGuid(LockNodeId);
            writer.WriteLong(LockId);
            writer.WriteTimestamp(LockTime);
        }

        /// <summary>
        /// Gets or sets the lock node id. Null means not locked.
        /// </summary>
        public Guid? LockNodeId { get; set; }

        /// <summary>
        /// Gets or sets the lock id.
        /// </summary>
        public long LockId { get; set; }

        /// <summary>
        /// Gets or sets the lock time.
        /// </summary>
        public DateTime? LockTime { get; set; }

        /// <summary>
        /// Deserializes the static objects.
        /// </summary>
        private static HttpStaticObjectsCollection DeserializeStaticObjects(byte[] bytes)
        {
            if (bytes == null)
                return new HttpStaticObjectsCollection();

            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return HttpStaticObjectsCollection.Deserialize(reader);
            }
        }

        /// <summary>
        /// Serializes the static objects.
        /// </summary>
        private byte[] SerializeStaticObjects()
        {
            if (StaticObjects == null || StaticObjects.Count == 0)
                return null;

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                StaticObjects.Serialize(writer);

                return stream.ToArray();
            }
        }
    }
}
