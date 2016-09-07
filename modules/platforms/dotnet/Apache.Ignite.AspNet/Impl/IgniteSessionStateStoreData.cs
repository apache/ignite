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
    using System.IO;
    using System.Web;
    using System.Web.SessionState;
    using Apache.Ignite.Core.Impl.AspNet;

    /// <summary>
    /// Wrapper for <see cref="SessionStateData"/>.
    /// </summary>
    internal class IgniteSessionStateStoreData : SessionStateStoreData
    {
        /** */
        private readonly SessionStateData _data;

        /** */
        private readonly IgniteSessionStateItemCollection _items;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="data">The data.</param>
        public IgniteSessionStateStoreData(SessionStateData data) 
            : base(null, DeserializeStaticObjects(data.StaticObjects), 0)
        {
            _data = data;
            _items = new IgniteSessionStateItemCollection(_data.Items);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="staticObjects">The static objects.</param>
        /// <param name="timeout">The timeout.</param>
        public IgniteSessionStateStoreData(HttpStaticObjectsCollection staticObjects, int timeout) 
            : base(null, staticObjects, 0)
        {
            _data = new SessionStateData
            {
                Timeout = timeout
            };

            _items = new IgniteSessionStateItemCollection(_data.Items);
        }

        /// <summary>
        /// The session variables and values for the current session.
        /// </summary>
        public override ISessionStateItemCollection Items
        {
            get { return _items; }
        }

        /// <summary>
        /// Gets and sets the amount of time, in minutes, allowed between requests before the session-state 
        /// provider terminates the session.
        /// </summary>
        public override int Timeout
        {
            get { return _data.Timeout; }
            set { _data.Timeout = value; }
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public SessionStateData Data
        {
            get
            {
                _data.StaticObjects = SerializeStaticObjects();

                return _data;
            }
        }

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
