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
    using System.Diagnostics;
    using System.IO;
    using System.Web.SessionState;

    /// <summary>
    /// <see cref="SessionStateStoreData"/> serializer.
    /// </summary>
    internal static class SessionStateStoreDataSerializer
    {
        /// <summary>
        /// Serializes the data.
        /// </summary>
        public static byte[] Serialize(SessionStateStoreData data)
        {
            Debug.Assert(data != null);

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(data.Timeout);

                return stream.ToArray();
            }
        }

        /// <summary>
        /// Deserializes the data.
        /// </summary>
        public static SessionStateStoreData Deserialize(byte[] bytes)
        {
            Debug.Assert(bytes != null);

            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                var timeout = reader.ReadInt32();


                return new SessionStateStoreData(null, null, timeout);
            }
        }
    }
}
