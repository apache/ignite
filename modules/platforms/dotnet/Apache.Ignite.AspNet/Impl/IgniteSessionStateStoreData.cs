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
    /// Wrapper for <see cref="BinarizableSessionStateStoreData"/>.
    /// </summary>
    internal class IgniteSessionStateStoreData : SessionStateStoreData
    {
        private readonly BinarizableSessionStateStoreData _data;

        private readonly IgniteSessionStateItemCollection _items;

        public IgniteSessionStateStoreData(BinarizableSessionStateStoreData data) 
            : base(null, DeserializeStaticObjects(data.StaticObjects), data.Timeout)
        {
            _data = data;
            _items = new IgniteSessionStateItemCollection(_data.Items);
        }

        public IgniteSessionStateStoreData(HttpStaticObjectsCollection staticObjects, int timeout) 
            : base(null, staticObjects, timeout)
        {
            // TODO: Copy statics
            _data = new BinarizableSessionStateStoreData();
            _items = new IgniteSessionStateItemCollection(_data.Items);
        }

        public override ISessionStateItemCollection Items
        {
            get { return _items; }
        }

        public BinarizableSessionStateStoreData Data
        {
            get { return _data; }
        }

        private static HttpStaticObjectsCollection DeserializeStaticObjects(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return reader.ReadBoolean() ? HttpStaticObjectsCollection.Deserialize(reader) : null;
            }
        }

        //public static byte[] Serialize(SessionStateStoreData data)
        //{
        //    Debug.Assert(data != null);

        //    using (var stream = new MemoryStream())
        //    using (var writer = new BinaryWriter(stream))
        //    {
        //        writer.Write(data.Timeout);

        //        var items = data.Items as SessionStateItemCollection;

        //        if (items != null)
        //        {
        //            writer.Write(true);
        //            items.Serialize(writer);
        //        }
        //        else
        //            writer.Write(false);

        //        if (data.StaticObjects != null)
        //        {
        //            writer.Write(true);
        //            data.StaticObjects.Serialize(writer);
        //        }
        //        else
        //            writer.Write(false);

        //        return stream.ToArray();
        //    }
        //}
    }
}
