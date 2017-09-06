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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Binary add processor.
    /// </summary>
    public class BinarizableAddArgCacheEntryProcessor : AddArgCacheEntryProcessor, IBinarizable
    {
        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var w = writer.GetRawWriter();

            w.WriteBoolean(ThrowErr);
            w.WriteBoolean(ThrowErrBinarizable);
            w.WriteBoolean(ThrowErrNonSerializable);
            w.WriteInt(ThrowOnKey);
            w.WriteBoolean(Remove);
            w.WriteBoolean(Exists);
        }

        /** <inheritdoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            var r = reader.GetRawReader();

            ThrowErr = r.ReadBoolean();
            ThrowErrBinarizable = r.ReadBoolean();
            ThrowErrNonSerializable = r.ReadBoolean();
            ThrowOnKey = r.ReadInt();
            Remove = r.ReadBoolean();
            Exists = r.ReadBoolean();
        }
    }
}