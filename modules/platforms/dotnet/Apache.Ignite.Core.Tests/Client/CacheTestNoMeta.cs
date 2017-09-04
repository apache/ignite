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

namespace Apache.Ignite.Core.Tests.Client
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using NUnit.Framework;

    /// <summary>
    /// Client cache test without metadata (no-op binary processor).
    /// </summary>
    [TestFixture]
    public class CacheTestNoMeta : CacheTest
    {
        /** <inheritdoc /> */
        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            var cfg = base.GetClientConfiguration();

            cfg.BinaryProcessor = new NoopBinaryProcessor();

            return cfg;
        }

        private class NoopBinaryProcessor : IBinaryProcessor
        {
            /** <inheritdoc /> */
            public BinaryType GetBinaryType(int typeId)
            {
                return null;
            }

            /** <inheritdoc /> */
            public List<IBinaryType> GetBinaryTypes()
            {
                return null;
            }

            /** <inheritdoc /> */
            public int[] GetSchema(int typeId, int schemaId)
            {
                return null;
            }

            /** <inheritdoc /> */
            public void PutBinaryTypes(ICollection<BinaryType> types)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public bool RegisterType(int id, string typeName)
            {
                return false;
            }

            /** <inheritdoc /> */
            public BinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values)
            {
                return null;
            }

            /** <inheritdoc /> */
            public string GetTypeName(int id)
            {
                return null;
            }
        }
    }
}