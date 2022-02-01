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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Tuple of two values, maps to a Java class with the same name.
    /// </summary>
    internal class IgniteBiTuple : Tuple<object, object>, IBinaryWriteAware
    {
        /// <summary>
        /// Initializes a new instance of <see cref="IgniteBiTuple"/> class.
        /// </summary>
        public IgniteBiTuple(object item1, object item2) : base(item1, item2)
        {
            // No-op.
        }

        public IgniteBiTuple(BinaryReader reader) : base(reader.ReadObject<object>(), reader.ReadObject<object>())
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = (BinaryWriter) writer.GetRawWriter();

            raw.WriteObjectDetached(Item1);
            raw.WriteObjectDetached(Item2);
        }
    }
}