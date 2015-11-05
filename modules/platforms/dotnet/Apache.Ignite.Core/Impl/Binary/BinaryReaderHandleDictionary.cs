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
    /// <summary>
    /// Object handle dictionary for <see cref="BinaryReader"/>.
    /// </summary>
    internal class BinaryReaderHandleDictionary : BinaryHandleDictionary<int, object>
    {
        /// <summary>
        /// Constructor with initial key-value pair.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public BinaryReaderHandleDictionary(int key, object val)
            : base(key, val)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override int EmptyKey
        {
            get { return -1; }
        }
    }
}