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

namespace Apache.Ignite.Core.Impl.Common
{
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Append-only PortableSchemaField array.
    /// </summary>
    internal class PortableSchemaArray
    {
        /** Array. */
        private PortableObjectSchemaField[] _arr;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="capacity">Capacity.</param>
        public PortableSchemaArray(int capacity)
        {
            _arr = new PortableObjectSchemaField[capacity];
        }

        /// <summary>
        /// Array.
        /// </summary>
        public PortableObjectSchemaField[] Array
        {
            get { return _arr; }
        }

        /// <summary>
        /// Count.
        /// </summary>
        public int Count { get; private set; }

        /// <summary>
        /// Add element.
        /// </summary>
        /// <param name="element">Element.</param>
        public void Add(PortableObjectSchemaField element)
        {
            if (Count == _arr.Length)
                System.Array.Resize(ref _arr, _arr.Length*2);

            _arr[Count++] = element;
        }
    }
}
