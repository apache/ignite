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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System.Collections.Generic;

    /// <summary>
    /// Holds and manages portable object schemas for a specific type.
    /// </summary>
    internal class PortableObjectSchemaDictionary
    {
        /** First schema id. */
        private volatile int _schemaId1;

        /** First schema. */
        private volatile int[] _schema1;

        /** Second schema id. */
        private volatile int _schemaId2;

        /** Second schema. */
        private volatile int[] _schema2;

        /** Other schemas. */
        private volatile Dictionary<int, int[]> _schemas;

        /// <summary>
        /// Gets the schema by id.
        /// </summary>
        /// <param name="id">Schema id.</param>
        /// <returns>Schema or null.</returns>
        public int[] Get(int id)
        {
            if (_schemaId1 == id)
                return _schema1;

            if (_schemaId2 == id)
                return _schema2;

            int[] res;

            if (_schemas != null && _schemas.TryGetValue(id, out res))
                return res;

            return null;
        }

        /// <summary>
        /// Adds the schema.
        /// </summary>
        /// <param name="id">Schema id.</param>
        /// <param name="schema">Schema.</param>
        public void Add(int id, int[] schema)
        {
            lock (this)
            {
                if (_schemaId1 == id || _schemaId2 == id || (_schemas != null && _schemas.ContainsKey(id)))
                    return;

                if (_schema1 == null)
                {
                    _schemaId1 = id;
                    _schema1 = schema;
                }
                else if (_schema2 == null)
                {
                    _schemaId2 = id;
                    _schema2 = schema;
                }
                else
                {
                    var schemas = _schemas == null 
                        ? new Dictionary<int, int[]>() 
                        : new Dictionary<int, int[]>(_schemas);

                    schemas.Add(id, schema);

                    _schemas = schemas;
                }
            }
        }
    }
}