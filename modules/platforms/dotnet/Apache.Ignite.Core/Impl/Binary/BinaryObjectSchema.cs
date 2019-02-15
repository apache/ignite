/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Collections.Generic;

    /// <summary>
    /// Holds and manages binary object schemas for a specific type.
    /// </summary>
    internal class BinaryObjectSchema
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

        /// <summary>
        /// Gets all schemas.
        /// </summary>
        public IEnumerable<KeyValuePair<int, int[]>> GetAll()
        {
            if (_schema1 == null)
                yield break;

            yield return new KeyValuePair<int, int[]>(_schemaId1, _schema1);

            if (_schema2 == null)
                yield break;

            yield return new KeyValuePair<int, int[]>(_schemaId2, _schema2);

            if (_schemas != null)
                foreach (var pair in _schemas)
                    yield return pair;
        }
    }
}