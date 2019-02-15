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

namespace Apache.Ignite.Core.Impl.Binary.Metadata
{
    using System.Collections.Generic;

    /// <summary>
    /// Metadata handler which uses hash set to determine whether field was already written or not.
    /// </summary>
    internal class BinaryTypeHashsetHandler : IBinaryTypeHandler
    {
        /** Empty fields collection. */
        private static readonly IDictionary<string, BinaryField> EmptyFields = new Dictionary<string, BinaryField>();

        /** IDs known when serialization starts. */
        private readonly ICollection<int> _ids;

        /** New fields. */
        private IDictionary<string, BinaryField> _fieldMap;

        /** */
        private readonly bool _newType;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ids">IDs.</param>
        /// <param name="newType">True is metadata for type is not saved.</param>
        public BinaryTypeHashsetHandler(ICollection<int> ids, bool newType)
        {
            _ids = ids;
            _newType = newType;
        }

        /** <inheritdoc /> */
        public void OnFieldWrite(int fieldId, string fieldName, int typeId)
        {
            if (!_ids.Contains(fieldId))
            {
                if (_fieldMap == null)
                    _fieldMap = new Dictionary<string, BinaryField>();

                if (!_fieldMap.ContainsKey(fieldName))
                    _fieldMap[fieldName] = new BinaryField(typeId, fieldId);
            }
        }

        /** <inheritdoc /> */
        public IDictionary<string, BinaryField> OnObjectWriteFinished()
        {
            return _fieldMap ?? (_newType ? EmptyFields : null);
        }
    }
}
