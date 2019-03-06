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
    using System;

    /// <summary>
    /// Binary builder field.
    /// </summary>
    internal class BinaryBuilderField
    {
        /** Remove marker. */
        public static readonly BinaryBuilderField RmvMarker = new BinaryBuilderField(null, null, 0);

        /** Type. */
        private readonly Type _type;

        /** Value. */
        private readonly object _value;
        
        /** Write action. */
        private readonly Action<BinaryWriter, object> _writeAction;
        
        /** Type id. */
        private readonly byte _typeId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="value">Value.</param>
        /// <param name="typeId">The type identifier.</param>
        /// <param name="writeAction">Optional write action.</param>
        public BinaryBuilderField(Type type, object value, byte typeId, Action<BinaryWriter, object> writeAction = null)
        {
            _type = type;
            _value = value;
            _typeId = typeId;
            _writeAction = writeAction;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get { return _type; }
        }

        /// <summary>
        /// Value.
        /// </summary>
        public object Value
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets the write action.
        /// </summary>
        public Action<BinaryWriter, object> WriteAction
        {
            get { return _writeAction; }
        }

        /// <summary>
        /// Gets the type identifier.
        /// </summary>
        public byte TypeId
        {
            get { return _typeId; }
        }
    }
}
