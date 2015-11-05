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
