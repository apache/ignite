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

namespace Apache.Ignite.Core.Impl.Portable.Structure
{
    using System.Diagnostics;

    /// <summary>
    /// Portable type structure entry. Might be either a normal field, a reference to jump table, or an empty entry.
    /// </summary>
    internal struct PortableStructureEntry
    {
        /** Field name. */
        private readonly string _name;

        /** Field ID. */
        private readonly int _id;

        /** Field type. */
        //private readonly byte _type;
        
        /// <summary>
        /// Constructor for jump table entry.
        /// </summary>
        /// <param name="jumpTblIdx">Jump table index.</param>
        public PortableStructureEntry(int jumpTblIdx)
        {
            Debug.Assert(jumpTblIdx > 0);

            _name = null;
            _id = jumpTblIdx;
        }

        /// <summary>
        /// Constructor for field entry.
        /// </summary>
        /// <param name="name">Field name.</param>
        /// <param name="id">Field ID.</param>
        public PortableStructureEntry(string name, int id)
        {
            Debug.Assert(name != null);

            _name = name;
            _id = id;
        }

        /// <summary>
        /// Check whether current field entry matches passed arguments.
        /// </summary>
        /// <param name="name">Field name.</param>
        /// <returns>True if expected.</returns>
        public bool IsExpected(string name)
        {
            // Perform reference equality check first because field name is a literal in most cases.
            if (ReferenceEquals(_name, name)) 
                return true;

            return name.Equals(_name);
        }

        /// <summary>
        /// Whether this is an empty entry.
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty
        {
            get { return _id == 0; }
        }

        /// <summary>
        /// Whether this is a jump table.
        /// </summary>
        public bool IsJumpTable
        {
            get { return _name == null && _id >= 0; }
        }

        /// <summary>
        /// Field name.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Field ID.
        /// </summary>
        public int Id
        {
            get { return _id; }
        }
    }
}
