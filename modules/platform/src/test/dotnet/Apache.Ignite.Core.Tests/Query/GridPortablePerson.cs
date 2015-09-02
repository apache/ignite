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

namespace Apache.Ignite.Core.Tests.Query
{
    using System;
    using Apache.Ignite.Core.Portable;

    /**
     * 
     */
    class GridPortablePerson : IPortableMarshalAware {
        /**
         * 
         */
        public GridPortablePerson(String _name, int _age) 
        {
            this.name = _name;
            this.age = _age;
        }

        /**
         * 
         */
        public GridPortablePerson(String _name, int _age, String _address)
        {
            this.name = _name;
            this.address = _address;
            this.age = _age;
        }

        /**
         * 
         */
        public String name 
        { 
            get; 
            set; 
        }

        /**
         * 
         */
        public String address
        {
            get;
            set;
        }

        /**
         * 
         */
        public int age 
        {
            get;
            set;
        }

        /**
         * <summary>Writes this object to the given writer.</summary>
         *
         * <param name="writer">Writer.</param>
         * <exception cref="System.IO.IOException">If write failed.</exception>
         */
        public void WritePortable(IPortableWriter writer) {
            writer.WriteString("name", name);
            writer.WriteString("address", address);
            writer.WriteInt("age", age);
        }

        /**
         * <summary>Reads this object from the given reader.</summary>
         *
         * <param name="reader">Reader.</param>
         * <exception cref="System.IO.IOException">If read failed.</exception>
         */
        public void ReadPortable(IPortableReader reader) {
            name = reader.ReadString("name");
            address = reader.ReadString("address");
            age = reader.ReadInt("age");
        }
    }
}
