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

namespace Apache.Ignite.Benchmarks.Model
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Company.
    /// </summary>
    internal class Company : IBinarizable
    {
        /// <summary>
        /// ID.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Size.
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Address.
        /// </summary>
        public Address Address { get; set; }

        /// <summary>
        /// Occupation.
        /// </summary>
        public string Occupation { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="name">Name.</param>
        /// <param name="size">Size.</param>
        /// <param name="address">Address.</param>
        /// <param name="occupation">Occupation.</param>
        public Company(int id, string name, int size, Address address, string occupation)
        {
            Id = id;
            Name = name;
            Size = size;
            Address = address;
            Occupation = occupation;
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("id", Id);
            writer.WriteInt("size", Size);
            writer.WriteString("name", Name);
            writer.WriteString("occupation", Occupation);
            writer.WriteObject("address", Address);
        }

        /** <inheritDoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            Id = reader.ReadInt("id");
            Size = reader.ReadInt("size");
            Name = reader.ReadString("name");
            Occupation = reader.ReadString("occupation");
            Address = reader.ReadObject<Address>("address");
        }
    }
}
