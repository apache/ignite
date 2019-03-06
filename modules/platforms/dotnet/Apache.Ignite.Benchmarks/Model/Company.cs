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
