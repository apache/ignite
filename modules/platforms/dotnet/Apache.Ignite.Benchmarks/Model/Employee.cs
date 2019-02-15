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
    /// Employee.
    /// </summary>
    internal class Employee : IBinarizable
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
        /// Company ID.
        /// </summary>
        public int CompanyId { get; set; }

        /// <summary>
        /// Age.
        /// </summary>
        public int Age { get; set; }

        /// <summary>
        /// Sex type.
        /// </summary>
        public Sex SexType { get; set; }

        /// <summary>
        /// Salary.
        /// </summary>
        public long Salary { get; set; }

        /// <summary>
        /// Address.
        /// </summary>
        public Address Address { get; set; }

        /// <summary>
        /// Department.
        /// </summary>
        public Department Department { get; set; }

        /// <summary>
        /// Payload.
        /// </summary>
        public byte[] Payload { get; set; }

        /// <summary>
        /// Points.
        /// </summary>
        public int Points { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="name">Name.</param>
        /// <param name="companyId">Company ID.</param>
        /// <param name="age">Age.</param>
        /// <param name="sexType">Sex type.</param>
        /// <param name="salary">Salary.</param>
        /// <param name="address">Address.</param>
        /// <param name="department">Department.</param>
        /// <param name="payloadSize">Payload size.</param>
        public Employee(int id, string name, int companyId, int age, Sex sexType, long salary, Address address,
            Department department, int payloadSize)
        {
            Id = id;
            Name = name;
            CompanyId = companyId;
            Age = age;
            SexType = sexType;
            Salary = salary;
            Address = address;
            Department = department;

            Payload = new byte[payloadSize];

            Points = 100;
        }

        /** <inheritDoc /> */
        void IBinarizable.WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("id", Id);
            writer.WriteInt("companyId", CompanyId);
            writer.WriteInt("age", Age);
            writer.WriteInt("points", Points);
            writer.WriteByte("sex", (byte)SexType);
            writer.WriteByte("department", (byte)Department);
            writer.WriteLong("salary", Salary);
            writer.WriteByteArray("payload", Payload);
            writer.WriteString("name", Name);
            writer.WriteObject("address", Address);
        }

        /** <inheritDoc /> */
        void IBinarizable.ReadBinary(IBinaryReader reader)
        {
            Id = reader.ReadInt("id");
            CompanyId = reader.ReadInt("companyId");
            Age = reader.ReadInt("age");
            Points = reader.ReadInt("points");
            SexType = (Sex)reader.ReadByte("sex");
            Department = (Department)reader.ReadByte("department");
            Salary = reader.ReadLong("salary");
            Payload = reader.ReadByteArray("payload");
            Name = reader.ReadString("name");
            Address = reader.ReadObject<Address>("address");
        }
    }
}
