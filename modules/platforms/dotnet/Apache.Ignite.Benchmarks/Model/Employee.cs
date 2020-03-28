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
    using System;
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
        /// Birthday.
        /// </summary>
        public DateTime Birthday { get; set; }
        
        /// <summary>
        /// Timespan.
        /// </summary>
        public DateTime Timestamp { get; set; }

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

            Birthday = new DateTime(2005, 5, 5, 1, 1, 1, DateTimeKind.Local).AddHours(id);
            Timestamp = new DateTime(2005, 5, 5, 1, 1, 1, DateTimeKind.Utc).AddMinutes(id);
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
            writer.WriteObject("birthday", Birthday);
            writer.WriteTimestamp("timestamp", Timestamp);
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
            Birthday = reader.ReadObject<DateTime>("birthday");
            Timestamp = reader.ReadTimestamp("timestamp").GetValueOrDefault();
        }
    }
}
