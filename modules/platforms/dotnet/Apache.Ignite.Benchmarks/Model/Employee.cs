/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Model
{
    using GridGain.Portable;

    /// <summary>
    /// Employee.
    /// </summary>
    public class Employee : IPortableMarshalAware
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
        void IPortableMarshalAware.WritePortable(IPortableWriter writer)
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
            writer.WriteObject<Address>("address", Address);

            //IGridPortableRawWriter rawWiter = writer.RawWriter();

            //rawWiter.WriteInt(Id);
            //rawWiter.WriteInt(CompanyId);
            //rawWiter.WriteInt(Age);
            //rawWiter.WriteInt(Points);
            //rawWiter.WriteByte((byte)SexType);
            //rawWiter.WriteByte((byte)Department);
            //rawWiter.WriteLong(Salary);
            //rawWiter.WriteByteArray(Payload);
            //rawWiter.WriteString(Name);
            //rawWiter.WriteObject<Address>(Address);
        }

        /** <inheritDoc /> */
        void IPortableMarshalAware.ReadPortable(IPortableReader reader)
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
