/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Barclays.Model
{
    using System;
    using GridGain.Client.Portable;

    [GridClientPortableId(3)]
    public class Employee : IGridClientPortable
    {
        public int id { get; set; }
        public string name { get; set; }
        public int companyId { get; set; }
        public int age { get; set; }
        public Sex sexType { get; set; }
        public long salary { get; set; }
        public Address address { get; set; }
        public Department department;
        public byte[] payload { get; set; }

        public Employee() : this(1024) 
        {
            // No-op.
        }

        public Employee(int payloadSize)
        {
            payload = new byte[payloadSize];
        }
        
        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteInt(id);
            rawWriter.WriteString(name);
            rawWriter.WriteInt(companyId);
            rawWriter.WriteInt(age);
            rawWriter.WriteInt((int)sexType);
            rawWriter.WriteLong(salary);
            rawWriter.WriteObject<Address>(address);
            rawWriter.WriteInt((int)department);
            rawWriter.WriteByteArray(payload);
        }

        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            id = rawReader.ReadInt();
            name = rawReader.ReadString();
            companyId = rawReader.ReadInt();
            age = rawReader.ReadInt();
            sexType = (Sex)rawReader.ReadInt();
            salary = rawReader.ReadLong();
            address = rawReader.ReadObject<Address>();
            department = (Department)rawReader.ReadInt();
            payload = rawReader.ReadByteArray();
        }
    }
}
