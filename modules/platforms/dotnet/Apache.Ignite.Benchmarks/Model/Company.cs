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
    /// Company.
    /// </summary>
    public class Company : IPortableMarshalAware
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

        public Company() { }

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
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteInt("id", Id);
            writer.WriteInt("size", Size);
            writer.WriteString("name", Name);
            writer.WriteString("occupation", Occupation);
            writer.WriteObject<Address>("address", Address);

            //IGridPortableRawWriter rawWriter = writer.RawWriter();

            //rawWriter.WriteInt(Id);
            //rawWriter.WriteInt(Size);
            //rawWriter.WriteString(Name);
            //rawWriter.WriteString(Occupation);
            //rawWriter.WriteObject<Address>(Address);
        }

        /** <inheritDoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadInt("id");
            Size = reader.ReadInt("size");
            Name = reader.ReadString("name");
            Occupation = reader.ReadString("occupation");
            Address = reader.ReadObject<Address>("address");
        }
    }
}
