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

    [GridClientPortableId(1)]
    public class Address : IGridClientPortable
    {
        public string city { get; set; }
        public string street { get; set; }
        public int streetNumber { get; set; }
        public int? flatNumber { get; set; }

        public Address() 
        {
            // No-op.
        }

        public Address(String city, String street, int streetNum, int flatNum)
        {
            this.city = city;
            this.street = street;
            this.streetNumber = streetNum;
            this.flatNumber = flatNum;
        }

        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteString(city);
            rawWriter.WriteString(street);
            rawWriter.WriteInt(streetNumber);
            rawWriter.WriteObject<int?>(flatNumber);
        }

        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            city = rawReader.ReadString();
            street = rawReader.ReadString();
            streetNumber = rawReader.ReadInt();
            flatNumber = rawReader.ReadObject<int?>();
        }
    }
}
