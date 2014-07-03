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

    [GridClientPortableId(2)]
    public class Company : IGridClientPortable
    {
        public int id { get; set; }
        public string name { get; set; }
        public int size { get; set; }
        public Address address { get; set; }
        public string occupation { get; set; }

        public Company()
        {
            // No-op.
        }

        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteInt(id);
            rawWriter.WriteString(name);
            rawWriter.WriteInt(size);
            rawWriter.WriteObject<Address>(address);
            rawWriter.WriteString(occupation);
        }

        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            id = rawReader.ReadInt();
            name = rawReader.ReadString();
            size = rawReader.ReadInt();
            address = rawReader.ReadObject<Address>();
            occupation = rawReader.ReadString();
        }
    }
}
