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

    [GridClientPortableId(4)]
    public class ExtendedEmployee : IGridClientPortable
    {
        public Employee employee { get; set; }
        public Company company { get; set; }

        public ExtendedEmployee()
        {
            // No-op.
        }

        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteObject<Employee>(employee);
            rawWriter.WriteObject<Company>(company);
        }

        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            employee = rawReader.ReadObject<Employee>();
            company = rawReader.ReadObject<Company>();
        }
    }
}
