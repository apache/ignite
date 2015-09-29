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
    using System;

    using GridGain.Portable;

    /// <summary>
    /// Address.
    /// </summary>
    public class Address : IPortableMarshalAware
    {
        /// <summary>
        /// City.
        /// </summary>
        public String City { get; set; }
        
        /// <summary>
        /// Street.
        /// </summary>
        public String Street { get; set; }
        
        /// <summary>
        /// Street number.
        /// </summary>
        public int StreetNumber { get; set; }

        /// <summary>
        /// Flat number.
        /// </summary>
        public int FlatNumber { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="city">City.</param>
        /// <param name="street">Street.</param>
        /// <param name="streetNum">Street number.</param>
        /// <param name="flatNum">Flat number.</param>
        public Address(String city, String street, int streetNum, int flatNum)
        {
            City = city;
            Street = street;
            StreetNumber = streetNum;
            FlatNumber = flatNum;
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteInt("streetNum", StreetNumber);
            writer.WriteInt("flatNum", FlatNumber);
            writer.WriteString("city", City);
            writer.WriteString("street", Street);

            //IGridPortableRawWriter rawWriter = writer.RawWriter();

            //rawWriter.WriteInt(StreetNumber);
            //rawWriter.WriteInt(FlatNumber);
            //rawWriter.WriteString(City);
            //rawWriter.WriteString(Street);
        }

        /** <inheritDoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            StreetNumber = reader.ReadInt("streetNum");
            FlatNumber = reader.ReadInt("flatNum");
            City = reader.ReadString("city");
            Street = reader.ReadString("street");
        }
    }
}
