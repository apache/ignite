/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Portable;

namespace GridGain.Examples.Portable
{
    /// <summary>
    /// Address.
    /// </summary>
    [Serializable]
    public class Address : IPortableMarshalAware
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="street">Street.</param>
        /// <param name="zip">ZIP code.</param>
        public Address(string street, int zip)
        {
            Street = street;
            Zip = zip;
        }
        
        /// <summary>
        /// Street.
        /// </summary>
        public string Street { get; set; }

        /// <summary>
        /// ZIP code.
        /// </summary>
        public int Zip { get; set; }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteString("street", Street);
            writer.WriteInt("zip", Zip);
        }

        /// <summary>
        /// Reads this object from the given reader.
        /// </summary>
        /// <param name="reader">Reader.</param>
        public void ReadPortable(IPortableReader reader)
        {
            Street = reader.ReadString("street");
            Zip = reader.ReadInt("zip");
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        override public string ToString()
        {
            return string.Format("{0} [street={1}, zip={2}]", typeof(Address).Name, Street, Zip);
        }
    }
}
