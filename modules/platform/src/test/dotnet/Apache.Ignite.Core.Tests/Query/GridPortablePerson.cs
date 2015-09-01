/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Query
{
    using System;
    using Apache.Ignite.Core.Portable;

    /**
     * 
     */
    class GridPortablePerson : IPortableMarshalAware {
        /**
         * 
         */
        public GridPortablePerson(String _name, int _age) 
        {
            this.name = _name;
            this.age = _age;
        }

        /**
         * 
         */
        public GridPortablePerson(String _name, int _age, String _address)
        {
            this.name = _name;
            this.address = _address;
            this.age = _age;
        }

        /**
         * 
         */
        public String name 
        { 
            get; 
            set; 
        }

        /**
         * 
         */
        public String address
        {
            get;
            set;
        }

        /**
         * 
         */
        public int age 
        {
            get;
            set;
        }

        /**
         * <summary>Writes this object to the given writer.</summary>
         *
         * <param name="writer">Writer.</param>
         * <exception cref="System.IO.IOException">If write failed.</exception>
         */
        public void WritePortable(IPortableWriter writer) {
            writer.WriteString("name", name);
            writer.WriteString("address", address);
            writer.WriteInt("age", age);
        }

        /**
         * <summary>Reads this object from the given reader.</summary>
         *
         * <param name="reader">Reader.</param>
         * <exception cref="System.IO.IOException">If read failed.</exception>
         */
        public void ReadPortable(IPortableReader reader) {
            name = reader.ReadString("name");
            address = reader.ReadString("address");
            age = reader.ReadInt("age");
        }
    }
}
