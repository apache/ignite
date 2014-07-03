// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using GridGain.Client.Portable;

    /**
     * 
     */
    class GridPortablePerson : IGridClientPortable {
        /**
         * 
         */
        public GridPortablePerson(String _name, int _age) {
            this.name = _name;
            this.age = _age;
        }

        /**
         * 
         */
        public String name { 
            get; 
            set; 
        }

        /**
         * 
         */
        public int age {
            get;
            set;
        }

        /**
         * <summary>Writes this object to the given writer.</summary>
         *
         * <param name="writer">Writer.</param>
         * <exception cref="System.IO.IOException">If write failed.</exception>
         */
        public void WritePortable(IGridClientPortableWriter writer) {
            writer.WriteString("name", name);
            writer.WriteInt("age", age);
        }

        /**
         * <summary>Reads this object from the given reader.</summary>
         *
         * <param name="reader">Reader.</param>
         * <exception cref="System.IO.IOException">If read failed.</exception>
         */
        public void ReadPortable(IGridClientPortableReader reader) {
            name = reader.ReadString("name");
            age = reader.ReadInt("age");
        }
    }
}
