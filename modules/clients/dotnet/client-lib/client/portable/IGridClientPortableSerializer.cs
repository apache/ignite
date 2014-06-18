using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GridGain.Client.Portable
{
    /**
     * <summary>Portable serializer.</summary>
     */ 
    interface IGridClientPortableSerializer
    {
        /**
         * <summary>Write portalbe object.</summary>
         * <param name="obj">Object.</param>
         * <param name="writer">Poratble writer.</param>
         */
        void WritePortable(object obj, IGridClientPortableWriter writer);

        /**
         * <summary>Read portable object.</summary>
         * <param name="obj">Instantiated empty object.</param>
         * <param name="reader">Poratble reader.</param>
         */
        T ReadPortable<T>(object obj, IGridClientPortableReader reader);
    }
}
