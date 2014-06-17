/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;

    /**
     * <summary>Interface to implement custom portable serialization logic.</summary>
     */
    public interface IGridClientPortableEx 
    {
        /**
         * <summary>Writes this object to the given writer.</summary>
         * 
         * <param name="writer">Writer.</param>
         * <exception cref="System.IO.IOException">If write failed.</exception>
         */
        void WritePortable(IGridClientPortableWriter writer);

        /**
         * <summary>Reads this object from the given reader.</summary>
         * 
         * <param name="reader">Reader.</param>
         * <exception cref="System.IO.IOException">If read failed.</exception>
         */
        void ReadPortable(IGridClientPortableReader reader);
    }
}
