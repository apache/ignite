/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable
{
    /// <summary>
    /// Portable serializer. 
    /// </summary> 
    public interface IPortableSerializer
    {
        /// <summary>
        /// Write portalbe object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="writer">Poratble writer.</param>
        void WritePortable(object obj, IPortableWriter writer);

        /// <summary>
        /// Read portable object.
        /// </summary>
        /// <param name="obj">Instantiated empty object.</param>
        /// <param name="reader">Poratble reader.</param>
        void ReadPortable(object obj, IPortableReader reader);
    }
}
