/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    using GridGain.Portable;

    /// <summary>
    /// Represents an object that can write itself to a portable writer.
    /// </summary>
    internal interface IPortableWriteAware
    {
        /// <summary>
        /// Writes this object to the given writer.
        /// </summary> 
        /// <param name="writer">Writer.</param>
        /// <exception cref="System.IO.IOException">If write failed.</exception>
        void WritePortable(IPortableWriter writer);
    }
}