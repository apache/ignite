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
    /// Serializer for system types that can create instances directly from a stream and does not support handles.
    /// </summary>
    internal interface IPortableSystemTypeSerializer : IPortableSerializer
    {
        /// <summary>
        /// Reads the instance from a reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>Deserialized instance.</returns>
        object ReadInstance(PortableReaderImpl reader);
    }
}