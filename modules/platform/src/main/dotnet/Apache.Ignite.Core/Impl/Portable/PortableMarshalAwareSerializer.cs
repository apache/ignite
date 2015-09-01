/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Portable
{
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable serializer which only supports <see cref="IPortableMarshalAware"/> types with a default ctor.
    /// Does not use reflection.
    /// </summary>
    internal class PortableMarshalAwareSerializer : IPortableSerializer
    {
        /// <summary>
        /// Default instance.
        /// </summary>
        public static readonly PortableMarshalAwareSerializer INSTANCE = new PortableMarshalAwareSerializer();

        /** <inheritdoc /> */
        public void WritePortable(object obj, IPortableWriter writer)
        {
            ((IPortableMarshalAware)obj).WritePortable(writer);
        }

        /** <inheritdoc /> */
        public void ReadPortable(object obj, IPortableReader reader)
        {
            ((IPortableMarshalAware)obj).ReadPortable(reader);
        }
    }
}