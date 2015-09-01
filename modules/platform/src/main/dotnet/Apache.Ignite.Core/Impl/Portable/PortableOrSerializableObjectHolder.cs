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
    /// Wraps portable/serializable item in a portable.
    /// </summary>
    internal class PortableOrSerializableObjectHolder : IPortableWriteAware
    {
        /** */
        private readonly object item;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="item">The item to wrap.</param>
        public PortableOrSerializableObjectHolder(object item)
        {
            this.item = item;
        }

        /// <summary>
        /// Gets or sets the item to wrap.
        /// </summary>
        public object Item
        {
            get { return item; }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();

            PortableUtils.WritePortableOrSerializable(writer0, Item);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableOrSerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableOrSerializableObjectHolder(IPortableReader reader)
        {
            item = PortableUtils.ReadPortableOrSerializable<object>((PortableReaderImpl)reader.RawReader());
        }
    }
}