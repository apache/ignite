/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Continuous query remote filter holder. Wraps real filter into portable object,
    /// so that it can be passed over wire to another node.
    /// </summary>
    public class ContinuousQueryFilterHolder : IPortableWriteAware
    {
        /** Key type. */
        private readonly Type keyTyp;

        /** Value type. */
        private readonly Type valTyp;

        /** Filter object. */
        private readonly object filter;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="keyTyp">Key type.</param>
        /// <param name="valTyp">Value type.</param>
        /// <param name="filter">Filter.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public ContinuousQueryFilterHolder(Type keyTyp, Type valTyp, object filter, bool keepPortable)
        {
            this.keyTyp = keyTyp;
            this.valTyp = valTyp;
            this.filter = filter;
            this.keepPortable = keepPortable;
        }

        /// <summary>
        /// Key type.
        /// </summary>
        internal Type KeyType
        {
            get { return keyTyp; }
        }

        /// <summary>
        /// Value type.
        /// </summary>
        internal Type ValueType
        {
            get { return valTyp; }
        }

        /// <summary>
        /// Filter.
        /// </summary>
        internal object Filter
        {
            get { return filter; }
        }

        /// <summary>
        /// Keep portable flag.
        /// </summary>
        internal bool KeepPortable
        {
            get { return keepPortable; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WritePortable(IPortableWriter writer)
        {
            PortableWriterImpl rawWriter = (PortableWriterImpl) writer.RawWriter();

            PortableUtils.WritePortableOrSerializable(rawWriter, keyTyp);
            PortableUtils.WritePortableOrSerializable(rawWriter, valTyp);
            PortableUtils.WritePortableOrSerializable(rawWriter, filter);

            rawWriter.WriteBoolean(keepPortable);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ContinuousQueryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ContinuousQueryFilterHolder(IPortableReader reader)
        {
            PortableReaderImpl rawReader = (PortableReaderImpl) reader.RawReader();

            keyTyp = PortableUtils.ReadPortableOrSerializable<Type>(rawReader);
            valTyp = PortableUtils.ReadPortableOrSerializable<Type>(rawReader);
            filter = PortableUtils.ReadPortableOrSerializable<object>(rawReader);
            keepPortable = rawReader.ReadBoolean();
        }
    }
}
