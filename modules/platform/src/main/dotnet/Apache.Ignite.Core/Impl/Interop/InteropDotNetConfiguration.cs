/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Java
{
    using System.Collections.Generic;
    using GridGain.Impl.Portable;
    using GridGain.Portable;

    /// <summary>
    /// .Net configuration as defined in Java configuration file.
    /// </summary>
    internal class InteropDotNetConfiguration : IPortableWriteAware
    {
        /// <summary>
        /// Portable configuration.
        /// </summary>
        public InteropDotNetPortableConfiguration PortableCfg { get; set; }

        /// <summary>
        /// Assemblies to load.
        /// </summary>
        public IList<string> Assemblies { get; set; }

        /** {@inheritDoc} */
        public void WritePortable(IPortableWriter writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteObject(PortableCfg);

            rawWriter.WriteGenericCollection(Assemblies);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InteropDotNetConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public InteropDotNetConfiguration(IPortableReader reader)
        {
            IPortableRawReader rawReader = reader.RawReader();

            PortableCfg = rawReader.ReadObject<InteropDotNetPortableConfiguration>();

            Assemblies = (List<string>) rawReader.ReadGenericCollection<string>();
        }
    }
}
