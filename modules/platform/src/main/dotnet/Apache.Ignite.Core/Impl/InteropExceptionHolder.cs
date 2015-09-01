/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl
{
    using System;
    using System.Runtime.Serialization.Formatters.Binary;

    using GridGain.Impl.Portable;
    using GridGain.Impl.Portable.IO;
    using GridGain.Portable;

    /// <summary>
    /// Holder of exception which must be serialized to Java and then backwards to the native platform.
    /// </summary>
    internal class InteropExceptionHolder : IPortableMarshalAware
    {
        /** Initial exception. */
        private Exception err;

        /// <summary>
        /// Constructor.
        /// </summary>
        public InteropExceptionHolder()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="err">Error.</param>
        public InteropExceptionHolder(Exception err)
        {
            this.err = err;
        }

        /// <summary>
        /// Underlying exception.
        /// </summary>
        public Exception Error
        {
            get { return err; }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            PortableWriterImpl writer0 = writer.RawWriter() as PortableWriterImpl;

            if (writer0.IsPortable(err))
            {
                writer0.WriteBoolean(true);
                writer0.WriteObject<Exception>(err);
            }
            else
            {
                writer0.WriteBoolean(false);

                BinaryFormatter bf = new BinaryFormatter();

                bf.Serialize(new PortableStreamAdapter(writer0.Stream), err);
            }
        }

        /** <inheritDoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            throw new NotImplementedException();
        }
    }
}
