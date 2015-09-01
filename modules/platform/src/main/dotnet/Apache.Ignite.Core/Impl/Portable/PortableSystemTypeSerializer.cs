﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    using System;
    using System.Diagnostics;
    using GridGain.Portable;

    /// <summary>
    /// Portable serializer for system types.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class PortableSystemTypeSerializer<T> : IPortableSystemTypeSerializer where T : IPortableWriteAware
    {
        /** Ctor delegate. */
        private readonly Func<PortableReaderImpl, T> ctor;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableSystemTypeSerializer{T}"/> class.
        /// </summary>
        /// <param name="ctor">Constructor delegate.</param>
        public PortableSystemTypeSerializer(Func<PortableReaderImpl, T> ctor)
        {
            Debug.Assert(ctor != null);

            this.ctor = ctor;
        }

        /** <inheritdoc /> */
        public void WritePortable(object obj, IPortableWriter writer)
        {
            ((T) obj).WritePortable(writer);
        }

        /** <inheritdoc /> */
        public void ReadPortable(object obj, IPortableReader reader)
        {
            throw new NotSupportedException("System serializer does not support ReadPortable.");
        }

        /** <inheritdoc /> */
        public object ReadInstance(PortableReaderImpl reader)
        {
            return ctor(reader);
        }
    }
}