/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Manages cache entry processing result in non-generic form.
    /// </summary>
    internal class CacheEntryProcessorResultHolder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResultHolder"/> class.
        /// </summary>
        /// <param name="entry">Entry.</param>
        /// <param name="processResult">Process result.</param>
        /// <param name="error">Error.</param>
        public CacheEntryProcessorResultHolder(IMutableCacheEntryInternal entry, object processResult, Exception error)
        {
            Entry = entry;
            ProcessResult = processResult;
            Error = error;
        }

        /// <summary>
        /// Gets the entry.
        /// </summary>
        public IMutableCacheEntryInternal Entry { get; private set; }

        /// <summary>
        /// Gets the process result.
        /// </summary>
        public object ProcessResult { get; private set; }

        /// <summary>
        /// Gets the error.
        /// </summary>
        public Exception Error { get; private set; }

        /// <summary>
        /// Writes this instance to the stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        public void Write(IPortableStream stream, PortableMarshaller marsh)
        {
            var writer = marsh.StartMarshal(stream);

            try
            {
                Marshal(writer);
            }
            finally
            {
                marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Marshal this instance.
        /// </summary>
        /// <param name="writer">Writer.</param>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any kind of exception can be thrown during user type marshalling.")]
        private void Marshal(PortableWriterImpl writer)
        {
            var pos = writer.Stream.Position;

            try
            {
                if (Error == null)
                {
                    writer.WriteByte((byte) Entry.State);

                    if (Entry.State == MutableCacheEntryState.VALUE_SET)
                        writer.Write(Entry.Value);

                    writer.Write(ProcessResult);
                }
                else
                {
                    writer.WriteByte((byte) MutableCacheEntryState.ERR_PORTABLE);
                    writer.Write(new PortableResultWrapper(Error));
                }
            }
            catch (Exception marshErr)
            {
                writer.Stream.Seek(pos, SeekOrigin.Begin);

                writer.WriteByte((byte) MutableCacheEntryState.ERR_STRING);

                if (Error == null)
                {
                    writer.WriteString(string.Format(
                    "CacheEntryProcessor completed with error, but result serialization failed [errType={0}, " +
                    "err={1}, serializationErrMsg={2}]", marshErr.GetType().Name, marshErr, marshErr.Message));
                }
                else
                {
                    writer.WriteString(string.Format(
                    "CacheEntryProcessor completed with error, and error serialization failed [errType={0}, " +
                    "err={1}, serializationErrMsg={2}]", marshErr.GetType().Name, marshErr, marshErr.Message));
                }
            }
        }
    }
}