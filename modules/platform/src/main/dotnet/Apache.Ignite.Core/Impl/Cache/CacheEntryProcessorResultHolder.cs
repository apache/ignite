/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

                    if (Entry.State == MutableCacheEntryState.ValueSet)
                        writer.Write(Entry.Value);

                    writer.Write(ProcessResult);
                }
                else
                {
                    writer.WriteByte((byte) MutableCacheEntryState.ErrPortable);
                    writer.Write(new PortableResultWrapper(Error));
                }
            }
            catch (Exception marshErr)
            {
                writer.Stream.Seek(pos, SeekOrigin.Begin);

                writer.WriteByte((byte) MutableCacheEntryState.ErrString);

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