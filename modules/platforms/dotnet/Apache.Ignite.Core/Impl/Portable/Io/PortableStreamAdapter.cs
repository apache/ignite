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

namespace Apache.Ignite.Core.Impl.Portable.IO
{
    using System;
    using System.IO;

    /// <summary>
    /// Adapter providing .Net streaming functionality over the portable stream.
    /// </summary>
    internal class PortableStreamAdapter : Stream
    {
        /// <summary>
        /// 
        /// </summary>
        private readonly IPortableStream _stream;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="stream">Stream.</param>
        public PortableStreamAdapter(IPortableStream stream)
        {
            _stream = stream;
        }

        /** <inheritDoc /> */
        public override void Write(byte[] buffer, int offset, int count)
        {
            _stream.Write(buffer, offset, count);
        }

        /** <inheritDoc /> */
        public override int Read(byte[] buffer, int offset, int count)
        {
            _stream.Read(buffer, offset, count);

            return count;
        }

        /** <inheritDoc /> */
        public override void Flush()
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public override bool CanRead
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public override bool CanWrite
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public override bool CanSeek
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("Stream is not seekable.");
        }

        /** <inheritDoc /> */
        public override long Position
        {
            get
            {
                throw new NotSupportedException("Stream is not seekable.");
            }
            set
            {
                throw new NotSupportedException("Stream is not seekable.");
            }
        }

        /** <inheritDoc /> */
        public override long Length
        {
            get 
            {
                throw new NotSupportedException("Stream is not seekable.");
            }
        }

        /** <inheritDoc /> */
        public override void SetLength(long value)
        {
            throw new NotSupportedException("Stream is not seekable.");
        }
    }
}
