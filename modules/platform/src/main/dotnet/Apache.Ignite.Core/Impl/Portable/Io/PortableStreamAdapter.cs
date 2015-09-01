/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable.IO
{
    using System;
    using System.IO;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Adapter providing .Net streaming functionality over the portable stream.
    /// </summary>
    internal class PortableStreamAdapter : Stream
    {
        /// <summary>
        /// 
        /// </summary>
        private readonly IPortableStream stream;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="stream">Stream.</param>
        public PortableStreamAdapter(IPortableStream stream)
        {
            this.stream = stream;
        }

        /** <inheritDoc /> */
        public override void Write(byte[] buffer, int offset, int count)
        {
            stream.Write(buffer, offset, count);
        }

        /** <inheritDoc /> */
        public override int Read(byte[] buffer, int offset, int count)
        {
            stream.Read(buffer, offset, count);

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
