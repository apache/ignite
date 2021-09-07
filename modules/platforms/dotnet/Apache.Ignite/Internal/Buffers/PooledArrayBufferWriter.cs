/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Buffers
{
    using System;
    using System.Buffers;
    using System.Diagnostics;
    using System.Net;
    using MessagePack;

    /// <summary>
    /// Pooled buffer writer: integrates <see cref="MessagePackWriter"/> with <see cref="ArrayPool{T}"/>,
    /// and adds the logic to prepend messages with size and other data (opcode, request id).
    /// <para />
    /// We reserve some bytes for the prefix because message size, op code and request ID are not known initially.
    /// <para />
    /// There are two ways to use <see cref="MessagePackWriter"/>: with a <see cref="SequencePool"/>,
    /// or with a <see cref="IBufferWriter{T}"/>. SequencePool approach uses buffer pooling too, but still allocates
    /// the final array with <c>MessagePackWriter.FlushAndGetArray</c>. We want to avoid all array allocations,
    /// so we implement our own <see cref="IBufferWriter{T}"/> here.
    /// <para />
    /// Based on <see cref="ArrayBufferWriter{T}"/>, but uses <see cref="ArrayPool{T}.Shared"/> to allocate arrays.
    /// <para />
    /// Not a struct because <see cref="GetMessageWriter"/> will cause boxing.
    /// </summary>
    internal sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
    {
        /** Reserved prefix size. */
        private const int ReservedPrefixSize = 4 + 4 + 9; // Size (4 bytes) + OpCode (4 bytes) + RequestId (9 bytes)/

        /** Underlying pooled array. */
        private byte[] _buffer;

        /** Index within the array. */
        private int _index;

        /** Index within the array: backup for prefix writer mode. */
        private int _indexBackup;

        /** Start index within the array. */
        private int _startIndex;

        /** Disposed flag. */
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> class.
        /// </summary>
        /// <param name="initialCapacity">Initial capacity.</param>
        public PooledArrayBufferWriter(int initialCapacity = PooledBuffer.DefaultCapacity)
        {
            // NOTE: Shared pool has 1M elements limit before .NET 6.
            // https://devblogs.microsoft.com/dotnet/performance-improvements-in-net-6/#buffering
            _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
            _index = ReservedPrefixSize;
            _startIndex = _index;
        }

        /// <summary>
        /// Gets the free capacity.
        /// </summary>
        private int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Gets the written memory.
        /// </summary>
        /// <returns>Written array.</returns>
        public unsafe ReadOnlyMemory<byte> GetWrittenMemory()
        {
            if (_indexBackup > 0)
            {
                _index = _indexBackup;
            }

            // Write big-endian message size to the start of the buffer.
            const int sizeLen = 4;
            Debug.Assert(_startIndex >= sizeLen, "_startIndex >= 4");

            var messageSize = _index - _startIndex;
            var startIndex = _startIndex - sizeLen;

            fixed (byte* bufPtr = &_buffer[startIndex])
            {
                *(int*)bufPtr = IPAddress.HostToNetworkOrder(messageSize);
            }

            return new(_buffer, startIndex, _index - startIndex);
        }

        /// <inheritdoc />
        public void Advance(int count)
        {
            if (count < 0)
            {
                throw new ArgumentException(null, nameof(count));
            }

            if (_index > _buffer.Length - count)
            {
                throw new InvalidOperationException("Can't advance past buffer limit.");
            }

            _index += count;
        }

        /// <inheritdoc />
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsMemory(_index);
        }

        /// <inheritdoc />
        public Span<byte> GetSpan(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsSpan(_index);
        }

        /// <summary>
        /// Gets the <see cref="MessagePackWriter"/> for this buffer.
        /// </summary>
        /// <returns><see cref="MessagePackWriter"/> for this buffer.</returns>
        public MessagePackWriter GetMessageWriter() => new(this);

        /// <summary>
        /// Gets the <see cref="MessagePackWriter"/> for this buffer prefix.
        /// </summary>
        /// <param name="prefixSize">Prefix size.</param>
        /// <returns><see cref="MessagePackWriter"/> for this buffer.</returns>
        public MessagePackWriter GetPrefixWriter(int prefixSize)
        {
            Debug.Assert(prefixSize < _startIndex, "prefixSize < _startIndex");

            _indexBackup = _index;
            _startIndex -= prefixSize;
            _index = _startIndex;

            return new(this);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _disposed = true;
            }
        }

        /// <summary>
        /// Checks underlying buffer and resizes if necessary.
        /// </summary>
        /// <param name="sizeHint">Size hint.</param>
        private void CheckAndResizeBuffer(int sizeHint)
        {
            if (sizeHint < 0)
            {
                throw new ArgumentException(null, nameof(sizeHint));
            }

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint <= FreeCapacity)
            {
                return;
            }

            int length = _buffer.Length;
            int increase = Math.Max(sizeHint, length);

            int newSize = length + increase;

            if ((uint)newSize > int.MaxValue)
            {
                newSize = length + sizeHint;
                if ((uint)newSize > int.MaxValue)
                {
                    throw new OutOfMemoryException($"Buffer can't be larger than {int.MaxValue} (int.MaxValue) bytes.");
                }
            }

            var newBuf = ArrayPool<byte>.Shared.Rent(newSize);

            Array.Copy(_buffer, newBuf, _index);

            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = newBuf;
        }
    }
}
