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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Interop;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Base class for interop targets.
    /// </summary>
    internal class PlatformJniTarget : IPlatformTargetInternal
    {
        /** */
        private static readonly Dictionary<Type, FutureType> IgniteFutureTypeMap
            = new Dictionary<Type, FutureType>
            {
                {typeof(bool), FutureType.Bool},
                {typeof(byte), FutureType.Byte},
                {typeof(char), FutureType.Char},
                {typeof(double), FutureType.Double},
                {typeof(float), FutureType.Float},
                {typeof(int), FutureType.Int},
                {typeof(long), FutureType.Long},
                {typeof(short), FutureType.Short}
            };
        
        /** Unmanaged target. */
        private readonly IUnmanagedTarget _target;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        public PlatformJniTarget(IUnmanagedTarget target, Marshaller marsh)
        {
            Debug.Assert(target != null);
            Debug.Assert(marsh != null);

            _target = target;
            _marsh = marsh;
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public IUnmanagedTarget Target
        {
            get { return _target; }
        }

        public long DoOutOp(int type, Action<IBinaryStream> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                action(stream);

                return UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());
            }
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns>Resulting object.</returns>
        private IUnmanagedTarget DoOutOpObject(int type, Action<BinaryWriter> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(stream);

                action(writer);

                FinishMarshal(writer);

                return UU.TargetInStreamOutObject(_target, type, stream.SynchronizeOutput());
            }
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <returns>Resulting object.</returns>
        private IUnmanagedTarget DoOutOpObject(int type)
        {
            return UU.TargetOutObject(_target, type);
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        private unsafe TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction,
            Func<IBinaryStream, IUnmanagedTarget, TR> inAction, void* arg)
        {
            PlatformMemoryStream outStream = null;
            long outPtr = 0;

            PlatformMemoryStream inStream = null;
            long inPtr = 0;

            try
            {
                if (outAction != null)
                {
                    outStream = IgniteManager.Memory.Allocate().GetStream();
                    var writer = _marsh.StartMarshal(outStream);
                    outAction(writer);
                    FinishMarshal(writer);
                    outPtr = outStream.SynchronizeOutput();
                }

                if (inAction != null)
                {
                    inStream = IgniteManager.Memory.Allocate().GetStream();
                    inPtr = inStream.MemoryPointer;
                }

                var res = UU.TargetInObjectStreamOutObjectStream(_target, type, arg, outPtr, inPtr);

                if (inAction == null)
                    return default(TR);

                inStream.SynchronizeInput();

                return inAction(inStream, res);

            }
            finally
            {
                try
                {
                    if (inStream != null)
                        inStream.Dispose();

                }
                finally
                {
                    if (outStream != null)
                        outStream.Dispose();
                }
            }
        }

        /// <summary>
        /// Finish marshaling.
        /// </summary>
        /// <param name="writer">Writer.</param>
        private void FinishMarshal(BinaryWriter writer)
        {
            _marsh.FinishMarshal(writer);
        }

        /// <summary>
        /// Creates a future and starts listening.
        /// </summary>
        /// <typeparam name="T">Future result type</typeparam>
        /// <param name="listenAction">The listen action.</param>
        /// <param name="keepBinary">Keep binary flag, only applicable to object futures. False by default.</param>
        /// <param name="convertFunc">The function to read future result from stream.</param>
        /// <returns>Created future.</returns>
        private Future<T> GetFuture<T>(Func<long, int, IUnmanagedTarget> listenAction, bool keepBinary = false,
            Func<BinaryReader, T> convertFunc = null)
        {
            var futType = FutureType.Object;

            var type = typeof(T);

            if (type.IsPrimitive)
                IgniteFutureTypeMap.TryGetValue(type, out futType);

            var fut = convertFunc == null && futType != FutureType.Object
                ? new Future<T>()
                : new Future<T>(new FutureConverter<T>(_marsh, keepBinary, convertFunc));

            var futHnd = _marsh.Ignite.HandleRegistry.Allocate(fut);

            IUnmanagedTarget futTarget;

            try
            {
                futTarget = listenAction(futHnd, (int)futType);
            }
            catch (Exception)
            {
                _marsh.Ignite.HandleRegistry.Release(futHnd);

                throw;
            }

            fut.SetTarget(new Listenable(futTarget, _marsh));

            return fut;
        }

        /// <summary>
        /// Creates a future and starts listening.
        /// </summary>
        /// <typeparam name="T">Future result type</typeparam>
        /// <param name="listenAction">The listen action.</param>
        /// <param name="keepBinary">Keep binary flag, only applicable to object futures. False by default.</param>
        /// <param name="convertFunc">The function to read future result from stream.</param>
        /// <returns>Created future.</returns>
        private Future<T> GetFuture<T>(Action<long, int> listenAction, bool keepBinary = false,
            Func<BinaryReader, T> convertFunc = null)
        {
            var futType = FutureType.Object;

            var type = typeof(T);

            if (type.IsPrimitive)
                IgniteFutureTypeMap.TryGetValue(type, out futType);

            var fut = convertFunc == null && futType != FutureType.Object
                ? new Future<T>()
                : new Future<T>(new FutureConverter<T>(_marsh, keepBinary, convertFunc));

            var futHnd = _marsh.Ignite.HandleRegistry.Allocate(fut);

            try
            {
                listenAction(futHnd, (int)futType);
            }
            catch (Exception)
            {
                _marsh.Ignite.HandleRegistry.Release(futHnd);

                throw;
            }

            return fut;
        }

        #region IPlatformTarget

        /** <inheritdoc /> */
        public long InLongOutLong(int type, long val)
        {
            return UU.TargetInLongOutLong(_target, type, val);
        }

        /** <inheritdoc /> */
        public long InStreamOutLong(int type, Action<IBinaryRawWriter> writeAction)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer);

                FinishMarshal(writer);

                return UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());
            }
        }

        /** <inheritdoc /> */
        public T InStreamOutStream<T>(int type, Action<IBinaryRawWriter> writeAction, 
            Func<IBinaryRawReader, T> readAction)
        {
            using (var outStream = IgniteManager.Memory.Allocate().GetStream())
            using (var inStream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(outStream);

                writeAction(writer);

                FinishMarshal(writer);

                UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                inStream.SynchronizeInput();

                return readAction(_marsh.StartUnmarshal(inStream));
            }
        }

        /** <inheritdoc /> */
        public IPlatformTarget InStreamOutObject(int type, Action<IBinaryRawWriter> writeAction)
        {
            return GetPlatformTarget(DoOutOpObject(type, writeAction));
        }

        /** <inheritdoc /> */
        public unsafe T InObjectStreamOutObjectStream<T>(int type, IPlatformTarget arg, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, IPlatformTarget, T> readAction)
        {
            return DoOutInOp(type, writeAction, (stream, obj) => readAction(_marsh.StartUnmarshal(stream),
                GetPlatformTarget(obj)), GetTargetPtr(arg));
        }

        /** <inheritdoc /> */
        public T OutStream<T>(int type, Func<IBinaryRawReader, T> readAction)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                UU.TargetOutStream(_target, type, stream.MemoryPointer);

                stream.SynchronizeInput();

                return readAction(_marsh.StartUnmarshal(stream));
            }
        }

        /** <inheritdoc /> */
        public IPlatformTarget OutObject(int type)
        {
            return GetPlatformTarget(DoOutOpObject(type));
        }

        /** <inheritdoc /> */
        public Task<T> DoOutOpAsync<T>(int type, Action<IBinaryRawWriter> writeAction = null, 
            Func<IBinaryRawReader, T> readAction = null)
        {
            var convertFunc = readAction != null
                ? r => readAction(r)
                : (Func<BinaryReader, T>)null;
            return GetFuture((futId, futType) =>
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    stream.WriteLong(futId);
                    stream.WriteInt(futType);

                    if (writeAction != null)
                    {
                        var writer = _marsh.StartMarshal(stream);

                        writeAction(writer);

                        FinishMarshal(writer);
                    }

                    UU.TargetInStreamAsync(_target, type, stream.SynchronizeOutput());
                }
            }, false, convertFunc).Task;
        }

        /** <inheritdoc /> */
        public Task<T> DoOutOpAsync<T>(int type, Action<IBinaryRawWriter> writeAction, 
            Func<IBinaryRawReader, T> readAction, CancellationToken cancellationToken)
        {
            var convertFunc = readAction != null
                ? r => readAction(r)
                : (Func<BinaryReader, T>) null;

            return GetFuture((futId, futType) =>
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    stream.WriteLong(futId);
                    stream.WriteInt(futType);

                    if (writeAction != null)
                    {
                        var writer = _marsh.StartMarshal(stream);

                        writeAction(writer);

                        FinishMarshal(writer);
                    }

                    return UU.TargetInStreamOutObjectAsync(_target, type, stream.SynchronizeOutput());
                }
            }, false, convertFunc).GetTask(cancellationToken);
        }

        /// <summary>
        /// Gets the platform target.
        /// </summary>
        private IPlatformTarget GetPlatformTarget(IUnmanagedTarget target)
        {
            return target == null ? null : new PlatformJniTarget(target, _marsh);
        }

        /// <summary>
        /// Gets the target pointer.
        /// </summary>
        private static unsafe void* GetTargetPtr(IPlatformTarget target)
        {
            return target == null ? null : ((PlatformJniTarget) target)._target.Target;
        }

        #endregion
    }
}
