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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;
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
        private readonly GlobalRef _target;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        public PlatformJniTarget(GlobalRef target, Marshaller marsh)
        {
            Debug.Assert(target != null);
            Debug.Assert(marsh != null);

            _target = target;
            _marsh = marsh;
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public GlobalRef Target
        {
            get { return _target; }
        }

        /** <inheritdoc /> */
        public Marshaller Marshaller { get { return _marsh; } }

        /** <inheritdoc /> */
        public long InStreamOutLong(int type, Action<IBinaryStream> writeAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    writeAction(stream);

                    return UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public IPlatformTargetInternal InStreamOutObject(int type, Action<IBinaryStream> writeAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    writeAction(stream);

                    var target = UU.TargetInStreamOutObject(_target, type, stream.SynchronizeOutput());

                    return target == null ? null : new PlatformJniTarget(target, _marsh);
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public IPlatformTargetInternal OutObjectInternal(int type)
        {
            try
            {
                return GetPlatformTarget(UU.TargetOutObject(_target, type));
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public T OutStream<T>(int type, Func<IBinaryStream, T> readAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    UU.TargetOutStream(_target, type, stream.MemoryPointer);

                    stream.SynchronizeInput();

                    return readAction(stream);
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public TR InStreamOutStream<TR>(int type, Action<IBinaryStream> writeAction, 
            Func<IBinaryStream, TR> readAction)
        {
            try
            {
                using (var outStream = IgniteManager.Memory.Allocate().GetStream())
                using (var inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    writeAction(outStream);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return readAction(inStream);
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public TR InStreamOutLong<TR>(int type, Action<IBinaryStream> outAction, Func<IBinaryStream, long, TR> inAction, 
            Func<IBinaryStream, Exception> readErrorAction)
        {
            try
            {
                Debug.Assert(readErrorAction != null);

                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    outAction(stream);

                    var res = UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());

                    if (res != PlatformTargetAdapter.Error && inAction == null)
                        return default(TR);  // quick path for void operations

                    stream.SynchronizeInput();

                    stream.Seek(0, SeekOrigin.Begin);

                    if (res != PlatformTargetAdapter.Error)
                    {
                        return inAction != null ? inAction(stream, res) : default(TR);
                    }

                    throw readErrorAction(stream);
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public TR InObjectStreamOutObjectStream<TR>(int type, Action<IBinaryStream> writeAction, 
            Func<IBinaryStream, IPlatformTargetInternal, TR> readAction, IPlatformTargetInternal arg)
        {
            PlatformMemoryStream outStream = null;
            long outPtr = 0;

            PlatformMemoryStream inStream = null;
            long inPtr = 0;

            try
            {
                if (writeAction != null)
                {
                    outStream = IgniteManager.Memory.Allocate().GetStream();
                    writeAction(outStream);
                    outPtr = outStream.SynchronizeOutput();
                }

                if (readAction != null)
                {
                    inStream = IgniteManager.Memory.Allocate().GetStream();
                    inPtr = inStream.MemoryPointer;
                }

                var res = UU.TargetInObjectStreamOutObjectStream(_target, type,
                    ((PlatformJniTarget) arg).Target, outPtr, inPtr);

                if (readAction == null)
                    return default(TR);

                inStream.SynchronizeInput();

                var target = res == null ? null : new PlatformJniTarget(res, _marsh);

                return readAction(inStream, target);

            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
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
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private Future<T> GetFuture<T>(Func<long, int, GlobalRef> listenAction, bool keepBinary = false,
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

            GlobalRef futTarget;

            try
            {
                futTarget = listenAction(futHnd, (int)futType);
            }
            catch (Exception)
            {
                _marsh.Ignite.HandleRegistry.Release(futHnd);

                throw;
            }

            fut.SetTarget(new Listenable(new PlatformJniTarget(futTarget, _marsh)));

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
            try
            {
                return UU.TargetInLongOutLong(_target, type, val);
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public long InStreamOutLong(int type, Action<IBinaryRawWriter> writeAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    var writer = _marsh.StartMarshal(stream);

                    writeAction(writer);

                    FinishMarshal(writer);

                    return UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public T InStreamOutStream<T>(int type, Action<IBinaryRawWriter> writeAction, 
            Func<IBinaryRawReader, T> readAction)
        {
            try
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
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public IPlatformTarget InStreamOutObject(int type, Action<IBinaryRawWriter> writeAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    var writer = _marsh.StartMarshal(stream);

                    writeAction(writer);

                    FinishMarshal(writer);

                    return GetPlatformTarget(UU.TargetInStreamOutObject(_target, type, stream.SynchronizeOutput()));
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public T InObjectStreamOutObjectStream<T>(int type, IPlatformTarget arg, 
            Action<IBinaryRawWriter> writeAction, Func<IBinaryRawReader, IPlatformTarget, T> readAction)
        {
            PlatformMemoryStream outStream = null;
            long outPtr = 0;

            PlatformMemoryStream inStream = null;
            long inPtr = 0;

            try
            {
                if (writeAction != null)
                {
                    outStream = IgniteManager.Memory.Allocate().GetStream();
                    var writer = _marsh.StartMarshal(outStream);
                    writeAction(writer);
                    FinishMarshal(writer);
                    outPtr = outStream.SynchronizeOutput();
                }

                if (readAction != null)
                {
                    inStream = IgniteManager.Memory.Allocate().GetStream();
                    inPtr = inStream.MemoryPointer;
                }

                var res = UU.TargetInObjectStreamOutObjectStream(_target, type, GetTargetPtr(arg), outPtr, inPtr);

                if (readAction == null)
                    return default(T);

                inStream.SynchronizeInput();

                return readAction(_marsh.StartUnmarshal(inStream), GetPlatformTarget(res));

            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
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

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public T OutStream<T>(int type, Func<IBinaryRawReader, T> readAction)
        {
            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    UU.TargetOutStream(_target, type, stream.MemoryPointer);

                    stream.SynchronizeInput();

                    return readAction(_marsh.StartUnmarshal(stream));
                }
            }
            catch (JavaException jex)
            {
                throw ConvertException(jex);
            }
        }

        /** <inheritdoc /> */
        public IPlatformTarget OutObject(int type)
        {
            return OutObjectInternal(type);
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

                    try
                    {
                        UU.TargetInStreamAsync(_target, type, stream.SynchronizeOutput());
                    }
                    catch (JavaException jex)
                    {
                        throw ConvertException(jex);
                    }
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

                    try
                    {
                        return UU.TargetInStreamOutObjectAsync(_target, type, stream.SynchronizeOutput());
                    }
                    catch (JavaException jex)
                    {
                        throw ConvertException(jex);
                    }
                }
            }, false, convertFunc).GetTask(cancellationToken);
        }

        /// <summary>
        /// Gets the platform target.
        /// </summary>
        private IPlatformTargetInternal GetPlatformTarget(GlobalRef target)
        {
            return target == null ? null : new PlatformJniTarget(target, _marsh);
        }

        /// <summary>
        /// Gets the target pointer.
        /// </summary>
        private static GlobalRef GetTargetPtr(IPlatformTarget target)
        {
            return target == null ? null : ((PlatformJniTarget) target)._target;
        }

        /// <summary>
        /// Converts the exception.
        /// </summary>
        private Exception ConvertException(JavaException jex)
        {
            return ExceptionUtils.GetException(_marsh.Ignite, jex);
        }

        #endregion

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            if (_target != null)
            {
                _target.Dispose();
            }
        }
    }
}
