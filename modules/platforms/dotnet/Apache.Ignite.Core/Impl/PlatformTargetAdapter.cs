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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Base class for interop targets, provides additional functionality over <see cref="IPlatformTargetInternal"/>.
    /// </summary>
    [SuppressMessage("ReSharper", "LocalVariableHidesMember")]
    internal class PlatformTargetAdapter
    {
        /** */
        internal const int False = 0;

        /** */
        internal const int True = 1;

        /** */
        internal const int Error = -1;

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
        private readonly IPlatformTargetInternal _target;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        protected PlatformTargetAdapter(IPlatformTargetInternal target)
        {
            Debug.Assert(target != null);

            _target = target;
            _marsh = target.Marshaller;
        }

        /// <summary>
        /// Unmanaged target.
        /// </summary>
        public IPlatformTargetInternal Target
        {
            get { return _target; }
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        #region OUT operations

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns></returns>
        protected long DoOutOp(int type, Action<IBinaryStream> action)
        {
            return _target.InStreamOutLong(type, action);
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns></returns>
        protected long DoOutOp(int type, Action<BinaryWriter> action)
        {
            return DoOutOp(type, stream => WriteToStream(action, stream, _marsh));
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns>Resulting object.</returns>
        protected IPlatformTargetInternal DoOutOpObject(int type, Action<BinaryWriter> action)
        {
            return _target.InStreamOutObject(type, stream => WriteToStream(action, stream, _marsh));
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns>Resulting object.</returns>
        protected IPlatformTargetInternal DoOutOpObject(int type, Action<IBinaryStream> action)
        {
            return _target.InStreamOutObject(type, action);
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <returns>Resulting object.</returns>
        protected IPlatformTargetInternal DoOutOpObject(int type)
        {
            return _target.OutObjectInternal(type);
        }

        /// <summary>
        /// Perform simple output operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val1">Value.</param>
        /// <returns>Result.</returns>
        protected long DoOutOp<T1>(int type, T1 val1)
        {
            return DoOutOp(type, writer =>
            {
                writer.Write(val1);
            });
        }

        /// <summary>
        /// Perform simple output operation accepting two arguments.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val1">Value 1.</param>
        /// <param name="val2">Value 2.</param>
        /// <returns>Result.</returns>
        protected long DoOutOp<T1, T2>(int type, T1 val1, T2 val2)
        {
            return DoOutOp(type, writer =>
            {
                writer.Write(val1);
                writer.Write(val2);
            });
        }

        #endregion

        #region IN operations

        /// <summary>
        /// Perform in operation.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="action">Action.</param>
        /// <returns>Result.</returns>
        protected T DoInOp<T>(int type, Func<IBinaryStream, T> action)
        {
            return _target.OutStream(type, action);
        }

        /// <summary>
        /// Perform simple in operation returning immediate result.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Result.</returns>
        protected T DoInOp<T>(int type)
        {
            return _target.OutStream(type, s => Unmarshal<T>(s));
        }

        #endregion

        #region OUT-IN operations
        
        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, TR> inAction)
        {
            return _target.InStreamOutStream(type, stream => WriteToStream(outAction, stream, _marsh), inAction);
        }

        /// <summary>
        /// Perform out-in operation with a single stream.
        /// </summary>
        /// <typeparam name="TR">The type of the r.</typeparam>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <param name="inErrorAction">The action to read an error.</param>
        /// <returns>
        /// Result.
        /// </returns>
        protected TR DoOutInOpX<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, long, TR> inAction,
            Func<IBinaryStream, Exception> inErrorAction)
        {
            return _target.InStreamOutLong(type, stream => WriteToStream(outAction, stream, _marsh), 
                inAction, inErrorAction);
        }

        /// <summary>
        /// Perform out-in operation with a single stream.
        /// </summary>
        /// <typeparam name="TR">The type of the r.</typeparam>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <param name="inErrorAction">The action to read an error.</param>
        /// <returns>
        /// Result.
        /// </returns>
        protected TR DoOutInOpX<TR>(int type, Func<BinaryWriter, bool> outAction, Func<IBinaryStream, long, TR> inAction,
            Func<IBinaryStream, Exception> inErrorAction)
        {
            return _target.InStreamOutLong(type, stream => WriteToStream(outAction, stream, _marsh), 
                inAction, inErrorAction);
        }

        /// <summary>
        /// Perform out-in operation with a single stream.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inErrorAction">The action to read an error.</param>
        /// <returns>
        /// Result.
        /// </returns>
        protected bool DoOutInOpX(int type, Action<BinaryWriter> outAction,
            Func<IBinaryStream, Exception> inErrorAction)
        {
            return _target.InStreamOutLong(type, stream => WriteToStream(outAction, stream, _marsh), 
                (stream, res) => res == True, inErrorAction);
        }

        /// <summary>
        /// Perform out-in operation with a single stream.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inErrorAction">The action to read an error.</param>
        /// <returns>
        /// Result.
        /// </returns>
        protected bool DoOutInOpX(int type, Func<BinaryWriter, bool> outAction,
            Func<IBinaryStream, Exception> inErrorAction)
        {
            return _target.InStreamOutLong(type, stream => WriteToStream(outAction, stream, _marsh), 
                (stream, res) => res == True, inErrorAction);
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction,
            Func<IBinaryStream, IPlatformTargetInternal, TR> inAction, IPlatformTargetInternal arg)
        {
            return _target.InObjectStreamOutObjectStream(type, stream => WriteToStream(outAction, stream, _marsh), 
                inAction, arg);
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction)
        {
            return _target.InStreamOutStream(type, stream => WriteToStream(outAction, stream, _marsh), 
                stream => Unmarshal<TR>(stream));
        }

        /// <summary>
        /// Perform simple out-in operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<T1, TR>(int type, T1 val)
        {
            return _target.InStreamOutStream(type, stream => WriteToStream(val, stream, _marsh),
                stream => Unmarshal<TR>(stream));
        }

        /// <summary>
        /// Perform simple out-in operation accepting two arguments.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        protected long DoOutInOp(int type, long val = 0)
        {
            return _target.InLongOutLong(type, val);
        }

        #endregion

        #region Async operations

        /// <summary>
        /// Performs async operation.
        /// </summary>
        /// <param name="type">The type code.</param>
        /// <param name="writeAction">The write action.</param>
        /// <returns>Task for async operation</returns>
        protected Task DoOutOpAsync(int type, Action<BinaryWriter> writeAction = null)
        {
            return DoOutOpAsync<object>(type, writeAction);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        /// <typeparam name="T">Type of the result.</typeparam>
        /// <param name="type">The type code.</param>
        /// <param name="writeAction">The write action.</param>
        /// <param name="keepBinary">Keep binary flag, only applicable to object futures. False by default.</param>
        /// <param name="convertFunc">The function to read future result from stream.</param>
        /// <returns>Task for async operation</returns>
        protected Task<T> DoOutOpAsync<T>(int type, Action<BinaryWriter> writeAction = null, bool keepBinary = false,
            Func<BinaryReader, T> convertFunc = null)
        {
            return GetFuture((futId, futType) => DoOutOp(type, w =>
            {
                if (writeAction != null)
                {
                    writeAction(w);
                }
                w.WriteLong(futId);
                w.WriteInt(futType);
            }), keepBinary, convertFunc).Task;
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        /// <typeparam name="T">Type of the result.</typeparam>
        /// <param name="type">The type code.</param>
        /// <param name="writeAction">The write action.</param>
        /// <returns>Future for async operation</returns>
        protected Future<T> DoOutOpObjectAsync<T>(int type, Action<BinaryWriter> writeAction)
        {
            return GetFuture<T>((futId, futType) => DoOutOpObject(type, w =>
            {
                writeAction(w);
                w.WriteLong(futId);
                w.WriteInt(futType);
            }));
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        /// <typeparam name="TR">Type of the result.</typeparam>
        /// <typeparam name="T1">The type of the first arg.</typeparam>
        /// <param name="type">The type code.</param>
        /// <param name="val1">First arg.</param>
        /// <returns>
        /// Task for async operation
        /// </returns>
        protected Task<TR> DoOutOpAsync<T1, TR>(int type, T1 val1)
        {
            return GetFuture<TR>((futId, futType) => DoOutOp(type, w =>
            {
                w.WriteObject(val1);
                w.WriteLong(futId);
                w.WriteInt(futType);
            })).Task;
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        /// <typeparam name="TR">Type of the result.</typeparam>
        /// <typeparam name="T1">The type of the first arg.</typeparam>
        /// <typeparam name="T2">The type of the second arg.</typeparam>
        /// <param name="type">The type code.</param>
        /// <param name="val1">First arg.</param>
        /// <param name="val2">Second arg.</param>
        /// <returns>
        /// Task for async operation
        /// </returns>
        protected Task<TR> DoOutOpAsync<T1, T2, TR>(int type, T1 val1, T2 val2)
        {
            return GetFuture<TR>((futId, futType) => DoOutOp(type, w =>
            {
                w.WriteObject(val1);
                w.WriteObject(val2);
                w.WriteLong(futId);
                w.WriteInt(futType);
            })).Task;
        }

        #endregion

        #region Miscelanneous

        /// <summary>
        /// Unmarshal object using the given stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Unmarshalled object.</returns>
        protected virtual T Unmarshal<T>(IBinaryStream stream)
        {
            return _marsh.Unmarshal<T>(stream);
        }

        /// <summary>
        /// Creates a future and starts listening.
        /// </summary>
        /// <typeparam name="T">Future result type</typeparam>
        /// <param name="listenAction">The listen action.</param>
        /// <param name="keepBinary">Keep binary flag, only applicable to object futures. False by default.</param>
        /// <param name="convertFunc">The function to read future result from stream.</param>
        /// <returns>Created future.</returns>
        private Future<T> GetFuture<T>(Func<long, int, IPlatformTargetInternal> listenAction, bool keepBinary = false,
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

            IPlatformTargetInternal futTarget;

            try
            {
                futTarget = listenAction(futHnd, (int)futType);
            }
            catch (Exception)
            {
                _marsh.Ignite.HandleRegistry.Release(futHnd);

                throw;
            }

            fut.SetTarget(new Listenable(futTarget));

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

        /// <summary>
        /// Writes to stream.
        /// </summary>
        private static bool WriteToStream(Func<BinaryWriter, bool> action, IBinaryStream stream, Marshaller marsh)
        {
            var writer = marsh.StartMarshal(stream);

            var res = action(writer);

            marsh.FinishMarshal(writer);

            return res;
        }

        /// <summary>
        /// Writes to stream.
        /// </summary>
        private static bool WriteToStream(Action<BinaryWriter> action, IBinaryStream stream, Marshaller marsh)
        {
            var writer = marsh.StartMarshal(stream);

            action(writer);

            marsh.FinishMarshal(writer);

            return true;
        }

        /// <summary>
        /// Writes to stream.
        /// </summary>
        private static void WriteToStream<T>(T obj, IBinaryStream stream, Marshaller marsh)
        {
            var writer = marsh.StartMarshal(stream);

            writer.WriteObject(obj);

            marsh.FinishMarshal(writer);
        }

        #endregion
    }
}
