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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Base class for interop targets.
    /// </summary>
    [SuppressMessage("ReSharper", "LocalVariableHidesMember")]
    internal abstract class PlatformTarget
    {
        /** */
        protected const int True = 1;

        /** */
        protected const int Error = -1;

        /** */
        private const int OpMeta = -1;

        /** */
        public const int OpNone = -2;

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
        protected PlatformTarget(IUnmanagedTarget target, Marshaller marsh)
        {
            Debug.Assert(target != null);
            Debug.Assert(marsh != null);

            _target = target;
            _marsh = marsh;
        }

        /// <summary>
        /// Unmanaged target.
        /// </summary>
        internal IUnmanagedTarget Target
        {
            get { return _target; }
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal Marshaller Marshaller
        {
            get { return _marsh; }
        }

        #region Static Helpers

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteCollection<T>(BinaryWriter writer, ICollection<T> vals)
        {
            return WriteCollection<T, T>(writer, vals, null);
        }

        /// <summary>
        /// Write nullable collection.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteNullableCollection<T>(BinaryWriter writer, ICollection<T> vals)
        {
            return WriteNullable(writer, vals, WriteCollection);
        }

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteCollection<T1, T2>(BinaryWriter writer,
            ICollection<T1> vals, Func<T1, T2> selector)
        {
            writer.WriteInt(vals.Count);

            if (selector == null)
            {
                foreach (var val in vals)
                    writer.Write(val);
            }
            else
            {
                foreach (var val in vals)
                    writer.Write(selector(val));
            }

            return writer;
        }

        /// <summary>
        /// Write enumerable.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteEnumerable<T>(BinaryWriter writer, IEnumerable<T> vals)
        {
            return WriteEnumerable<T, T>(writer, vals, null);
        }

        /// <summary>
        /// Write enumerable.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteEnumerable<T1, T2>(BinaryWriter writer,
            IEnumerable<T1> vals, Func<T1, T2> selector)
        {
            var col = vals as ICollection<T1>;

            if (col != null)
                return WriteCollection(writer, col, selector);
            
            var stream = writer.Stream;

            var pos = stream.Position;

            stream.Seek(4, SeekOrigin.Current);

            var size = 0;

            if (selector == null)
            {
                foreach (var val in vals)
                {
                    writer.Write(val);

                    size++;
                }
            }
            else
            {
                foreach (var val in vals)
                {
                    writer.Write(selector(val));

                    size++;
                }
            }

            stream.WriteInt(pos, size);
                
            return writer;
        }

        /// <summary>
        /// Write dictionary.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer.</returns>
        protected static BinaryWriter WriteDictionary<T1, T2>(BinaryWriter writer, 
            IDictionary<T1, T2> vals)
        {
            writer.WriteInt(vals.Count);

            foreach (KeyValuePair<T1, T2> pair in vals)
            {
                writer.Write(pair.Key);
                writer.Write(pair.Value);
            }

            return writer;
        }

        /// <summary>
        /// Write a nullable item.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="item">Item.</param>
        /// <param name="writeItem">Write action to perform on item when it is not null.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static BinaryWriter WriteNullable<T>(BinaryWriter writer, T item,
            Func<BinaryWriter, T, BinaryWriter> writeItem) where T : class
        {
            if (item == null)
            {
                writer.WriteBoolean(false);

                return writer;
            }

            writer.WriteBoolean(true);

            return writeItem(writer, item);
        }

        #endregion

        #region OUT operations

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns></returns>
        protected long DoOutOp(int type, Action<IBinaryStream> action)
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
        /// <returns></returns>
        protected long DoOutOp(int type, Action<BinaryWriter> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(stream);

                action(writer);

                FinishMarshal(writer);

                return UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());
            }
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns></returns>
        protected IUnmanagedTarget DoOutOpObject(int type, Action<BinaryWriter> action)
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

        /// <summary>
        /// Perform simple output operation accepting three arguments.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val1">Value 1.</param>
        /// <param name="val2">Value 2.</param>
        /// <param name="val3">Value 3.</param>
        /// <returns>Result.</returns>
        protected long DoOutOp<T1, T2, T3>(int type, T1 val1, T2 val2, T3 val3)
        {
            return DoOutOp(type, writer =>
            {
                writer.Write(val1);
                writer.Write(val2);
                writer.Write(val3);
            });
        }

        #endregion

        #region IN operations

        /// <summary>
        /// Perform in operation.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="action">Action.</param>
        protected void DoInOp(int type, Action<IBinaryStream> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                UU.TargetOutStream(_target, type, stream.MemoryPointer);
                
                stream.SynchronizeInput();

                action(stream);
            }
        }

        /// <summary>
        /// Perform in operation.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="action">Action.</param>
        /// <returns>Result.</returns>
        protected T DoInOp<T>(int type, Func<IBinaryStream, T> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                UU.TargetOutStream(_target, type, stream.MemoryPointer);

                stream.SynchronizeInput();

                return action(stream);
            }
        }

        /// <summary>
        /// Perform simple in operation returning immediate result.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Result.</returns>
        protected T DoInOp<T>(int type)
        {
            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                UU.TargetOutStream(_target, type, stream.MemoryPointer);

                stream.SynchronizeInput();

                return Unmarshal<T>(stream);
            }
        }

        #endregion

        #region OUT-IN operations
        
        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        protected void DoOutInOp(int type, Action<BinaryWriter> outAction, Action<IBinaryStream> inAction)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    inAction(inStream);
                }
            }
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, TR> inAction)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return inAction(inStream);
                }
            }
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
            Debug.Assert(inErrorAction != null);

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(stream);

                outAction(writer);

                FinishMarshal(writer);

                var res = UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());

                if (res != Error && inAction == null)
                    return default(TR);  // quick path for void operations

                stream.SynchronizeInput();

                stream.Seek(0, SeekOrigin.Begin);

                if (res != Error)
                    return inAction != null ? inAction(stream, res) : default(TR);

                throw inErrorAction(stream);
            }
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
            Debug.Assert(inErrorAction != null);

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = _marsh.StartMarshal(stream);

                outAction(writer);

                FinishMarshal(writer);

                var res = UU.TargetInStreamOutLong(_target, type, stream.SynchronizeOutput());

                if (res != Error)
                    return res == True;

                stream.SynchronizeInput();

                stream.Seek(0, SeekOrigin.Begin);

                throw inErrorAction(stream);
            }
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <param name="inAction">In action.</param>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        protected unsafe TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, TR> inAction, void* arg)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInObjectStreamOutStream(_target, type, arg, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return inAction(inStream);
                }
            }
        }
        
        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<TR>(inStream);
                }
            }
        }

        /// <summary>
        /// Perform simple out-in operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<T1, TR>(int type, T1 val)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    writer.WriteObject(val);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<TR>(inStream);
                }
            }
        }

        /// <summary>
        /// Perform simple out-in operation accepting two arguments.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val1">Value.</param>
        /// <param name="val2">Value.</param>
        /// <returns>Result.</returns>
        protected TR DoOutInOp<T1, T2, TR>(int type, T1 val1, T2 val2)
        {
            using (PlatformMemoryStream outStream = IgniteManager.Memory.Allocate().GetStream())
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Allocate().GetStream())
                {
                    BinaryWriter writer = _marsh.StartMarshal(outStream);

                    writer.WriteObject(val1);
                    writer.WriteObject(val2);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(_target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<TR>(inStream);
                }
            }
        }

        #endregion

        #region Miscelanneous

        /// <summary>
        /// Finish marshaling.
        /// </summary>
        /// <param name="writer">Writer.</param>
        internal void FinishMarshal(BinaryWriter writer)
        {
            _marsh.FinishMarshal(writer);
        }

        /// <summary>
        /// Put binary types to Grid.
        /// </summary>
        /// <param name="types">Binary types.</param>
        internal void PutBinaryTypes(ICollection<BinaryType> types)
        {
            DoOutOp(OpMeta, stream =>
            {
                BinaryWriter w = _marsh.StartMarshal(stream);

                w.WriteInt(types.Count);

                foreach (var meta in types)
                {
                    w.WriteInt(meta.TypeId);
                    w.WriteString(meta.TypeName);
                    w.WriteString(meta.AffinityKeyFieldName);

                    IDictionary<string, int> fields = meta.GetFieldsMap();

                    w.WriteInt(fields.Count);

                    foreach (var field in fields)
                    {
                        w.WriteString(field.Key);
                        w.WriteInt(field.Value);
                    }

                    w.WriteBoolean(meta.IsEnum);

                    // Send schemas
                    var desc = meta.Descriptor;
                    Debug.Assert(desc != null);

                    var count = 0;
                    var countPos = stream.Position;
                    w.WriteInt(0);  // Reserve for count

                    foreach (var schema in desc.Schema.GetAll())
                    {
                        w.WriteInt(schema.Key);

                        var ids = schema.Value;
                        w.WriteInt(ids.Length);

                        foreach (var id in ids)
                            w.WriteInt(id);

                        count++;
                    }

                    stream.WriteInt(countPos, count);
                }

                _marsh.FinishMarshal(w);
            });

            _marsh.OnBinaryTypesSent(types);
        }

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
        protected Future<T> GetFuture<T>(Func<long, int, IUnmanagedTarget> listenAction, bool keepBinary = false,
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

            var futTarget = listenAction(futHnd, (int) futType);

            fut.SetTarget(futTarget);

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
        protected Future<T> GetFuture<T>(Action<long, int> listenAction, bool keepBinary = false,
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

            listenAction(futHnd, (int)futType);

            return fut;
        }

        /// <summary>
        /// Creates a task to listen for the last async op.
        /// </summary>
        protected Task GetTask()
        {
            return GetTask<object>();
        }

        /// <summary>
        /// Creates a task to listen for the last async op.
        /// </summary>
        protected Task<T> GetTask<T>()
        {
            return GetFuture<T>((futId, futTyp) => UU.TargetListenFuture(Target, futId, futTyp)).Task;
        }

        #endregion
    }

    /// <summary>
    /// PlatformTarget with IDisposable pattern.
    /// </summary>
    internal abstract class PlatformDisposableTarget : PlatformTarget, IDisposable
    {
        /** Disposed flag. */
        private volatile bool _disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        protected PlatformDisposableTarget(IUnmanagedTarget target, Marshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (this)
            {
                if (_disposed)
                    return;

                Dispose(true);

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose;  <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            Target.Dispose();
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name, "Object has been disposed.");
        }
    }
}
