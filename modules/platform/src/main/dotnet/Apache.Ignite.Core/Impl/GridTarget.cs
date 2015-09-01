/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Base class for interop targets.
    /// </summary>
    [SuppressMessage("ReSharper", "LocalVariableHidesMember")]
    internal abstract class GridTarget
    {
        /** */
        protected const int TRUE = 1;

        /** */
        private const int OP_META = -1;

        /** */
        public const int OP_NONE = -2;

        /** */
        private static readonly Dictionary<Type, FutureType> GRID_FUTURE_TYPE_MAP
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
        protected readonly IUnmanagedTarget target;

        /** Marshaller. */
        protected readonly PortableMarshaller marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        protected GridTarget(IUnmanagedTarget target, PortableMarshaller marsh)
        {
            this.target = target;
            this.marsh = marsh;
        }

        /// <summary>
        /// Unmanaged target.
        /// </summary>
        internal IUnmanagedTarget Target
        {
            get { return target; }
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
        {
            get { return marsh; }
        }

        #region Static Helpers

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteCollection<T>(PortableWriterImpl writer, ICollection<T> vals)
        {
            return WriteCollection<T, T>(writer, vals, null);
        }

        /// <summary>
        /// Write nullable collection.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteNullableCollection<T>(PortableWriterImpl writer, ICollection<T> vals)
        {
            return WriteNullable(writer, vals, WriteCollection);
        }

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteCollection<T1, T2>(PortableWriterImpl writer,
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
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteEnumerable<T>(PortableWriterImpl writer, IEnumerable<T> vals)
        {
            return WriteEnumerable<T, T>(writer, vals, null);
        }

        /// <summary>
        /// Write enumerable.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteEnumerable<T1, T2>(PortableWriterImpl writer,
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
        /// <param name="writer">Portable writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer.</returns>
        protected static PortableWriterImpl WriteDictionary<T1, T2>(PortableWriterImpl writer, 
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
        /// <param name="writer">Portable writer.</param>
        /// <param name="item">Item.</param>
        /// <param name="writeItem">Write action to perform on item when it is not null.</param>
        /// <returns>The same writer for chaining.</returns>
        protected static PortableWriterImpl WriteNullable<T>(PortableWriterImpl writer, T item,
            Func<PortableWriterImpl, T, PortableWriterImpl> writeItem)
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
        protected long DoOutOp(int type, Action<IPortableStream> action)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                action(stream);

                return UU.TargetInStreamOutLong(target, type, stream.SynchronizeOutput());
            }
        }

        /// <summary>
        /// Perform out operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action to be performed on the stream.</param>
        /// <returns></returns>
        protected long DoOutOp(int type, Action<PortableWriterImpl> action)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                var writer = marsh.StartMarshal(stream);

                action(writer);

                FinishMarshal(writer);

                return UU.TargetInStreamOutLong(target, type, stream.SynchronizeOutput());
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
        protected void DoInOp(int type, Action<IPortableStream> action)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                UU.TargetOutStream(target, type, stream.MemoryPointer);
                
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
        protected T DoInOp<T>(int type, Func<IPortableStream, T> action)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                UU.TargetOutStream(target, type, stream.MemoryPointer);

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
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                UU.TargetOutStream(target, type, stream.MemoryPointer);

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
        protected void DoOutInOp(int type, Action<PortableWriterImpl> outAction, Action<IPortableStream> inAction)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

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
        protected R DoOutInOp<R>(int type, Action<PortableWriterImpl> outAction, Func<IPortableStream, R> inAction)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

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
        /// <param name="inAction">In action.</param>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        protected unsafe R DoOutInOp<R>(int type, Action<PortableWriterImpl> outAction, Func<IPortableStream, R> inAction, void* arg)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInObjectStreamOutStream(target, type, arg, outStream.SynchronizeOutput(), inStream.MemoryPointer);

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
        protected R DoOutInOp<R>(int type, Action<PortableWriterImpl> outAction)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    outAction(writer);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<R>(inStream);
                }
            }
        }

        /// <summary>
        /// Perform simple out-in operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        protected R DoOutInOp<T1, R>(int type, T1 val)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    writer.WriteObject(val);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<R>(inStream);
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
        protected R DoOutInOp<T1, T2, R>(int type, T1 val1, T2 val2)
        {
            using (PlatformMemoryStream outStream = GridManager.Memory.Allocate().Stream())
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Allocate().Stream())
                {
                    PortableWriterImpl writer = marsh.StartMarshal(outStream);

                    writer.WriteObject(val1);
                    writer.WriteObject(val2);

                    FinishMarshal(writer);

                    UU.TargetInStreamOutStream(target, type, outStream.SynchronizeOutput(), inStream.MemoryPointer);

                    inStream.SynchronizeInput();

                    return Unmarshal<R>(inStream);
                }
            }
        }

        #endregion

        #region Miscelanneous

        /// <summary>
        /// Finish marshaling.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        internal void FinishMarshal(PortableWriterImpl writer)
        {
            marsh.FinishMarshal(writer);
        }

        /// <summary>
        /// Put metadata to Grid.
        /// </summary>
        /// <param name="metas">Metadatas.</param>
        internal void PutMetadata(IDictionary<int, IPortableMetadata> metas)
        {
            DoOutOp(OP_META, stream =>
            {
                PortableWriterImpl metaWriter = marsh.StartMarshal(stream);

                metaWriter.WriteInt(metas.Count);

                foreach (var meta in metas.Values)
                {
                    PortableMetadataImpl meta0 = (PortableMetadataImpl)meta;

                    metaWriter.WriteInt(meta0.TypeId);
                    metaWriter.WriteString(meta0.TypeName);
                    metaWriter.WriteString(meta0.AffinityKeyFieldName);

                    IDictionary<string, int> fields = meta0.FieldsMap();

                    metaWriter.WriteInt(fields.Count);

                    foreach (var field in fields)
                    {
                        metaWriter.WriteString(field.Key);
                        metaWriter.WriteInt(field.Value);
                    }
                }

                marsh.FinishMarshal(metaWriter);
            });

            marsh.OnMetadataSent(metas);
        }

        /// <summary>
        /// Unmarshal object using the given stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Unmarshalled object.</returns>
        protected virtual T Unmarshal<T>(IPortableStream stream)
        {
            return marsh.Unmarshal<T>(stream);
        }

        /// <summary>
        /// Creates a future and starts listening.
        /// </summary>
        /// <typeparam name="T">Future result type</typeparam>
        /// <param name="listenAction">The listen action.</param>
        /// <param name="keepPortable">Keep portable flag, only applicable to object futures. False by default.</param>
        /// <param name="convertFunc">The function to read future result from stream.</param>
        /// <returns>Created future.</returns>
        protected IFuture<T> GetFuture<T>(Action<long, int> listenAction, bool keepPortable = false,
            Func<PortableReaderImpl, T> convertFunc = null)
        {
            var futType = FutureType.Object;

            var type = typeof(T);

            if (type.IsPrimitive)
                GRID_FUTURE_TYPE_MAP.TryGetValue(type, out futType);

            var fut = convertFunc == null && futType != FutureType.Object
                ? new Future<T>()
                : new Future<T>(new FutureConverter<T>(marsh, keepPortable, convertFunc));

            var futHnd = marsh.Grid.HandleRegistry.Allocate(fut);

            listenAction(futHnd, (int)futType);

            return fut;
        }

        #endregion
    }

    /// <summary>
    /// GridTarget with IDisposable pattern.
    /// </summary>
    internal abstract class GridDisposableTarget : GridTarget, IDisposable
    {
        /** Disposed flag. */
        private volatile bool disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        protected GridDisposableTarget(IUnmanagedTarget target, PortableMarshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (this)
            {
                if (disposed)
                    return;

                Dispose(true);

                GC.SuppressFinalize(this);

                disposed = true;
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
            target.Dispose();
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (disposed)
                throw new ObjectDisposedException(GetType().Name, "Object has been disposed.");
        }

        /// <summary>
        /// Gets a value indicating whether this instance is disposed.
        /// </summary>
        protected bool IsDisposed
        {
            get { return disposed; }
        }
    }
}
