/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;

    using GridGain.Cache;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Portable;

    /// <summary>
    /// Portable wrapper for the <see cref="ICacheEntryProcessor{K, V, A, R}"/> and it's argument.
    /// Marshals and executes wrapped processor with a non-generic interface.
    /// </summary>
    internal class CacheEntryProcessorHolder : IPortableWriteAware
    {
        // generic processor
        private readonly object proc;

        // argument
        private readonly object arg;

        // func to invoke Process method on ICacheEntryProcessor in form of object.
        private readonly Func<IMutableCacheEntryInternal, object, object> processFunc;

        // entry creator delegate
        private readonly Func<object, object, bool, IMutableCacheEntryInternal> entryCtor;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorHolder"/> class.
        /// </summary>
        /// <param name="proc">The processor to wrap.</param>
        /// <param name="arg">The argument.</param>
        /// <param name="processFunc">Delegate to call generic <see cref="ICacheEntryProcessor{K, V, A, R}.Process"/> on local node.</param>
        /// <param name="keyType">Type of the key.</param>
        /// <param name="valType">Type of the value.</param>
        public CacheEntryProcessorHolder(object proc, object arg, 
            Func<IMutableCacheEntryInternal, object, object> processFunc, Type keyType, Type valType)
        {
            Debug.Assert(proc != null);
            Debug.Assert(processFunc != null);

            this.proc = proc;
            this.arg = arg;
            this.processFunc = processFunc;

            this.processFunc = GetProcessFunc(this.proc);

            entryCtor = MutableCacheEntry.GetCtor(keyType, valType);
        }

        /// <summary>
        /// Processes specified cache entry.
        /// </summary>
        /// <param name="key">The cache entry key.</param>
        /// <param name="value">The cache entry value.</param>
        /// <param name="exists">Indicates whether cache entry exists.</param>
        /// <param name="grid"></param>
        /// <returns>
        /// Pair of resulting cache entry and result of processing it.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", 
            Justification = "User processor can throw any exception")]
        public CacheEntryProcessorResultHolder Process(object key, object value, bool exists, GridImpl grid)
        {
            ResourceProcessor.Inject(proc, grid);

            var entry = entryCtor(key, value, exists);

            try
            {
                return new CacheEntryProcessorResultHolder(entry, processFunc(entry, arg), null);
            }
            catch (TargetInvocationException ex)
            {
                return new CacheEntryProcessorResultHolder(null, null, ex.InnerException);
            }
            catch (Exception ex)
            {
                return new CacheEntryProcessorResultHolder(null, null, ex);
            }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, proc);
            PortableUtils.WritePortableOrSerializable(writer0, arg);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheEntryProcessorHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();

            proc = PortableUtils.ReadPortableOrSerializable<object>(reader0);
            arg = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            processFunc = GetProcessFunc(proc);

            var kvTypes = DelegateTypeDescriptor.GetCacheEntryProcessorTypes(proc.GetType());

            entryCtor = MutableCacheEntry.GetCtor(kvTypes.Item1, kvTypes.Item2);
        }

        /// <summary>
        /// Gets a delegate to call generic <see cref="ICacheEntryProcessor{K, V, A, R}.Process"/>.
        /// </summary>
        /// <param name="proc">The processor instance.</param>
        /// <returns>
        /// Delegate to call generic <see cref="ICacheEntryProcessor{K, V, A, R}.Process"/>.
        /// </returns>
        private static Func<IMutableCacheEntryInternal, object, object> GetProcessFunc(object proc)
        {
            var func = DelegateTypeDescriptor.GetCacheEntryProcessor(proc.GetType());
            
            return (entry, arg) => func(proc, entry, arg);
        }
    }
}