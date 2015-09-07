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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;

    /// <summary>
    /// Type descriptor with precompiled delegates for known methods.
    /// </summary>
    internal class DelegateTypeDescriptor
    {
        /** Cached decriptors. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, DelegateTypeDescriptor> Descriptors 
            = new CopyOnWriteConcurrentDictionary<Type, DelegateTypeDescriptor>();

        /** */
        private readonly Func<object, object> _computeOutFunc;

        /** */
        private readonly Func<object, object, object> _computeFunc;

        /** */
        private readonly Func<object, Guid, object, bool> _eventFilter;

        /** */
        private readonly Func<object, object, object, bool> _cacheEntryFilter;
        
        /** */
        private readonly Tuple<Func<object, IMutableCacheEntryInternal, object, object>, Tuple<Type, Type>> 
            _cacheEntryProcessor;

        /** */
        private readonly Func<object, Guid, object, bool> _messageFilter;

        /** */
        private readonly Func<object, object> _computeJobExecute;

        /** */
        private readonly Action<object> _computeJobCancel;

        /** */
        private readonly Action<object, Ignite, IUnmanagedTarget, IPortableStream, bool> _streamReceiver;

        /** */
        private readonly Func<object, object> _streamTransformerCtor;

        /// <summary>
        /// Gets the <see cref="IComputeFunc{T}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, object> GetComputeOutFunc(Type type)
        {
            return Get(type)._computeOutFunc;
        }

        /// <summary>
        /// Gets the <see cref="IComputeFunc{T, R}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, object, object> GetComputeFunc(Type type)
        {
            return Get(type)._computeFunc;
        }

        /// <summary>
        /// Gets the <see cref="IEventFilter{T}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, Guid, object, bool> GetEventFilter(Type type)
        {
            return Get(type)._eventFilter;
        }

        /// <summary>
        /// Gets the <see cref="ICacheEntryFilter{TK,TV}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, object, object, bool> GetCacheEntryFilter(Type type)
        {
            return Get(type)._cacheEntryFilter;
        }
        
        /// <summary>
        /// Gets the <see cref="ICacheEntryProcessor{K, V, A, R}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, IMutableCacheEntryInternal, object, object> GetCacheEntryProcessor(Type type)
        {
            return Get(type)._cacheEntryProcessor.Item1;
        }

        /// <summary>
        /// Gets key and value types for the <see cref="ICacheEntryProcessor{K, V, A, R}" />.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Key and value types.</returns>
        public static Tuple<Type, Type> GetCacheEntryProcessorTypes(Type type)
        {
            return Get(type)._cacheEntryProcessor.Item2;
        }

        /// <summary>
        /// Gets the <see cref="IMessageFilter{T}" /> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, Guid, object, bool> GetMessageFilter(Type type)
        {
            return Get(type)._messageFilter;
        }

        /// <summary>
        /// Gets the <see cref="IComputeJob{T}.Execute" /> and <see cref="IComputeJob{T}.Cancel" /> invocators.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="execute">Execute invocator.</param>
        /// <param name="cancel">Cancel invocator.</param>
        public static void GetComputeJob(Type type, out Func<object, object> execute, out Action<object> cancel)
        {
            var desc = Get(type);

            execute = desc._computeJobExecute;
            cancel = desc._computeJobCancel;
        }

        /// <summary>
        /// Gets the <see cref="IStreamReceiver{TK,TV}"/> invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Action<object, Ignite, IUnmanagedTarget, IPortableStream, bool> GetStreamReceiver(Type type)
        {
            return Get(type)._streamReceiver;
        }

        /// <summary>
        /// Gets the <see cref="StreamTransformer{K, V, A, R}"/>> ctor invocator.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Precompiled invocator delegate.</returns>
        public static Func<object, object> GetStreamTransformerCtor(Type type)
        {
            return Get(type)._streamTransformerCtor;
        }

        /// <summary>
        /// Gets the <see cref="DelegateTypeDescriptor" /> by type.
        /// </summary>
        private static DelegateTypeDescriptor Get(Type type)
        {
            DelegateTypeDescriptor result;

            return Descriptors.TryGetValue(type, out result)
                ? result
                : Descriptors.GetOrAdd(type, t => new DelegateTypeDescriptor(t));
        }

        /// <summary>
        /// Throws an exception if first argument is not null.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void ThrowIfMultipleInterfaces(object check, Type userType, Type interfaceType)
        {
            if (check != null)
                throw new InvalidOperationException(
                    string.Format("Not Supported: Type {0} implements interface {1} multiple times.", userType,
                        interfaceType));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateTypeDescriptor"/> class.
        /// </summary>
        /// <param name="type">The type.</param>
        private DelegateTypeDescriptor(Type type)
        {
            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType)
                    continue;

                var genericTypeDefinition = iface.GetGenericTypeDefinition();

                if (genericTypeDefinition == typeof (IComputeFunc<>))
                {
                    ThrowIfMultipleInterfaces(_computeOutFunc, type, typeof(IComputeFunc<>));

                    _computeOutFunc = DelegateConverter.CompileFunc(iface);
                }
                else if (genericTypeDefinition == typeof (IComputeFunc<,>))
                {
                    ThrowIfMultipleInterfaces(_computeFunc, type, typeof(IComputeFunc<,>));

                    var args = iface.GetGenericArguments();

                    _computeFunc = DelegateConverter.CompileFunc<Func<object, object, object>>(iface, new[] {args[0]});
                }
                else if (genericTypeDefinition == typeof (IEventFilter<>))
                {
                    ThrowIfMultipleInterfaces(_eventFilter, type, typeof(IEventFilter<>));

                    var args = iface.GetGenericArguments();

                    _eventFilter = DelegateConverter.CompileFunc<Func<object, Guid, object, bool>>(iface, 
                        new[] {typeof (Guid), args[0]}, new[] {false, true, false});
                }
                else if (genericTypeDefinition == typeof (ICacheEntryFilter<,>))
                {
                    ThrowIfMultipleInterfaces(_cacheEntryFilter, type, typeof(ICacheEntryFilter<,>));

                    var args = iface.GetGenericArguments();

                    var entryType = typeof (ICacheEntry<,>).MakeGenericType(args);

                    var invokeFunc = DelegateConverter.CompileFunc<Func<object, object, bool>>(iface,
                        new[] { entryType }, new[] { true, false });

                    var ctor = DelegateConverter.CompileCtor<Func<object, object, object>>(
                            typeof (CacheEntry<,>).MakeGenericType(args), args);

                    // Resulting func constructs CacheEntry and passes it to user implementation
                    _cacheEntryFilter = (obj, k, v) => invokeFunc(obj, ctor(k, v));
                }
                else if (genericTypeDefinition == typeof (ICacheEntryProcessor<,,,>))
                {
                    ThrowIfMultipleInterfaces(_cacheEntryProcessor, type, typeof(ICacheEntryProcessor<,,,>));

                    var args = iface.GetGenericArguments();

                    var entryType = typeof (IMutableCacheEntry<,>).MakeGenericType(args[0], args[1]);

                    var func = DelegateConverter.CompileFunc<Func<object, object, object, object>>(iface,
                        new[] { entryType, args[2] }, null, "Process");

                    var types = new Tuple<Type, Type>(args[0], args[1]);

                    _cacheEntryProcessor = new Tuple<Func<object, IMutableCacheEntryInternal, object, object>, Tuple<Type, Type>>
                        (func, types);

                    var transformerType = typeof (StreamTransformer<,,,>).MakeGenericType(args);

                    _streamTransformerCtor = DelegateConverter.CompileCtor<Func<object, object>>(transformerType,
                        new[] {iface});
                }
                else if (genericTypeDefinition == typeof (IMessageFilter<>))
                {
                    ThrowIfMultipleInterfaces(_messageFilter, type, typeof(IMessageFilter<>));

                    var arg = iface.GetGenericArguments()[0];

                    _messageFilter = DelegateConverter.CompileFunc<Func<object, Guid, object, bool>>(iface,
                        new[] { typeof(Guid), arg }, new[] { false, true, false });
                }
                else if (genericTypeDefinition == typeof (IComputeJob<>))
                {
                    ThrowIfMultipleInterfaces(_messageFilter, type, typeof(IComputeJob<>));

                    _computeJobExecute = DelegateConverter.CompileFunc<Func<object, object>>(iface, new Type[0], 
                        methodName: "Execute");

                    _computeJobCancel = DelegateConverter.CompileFunc<Action<object>>(iface, new Type[0],
                        new[] {false}, "Cancel");
                }
                else if (genericTypeDefinition == typeof (IStreamReceiver<,>))
                {
                    ThrowIfMultipleInterfaces(_streamReceiver, type, typeof (IStreamReceiver<,>));

                    var method =
                        typeof (StreamReceiverHolder).GetMethod("InvokeReceiver")
                            .MakeGenericMethod(iface.GetGenericArguments());

                    _streamReceiver = DelegateConverter
                        .CompileFunc<Action<object, Ignite, IUnmanagedTarget, IPortableStream, bool>>(
                            typeof (StreamReceiverHolder),
                            method,
                            new[]
                            {
                                iface, typeof (Ignite), typeof (IUnmanagedTarget), typeof (IPortableStream),
                                typeof (bool)
                            },
                            new[] {true, false, false, false, false, false});
                }
            }
        }
    }
}