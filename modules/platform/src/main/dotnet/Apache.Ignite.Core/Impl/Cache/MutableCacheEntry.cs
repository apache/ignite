/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents a cache entry.
    /// </summary>
    internal class MutableCacheEntry<K, V> : IMutableCacheEntry<K, V>, IMutableCacheEntryInternal
    {
        // Entry value
        private V value;

        /// <summary>
        /// Initializes a new instance of the <see cref="MutableCacheEntry{K, V}"/> class.
        /// </summary>
        /// <param name="key">The key.</param>
        public MutableCacheEntry(K key)
        {
            Key = key;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MutableCacheEntry{K, V}"/> class.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public MutableCacheEntry(K key, V value)
        {
            Key = key;
            this.value = value;
            Exists = true;
        }

        /** <inheritdoc /> */
        public K Key { get; private set; }

        /** <inheritdoc /> */
        object IMutableCacheEntryInternal.Key
        {
            get { return Key; }
        }

        /** <inheritdoc /> */
        public V Value
        {
            get { return value; }
            set
            {
                this.value = value;
                Exists = true;
                State = MutableCacheEntryState.VALUE_SET;
            }
        }

        /** <inheritdoc /> */
        object IMutableCacheEntryInternal.Value
        {
            get { return Value; }
        }

        /** <inheritdoc /> */
        public bool Exists { get; private set; }

        /** <inheritdoc /> */
        public void Remove()
        {
            Value = default(V);
            Exists = false;
            State = MutableCacheEntryState.REMOVED;
        }

        /** <inheritdoc /> */
        public MutableCacheEntryState State { get; private set; }
    }

    /// <summary>
    /// Internal non-generic representation of a mutable cache entry.
    /// </summary>
    internal interface IMutableCacheEntryInternal
    {
        /// <summary>
        /// Gets the key.
        /// </summary>
        object Key { get; }

        /// <summary>
        /// Gets the value.
        /// </summary>
        object Value { get; }

        /// <summary>
        /// Gets a value indicating whether cache entry exists.
        /// </summary>
        bool Exists { get; }

        /// <summary>
        /// Gets the state indicating user operation on this instance.
        /// </summary>
        MutableCacheEntryState State { get; }
    }

    /// <summary>
    /// Mutable cache entry factory.
    /// </summary>
    internal static class MutableCacheEntry
    {
        private static readonly CopyOnWriteConcurrentDictionary<Tuple<Type, Type>, Func<object, object, bool, IMutableCacheEntryInternal>> 
            CTORS = new CopyOnWriteConcurrentDictionary<Tuple<Type, Type>, Func<object, object, bool, IMutableCacheEntryInternal>>();

        public static Func<object, object, bool, IMutableCacheEntryInternal> GetCtor(Type keyType, Type valType)
        {
            Func<object, object, bool, IMutableCacheEntryInternal> result;
            var funcKey = new Tuple<Type, Type>(keyType, valType);

            return CTORS.TryGetValue(funcKey, out result)
                ? result
                : CTORS.GetOrAdd(funcKey, x =>
                {
                    var entryType = typeof (MutableCacheEntry<,>).MakeGenericType(keyType, valType);

                    var oneArg = DelegateConverter.CompileCtor<Func<object, IMutableCacheEntryInternal>>(entryType,
                        new[] {keyType}, false);

                    var twoArg =
                        DelegateConverter.CompileCtor<Func<object, object, IMutableCacheEntryInternal>>(entryType, 
                        new[] {keyType, valType}, false);

                    return (k, v, exists) => exists ? twoArg(k, v) : oneArg(k);
                });
        }
    }

    /// <summary>
    /// Represents result of user operation on a mutable cache entry.
    /// </summary>
    internal enum MutableCacheEntryState : byte
    {
        INTACT = 0,
        VALUE_SET = 1,
        REMOVED = 2,
        ERR_PORTABLE = 3,
        ERR_STRING = 4
    }
}