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

    /// <summary>
    /// Represents a result of <see cref="ICacheEntryProcessor{K, V, A, R}"/> invocation.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    internal class CacheEntryProcessorResult<T> : ICacheEntryProcessorResult<T>
    {
        // Result
        private readonly T res;

        // Error
        private readonly Exception err;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{T}"/> class.
        /// </summary>
        /// <param name="result">The result.</param>
        public CacheEntryProcessorResult(T result)
        {
            res = result;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{T}"/> class.
        /// </summary>
        /// <param name="error">The error.</param>
        public CacheEntryProcessorResult(Exception error)
        {
            err = error;
        }

        /** <inheritdoc /> */
        public T Result
        {
            get
            {
                if (err != null)
                    throw err as CacheEntryProcessorException ?? new CacheEntryProcessorException(err);

                return res;
            }
        }
    }
}