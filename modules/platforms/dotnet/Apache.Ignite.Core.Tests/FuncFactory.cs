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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Factory from a func.
    /// </summary>
    [Serializable]
    public class FuncFactory<T> : IFactory<T>
    {
        /** */
        private readonly Func<T> _factory;

        /// <summary>
        /// Initializes a new instance of the <see cref="FuncFactory{T}"/> class.
        /// </summary>
        public FuncFactory(Func<T> factory)
        {
            _factory = factory;
        }

        /** <inheritdoc /> */
        public T CreateInstance()
        {
            return _factory();
        }
    }
}
