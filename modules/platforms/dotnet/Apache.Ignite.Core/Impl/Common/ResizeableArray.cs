﻿/*
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
    using System.Collections.Generic;

    /// <summary>
    /// Simple append-only <see cref="List{T}"/> alternative which exposes internal array.
    /// </summary>
    internal class ResizeableArray<T>
    {
        private T[] _arr;
        private int _count;

        public ResizeableArray(int capacity)
        {
            _arr = new T[capacity];
        }

        public T[] Array
        {
            get { return _arr; }
        }

        public int Count
        {
            get { return _count; }
        }

        public void Add(T element)
        {
            if (_count == _arr.Length)
                System.Array.Resize(ref _arr, _arr.Length*2);

            _arr[_count++] = element;
        }
    }
}
