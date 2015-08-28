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
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Ignite extensibility context default implementation.
    /// </summary>
    internal class IgniteContext : IIgniteContext
    {
        /** <inheritdoc /> */
        public Exception ConvertException(Exception exception)
        {
            return exception;
        }

        /** <inheritdoc /> */
        public object CreateSerializableObjectHolder(object obj)
        {
            return new SerializableObjectHolder(obj);
        }

        /** <inheritdoc /> */
        public object UnwrapSerializableObjectHolder(object obj)
        {
            var holder = obj as SerializableObjectHolder;

            return holder == null ? obj : holder.Item;
        }
    }
}
