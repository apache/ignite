/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Proto
{
    /// <summary>
    /// Client data types.
    /// </summary>
    internal enum ClientDataType
    {
        /// <summary>
        /// Byte.
        /// </summary>
        Int8 = 1,

        /// <summary>
        /// Short.
        /// </summary>
        Int16 = 2,

        /// <summary>
        /// Int.
        /// </summary>
        Int32 = 3,

        /// <summary>
        /// Long.
        /// </summary>
        Int64 = 4,

        /// <summary>
        /// Float.
        /// </summary>
        Float = 5,

        /// <summary>
        /// Double.
        /// </summary>
        Double = 6,

        /// <summary>
        /// Decimal.
        /// </summary>
        Decimal = 7,

        /// <summary>
        /// UUID / Guid.
        /// </summary>
        Uuid = 8,

        /// <summary>
        /// String.
        /// </summary>
        String = 9,

        /// <summary>
        /// Byte array.
        /// </summary>
        Bytes = 10,

        /// <summary>
        /// BitMask.
        /// </summary>
        BitMask = 11,
    }
}
