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
    /// Client MessagePack extension type codes.
    /// </summary>
    internal enum ClientMessagePackType
    {
        /// <summary>
        /// Number.
        /// </summary>
        Number = 1,

        /// <summary>
        /// Decimal.
        /// </summary>
        Decimal = 2,

        /// <summary>
        /// UUID / Guid.
        /// </summary>
        Uuid = 3,

        /// <summary>
        /// Date.
        /// </summary>
        Date = 4,

        /// <summary>
        /// Time.
        /// </summary>
        Time = 5,

        /// <summary>
        /// DateTime.
        /// </summary>
        Datetime = 6,

        /// <summary>
        /// Timestamp.
        /// </summary>
        Timestamp = 7,

        /// <summary>
        /// Bitmask.
        /// </summary>
        Bitmask = 8,
    }
}
