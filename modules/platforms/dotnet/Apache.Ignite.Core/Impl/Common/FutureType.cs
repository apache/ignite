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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Future types.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1008:EnumsShouldHaveZeroValue", Justification = "Interoperability")]
    public enum FutureType
    {
        /// <summary> Future type: byte. </summary>
        Byte = 1,

        /// <summary> Future type: boolean. </summary>
        Bool = 2,

        /// <summary> Future type: short. </summary>
        Short = 3,

        /// <summary> Future type: char. </summary>
        Char = 4,

        /// <summary> Future type: int. </summary>
        Int = 5,

        /// <summary> Future type: float. </summary>
        Float = 6,

        /// <summary> Future type: long. </summary>
        Long = 7,

        /// <summary> Future type: double. </summary>
        Double = 8,

        /// <summary> Future type: object. </summary>
        Object = 9
    }
}