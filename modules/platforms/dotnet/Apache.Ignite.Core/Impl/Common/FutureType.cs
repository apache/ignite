/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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