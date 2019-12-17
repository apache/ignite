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

namespace Apache.Ignite.Core.Log
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Returns <see cref="DateTime.Now"/>.
    /// </summary>
    public class LocalDateTimeProvider : IDateTimeProvider
    {
        /// <summary>
        /// Default instance.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", 
            Justification = "Type is immutable.")]
        public static readonly LocalDateTimeProvider Instance = new LocalDateTimeProvider();
        
        /// <summary>
        /// Gets current <see cref="DateTime"/>.
        /// </summary>
        public DateTime Now()
        {
            return DateTime.Now;
        }
    }
}