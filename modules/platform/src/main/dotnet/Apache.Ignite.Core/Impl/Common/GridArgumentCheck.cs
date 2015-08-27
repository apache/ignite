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
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Arguments check helpers.
    /// </summary>
    public static class GridArgumentCheck
    {
        /// <summary>
        /// Throws an ArgumentNullException if specified arg is null.
        /// </summary>
        /// <param name="arg">The argument.</param>
        /// <param name="argName">Name of the argument.</param>
        public static void NotNull(object arg, string argName)
        {
            if (arg == null)
                throw new ArgumentNullException(argName);
        }

        /// <summary>
        /// Throws an ArgumentException if specified arg is null or empty string.
        /// </summary>
        /// <param name="arg">The argument.</param>
        /// <param name="argName">Name of the argument.</param>
        public static void NotNullOrEmpty(string arg, string argName)
        {
            if (string.IsNullOrEmpty(arg))
                throw new ArgumentException(string.Format("'{0}' argument should not be null or empty.", argName),
                    argName);
        }

        /// <summary>
        /// Throws an ArgumentException if specified arg is null or empty string.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="argName">Name of the argument.</param>
        public static void NotNullOrEmpty<T>(ICollection<T> collection, string argName)
        {
            if (collection == null || collection.Count == 0)
                throw new ArgumentException(string.Format("'{0}' argument should not be null or empty.", argName),
                    argName);
        }

        /// <summary>
        /// Throws an ArgumentException if specified condition is false.
        /// </summary>
        /// <param name="condition">Condition.</param>
        /// <param name="argName">Name of the argument.</param>
        /// <param name="message">Message.</param>
        public static void Ensure(bool condition, string argName, string message)
        {
            if (!condition)
                throw new ArgumentException(string.Format("'{0}' argument is invalid: {1}", argName, message), 
                    argName);
        }
    }
}