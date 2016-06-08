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

namespace Apache.Ignite.Core.Log
{
    using System;
    using System.ComponentModel;

    /// <summary>
    /// Defines Ignite logging interface.
    /// <para />
    /// This interface only provides essential log methods.
    /// All convenience overloads are in <see cref="LoggerExtensions"/>.
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Logs the specified message.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <param name="ex">The exception. Can be null.</param>
        /// <param name="formatProvider">The format provider. Can be null if <paramref name="args"/> is null.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments to format <paramref name="message"/>. 
        /// Can be null (formatting will not occur).</param>
        void Log(LogLevel level, Exception ex, IFormatProvider formatProvider, [Localizable(false)] string message,
            object[] args);

        /// <summary>
        /// Determines whether the specified log level is enabled.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <returns>Value indicating whether the specified log level is enabled</returns>
        bool IsEnabled(LogLevel level);
    }
}
