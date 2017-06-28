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
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Wrapping logger with a predefined category.
    /// <para />
    /// When <see cref="Log"/> method is called, and <c>category</c> parameter is null, predefined category 
    /// will be used.
    /// </summary>
    public class CategoryLogger : ILogger
    {
        /** Wrapped logger. */
        private readonly ILogger _logger;

        /** Category to use. */
        private readonly string _category;

        /// <summary>
        /// Initializes a new instance of the <see cref="CategoryLogger"/> class.
        /// </summary>
        /// <param name="logger">The logger to wrap.</param>
        /// <param name="category">The category.</param>
        public CategoryLogger(ILogger logger, string category)
        {
            IgniteArgumentCheck.NotNull(logger, "log");

            // If logger is already a CategoryLogger, get underlying logger instead to avoid unnecessary nesting.
            var catLogger = logger as CategoryLogger;
            _logger = catLogger != null ? catLogger._logger : logger;

            _category = category;
        }

        /// <summary>
        /// Logs the specified message.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments to format <paramref name="message" />.
        /// Can be null (formatting will not occur).</param>
        /// <param name="formatProvider">The format provider. Can be null if <paramref name="args" /> is null.</param>
        /// <param name="category">The logging category name.</param>
        /// <param name="nativeErrorInfo">The native error information.</param>
        /// <param name="ex">The exception. Can be null.</param>
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            _logger.Log(level, message, args, formatProvider, category ?? _category, nativeErrorInfo, ex);
        }

        /// <summary>
        /// Determines whether the specified log level is enabled.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <returns>
        /// Value indicating whether the specified log level is enabled
        /// </returns>
        public bool IsEnabled(LogLevel level)
        {
            return _logger.IsEnabled(level);
        }
    }
}
