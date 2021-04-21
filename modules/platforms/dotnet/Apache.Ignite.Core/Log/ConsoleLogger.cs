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
    using System.Text;

    /// <summary>
    /// Logs to Console.
    /// <para />
    /// Simple logger implementation without dependencies, provided out of the box for convenience.
    /// For anything more complex please use NLog/log4net integrations.
    /// </summary>
    public class ConsoleLogger : ILogger
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ConsoleLogger"/> class.
        /// Uses <see cref="LogLevel.Warn"/> minimum level.
        /// </summary>
        public ConsoleLogger()
        {
            MinLevel = LogLevel.Warn;
        }

        /// <summary>
        /// Gets the minimum level to be logged. Any levels lower than that are ignored.
        /// Default is <see cref="LogLevel.Warn"/>.
        /// </summary>
        public LogLevel MinLevel { get; set; }
        
        /// <summary>
        /// Gets or sets DateTime provider.
        /// </summary>
        public IDateTimeProvider DateTimeProvider { get; set; }

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
            if (!IsEnabled(level))
            {
                return;
            }

            var dateTimeProvider = DateTimeProvider ?? LocalDateTimeProvider.Instance;

            var sb = new StringBuilder().AppendFormat(
                "[{0:HH:mm:ss}] [{1}] [{2}] ", dateTimeProvider.Now(), level, category);
            
            if (args != null)
            {
                sb.AppendFormat(formatProvider, message, args);
            }
            else
            {
                sb.Append(message);
            }

            if (ex != null)
            {
                sb.AppendFormat(" (exception: {0})", ex);
            }
            
            Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Determines whether the specified log level is enabled.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <returns>Value indicating whether the specified log level is enabled</returns>
        public bool IsEnabled(LogLevel level)
        {
            return level >= MinLevel;
        }
    }
}