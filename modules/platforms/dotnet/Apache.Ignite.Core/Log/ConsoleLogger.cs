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

using System;

namespace Apache.Ignite.Core.Log
{
    using System.Globalization;
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Logs to console.
    /// </summary>
    public class ConsoleLogger : ILogger
    {
        /** */
        private readonly LogLevel[] _enabledLevels;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleLogger"/> class.
        /// </summary>
        /// <param name="enabledLevels">The enabled levels.</param>
        public ConsoleLogger(params LogLevel[] enabledLevels)
        {
            IgniteArgumentCheck.NotNull(enabledLevels, "enabledLevels");

            _enabledLevels = enabledLevels;
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category, 
            string nativeErrorInfo, Exception ex)
        {
            IgniteArgumentCheck.NotNull(message, "message");

            if (!IsEnabled(level))
                return;

            var sb = new StringBuilder();

            sb.AppendFormat("[{0}] ", DateTime.Now.ToString("T", CultureInfo.InvariantCulture));

            if (args != null)
            {
                IgniteArgumentCheck.NotNull(formatProvider, "formatProvider");

                sb.AppendFormat(formatProvider, message, args);
            }
            else
            {
                sb.Append(message);
            }

            if (!string.IsNullOrWhiteSpace(nativeErrorInfo))
                sb.AppendFormat("\nNative error: {0}", nativeErrorInfo);

            if (ex != null)
                sb.AppendFormat("\nException: {0}", ex);

            Console.WriteLine(sb.ToString());

        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            // ReSharper disable once ForCanBeConvertedToForeach (performance)
            // ReSharper disable once LoopCanBeConvertedToQuery (performance)
            for (var i = 0; i < _enabledLevels.Length; i++)
            {
                if (_enabledLevels[i] == level)
                    return true;
            }

            return false;
        }
    }
}
