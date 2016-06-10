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
    using System.Globalization;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Extension methods for <see cref="ILogger"/>
    /// </summary>
    public static class LoggerExtensions
    {
        public static void Log(this ILogger logger, LogLevel level, string message)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, null, null, null, null, null);
        }

        public static void Log(this ILogger logger, LogLevel level, string message, params object[] args)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, args, CultureInfo.InvariantCulture, null, null, null);
        }

        public static void Log(this ILogger logger, LogLevel level, Exception ex, string message, params object[] args)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, args, CultureInfo.InvariantCulture, null, null, ex);
        }

        /// <summary>
        /// Gets the <see cref="CategoryLogger"/> with a specified category that wraps provided logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="category">The category.</param>
        /// <returns>Logger that always uses specified category.</returns>
        public static ILogger GetLogger(this ILogger logger, string category)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            return new CategoryLogger(logger, category);
        }
    }
}