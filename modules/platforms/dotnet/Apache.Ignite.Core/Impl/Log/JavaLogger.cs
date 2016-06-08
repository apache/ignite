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

namespace Apache.Ignite.Core.Impl.Log
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Log;
    /// <summary>
    /// Logger that delegates to Java.
    /// </summary>
    internal class JavaLogger : ILogger
    {
        /** */
        private readonly IUnmanagedTarget _interopProcessor;

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaLogger"/> class.
        /// </summary>
        /// <param name="interopProcessor">The interop processor.</param>
        public JavaLogger(IUnmanagedTarget interopProcessor)
        {
            Debug.Assert(interopProcessor != null);

            _interopProcessor = interopProcessor;
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category, 
            Exception ex)
        {
            // TODO: Native call
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            // TODO: ???
            return true;
        }
    }
}
