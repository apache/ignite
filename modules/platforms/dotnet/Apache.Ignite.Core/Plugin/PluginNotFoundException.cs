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

namespace Apache.Ignite.Core.Plugin
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Indicates missing Ignite plugin.
    /// </summary>
    [Serializable]
    public class PluginNotFoundException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PluginNotFoundException"/> class.
        /// </summary>
        public PluginNotFoundException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginNotFoundException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public PluginNotFoundException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginNotFoundException"/> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, 
        /// or a null reference if no inner exception is specified.</param>
        public PluginNotFoundException(string message, Exception innerException) : base(message, innerException)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginNotFoundException"/> class.
        /// </summary>
        protected PluginNotFoundException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            // No-op.
        }
    }
}