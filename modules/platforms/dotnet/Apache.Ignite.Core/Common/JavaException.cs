/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Common
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Indicates an error on Java side and contains full Java stack trace.
    /// </summary>
    [Serializable]
    public class JavaException : IgniteException
    {
        /** JavaClassName field. */
        private const string JavaClassNameField = "JavaClassName";

        /** JavaMessage field. */
        private const string JavaMessageField = "JavaMessage";

        /** Java exception class name. */
        private readonly string _javaClassName;
        
        /** Java exception message. */
        private readonly string _javaMessage;

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException"/> class.
        /// </summary>
        public JavaException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public JavaException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException" /> class.
        /// </summary>
        /// <param name="javaClassName">Java exception class name.</param>
        /// <param name="javaMessage">Java exception message.</param>
        /// <param name="stackTrace">Java stack trace.</param>
        public JavaException(string javaClassName, string javaMessage, string stackTrace)
            : this(javaClassName, javaMessage, stackTrace, null)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException" /> class.
        /// </summary>
        /// <param name="javaClassName">Java exception class name.</param>
        /// <param name="javaMessage">Java exception message.</param>
        /// <param name="stackTrace">Java stack trace.</param>
        /// <param name="cause">The cause.</param>
        public JavaException(string javaClassName, string javaMessage, string stackTrace, Exception cause)
            : base(stackTrace ?? javaMessage, cause)
        {
            // Send stackTrace to base ctor because it has all information, including class names and messages.
            // Store ClassName and Message separately for mapping purposes.
            _javaClassName = javaClassName;
            _javaMessage = javaMessage;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public JavaException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected JavaException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
            _javaClassName = info.GetString(JavaClassNameField);
            _javaMessage = info.GetString(JavaMessageField);
        }

        /// <summary>
        /// When overridden in a derived class, sets the <see cref="SerializationInfo" /> 
        /// with information about the exception.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo" /> that holds the serialized object data
        /// about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext" /> that contains contextual information
        /// about the source or destination.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(JavaClassNameField, _javaClassName);
            info.AddValue(JavaMessageField, _javaMessage);
        }

        /// <summary>
        /// Gets the Java exception class name.
        /// </summary>
        public string JavaClassName
        {
            get { return _javaClassName; }
        }

        /// <summary>
        /// Gets the Java exception message.
        /// </summary>
        public string JavaMessage
        {
            get { return _javaMessage; }
        }
    }
}
