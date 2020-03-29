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

namespace Apache.Ignite.Core.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Indicates an error during Grid Services deployment.
    /// </summary>
    [Serializable]
    public class ServiceDeploymentException : IgniteException
    {
        /** Serializer key for BinaryCause. */
        private const string KeyBinaryCause = "BinaryCause";

        /** Serializer key for Failed Configurations. */
        private const string KeyFailedConfigurations = "FailedConfigurations";

        /** Cause. */
        private readonly IBinaryObject _binaryCause;

        /** Configurations of services that failed to deploy */
        private readonly ICollection<ServiceConfiguration> _failedCfgs;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDeploymentException"/> class.
        /// </summary>
        public ServiceDeploymentException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDeploymentException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ServiceDeploymentException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDeploymentException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ServiceDeploymentException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDeploymentException"/> class with failed configurations.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        /// <param name="failedCfgs">List of failed configurations</param>
        public ServiceDeploymentException(string message, Exception cause, ICollection<ServiceConfiguration> failedCfgs) 
            : base(message, cause)
        {
            _failedCfgs = failedCfgs;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDeploymentException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="binaryCause">The binary cause.</param>
        /// <param name="failedCfgs">List of failed configurations</param>
        public ServiceDeploymentException(string message, IBinaryObject binaryCause,
            ICollection<ServiceConfiguration> failedCfgs) : base(message)
        {
            _binaryCause = binaryCause;
            _failedCfgs = failedCfgs;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ServiceDeploymentException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            _binaryCause = (IBinaryObject)info.GetValue(KeyBinaryCause, typeof(IBinaryObject));
            _failedCfgs = (ICollection<ServiceConfiguration>)info.GetValue(KeyFailedConfigurations, 
                typeof(ICollection<ServiceConfiguration>));
        }

        /// <summary>
        /// Gets the binary cause.
        /// </summary>
        public IBinaryObject BinaryCause
        {
            get { return _binaryCause; }
        }

        /// <summary>
        /// When overridden in a derived class, sets the <see cref="SerializationInfo" />
        /// with information about the exception.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo" /> that holds the serialized object data
        /// about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext" /> that contains contextual information
        /// about the source or destination.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(KeyBinaryCause, _binaryCause);
            info.AddValue(KeyFailedConfigurations, _failedCfgs);

            base.GetObjectData(info, context);
        }

        /// <summary>
        /// Configurations of services that failed to deploy, could be null
        /// </summary>
        public ICollection<ServiceConfiguration> FailedConfigurations
        {
            get { return _failedCfgs; }
        }
    }
}