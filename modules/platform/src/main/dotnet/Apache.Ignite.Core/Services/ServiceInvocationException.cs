/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Services
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Indicates an error during Grid Services invocation.
    /// </summary>
    [Serializable]
    public class ServiceInvocationException : IgniteException
    {
        /** Serializer key. */
        private const string KEY_PORTABLE_CAUSE = "PortableCause";

        /** Cause. */
        private readonly IPortableObject portableCause;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        public ServiceInvocationException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ServiceInvocationException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ServiceInvocationException(string message, Exception cause) : base(message, cause)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="portableCause">The portable cause.</param>
        public ServiceInvocationException(string message, IPortableObject portableCause)
            :base(message)
        {
            this.portableCause = portableCause;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ServiceInvocationException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            portableCause = (IPortableObject) info.GetValue(KEY_PORTABLE_CAUSE, typeof (IPortableObject));
        }

        /// <summary>
        /// Gets the portable cause.
        /// </summary>
        public IPortableObject PortableCause
        {
            get { return portableCause; }
        }

        /** <inheritdoc /> */
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(KEY_PORTABLE_CAUSE, portableCause);

            base.GetObjectData(info, context);
        }
    }
}