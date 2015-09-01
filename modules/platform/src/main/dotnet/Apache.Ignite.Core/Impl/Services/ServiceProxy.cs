/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.Remoting.Messaging;
    using System.Runtime.Remoting.Proxies;

    /// <summary>
    /// Service proxy: user works with a remote service as if it is a local object.
    /// </summary>
    /// <typeparam name="T">User type to be proxied.</typeparam>
    internal class ServiceProxy<T> : RealProxy
    {
        /** Services. */
        private readonly Func<MethodBase, object[], object> invokeAction;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxy{T}" /> class.
        /// </summary>
        /// <param name="invokeAction">Method invoke action.</param>
        public ServiceProxy(Func<MethodBase, object[], object> invokeAction)
            : base(typeof (T))
        {
            Debug.Assert(invokeAction != null);

            this.invokeAction = invokeAction;
        }

        /** <inheritdoc /> */
        public override IMessage Invoke(IMessage msg)
        {
            var methodCall = msg as IMethodCallMessage;

            if (methodCall == null)
                throw new NotSupportedException("Service proxy operation type not supported: " + msg.GetType() +
                                                ". Only method and property calls are supported.");

            if (methodCall.InArgCount != methodCall.ArgCount)
                throw new NotSupportedException("Service proxy does not support out arguments: "
                                                + methodCall.MethodBase);

            var result = invokeAction(methodCall.MethodBase, methodCall.Args);

            return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
        }

        /** <inheritdoc /> */
        public new T GetTransparentProxy()
        {
            return (T) base.GetTransparentProxy();
        }
    }
}