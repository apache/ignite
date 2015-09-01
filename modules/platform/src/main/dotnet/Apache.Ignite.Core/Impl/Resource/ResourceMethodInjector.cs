/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Resource
{
    using System;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Method resource injector.
    /// </summary>
    internal class ResourceMethodInjector : IResourceInjector
    {
        /** */
        private readonly Action<object, object> inject;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="mthd">Method.</param>
        public ResourceMethodInjector(MethodInfo mthd)
        {
            inject = DelegateConverter.CompileFunc<Action<object, object>>(mthd.DeclaringType, mthd,
                new[] {mthd.GetParameters()[0].ParameterType}, new[] {true, false});
        }

        /** <inheritDoc /> */
        public void Inject(object target, object val)
        {
            inject(target, val);
        }
    }
}
