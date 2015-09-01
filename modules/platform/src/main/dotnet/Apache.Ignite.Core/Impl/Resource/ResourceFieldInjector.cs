/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Resource
{
    using System;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Field resource injector.
    /// </summary>
    internal class ResourceFieldInjector : IResourceInjector
    {
        /** */
        private readonly Action<object, object> inject;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="field">Field.</param>
        public ResourceFieldInjector(FieldInfo field)
        {
            inject = DelegateConverter.CompileFieldSetter(field);
        }

        /** <inheritDoc /> */
        public void Inject(object target, object val)
        {
            inject(target, val);
        }
    }
}
