/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Config
{
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which is capable of setting configuration properties taken from somewhere.
    /// </summary>
    internal interface IGridConfigurator<T>
    {
        /// <summary>
        /// Set configuration.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="src">Source.</param>
        void Configure(IgniteConfiguration cfg, T src);
    }
}
