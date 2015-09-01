/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl
{
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Interop callback.
    /// </summary>
    internal interface IInteropCallback
    {
        /// <summary>
        /// Invokes callback.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Invocation result.</returns>
        int Invoke(IPortableStream stream);
    }
}