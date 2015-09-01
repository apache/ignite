/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    /// <summary>
    /// Portable mode.
    /// </summary>
    internal enum PortableMode
    {
        /// <summary>
        /// Deserialize top-level portable objects, but leave nested portable objects in portable form.
        /// </summary>
        DESERIALIZE,

        /// <summary>
        /// Keep portable objects in portable form.
        /// </summary>
        KEEP_PORTABLE,

        /// <summary>
        /// Always return IPortableObject.
        /// </summary>
        FORCE_PORTABLE
    }
}