/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Enumeration of all supported cache peek modes.
    /// </summary>
    [Flags]
    [SuppressMessage("Microsoft.Naming", "CA1714:FlagsEnumsShouldHavePluralNames")]
    public enum CachePeekMode
    {
        /// <summary>
        /// Peeks into all available cache storages.
        /// </summary>
        ALL = 0x01,

        /// <summary>
        /// Peek into near cache only (don't peek into partitioned cache).
        /// In case of LOCAL cache, behaves as <see cref="ALL"/> mode.
        /// </summary>
        NEAR = 0x02,

        /// <summary>
        /// Peek value from primary copy of partitioned cache only (skip near cache).
        /// In case of LOCAL cache, behaves as <see cref="ALL"/> mode.
        /// </summary>
        PRIMARY = 0x04,

        /// <summary>
        /// Peek value from backup copies of partitioned cache only (skip near cache).
        /// In case of LOCAL cache, behaves as <see cref="ALL"/> mode.
        /// </summary>
        BACKUP = 0x08,

        /// <summary>
        /// Peeks value from the on-heap storage only.
        /// </summary>
        ONHEAP = 0x10,

        /// <summary>
        /// Peeks value from the off-heap storage only, without loading off-heap value into cache.
        /// </summary>
        OFFHEAP = 0x20,

        /// <summary>
        /// Peeks value from the swap storage only, without loading swapped value into cache.
        /// </summary>
        SWAP = 0x40
    }
}
