// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;

    /** <summary>Cache projection flags that specify projection behaviour.</summary> */
    [FlagsAttribute]
    public enum GridClientCacheFlag {
        /** <summary>No cache flags, use default configuration.</summary> */
        None = 0x00,

        /** <summary>Skips store, i.e. no read-through and no write-through behavior.</summary> */
        SkipStore = 0x01,

        /** <summary>Skip swap space for reads and writes.</summary> */
        SkipSwap = 0x02,

        /** <summary>Synchronous commit.</summary> */
        SyncCommit = 0x04,

        /** <summary>Synchronous rollback.</summary> */
        SyncRollback = 0x08,

        /**
         * <summary>
         * Switches a cache projection to work in {@code 'invalidation'} mode.
         * Instead of updating remote entries with new values, small invalidation
         * messages will be sent to set the values to {@code null}.</summary>
         */
        Invalidate = 0x10
    }
}
