/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /** <summary>Available cache operations.</summary> */
    internal enum GridClientCacheRequestOperation {
        /** <summary>Cache put.</summary> */
        Put,

        /** <summary>Cache put all.</summary> */
        PutAll,

        /** <summary>Cache get.</summary> */
        Get,

        /** <summary>Cache get all.</summary> */
        GetAll,

        /** <summary>Cache remove.</summary> */
        Rmv,

        /** <summary>Cache remove all.</summary> */
        RmvAll,

        /** <summary>Cache replace (put only if exists).</summary> */
        Replace,

        /** <summary>Cache compare and set.</summary> */
        Cas,

        /** <summary>Cache metrics request.</summary> */
        Metrics,

        /** <summary>Append requested value to already cached one.</summary> */
        Append,

        /** <summary>Prepend requested value to already cached one.</summary> */
        Prepend,
    }
}
