// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    /** <summary>Cache type on remote node.</summary> */
    public enum GridClientCacheMode {
        /** <summary>Local cache.</summary> */
        Local = 0x00,

        /** <summary>Replicated cache.</summary> */
        Replicated = 0x01,

        /** <summary>Partitioned cache.</summary> */
        Partitioned = 0x02
    }
}
