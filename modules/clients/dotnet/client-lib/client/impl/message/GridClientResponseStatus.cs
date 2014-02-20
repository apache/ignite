// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    /** <summary>Response status codes.</summary> */
    internal enum GridClientResponseStatus {
        /** <summary>Command succeeded.</summary> */
        Success = 0,

        /** <summary>Command failed.</summary> */
        Failed = 1,

        /** <summary>Authentication failure.</summary> */
        AuthFailure = 2
    }
}
