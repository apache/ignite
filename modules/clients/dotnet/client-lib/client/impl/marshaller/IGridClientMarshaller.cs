// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller {
    using System;

    /** <summary>Marshaller for binary protocol messages.</summary> */
    internal interface IGridClientMarshaller {
        /**
         * <summary>
         * Marshals object to byte array.</summary>
         *
         * <param name="val">Object to marshal.</param>
         * <returns>Byte array.</returns>
         * <exception cref="System.IO.IOException">If marshalling failed.</exception>
         */
        byte[] Marshal(Object val);

        /**
         * <summary>
         * Unmarshalls object from byte array.</summary>
         *
         * <param name="data">Byte array.</param>
         * <returns>Unmarshalled object.</returns>
         * <exception cref="System.IO.IOException">If unmarshalling failed.</exception>
         */
        T Unmarshal<T>(byte[] data);
    }
}
