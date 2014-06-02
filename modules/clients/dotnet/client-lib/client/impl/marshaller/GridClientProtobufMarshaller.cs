/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller {
    using System;
    using System.IO;
    using GridGain.Client.Impl.Protobuf;

    /** <summary>Client messages marshaller based on protocol buffers compiled code.</summary> */
    internal class GridClientProtobufMarshaller : IGridClientMarshaller {
        /** <inheritdoc /> */
        public byte[] Marshal(Object val) {
            return GridClientProtobufConverter.WrapObject(val).ToByteArray();
        }

        /** <inheritdoc /> */
        public T Unmarshal<T>(byte[] data) {
            return GridClientProtobufBackConverter.WrapObject<T>(ObjectWrapper.ParseFrom(data));
        }

        /** <inheritdoc /> */
        public void Marshal(object val, Stream output) {
            GridClientProtobufConverter.WrapObject(val).WriteTo(output);
        }

        /** <inheritdoc /> */
        public T Unmarshal<T>(Stream input) {
            return GridClientProtobufBackConverter.WrapObject<T>(ObjectWrapper.ParseFrom(input));
        }
    }
}
