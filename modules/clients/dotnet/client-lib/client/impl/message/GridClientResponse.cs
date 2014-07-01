/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Bean representing client operation result.</summary> */
    [GridClientPortableId(PU.TYPE_RESP)]
    internal class GridClientResponse : IGridClientPortable {
        /**
         * <summary>
         * Tries to find enum value by operation code.</summary>
         *
         * <param name="val">Operation code value.</param>
         * <returns>Enum value.</returns>
         */
        public static GridClientResponseStatus FindByCode(int val) {
            foreach (GridClientResponseStatus code in Enum.GetValues(typeof(GridClientResponseStatus)))
                if (val == (int)code)
                    return code;

            throw new InvalidOperationException("Invalid status code: " + val);
        }

        /** <summary>Request id.</summary> */
        public long RequestId {
            get;
            set;
        }

        /** <summary>Client id.</summary> */
        public Guid ClientId {
            get;
            set;
        }

        /** <summary>Destination node id.</summary> */
        public Guid DestNodeId {
            get;
            set;
        }

        /** <summary>Client session token.</summary> */
        public byte[] SessionToken {
            get;
            set;
        }
        
        /** <summary>Response status code.</summary> */
        public GridClientResponseStatus Status {
            get;
            set;
        }

        /** <summary>Error message, if any error occurred, or <c>null</c>.</summary> */
        public String ErrorMessage {
            get;
            set;
        }

        /** <summary>Result object.</summary> */
        public Object Result {
            get;
            set;
        }

        /** <inheritdoc /> */
        public void WritePortable(IGridClientPortableWriter writer) {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteByteArray(SessionToken);
            rawWriter.WriteInt((int)Status);
            rawWriter.WriteString(ErrorMessage);
            rawWriter.WriteObject(Result);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader) {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            SessionToken = rawReader.ReadByteArray();
            Status = (GridClientResponseStatus)rawReader.ReadInt();
            ErrorMessage = rawReader.ReadString();
            Result = rawReader.ReadObject<object>();
        }
    }
}
