using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GridGain.Client.Impl.Message
{
    /**
     * <summary>Handshake request.</summary>
     */ 
    internal static class GridClientHandshake
    {
        /** Signal char. */
        private const byte SIGNAL_CHAR = 0x91;

        /** Portable serializer protocol ID. */
        public const byte PROTO_ID = 4;

        /** Response: OK. */
        public const int CODE_OK = 0;

        /** Response: unknown protocol ID. */
        public const int CODE_UNKNOWN_PROTO_ID = 2;

        /** Bytes. */
        public static readonly byte[] BYTES = new byte[] { SIGNAL_CHAR, 0, 0, 0, 0, PROTO_ID };
    }
}
