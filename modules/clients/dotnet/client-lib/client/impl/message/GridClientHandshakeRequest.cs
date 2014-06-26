using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GridGain.Client.Impl.Message
{
    /**
     * <summary>Handshake request.</summary>
     */ 
    class GridClientHandshakeRequest
    {
        /** Packet size. */
        public const int PACKET_SIZE = 6;

        /** Signal char. */
        public const byte SIGNAL_CHAR = 0x91;

        /** Portable serializer protocol ID. */
        public const byte PROTO_ID = 4;

        
        //private byte[] verArr;



    }
}
