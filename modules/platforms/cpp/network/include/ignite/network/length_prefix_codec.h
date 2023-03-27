/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_NETWORK_LENGTH_PREFIX_CODEC
#define _IGNITE_NETWORK_LENGTH_PREFIX_CODEC

#include <ignite/ignite_error.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/codec.h>

namespace ignite
{
    namespace network
    {
        /**
         * Codec that decodes messages prefixed with int32 length.
         */
        class IGNITE_IMPORT_EXPORT LengthPrefixCodec : public Codec
        {
        public:
            enum
            {
                /** Packet length header size. */
                PACKET_HEADER_SIZE = 4
            };

            /**
             * Constructor.
             */
            LengthPrefixCodec();

            /**
             * Destructor.
             */
            virtual ~LengthPrefixCodec();

            /**
             * Encode provided data.
             *
             * @param data Data to encode.
             * @return Encoded data. Returning null is ok.
             *
             * @throw IgniteError on error.
             */
            virtual DataBuffer Encode(DataBuffer& data);

            /**
             * Decode provided data.
             *
             * @param data Data to decode.
             * @return Decoded data. Returning null means data is not yet ready.
             *
             * @throw IgniteError on error.
             */
            virtual DataBuffer Decode(DataBuffer& data);

        private:
            /**
             * Consume the right amount of provided data to make packet closer to desired size.
             *
             * @param data Data to consume.
             * @param desired Desired resulting size of packet.
             */
            void Consume(DataBuffer& data, int32_t desired);

            /** Size of the current packet. */
            int32_t packetSize;

            /** Current packet */
            impl::interop::SP_InteropMemory packet;
        };

        /**
         * Factory for LengthPrefixCodec.
         */
        class IGNITE_IMPORT_EXPORT LengthPrefixCodecFactory : public CodecFactory
        {
        public:
            /**
             * Constructor.
             */
            LengthPrefixCodecFactory()
            {
                // No-op.
            }

            /**
             * Build instance.
             *
             * @return New instance of type @c T.
             */
            virtual SP_Codec Build()
            {
                return SP_Codec(new LengthPrefixCodec());
            }
        };
    }
}

#endif //_IGNITE_NETWORK_LENGTH_PREFIX_CODEC