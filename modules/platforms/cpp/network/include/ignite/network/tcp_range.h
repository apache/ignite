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

#ifndef _IGNITE_NETWORK_TCP_RANGE
#define _IGNITE_NETWORK_TCP_RANGE

#include <stdint.h>
#include <string>

namespace ignite
{
    namespace network
    {
        /**
         * TCP port range.
         */
        struct TcpRange
        {
            /**
             * Default constructor.
             */
            TcpRange() :
                port(0),
                range(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param host Host.
             * @param port Port.
             * @param range Number of ports after the @c port that
             *    should be tried if the previous are unavailable.
             */
            TcpRange(const std::string& host, uint16_t port, uint16_t range = 0) :
                host(host),
                port(port),
                range(range)
            {
                // No-op.
            }

            /**
             * Compare to another instance.
             *
             * @param other Another instance.
             * @return Negative value if less, positive if larger and
             *    zero, if equals another instance.
             */
            int Compare(const TcpRange& other) const
            {
                if (port < other.port)
                    return -1;

                if (port > other.port)
                    return 1;

                if (range < other.range)
                    return -1;

                if (range > other.range)
                    return 1;

                return host.compare(other.host);
            }

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if equal.
             */
            friend bool operator==(const TcpRange& val1, const TcpRange& val2)
            {
                return val1.port == val2.port && val1.range == val2.range && val1.host == val2.host;
            }


            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if not equal.
             */
            friend bool operator!=(const TcpRange& val1, const TcpRange& val2)
            {
                return !(val1 == val2);
            }

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if less.
             */
            friend bool operator<(const TcpRange& val1, const TcpRange& val2)
            {
                return val1.Compare(val2) < 0;
            }

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if less or equal.
             */
            friend bool operator<=(const TcpRange& val1, const TcpRange& val2)
            {
                return val1.Compare(val2) <= 0;
            }

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if gretter.
             */
            friend bool operator>(const TcpRange& val1, const TcpRange& val2)
            {
                return val1.Compare(val2) > 0;
            }

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if gretter or equal.
             */
            friend bool operator>=(const TcpRange& val1, const TcpRange& val2)
            {
                return val1.Compare(val2) >= 0;
            }

            /** Remote host. */
            std::string host;

            /** TCP port. */
            uint16_t port;

            /**
             * Number of ports after the port that should be tried if
             * the previous are unavailable.
             */
            uint16_t range;
        };
    }
}

#endif //_IGNITE_NETWORK_TCP_RANGE