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

/**
 * @file
 * Declares ignite::Guid class.
 */

#ifndef _IGNITE_GUID
#define _IGNITE_GUID

#include <stdint.h>
#include <iomanip>

#include <ignite/common/common.h>

namespace ignite
{
    /**
     * Global universally unique identifier (GUID).
     */
    class IGNITE_IMPORT_EXPORT Guid
    {
    public:
        /**
         * Default constructor.
         */
        Guid();

        /**
         * Constructor.
         *
         * @param most Most significant bits.
         * @param least Least significant bits.
         */
        Guid(int64_t most, int64_t least);

        /**
         * Returns the most significant 64 bits of this instance.
         *
         * @return The most significant 64 bits of this instance.
         */
        int64_t GetMostSignificantBits() const;

        /**
         * Returns the least significant 64 bits of this instance.
         *  
         * @return The least significant 64 bits of this instance.
         */
        int64_t GetLeastSignificantBits() const;

        /**
         * The version number associated with this instance.  The version
         * number describes how this Guid was generated.
         *
         * The version number has the following meaning:
         * 1    Time-based UUID;
         * 2    DCE security UUID;
         * 3    Name-based UUID;
         * 4    Randomly generated UUID.
         *
         * @return The version number of this instance.
         */
        int32_t GetVersion() const;

        /**
         * The variant number associated with this instance. The variant
         * number describes the layout of the Guid.
         *
         * The variant number has the following meaning:
         * 0    Reserved for NCS backward compatibility;
         * 2    IETF RFC 4122 (Leach-Salz), used by this class;
         * 6    Reserved, Microsoft Corporation backward compatibility;
         * 7    Reserved for future definition.
         *
         * @return The variant number of this instance.
         */
        int32_t GetVariant() const;

        /**
         * Get hash code of this instance (used in serialization).
         *
         * @return Hash code.
         */
        int32_t GetHashCode() const;

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator== (const Guid& val1, const Guid& val2);

        /**
         * Compare to another value.
         *
         * @param other Instance to compare to.
         * @return Zero if equals, negative number if less and positive if more.
         */
        int64_t Compare(const Guid& other) const;

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator==(const Guid& val1, const Guid& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if not equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator!=(const Guid& val1, const Guid& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<(const Guid& val1, const Guid& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<=(const Guid& val1, const Guid& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if greater.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>(const Guid& val1, const Guid& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if greater or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>=(const Guid& val1, const Guid& val2);

    private:
        /** Most significant bits. */
        int64_t most;  

        /** Least significant bits. */
        int64_t least; 
    };

    /**
     * Output operator.
     *
     * @param os Output stream.
     * @param guid Guid to output.
     * @return Reference to the first param.
     */
    template<typename C>
    ::std::basic_ostream<C>& operator<<(std::basic_ostream<C>& os, const Guid& guid)
    {
        uint32_t part1 = static_cast<uint32_t>(guid.GetMostSignificantBits() >> 32);
        uint16_t part2 = static_cast<uint16_t>(guid.GetMostSignificantBits() >> 16);
        uint16_t part3 = static_cast<uint16_t>(guid.GetMostSignificantBits());
        uint16_t part4 = static_cast<uint16_t>(guid.GetLeastSignificantBits() >> 48);
        uint64_t part5 = guid.GetLeastSignificantBits() & 0x0000FFFFFFFFFFFFU;

        os  << std::hex 
            << std::setfill<C>('0') << std::setw(8)  << part1 << '-'
            << std::setfill<C>('0') << std::setw(4)  << part2 << '-'
            << std::setfill<C>('0') << std::setw(4)  << part3 << '-'
            << std::setfill<C>('0') << std::setw(4)  << part4 << '-'
            << std::setfill<C>('0') << std::setw(12) << part5 << std::dec;

        return os;
    }

    /**
     * Input operator.
     *
     * @param is Input stream.
     * @param guid Guid to input.
     * @return Reference to the first param.
     */
    template<typename C>
    ::std::basic_istream<C>& operator>>(std::basic_istream<C>& is, Guid& guid)
    {
        uint64_t parts[5];

        C delim;

        for (int i = 0; i < 4; ++i)
        {
            is >> std::hex >> parts[i] >> delim;

            if (delim != static_cast<C>('-'))
                return is;
        }

        is >> std::hex >> parts[4];

        guid = Guid((parts[0] << 32) | (parts[1] << 16) | parts[2], (parts[3] << 48) | parts[4]);

        return is;
    }
}

#endif
