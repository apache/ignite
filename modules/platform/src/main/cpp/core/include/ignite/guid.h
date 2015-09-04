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

#ifndef _IGNITE_GUID
#define _IGNITE_GUID

#include <stdint.h>

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
        friend bool IGNITE_IMPORT_EXPORT operator== (Guid& val1, Guid& val2);
    private:
        /** Most significant bits. */
        int64_t most;  

        /** Least significant bits. */
        int64_t least; 
    };
}

#endif