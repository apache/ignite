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
 * Declares ignite::binary::BinaryEnumEntry class.
 */

#ifndef _IGNITE_BINARY_BINARY_ENUM_ENTRY
#define _IGNITE_BINARY_BINARY_ENUM_ENTRY

#include <stdint.h>

#include <ignite/common/common.h>

namespace ignite
{
    namespace binary
    {
        /**
         * Binary enum entry.
         *
         * Represents a single entry of enum in a binary form.
         */
        class IGNITE_IMPORT_EXPORT BinaryEnumEntry
        {
        public:
            /**
             * Default constructor.
             */
            BinaryEnumEntry() :
                typeId(0),
                ordinal(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param typeId Type ID of the enum.
             * @param ordinal Ordinal of the enum value.
             */
            BinaryEnumEntry(int32_t typeId, int32_t ordinal) :
                typeId(typeId),
                ordinal(ordinal)
            {
                // No-op.
            }

            /**
             * Get type ID.
             * Type ID can never equal zero. If the Type ID equals zero, the instance is not valid, and could only be
             * acquired by manual construction or by reading NULL value.
             *
             * @return Type ID.
             */
            int32_t GetTypeId() const
            {
                return typeId;
            }

            /**
             * Get ordinal of the enum value.
             *
             * @return Ordinal.
             */
            int32_t GetOrdinal() const
            {
                return ordinal;
            }

            /**
             * Check whether value was acquired by reading a NULL value.
             *
             * @return @c true if acquired by reading a NULL value.
             */
            bool IsNull() const
            {
                return typeId == 0;
            }

        private:
            /** Type ID. */
            int32_t typeId;

            /** Ordinal value. */
            int32_t ordinal;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_ENUM_ENTRY
