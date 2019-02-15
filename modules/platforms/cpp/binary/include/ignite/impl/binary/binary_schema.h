/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_IMPL_BINARY_BINARY_SCHEMA
#define _IGNITE_IMPL_BINARY_BINARY_SCHEMA

#include <vector>
#include <stdint.h>

#include <ignite/common/common.h>

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            /* Forward declaration */
            class InteropOutputStream;
        }

        namespace binary
        {
            /**
             * Schema size variants.
             */
            struct BinaryOffsetType
            {
                enum Type
                {
                    /** Means all field offsets can be fit in one byte. */
                    ONE_BYTE,

                    /** Means all field offsets can be fit in two bytes. */
                    TWO_BYTES,

                    /** Means field offsets should be stored in four bytes. */
                    FOUR_BYTES
                };
            };

            /**
             * Binary schema.
             */
            class IGNITE_IMPORT_EXPORT BinarySchema
            {
            public:
                /**
                 * Default constructor.
                 */
                BinarySchema();

                /**
                 * Destructor.
                 */
                ~BinarySchema();

                /**
                 * Add another field to schema.
                 *
                 * @param fieldId Field id.
                 * @param offset Field offset.
                 */
                void AddField(int32_t fieldId, int32_t offset);

                /**
                 * Write Schema to stream.
                 *
                 * @param out Stream to write schema to.
                 */
                void Write(interop::InteropOutputStream& out) const;

                /**
                 * Get Schema ID.
                 *
                 * @return Schema id.
                 */
                int32_t GetId() const
                {
                    return id;
                }

                /** 
                 * Check if the schema contains field info.
                 *
                 * @return True if does not contain field info.
                 */
                bool Empty() const;

                /** 
                 * Clear schema info.
                 */
                void Clear();

                /**
                 * Get type of schema.
                 *
                 * @return Type of schema.
                 */
                BinaryOffsetType::Type GetType() const;

            private:
                /**
                 * Single schema field info.
                 */
                struct BinarySchemaFieldInfo
                {
                    int32_t id;
                    int32_t offset;
                };

                /** Type alias for vector of field info. */
                typedef std::vector<BinarySchemaFieldInfo> FieldContainer;

                /** Schema ID. */
                int32_t id;

                /** Information about written fields. */
                FieldContainer* fieldsInfo;

                IGNITE_NO_COPY_ASSIGNMENT(BinarySchema)
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_SCHEMA