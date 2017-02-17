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

#ifndef _IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT
#define _IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT

#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Type snapshot.
             */
            class BinaryTypeSnapshot
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param typeName Type name.
                 * @param typeId Type ID.
                 * @param fieldIds Field IDs.
                 * @param fields Fields.
                 */
                BinaryTypeSnapshot(std::string typeName, int32_t typeId, std::set<int32_t>* fieldIds, 
                    std::map<std::string, int32_t>* fields);

                /**
                 * Destructor.
                 */
                ~BinaryTypeSnapshot();

                /**
                 * Check whether snapshot contains a field with the given ID.
                 *
                 * @param fieldId Field ID.
                 * @return True if contains, false otherwise.
                 */
                bool ContainsFieldId(int32_t fieldId);

                /**
                 * Get type name.
                 *
                 * @return Type name.
                 */
                std::string GetTypeName();

                /**
                 * Get type ID.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId();

                /**
                 * Whether snapshot contains any fields.
                 *
                 * @return True if fields exist.
                 */
                bool HasFields();

                /** 
                 * Get field IDs.
                 *
                 * @return Field IDs.
                 */
                std::set<int32_t>* GetFieldIds();

                /**
                 * Get fields.
                 *
                 * @return Fields.
                 */
                std::map<std::string, int32_t>* GetFields();

            private:
                /** Type name. */
                std::string typeName;

                /** Type ID. */
                int32_t typeId;

                /** Known field IDs. */
                std::set<int32_t>* fieldIds;

                /** Field name-type mappings. */
                std::map<std::string, int32_t>* fields; 

                IGNITE_NO_COPY_ASSIGNMENT(BinaryTypeSnapshot)
            };

            typedef BinaryTypeSnapshot Snap;
            typedef ignite::common::concurrent::SharedPointer<Snap> SPSnap;
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_SNAPSHOT