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

#include "ignite/impl/binary/binary_type_handler.h"

using namespace ignite::common::concurrent;

namespace ignite
{    
    namespace impl
    {
        namespace binary
        {
            BinaryTypeHandler::BinaryTypeHandler(SPSnap snap) :
                origin(snap),
                updated()
            {
                // No-op.
            }

            void BinaryTypeHandler::OnFieldWritten(int32_t fieldId, std::string fieldName, int32_t fieldTypeId)
            {
                if (!origin.Get() || !origin.Get()->ContainsFieldId(fieldId))
                {
                    if (!updated.Get())
                        updated = SPSnap(new Snap(*origin.Get()));

                    updated.Get()->AddField(fieldId, fieldName, fieldTypeId);
                }
            }
        }
    }
}
