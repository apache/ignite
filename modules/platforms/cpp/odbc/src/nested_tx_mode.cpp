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

#include "ignite/odbc/nested_tx_mode.h"
#include "ignite/common/utils.h"

namespace
{
    using ignite::odbc::NestedTxMode;
    NestedTxMode::Type validValues0[] = {
        NestedTxMode::AI_COMMIT,
        NestedTxMode::AI_IGNORE,
        NestedTxMode::AI_ERROR
    };

    NestedTxMode::ModeSet validValues(validValues0, validValues0 + (sizeof(validValues0) / sizeof(validValues0[0])));
}


namespace ignite
{
    namespace odbc
    {
        NestedTxMode::Type NestedTxMode::FromString(const std::string& str, Type dflt)
        {
            std::string lower = common::ToLower(str);

            if (lower == "commit")
                return AI_COMMIT;

            if (lower == "ignore")
                return AI_IGNORE;

            if (lower == "error")
                return AI_ERROR;

            return dflt;
        }

        std::string NestedTxMode::ToString(Type value)
        {
            switch (value)
            {
                case AI_COMMIT:
                    return "commit";

                case AI_IGNORE:
                    return "ignore";

                case AI_ERROR:
                    return "error";

                default:
                    break;
            }

            return "default";
        }

        const NestedTxMode::ModeSet& NestedTxMode::GetValidValues()
        {
            return validValues;
        }
    }
}

