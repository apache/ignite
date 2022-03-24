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

#include "ignite/odbc/engine_mode.h"
#include "ignite/common/utils.h"

namespace
{
    using ignite::odbc::EngineMode;
    EngineMode::Type validValues0[] = {
            EngineMode::DEFAULT,
            EngineMode::H2,
            EngineMode::CALCITE
    };
}

namespace ignite
{
    namespace odbc
    {
        EngineMode::Type EngineMode::FromString(const std::string &val, Type dflt)
        {
            std::string lowerVal = common::ToLower(val);

            common::StripSurroundingWhitespaces(lowerVal);

            if (lowerVal == "h2")
                return EngineMode::H2;

            if (lowerVal == "calcite")
                return EngineMode::CALCITE;

            if (lowerVal == "default")
                return EngineMode::DEFAULT;

            return dflt;
        }

        std::string EngineMode::ToString(Type val)
        {
            switch (val)
            {
                case EngineMode::H2:
                    return "h2";

                case EngineMode::CALCITE:
                    return "calcite";

                case EngineMode::DEFAULT:
                    return "default";

                default:
                    return "unknown";
            }
        }

        const EngineMode::ModeSet& EngineMode::GetValidValues() {
            static EngineMode::ModeSet validValues(validValues0,
                                                   validValues0 + (sizeof(validValues0) / sizeof(validValues0[0])));
            return validValues;
        }
    }
}
