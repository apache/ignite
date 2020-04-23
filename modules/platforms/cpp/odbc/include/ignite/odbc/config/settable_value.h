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

#ifndef _IGNITE_ODBC_CONFIG_SETTABLE_VALUE
#define _IGNITE_ODBC_CONFIG_SETTABLE_VALUE

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * Simple abstraction for value, that have default value but can be
             * set to a different value.
             *
             * @tparam T Type of the value.
             */
            template<typename T>
            class SettableValue
            {
            public:
                /** Type of the value. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param defaultValue Default value to return when is not set.
                 */
                SettableValue(const ValueType& defaultValue) :
                    val(defaultValue),
                    set(false)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~SettableValue()
                {
                    // No-op.
                }
                
                /**
                 * Set non-default value.
                 *
                 * @param value Value.
                 * @param dflt Set value as default or not.
                 */
                void SetValue(const ValueType& value, bool dflt = false)
                {
                    val = value;
                    set = !dflt;
                }

                /**
                 * Get value.
                 *
                 * @return Value or default value if not set.
                 */
                const ValueType& GetValue() const
                {
                    return val;
                }

                /**
                 * Check whether value is set to non-default.
                 */
                bool IsSet() const
                {
                    return set;
                }

            private:
                /** Current value. */
                ValueType val;

                /** Flag showing whether value was set to non-default value. */
                bool set;
            };
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_SETTABLE_VALUE