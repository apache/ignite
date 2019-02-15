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