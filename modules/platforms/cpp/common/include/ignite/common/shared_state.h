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

/**
 * @file
 * Declares ignite::commom::SharedState class template.
 */

#ifndef _IGNITE_SHARED_STATE
#define _IGNITE_SHARED_STATE

#include <memory>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/common/cancelable.h>
#include <ignite/ignite_error.h>

namespace ignite
{
    namespace common
    {
        template<typename T>
        class SharedState
        {
        public:
            /** Template value type */
            typedef T ValueType;

            /**
             * Default constructor.
             * Constructs non-set SharedState instance.
             */
            SharedState() :
                value(),
                error()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~SharedState()
            {
                // No-op.
            }

            /**
             * Checks if the value or error set for the state.
             * @return True if the value or error set for the state.
             */
            bool IsSet() const
            {
                return value.get() || error.GetCode() != IgniteError::IGNITE_SUCCESS;
            }

            /**
             * Set value.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param val Value to set.
             */
            void SetValue(std::auto_ptr<ValueType> val)
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                {
                    if (value.get())
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                value = val;

                cond.NotifyAll();
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                {
                    if (value.get())
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                error = err;

                cond.NotifyAll();
            }

            /**
             * Wait for value to be set.
             * Active thread will be blocked until value or error will be set.
             */
            void Wait() const
            {
                concurrent::CsLockGuard guard(mutex);

                while (!IsSet())
                    cond.Wait(mutex);
            }

            /**
             * Wait for value to be set for specified time.
             * Active thread will be blocked until value or error will be set or timeout will end.
             *
             * @param msTimeout Timeout in milliseconds.
             * @return True if the object has been triggered and false in case of timeout.
             */
            bool WaitFor(int32_t msTimeout) const
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                    return true;

                return cond.WaitFor(mutex, msTimeout);
            }

            /**
             * Get the set value.
             * Active thread will be blocked until value or error will be set.
             *
             * @throw IgniteError if error has been set.
             * @return Value that has been set on success.
             */
            const ValueType& GetValue() const
            {
                Wait();

                if (value.get())
                    return *value;

                assert(error.GetCode() != IgniteError::IGNITE_SUCCESS);

                throw error;
            }

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                concurrent::CsLockGuard guard(mutex);

                cancelTarget = target;
            }

            /**
             * Cancel related operation.
             */
            void Cancel()
            {
                concurrent::CsLockGuard guard(mutex);

                if (cancelTarget.get())
                    cancelTarget->Cancel();
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(SharedState);

            /** Cancel target. */
            std::auto_ptr<Cancelable> cancelTarget;

            /** Value. */
            std::auto_ptr<ValueType> value;

            /** Error. */
            IgniteError error;

            /** Condition variable which serves to signal that value is set. */
            mutable concurrent::ConditionVariable cond;

            /** Lock that used to prevent double-set of the value. */
            mutable concurrent::CriticalSection mutex;
        };

        /**
         * Specialization for void type.
         */
        template<>
        class SharedState<void>
        {
        public:
            /** Template value type */
            typedef void ValueType;

            /**
             * Default constructor.
             * Constructs non-set SharedState instance.
             */
            SharedState() :
                done(false),
                error()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~SharedState()
            {
                // No-op.
            }

            /**
             * Checks if the value or error set for the state.
             * @return True if the value or error set for the state.
             */
            bool IsSet() const
            {
                return done || error.GetCode() != IgniteError::IGNITE_SUCCESS;
            }

            /**
             * Set value.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             */
            void SetValue()
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                {
                    if (done)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                done = true;

                cond.NotifyAll();
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                {
                    if (done)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                error = err;

                cond.NotifyAll();
            }

            /**
             * Wait for value to be set.
             * Active thread will be blocked until value or error will be set.
             */
            void Wait() const
            {
                concurrent::CsLockGuard guard(mutex);

                while (!IsSet())
                    cond.Wait(mutex);
            }

            /**
             * Wait for value to be set for specified time.
             * Active thread will be blocked until value or error will be set or timeout will end.
             *
             * @param msTimeout Timeout in milliseconds.
             * @return True if the object has been triggered and false in case of timeout.
             */
            bool WaitFor(int32_t msTimeout) const
            {
                concurrent::CsLockGuard guard(mutex);

                if (IsSet())
                    return true;

                return cond.WaitFor(mutex, msTimeout);
            }

            /**
             * Get the set value.
             * Active thread will be blocked until value or error will be set.
             *
             * @throw IgniteError if error has been set.
             */
            void GetValue() const
            {
                Wait();

                if (done)
                    return;

                assert(error.GetCode() != IgniteError::IGNITE_SUCCESS);

                throw error;
            }

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                concurrent::CsLockGuard guard(mutex);

                cancelTarget = target;
            }

            /**
             * Cancel related operation.
             */
            void Cancel()
            {
                concurrent::CsLockGuard guard(mutex);

                if (cancelTarget.get())
                    cancelTarget->Cancel();
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(SharedState);

            /** Cancel target. */
            std::auto_ptr<Cancelable> cancelTarget;

            /** Marker. */
            bool done;

            /** Error. */
            IgniteError error;

            /** Condition variable which serves to signal that value is set. */
            mutable concurrent::ConditionVariable cond;

            /** Lock that used to prevent double-set of the value. */
            mutable concurrent::CriticalSection mutex;
        };
    }
}

#endif //_IGNITE_SHARED_STATE
