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
 * Declares ignite::commom::SharedState class template.
 */

#ifndef _IGNITE_SHARED_STATE
#define _IGNITE_SHARED_STATE

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
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
            bool IsSet()
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
                concurrent::CsLockGuard guard(writeLock);

                if (IsSet())
                {
                    if (value.get())
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                value = val;

                evt.Set();
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                concurrent::CsLockGuard guard(writeLock);

                if (IsSet())
                {
                    if (value.get())
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                error = err;

                evt.Set();
            }

            /**
             * Wait for value to be set.
             * Active thread will be blocked until value or error will be set.
             */
            void Wait() const
            {
                evt.Wait();
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
                return evt.WaitFor(msTimeout);
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

        private:
            IGNITE_NO_COPY_ASSIGNMENT(SharedState);

            /** Value. */
            std::auto_ptr<ValueType> value;

            /** Error. */
            IgniteError error;

            /** Event which serves to signal that value is set. */
            mutable concurrent::ManualEvent evt;

            /** Lock that used to prevent double-set of the value. */
            mutable concurrent::CriticalSection writeLock;
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
            bool IsSet()
            {
                return done || error.GetCode() != IgniteError::IGNITE_SUCCESS;
            }

            /**
             * Mark as complete.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             */
            void SetValue()
            {
                concurrent::CsLockGuard guard(writeLock);

                if (IsSet())
                {
                    if (done)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                done = true;

                evt.Set();
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                concurrent::CsLockGuard guard(writeLock);

                if (IsSet())
                {
                    if (done)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future value already set");

                    if (error.GetCode() != IgniteError::IGNITE_SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE, "Future error already set");
                }

                error = err;

                evt.Set();
            }

            /**
             * Wait for value to be set.
             * Active thread will be blocked until value or error will be set.
             */
            void Wait() const
            {
                evt.Wait();
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
                return evt.WaitFor(msTimeout);
            }

            /**
             * Wait for operation complition or error.
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

        private:
            IGNITE_NO_COPY_ASSIGNMENT(SharedState);

            /** Completion flag. */
            bool done;

            /** Error. */
            IgniteError error;

            /** Event which serves to signal that value is set. */
            mutable concurrent::ManualEvent evt;

            /** Lock that used to prevent double-set of the value. */
            mutable concurrent::CriticalSection writeLock;
        };
    }
}

#endif //_IGNITE_SHARED_STATE
