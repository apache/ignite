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
 * Declares ignite::Future class.
 */


#ifndef _IGNITE_FUTURE
#define _IGNITE_FUTURE

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

namespace ignite
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
            return value || error.GetCode() != IgniteError::IGNITE_SUCCESS;
        }

        /**
         * Set value.
         *
         * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
         * @param val Value to set.
         */
        void SetValue(std::auto_ptr<ValueType> val)
        {
            common::concurrent::CsLockGuard guard(writeLock);

            if (IsSet())
            {
                if (value)
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
            common::concurrent::CsLockGuard guard(writeLock);

            if (IsSet())
            {
                if (value)
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

            if (value)
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
        mutable common::concurrent::ManualEvent evt;

        /** Lock that used to prevent double-set of the value. */
        mutable common::concurrent::CriticalSection writeLock;
    };

    // Forward declaration
    template<typename T>
    class Promise;

    /**
     * Future class template. Used to get result of the asynchroniously
     * started computation.
     *
     * @tparam T Future value type.
     */
    template<typename T>
    class Future
    {
        friend class Promise<T>;

    public:
        /** Template value type */
        typedef T ValueType;

        /**
         * Copy constructor.
         *
         * @param src Instance to copy.
         */
        Future(const Future<ValueType>& src) :
            state(src.state)
        {
            // No-op.
        }

        /**
         * Assignment operator.
         *
         * @param other Other instance.
         * @return *this.
         */
        Future& operator=(const Future<ValueType>& other)
        {
            state = other.state;

            return *this;
        }

        /**
         * Wait for value to be set.
         * Active thread will be blocked until value or error will be set.
         */
        void Wait() const
        {
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state.Get()->Wait();
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
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state.Get()->WaitFor(msTimeout);
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
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state.Get()->GetValue();
        }

    private:
        /**
         * Constructor.
         *
         * @param state0 Shared state instance.
         */
        Future(common::concurrent::SharedPointer< SharedState<ValueType> > state0) :
            state(state0)
        {
            // No-op.
        }

        /** Shared state. */
        common::concurrent::SharedPointer< SharedState<ValueType> > state;
    };

    /**
     * Promise class template. Used to set result of the asynchroniously
     * started computation.
     *
     * @tparam T Promised value type.
     */
    template<typename T>
    class Promise
    {
    public:
        /** Template value type */
        typedef T ValueType;

        /**
         * Constructor.
         */
        Promise() :
            state(new SharedState<ValueType>())
        {
            // No-op.
        }

        /**
         * Destructor.
         */
        ~Promise()
        {
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            if (!state0->IsSet())
                state0->SetError(IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE,
                    "Broken promise. Value will never be set due to internal error."));
        }


        /**
         * Get future for this promise.
         *
         * @return New future instance.
         */
        Future<ValueType> GetFuture() const
        {
            return Future<ValueType>(state);
        }

        /**
         * Set value.
         *
         * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
         * @param val Value to set.
         */
        void SetValue(std::auto_ptr<ValueType> val)
        {
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state.Get()->SetValue(val);
        }

        /**
         * Set error.
         *
         * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
         * @param err Error to set.
         */
        void SetError(const IgniteError& err)
        {
            SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state.Get()->SetError(err);
        }

    private:
        IGNITE_NO_COPY_ASSIGNMENT(Promise);

        common::concurrent::SharedPointer< SharedState<ValueType> > state;
    };
}

#endif //_IGNITE_FUTURE
