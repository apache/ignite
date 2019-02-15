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
 * Declares ignite::Future class template.
 */


#ifndef _IGNITE_FUTURE
#define _IGNITE_FUTURE

#include <ignite/common/shared_state.h>
#include <ignite/ignite_error.h>

namespace ignite
{
    namespace common
    {
        // Forward declaration
        template<typename T>
        class Promise;
    }

    /**
     * Future class template. Used to get result of the asynchroniously
     * started computation.
     *
     * @tparam T Future value type.
     */
    template<typename T>
    class Future
    {
        friend class common::Promise<T>;

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
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state0->Wait();
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
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state0->WaitFor(msTimeout);
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
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state0->GetValue();
        }

        /**
         * Cancel related operation.
         */
        void Cancel()
        {
            common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state0->Cancel();
        }

        /**
         * Check if the future ready.
         */
        bool IsReady()
        {
            common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state0->IsSet();
        }

    private:
        /**
         * Constructor.
         *
         * @param state0 Shared state instance.
         */
        Future(common::concurrent::SharedPointer< common::SharedState<ValueType> > state0) :
            state(state0)
        {
            // No-op.
        }

        /** Shared state. */
        common::concurrent::SharedPointer< common::SharedState<ValueType> > state;
    };

    /**
     * Specialization for void type.
     */
    template<>
    class Future<void>
    {
        friend class common::Promise<void>;

    public:
        /** Template value type */
        typedef void ValueType;

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
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state0->Wait();
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
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state0->WaitFor(msTimeout);
        }

        /**
         * Wait for operation complition or error.
         * Active thread will be blocked until value or error will be set.
         *
         * @throw IgniteError if error has been set.
         */
        void GetValue() const
        {
            const common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state0->GetValue();
        }

        /**
         * Cancel related operation.
         */
        void Cancel()
        {
            common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            state0->Cancel();
        }

        /**
         * Check if the future ready.
         */
        bool IsReady()
        {
            common::SharedState<ValueType>* state0 = state.Get();

            assert(state0 != 0);

            return state0->IsSet();
        }

    private:
        /**
         * Constructor.
         *
         * @param state0 Shared state instance.
         */
        Future(common::concurrent::SharedPointer< common::SharedState<ValueType> > state0) :
            state(state0)
        {
            // No-op.
        }

        /** Shared state. */
        common::concurrent::SharedPointer< common::SharedState<ValueType> > state;
    };
}

#endif //_IGNITE_FUTURE
