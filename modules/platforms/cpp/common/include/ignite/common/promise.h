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
 * Declares ignite::commom::Promise class template.
 */


#ifndef _IGNITE_PROMISE
#define _IGNITE_PROMISE

#include <ignite/common/common.h>
#include <ignite/common/shared_state.h>

#include <ignite/ignite_error.h>
#include <ignite/future.h>

namespace ignite
{
    namespace common
    {
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

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                state.Get()->SetCancelTarget(target);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Promise);

            /** Shared state. */
            concurrent::SharedPointer< SharedState<ValueType> > state;
        };

        /**
         * Specialization for void.
         */
        template<>
        class Promise<void>
        {
        public:
            /** Template value type */
            typedef void ValueType;

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
             * Mark as complete.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             */
            void SetValue()
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                return state.Get()->SetValue();
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

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                state.Get()->SetCancelTarget(target);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Promise);

            /** Shared state. */
            concurrent::SharedPointer< SharedState<ValueType> > state;
        };
    }
}

#endif //_IGNITE_PROMISE
