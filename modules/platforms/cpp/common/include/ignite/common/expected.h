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

#ifndef _IGNITE_COMMON_EXPECTED
#define _IGNITE_COMMON_EXPECTED

#include <ignite/common/default_allocator.h>

namespace ignite
{
    namespace common
    {
        /**
         * Helper class to construct Expected class with error value.
         */
        template<typename E>
        struct Unexpected
        {
            /** Value type. */
            typedef E ValueType;

            /**
             * Constructor.
             *
             * @param e Error value reference.
             */
            Unexpected(const ValueType& e) : err(e)
            {
                // No-op;
            }

            /** Error. */
            const ValueType& err;
        };

        /**
         * Operation result wrapper.
         *
         * Represents a type, which can accept one of two value types - expected
         * result or error.
         *
         * @tparam R Result type.
         * @tparam E Error type.
         * @tparam AR Allocator type used for the Result type.
         * @tparam AE Allocator type used for the Error type.
         */
        template<
            typename R,
            typename E,
            typename AR = DefaultAllocator<R>,
            typename AE = DefaultAllocator<E> >
        class Expected
        {
        public:
            /** Result type. */
            typedef R ResultType;

            /** Error type. */
            typedef E ErrorType;

            /** Allocator type used for the ResultType. */
            typedef AR ResultAllocatorType;

            /** Allocator type used for the ErrorType. */
            typedef AE ErrorAllocatorType;

            /**
             * Constructor.
             *
             * Creates new instance, containing expected value.
             * @param res Result.
             */
            Expected(const ResultType& res) :
                ok(true)
            {
                ResultAllocatorType ral;

                ral.construct(&value.res, res);
            }

            /**
             * Constructor.
             *
             * Creates new instance, containing error.
             * @param err Result.
             */
            explicit Expected(Unexpected<ErrorType> err) :
                ok(false)
            {
                ErrorAllocatorType ral;

                ral.construct(&value.err, err.err);
            }

            /**
             * Check if the value is OK.
             *
             * @return @c false if the value is an error and @c true otherwise.
             */
            bool IsOk() const
            {
                return ok;
            }

            /**
             * Bool cast operator.
             *
             * Act the same as @c IsOk() method.
             * Can be used for checks like if (expected) {...}.
             *
             * @return @c false if the value is an error and @c true otherwise.
             */
            operator bool() const
            {
                return IsOk();
            }

            /**
             * Get result. Constant accesser.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            const ResultType& GetResult() const
            {
                if (!ok)
                    throw value.err;

                return value.res;
            }

            /**
             * Get result.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            ResultType& GetResult()
            {
                if (!ok)
                    throw value.err;

                return value.res;
            }

            /**
             * Get result. Constant accesser.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            const ResultType& operator*() const
            {
                return GetResult();
            }

            /**
             * Get result.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            ResultType& operator*()
            {
                return GetResult();
            }

            /**
             * Get result. Constant accesser.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            const ResultType& operator->() const
            {
                return GetResult();
            }

            /**
             * Get result.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            ResultType& operator->()
            {
                return GetResult();
            }

            /**
             * Get error.
             *
             * @return Error if it was set before. If there is no error, default
             * constructed error is returned (which is expected to be "No error").
             */
            const ResultType& GetError() const
            {
                static ErrorType noError;

                if (ok)
                    return noError;

                return value.err;
            }

        private:
            /**
             * Storage type. Can store either result or error, but not both.
             * Used to pack data.
             */
            union StorageType
            {
                /** Result. */
                ResultType res;

                /** Error. */
                ErrorType err;
            };

            /** Stored value. */
            StorageType value;

            /** Result flag. Set to @c false if the value is an error. */
            bool ok;
        };
    }
}

#endif // _IGNITE_COMMON_EXPECTED