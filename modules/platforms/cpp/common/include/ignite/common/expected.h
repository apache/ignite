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

#include <memory>

#include <ignite/common/utils.h>

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
            typename AR = std::allocator<R>,
            typename AE = std::allocator<E> >
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

                ral.construct(AsResult(), res);
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

                ral.construct(AsError(), err.err);
            }

            /**
             * Copy constructor.
             *
             * @param other Other.
             */
            Expected(const Expected& other) :
                ok(other.ok)
            {
                if (ok)
                {
                    ResultAllocatorType ral;

                    ral.construct(AsResult(), *other.AsResult());
                }
                else
                {
                    ErrorAllocatorType ral;

                    ral.construct(AsError(), *other.AsError());
                }
            }

            /**
             * Destructor.
             */
            ~Expected()
            {
                if (ok)
                {
                    ResultAllocatorType ral;

                    ral.destroy(AsResult());
                }
                else
                {
                    ErrorAllocatorType ral;

                    ral.destroy(AsError());
                }
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
             * Get result. Constant accesser.
             *
             * @return Result if it was set before.
             * @throw ErrorType if there is no result.
             */
            const ResultType& GetResult() const
            {
                if (!ok)
                    throw *AsError();

                return *AsResult();
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
                    throw *AsError();

                return *AsResult();
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
            const ErrorType& GetError() const
            {
                static ErrorType noError;

                if (ok)
                    return noError;

                return *AsError();
            }

        private:
            /**
             * Get storage as an result.
             *
             * @return Storage pointer as an result pointer.
             */
            ResultType* AsResult()
            {
                return reinterpret_cast<ResultType*>(&storage);
            }

            /**
             * Get storage as an result.
             *
             * @return Storage pointer as an result pointer.
             */
            const ResultType* AsResult() const
            {
                return reinterpret_cast<const ResultType*>(&storage);
            }

            /**
             * Get storage as an error.
             *
             * @return Storage pointer as an error pointer.
             */
            ErrorType* AsError()
            {
                return reinterpret_cast<ErrorType*>(&storage);
            }

            /**
             * Get storage as an error.
             *
             * @return Storage pointer as an error pointer.
             */
            const ErrorType* AsError() const
            {
                return reinterpret_cast<const ErrorType*>(&storage);
            }

            /** Storage. */
            int8_t storage[sizeof(typename Bigger<ResultType, ErrorType>::type)];

            /** Result flag. Set to @c false if the value is an error. */
            bool ok;
        };
    }
}

#endif // _IGNITE_COMMON_EXPECTED