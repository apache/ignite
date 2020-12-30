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

#ifndef _IGNITE_IMPL_THIN_COPYABLE_WRITABLE
#define _IGNITE_IMPL_THIN_COPYABLE_WRITABLE

#include <ignite/impl/thin/copyable.h>
#include <ignite/impl/thin/writable.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Copyable Writable value.
             */
            class CopyableWritable : public Writable, Copyable<CopyableWritable>
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~CopyableWritable()
                {
                    // No-op.
                }

                /**
                 * Copy value.
                 *
                 * @return A copy of the object.
                 */
                virtual CopyableWritable* Copy() const = 0;

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const = 0;
            };

            /**
             * Copyable Writable value implementation for a concrete type.
             *
             * @tparam T Value type.
             */
            template<typename T>
            class CopyableWritableImpl : public CopyableWritable
            {
            public:
                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                CopyableWritableImpl(const ValueType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CopyableWritableImpl()
                {
                    // No-op.
                }

                /**
                 * Copy value.
                 *
                 * @return A copy of the object.
                 */
                virtual CopyableWritable* Copy() const
                {
                    return new CopyableWritableImpl(value);
                }

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    writer.WriteObject(value);
                }

            private:
                /** Value. */
                ValueType value;
            };

            /**
             * Copyable Writable value implementation for int8_t array type.
             *
             * @tparam Iter Iterator type.
             */
            template<typename Iter>
            class CopyableWritableInt8ArrayImpl : public CopyableWritable
            {
            public:
                /** Iterator type. */
                typedef Iter IteratorType;

                /**
                 * Constructor.
                 *
                 * @param begin Begin iterator.
                 * @param end End iterator.
                 */
                CopyableWritableInt8ArrayImpl(IteratorType begin, IteratorType end) :
                    values(begin, end)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CopyableWritableInt8ArrayImpl()
                {
                    // No-op.
                }

                /**
                 * Copy value.
                 *
                 * @return A copy of the object.
                 */
                virtual CopyableWritable* Copy() const
                {
                    return new CopyableWritableInt8ArrayImpl(values.begin(), values.end());
                }

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    writer.WriteInt8Array(&values[0], static_cast<int32_t>(values.size()));
                }

            private:
                /** Value. */
                std::vector<int8_t> values;
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_COPYABLE_WRITABLE
