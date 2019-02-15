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

#ifndef _IGNITE_IMPL_THIN_WRITABLE
#define _IGNITE_IMPL_THIN_WRITABLE

#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Abstraction to any type that can be written to a binary stream.
             */
            class Writable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Writable()
                {
                    // No-op.
                }

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const = 0;
            };

            /**
             * Implementation of the Writable class for a concrete type.
             *
             * @tparam T Value type.
             */
            template<typename T>
            class WritableImpl : public Writable
            {
            public:
                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableImpl(const ValueType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableImpl()
                {
                    // No-op.
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
                const ValueType& value;
            };

            /**
             * Implementation of the Writable class for a set of values.
             *
             * @tparam T Value type.
             * @tparam I Iterator type.
             */
            template<typename T, typename I>
            class WritableSetImpl : public Writable
            {
            public:
                /** Element type. */
                typedef T ElementType;

                /** Iterator type. */
                typedef I IteratorType;

                /**
                 * Constructor.
                 *
                 * @param begin Begin of the sequence.
                 * @param end Sequence end.
                 */
                WritableSetImpl(IteratorType begin, IteratorType end) :
                    begin(begin),
                    end(end)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableSetImpl()
                {
                    // No-op.
                }

                /**
                 * Write sequence using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    using namespace ignite::binary;

                    interop::InteropOutputStream* out = writer.GetStream();

                    int32_t cntPos = out->Reserve(4);

                    out->Synchronize();

                    int32_t cnt = 0;
                    for (IteratorType it = begin; it != end; ++it)
                    {
                        writer.WriteObject(*it);

                        ++cnt;
                    }

                    out->WriteInt32(cntPos, cnt);

                    out->Synchronize();
                }

            private:
                /** Sequence begin. */
                IteratorType begin;

                /** Sequence end. */
                IteratorType end;
            };

            /**
             * Implementation of the Writable class for a map.
             *
             * @tparam K Key type.
             * @tparam V Value type.
             * @tparam I Iterator type.
             */
            template<typename K, typename V, typename I>
            class WritableMapImpl : public Writable
            {
            public:
                /** Key type. */
                typedef K KeyType;

                /** Value type. */
                typedef V ValueType;

                /** Iterator type. */
                typedef I IteratorType;

                /**
                 * Constructor.
                 *
                 * @param begin Begin of the sequence.
                 * @param end Sequence end.
                 */
                WritableMapImpl(IteratorType begin, IteratorType end) :
                    begin(begin),
                    end(end)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableMapImpl()
                {
                    // No-op.
                }

                /**
                 * Write sequence using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    using namespace ignite::binary;

                    interop::InteropOutputStream* out = writer.GetStream();

                    int32_t cntPos = out->Reserve(4);

                    out->Synchronize();

                    int32_t cnt = 0;
                    for (IteratorType it = begin; it != end; ++it)
                    {
                        writer.WriteObject(it->first);
                        writer.WriteObject(it->second);

                        ++cnt;
                    }

                    out->WriteInt32(cntPos, cnt);

                    out->Synchronize();
                }

            private:
                /** Sequence begin. */
                IteratorType begin;

                /** Sequence end. */
                IteratorType end;
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_WRITABLE
