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

#ifndef _IGNITE_IMPL_THIN_WRITABLE_KEY
#define _IGNITE_IMPL_THIN_WRITABLE_KEY

#include <ignite/common/decimal.h>

#include <ignite/impl/binary/binary_writer_impl.h>

#include <ignite/impl/thin/writable.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Abstraction to any type that can be written to a binary stream.
             */
            class WritableKey : public Writable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~WritableKey()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const = 0;
            };

            /**
             * Implementation of the Writable class for a user type.
             */
            template<typename T>
            class WritableKeyImpl : public WritableKey
            {
            public:
                enum { BUFFER_SIZE = 1024 };

                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const ValueType& value) :
                    value(value),
                    mem(BUFFER_SIZE)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
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
                    if (mem.Length() == 0)
                    {
                        writer.WriteObject(value);
                    }
                    else
                    {
                        interop::InteropOutputStream *stream = writer.GetStream();

                        stream->WriteInt8Array(mem.Data(), mem.Length());
                    }
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    if (mem.Length() == 0)
                    {
                        interop::InteropOutputStream stream(&mem);
                        binary::BinaryWriterImpl writer(&stream, 0);
                        
                        writer.WriteObject(value);

                        stream.Synchronize();
                    }

                    binary::BinaryObjectImpl binaryKey(mem, 0, 0, 0);

                    return binaryKey.GetHashCode();
                }

            private:
                /** Value. */
                const ValueType& value;

                /** Memory. */
                mutable interop::InteropUnpooledMemory mem;
            };

            /**
             * Implementation of the Writable class for an basic type.
             */
            template<typename T>
            class WritableBasicKeyImpl : public WritableKey
            {
            public:
                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableBasicKeyImpl(const ValueType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableBasicKeyImpl()
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

            protected:
                /** Value. */
                const ValueType& value;
            };

            /**
             * Specializatoin for int8_t type.
             */
            template<>
            class WritableKeyImpl<int8_t> : public WritableBasicKeyImpl<int8_t>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const int8_t& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return static_cast<int32_t>(value);
                }
            };

            /**
             * Specializatoin for int16_t type.
             */
            template<>
            class WritableKeyImpl<int16_t> : public WritableBasicKeyImpl<int16_t>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const int16_t& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return static_cast<int32_t>(value);
                }
            };

            /**
             * Specializatoin for int32_t type.
             */
            template<>
            class WritableKeyImpl<int32_t> : public WritableBasicKeyImpl<int32_t>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const int32_t& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return value;
                }
            };

            /**
             * Specializatoin for uint16_t type.
             */
            template<>
            class WritableKeyImpl<uint16_t> : public WritableBasicKeyImpl<uint16_t>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const uint16_t& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return static_cast<int32_t>(value);
                }
            };

            /**
             * Specializatoin for boolean type.
             */
            template<>
            class WritableKeyImpl<bool> : public WritableBasicKeyImpl<bool>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const bool& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return value ? 1231 : 1237;
                }
            };

            /**
             * Specializatoin for int64_t type.
             */
            template<>
            class WritableKeyImpl<int64_t> : public WritableBasicKeyImpl<int64_t>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const int64_t& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    return static_cast<int32_t>(value ^ ((value >> 32) & 0xFFFFFFFF));
                }
            };

            /**
             * Specializatoin for float type.
             */
            template<>
            class WritableKeyImpl<float> : public WritableBasicKeyImpl<float>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const float& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    const int32_t *res = reinterpret_cast<const int32_t*>(&value);

                    return *res;
                }
            };

            /**
             * Specializatoin for double type.
             */
            template<>
            class WritableKeyImpl<double> : public WritableBasicKeyImpl<double>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const double& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    const int64_t *res = reinterpret_cast<const int64_t*>(&value);
                    
                    return static_cast<int32_t>(*res ^ ((*res >> 32) & 0xFFFFFFFF));
                }
            };

            /**
             * Specializatoin for std::string type.
             */
            template<>
            class WritableKeyImpl<std::string> : public WritableBasicKeyImpl<std::string>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const std::string& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    int32_t hash = 0;

                    for (size_t i = 0; i < value.size(); ++i)
                        hash = 31 * hash + value[i];

                    return hash;
                }
            };

            /**
             * Specializatoin for Guid type.
             */
            template<>
            class WritableKeyImpl<Guid> : public WritableBasicKeyImpl<Guid>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const Guid& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    int64_t hilo = value.GetMostSignificantBits() ^ value.GetLeastSignificantBits();
                    
                    return static_cast<int32_t>(hilo ^ ((hilo >> 32) & 0xFFFFFFFF));
                }
            };

            /**
             * Specializatoin for Date type.
             */
            template<>
            class WritableKeyImpl<Date> : public WritableBasicKeyImpl<Date>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const Date& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    int64_t ht = value.GetMilliseconds();
                    
                    return static_cast<int32_t>(ht ^ ((ht >> 32) & 0xFFFFFFFF));
                }
            };

            /**
             * Specializatoin for Timestamp type.
             */
            template<>
            class WritableKeyImpl<Timestamp> : public WritableBasicKeyImpl<Timestamp>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const Timestamp& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    int64_t ht = value.GetMilliseconds();
                    
                    return static_cast<int32_t>(ht ^ ((ht >> 32) & 0xFFFFFFFF));
                }
            };

            /**
             * Specializatoin for Time type.
             */
            template<>
            class WritableKeyImpl<Time> : public WritableBasicKeyImpl<Time>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableKeyImpl(const Time& value) :
                    WritableBasicKeyImpl(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableKeyImpl()
                {
                    // No-op.
                }

                /**
                 * Get hash code of the value.
                 *
                 * @return Hash code of the value.
                 */
                virtual int32_t GetHashCode() const
                {
                    int64_t ht = value.GetMilliseconds();
                    
                    return static_cast<int32_t>(ht ^ ((ht >> 32) & 0xFFFFFFFF));
                }
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_WRITABLE_KEY
