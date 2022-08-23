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
 * Declares ignite::cache::query::WritableObject class template and
 * ignite::cache::query::WritableObjectBase interface.
 */

#ifndef _IGNITE_IMPL_WRITABLE_OBJECT
#define _IGNITE_IMPL_WRITABLE_OBJECT

#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Base class for all writable objects.
         */
        class WritableObjectBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~WritableObjectBase()
            {
                // No-op.
            }

            /**
             * Copy argument.
             *
             * @return Copy of this argument instance.
             */
            virtual WritableObjectBase* Copy() const = 0;

            /**
             * Write argument using provided writer.
             *
             * @param writer Writer to use to write this argument.
             */
            virtual void Write(ignite::binary::BinaryRawWriter& writer) = 0;
        };

        /**
         * Writable object class template.
         *
         * Template argument type should be copy-constructable and
         * assignable. Also BinaryType class template should be specialized
         * for this type.
         */
        template<typename T>
        class WritableObject : public WritableObjectBase
        {
        public:
            /**
             * Constructor.
             *
             * @param val Value.
             */
            WritableObject(const T& val) :
                val(val)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            WritableObject(const WritableObject& other) :
                val(other.val)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return *this.
             */
            WritableObject& operator=(const WritableObject& other)
            {
                if (this != &other)
                    val = other.val;

                return *this;
            }

            virtual ~WritableObject()
            {
                // No-op.
            }

            virtual WritableObjectBase* Copy() const
            {
                return new WritableObject(val);
            }

            virtual void Write(ignite::binary::BinaryRawWriter& writer)
            {
                writer.WriteObject<T>(val);
            }

        private:
            /** Value. */
            T val;
        };

        /**
         * Writable object specialization for the bytes array.
         */
        class WritableObjectInt8Array : public WritableObjectBase
        {
        public:
            /**
             * Constructor.
             *
             * @param src Array.
             * @param len Array length.
             */
            WritableObjectInt8Array(const int8_t* src, int32_t len) :
                val(src, src + len)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            WritableObjectInt8Array(const WritableObjectInt8Array& other) :
                val(other.val)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return *this.
             */
            WritableObjectInt8Array& operator=(const WritableObjectInt8Array& other)
            {
                if (this != &other)
                    val = other.val;

                return *this;
            }

            virtual ~WritableObjectInt8Array()
            {
                // No-op.
            }

            virtual WritableObjectBase* Copy() const
            {
                return new WritableObjectInt8Array(*this);
            }

            virtual void Write(ignite::binary::BinaryRawWriter& writer)
            {
                writer.WriteInt8Array(&val[0], static_cast<int32_t>(val.size()));
            }

        private:
            /** Value. */
            std::vector<int8_t> val;
        };
    }
}

#endif //_IGNITE_IMPL_WRITABLE_OBJECT