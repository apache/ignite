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

#ifndef _IGNITE_IMPL_OPERATIONS
#define _IGNITE_IMPL_OPERATIONS

#include <map>
#include <set>
#include <vector>

#include <ignite/common/common.h>

#include "ignite/cache/cache_entry.h"
#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/binary/binary.h"

namespace ignite
{
    namespace impl
    {
        /**
         * Input operation.
         */
        class InputOperation
        {
        public:
            /**
             * Destructor.
             */
            virtual ~InputOperation()
            {
                // No-op.
            }

            /**
             * Process input.
             *
             * @param writer Writer.
             */
            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer) = 0;
        };

        /**
         * Input operation accepting a single argument.
         */
        template<typename T>
        class In1Operation : public InputOperation
        {
        public:
            /**
             * Constructor.
             * 
             * @param val Value.
             */
            In1Operation(const T* val) : val(val)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.WriteTopObject<T>(*val);
            }
        private:
            /** Value. */
            const T* val; 

            IGNITE_NO_COPY_ASSIGNMENT(In1Operation)
        };

        /**
         * Input operation accepting two single objects.
         */
        template<typename T1, typename T2>
        class In2Operation : public InputOperation
        {
        public:
            /**
             * Constructor.
             *
             * @param val1 First value.
             * @param val2 Second value.
             */
            In2Operation(const T1* val1, const T2* val2) : val1(val1), val2(val2)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.WriteTopObject<T1>(*val1);
                writer.WriteTopObject<T2>(*val2);
            }
        private:
            /** First value. */
            const T1* val1; 

            /** Second value. */
            const T2* val2; 

            IGNITE_NO_COPY_ASSIGNMENT(In2Operation)
        };

        /**
         * Input operation accepting three single objects.
         */
        template<typename T1, typename T2, typename T3>
        class In3Operation : public InputOperation
        {
        public:
            /**
             * Constructor.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @param val3 Third value.
             */
            In3Operation(const T1* val1, const T2* val2, const T3* val3) : val1(val1), val2(val2), val3(val3)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.WriteTopObject<T1>(*val1);
                writer.WriteTopObject<T2>(*val2);
                writer.WriteTopObject<T3>(*val3);
            }
        private:
            /** First value. */
            const T1* val1;

            /** Second value. */
            const T2* val2;

            /** Third value. */
            const T3* val3;

            IGNITE_NO_COPY_ASSIGNMENT(In3Operation)
        };

        /**
         * Input set operation.
         */
        template<typename T>
        class InSetOperation : public InputOperation
        {
        public:
            /**
             * Constructor.
             *
             * @param val Value.
             */
            InSetOperation(const std::set<T>* val) : val(val)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.GetStream()->WriteInt32(static_cast<int32_t>(val->size()));

                for (typename std::set<T>::const_iterator it = val->begin(); it != val->end(); ++it)
                    writer.WriteTopObject<T>(*it);
            }
        private:
            /** Value. */
            const std::set<T>* val; 

            IGNITE_NO_COPY_ASSIGNMENT(InSetOperation)
        };

        /**
         * Input map operation.
         */
        template<typename K, typename V>
        class InMapOperation : public InputOperation
        {
        public:
            /**
             * Constructor.
             *
             * @param val Value.
             */
            InMapOperation(const std::map<K, V>* val) : val(val)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.GetStream()->WriteInt32(static_cast<int32_t>(val->size()));

                for (typename std::map<K, V>::const_iterator it = val->begin(); it != val->end(); ++it) {
                    writer.WriteTopObject<K>(it->first);
                    writer.WriteTopObject<V>(it->second);
                }
            }
        private:
            /** Value. */
            const std::map<K, V>* val; 

            IGNITE_NO_COPY_ASSIGNMENT(InMapOperation)
        };

        /**
         * Cache LocalPeek input operation.
         */
        template<typename T>
        class InCacheLocalPeekOperation : public InputOperation
        {
        public:
            /**
             * Constructor.
             *
             * @param key Key.
             * @param peekModes Peek modes.
             */
            InCacheLocalPeekOperation(const T* key, int32_t peekModes) : key(key), peekModes(peekModes)
            {
                // No-op.
            }

            virtual void ProcessInput(ignite::impl::binary::BinaryWriterImpl& writer)
            {
                writer.WriteTopObject<T>(*key);
                writer.GetStream()->WriteInt32(peekModes);
            }
        private:
            /** Key. */
            const T* key;   

            /** Peek modes. */
            int32_t peekModes; 

            IGNITE_NO_COPY_ASSIGNMENT(InCacheLocalPeekOperation)
        };

        /**
         * Output operation.
         */
        class OutputOperation
        {
        public:
            /**
             * Destructor.
             */
            virtual ~OutputOperation()
            {
                // No-op.
            }

            /**
             * Process output.
             *
             * @param reader Reader.
             */
            virtual void ProcessOutput(binary::BinaryReaderImpl& reader) = 0;

            /**
             * Sets result to null value.
             */
            virtual void SetNull() = 0;
        };

        /**
         * Output operation returning single object.
         */
        template<typename T>
        class Out1Operation : public OutputOperation
        {
        public:
            /**
             * Constructor.
             */
            Out1Operation()
            {
                // No-op.
            }

            virtual void ProcessOutput(binary::BinaryReaderImpl& reader)
            {
                val = reader.ReadTopObject<T>();
            }

            virtual void SetNull()
            {
                val = binary::BinaryUtils::GetDefaultValue<T>();
            }

            /**
             * Get value.
             *
             * @param Value.
             */
            T GetResult()
            {
                return val;
            }
        private:
            /** Value. */
            T val; 

            IGNITE_NO_COPY_ASSIGNMENT(Out1Operation)
        };

        /**
         * Output operation returning two objects.
         */
        template<typename T1, typename T2>
        class Out2Operation : public OutputOperation
        {
        public:
            /**
             * Constructor.
             */
            Out2Operation()
            {
                // No-op.
            }

            virtual void ProcessOutput(binary::BinaryReaderImpl& reader)
            {
                val1 = reader.ReadTopObject<T1>();
                val2 = reader.ReadTopObject<T2>();
            }

            virtual void SetNull()
            {
                val1 = binary::BinaryUtils::GetDefaultValue<T1>();
                val2 = binary::BinaryUtils::GetDefaultValue<T2>();
            }

            /**
             * Get value 1.
             *
             * @param Value 1.
             */
            T1& Get1()
            {
                return val1;
            }

            /**
             * Get value 2.
             *
             * @param Value 2.
             */
            T2& Get2()
            {
                return val2;
            }

        private:
            /** Value 1. */
            T1 val1; 
            
            /** Value 2. */
            T2 val2; 

            IGNITE_NO_COPY_ASSIGNMENT(Out2Operation)
        };

        /**
         * Output operation returning four objects.
         */
        template<typename T1, typename T2, typename T3, typename T4>
        class Out4Operation : public OutputOperation
        {
        public:
            /**
             * Constructor.
             */
            Out4Operation()
            {
                // No-op.
            }

            virtual void ProcessOutput(binary::BinaryReaderImpl& reader)
            {
                val1 = reader.ReadTopObject<T1>();
                val2 = reader.ReadTopObject<T2>();
                val3 = reader.ReadTopObject<T3>();
                val4 = reader.ReadTopObject<T4>();
            }

            virtual void SetNull()
            {
                val1 = binary::BinaryUtils::GetDefaultValue<T1>();
                val2 = binary::BinaryUtils::GetDefaultValue<T2>();
                val3 = binary::BinaryUtils::GetDefaultValue<T3>();
                val4 = binary::BinaryUtils::GetDefaultValue<T4>();
            }

            /**
             * Get value 1.
             *
             * @param Value 1.
             */
            T1& Get1()
            {
                return val1;
            }

            /**
             * Get value 2.
             *
             * @param Value 2.
             */
            T2& Get2()
            {
                return val2;
            }

            /**
             * Get value 3.
             *
             * @param Value 3.
             */
            T3& Get3()
            {
                return val3;
            }

            /**
             * Get value 4.
             *
             * @param Value 4.
             */
            T4& Get4()
            {
                return val4;
            }

        private:
            /** Value 1. */
            T1 val1; 
            
            /** Value 2. */
            T2 val2;

            /** Value 3. */
            T3 val3;

            /** Value 4. */
            T4 val4;

            IGNITE_NO_COPY_ASSIGNMENT(Out4Operation)
        };

        /**
         * Output map operation.
         */
        template<typename T1, typename T2>
        class OutMapOperation :public OutputOperation
        {
        public:
            /**
             * Constructor.
             */
            OutMapOperation()
            {
                // No-op.
            }

            virtual void ProcessOutput(binary::BinaryReaderImpl& reader)
            {
                bool exists = reader.GetStream()->ReadBool();

                if (exists)
                {
                    int32_t cnt = reader.GetStream()->ReadInt32();

                    std::map<T1, T2> val0;

                    for (int i = 0; i < cnt; i++) {
                        T1 t1 = reader.ReadTopObject<T1>();
                        T2 t2 = reader.ReadTopObject<T2>();

                        val0[t1] = t2;
                    }

                    val = val0;
                }
            }

            virtual void SetNull()
            {
                // No-op.
            }

            /**
             * Get value.
             *
             * @param Value.
             */
            std::map<T1, T2> GetResult()
            {
                return val;
            }
        private:
            /** Value. */
            std::map<T1, T2> val;

            IGNITE_NO_COPY_ASSIGNMENT(OutMapOperation)
        };

        /**
         * Output query GET ALL operation.
         */
        template<typename K, typename V>
        class OutQueryGetAllOperation : public OutputOperation
        {
        public:
            /**
             * Constructor.
             */
            OutQueryGetAllOperation(std::vector<ignite::cache::CacheEntry<K, V> >* res) : res(res)
            {
                // No-op.
            }

            virtual void ProcessOutput(binary::BinaryReaderImpl& reader)
            {
                int32_t cnt = reader.ReadInt32();

                for (int i = 0; i < cnt; i++) 
                {
                    K key = reader.ReadTopObject<K>();
                    V val = reader.ReadTopObject<V>();

                    res->push_back(ignite::cache::CacheEntry<K, V>(key, val));
                }
            }

            virtual void SetNull()
            {
                res->clear();
            }

        private:
            /** Entries. */
            std::vector<ignite::cache::CacheEntry<K, V> >* res;
            
            IGNITE_NO_COPY_ASSIGNMENT(OutQueryGetAllOperation)
        };
    }
}

#endif //_IGNITE_IMPL_OPERATIONS