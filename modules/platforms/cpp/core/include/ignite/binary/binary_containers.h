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

#ifndef _IGNITE_BINARY_CONTAINERS
#define _IGNITE_BINARY_CONTAINERS

#include <stdint.h>

#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/impl/utils.h"
#include "ignite/binary/binary_consts.h"

namespace ignite
{
    namespace binary
    {
        /**
         * Binary string array writer.
         */
        class IGNITE_IMPORT_EXPORT BinaryStringArrayWriter
        {
        public:
            /**
             * Constructor.
             * 
             * @param id Identifier.
             * @param impl Writer.
             */
            BinaryStringArrayWriter(impl::binary::BinaryWriterImpl* impl, int32_t id);

            /**
             * Write string.
             *
             * @param val Null-terminated character sequence.
             */
            void Write(const char* val);

            /**
             * Write string.
             *
             * @param val String.
             * @param len String length (characters).
             */
            void Write(const char* val, int32_t len);

            /**
             * Write string.
             *
             * @param val String.
             */
            void Write(const std::string& val)
            {
                Write(val.c_str());
            }

            /**
             * Close the writer.
             */
            void Close();
        private:
            /** Implementation delegate. */
            impl::binary::BinaryWriterImpl* impl; 

            /** Idnetifier. */
            const int32_t id;    
        };

        /**
         * Binary collection writer.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT BinaryArrayWriter
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Writer.
             * @param id Identifier.
             */
            BinaryArrayWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) : impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a value.
             *
             * @param val Value.
             */
            void Write(const T& val)
            {
                impl->WriteElement<T>(id, val);
            }

            /**
             * Close the writer.
             */
            void Close()
            {
                impl->CommitContainer(id);
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryWriterImpl* impl; 

            /** Idnetifier. */
            const int32_t id;      
        };

        /**
         * Binary collection writer.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT BinaryCollectionWriter
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Writer.
             * @param id Identifier.
             */
            BinaryCollectionWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) : impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a value.
             *
             * @param val Value.
             */
            void Write(const T& val)
            {
                impl->WriteElement<T>(id, val);
            }

            /**
             * Close the writer.
             */
            void Close()
            {
                impl->CommitContainer(id);
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryWriterImpl* impl; 

            /** Identifier. */
            const int32_t id;    
        };

        /**
         * Binary map writer.
         */
        template<typename K, typename V>
        class IGNITE_IMPORT_EXPORT BinaryMapWriter
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Writer.
             */
            BinaryMapWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) : impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a value.
             *
             * @param key Key.
             * @param val Value.
             */
            void Write(const K& key, const V& val)
            {
                impl->WriteElement<K, V>(id, key, val);
            }

            /**
             * Close the writer.
             */
            void Close()
            {
                impl->CommitContainer(id);
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryWriterImpl* impl; 

            /** Identifier. */
            const int32_t id;      
        };

        /**
         * Binary string array reader.
         */
        class IGNITE_IMPORT_EXPORT BinaryStringArrayReader
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Reader.
             * @param id Identifier.
             * @param size Array size.
             */
            BinaryStringArrayReader(impl::binary::BinaryReaderImpl* impl, int32_t id, int32_t size);

            /**
             * Check whether next element is available for read.
             *
             * @return True if available.
             */
            bool HasNext();

            /**
             * Get next element.
             *
             * @param res Array to store data to. 
             * @param len Expected length of string. NULL terminator will be set in case len is 
             *     greater than real string length.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t GetNext(char* res, int32_t len);

            /**
             * Get next element.
             *
             * @return String. 
             */
            std::string GetNext()
            {
                int32_t len = GetNext(NULL, 0);

                if (len != -1)
                {
                    impl::utils::SafeArray<char> arr(len + 1);

                    GetNext(arr.target, len + 1);

                    return std::string(arr.target);
                }
                else
                    return std::string();
            }

            /**
             * Get array size.
             *
             * @return Size or -1 if array is NULL.
             */
            int32_t GetSize() const;

            /**
             * Whether array is NULL.
             */
            bool IsNull() const;
        private:
            /** Implementation delegate. */
            impl::binary::BinaryReaderImpl* impl;  

            /** Identifier. */
            const int32_t id;    

            /** Size. */
            const int32_t size;                              
        };

        /**
         * Binary array reader.
         */
        template<typename T>
        class BinaryArrayReader
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Reader.
             * @param id Identifier.
             * @param size Array size.
             */
            BinaryArrayReader(impl::binary::BinaryReaderImpl* impl, int32_t id, int32_t size) : 
                impl(impl), id(id), size(size)
            {
                // No-op.
            }

            /**
             * Check whether next element is available for read.
             *
             * @return True if available.
             */
            bool HasNext()
            {
                return impl->HasNextElement(id);
            }

            /**
             * Read next element.
             *
             * @return Next element.
             */
            T GetNext()
            {
                return impl->ReadElement<T>(id);
            }

            /**
             * Get array size.
             *
             * @return Size or -1 if array is NULL.
             */
            int32_t GetSize()
            {
                return size;
            }

            /**
             * Whether array is NULL.
             */
            bool IsNull()
            {
                return size == -1;
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryReaderImpl* impl;

            /** Identifier. */
            const int32_t id;

            /** Size. */
            const int32_t size;
        };

        /**
         * Binary collection reader.
         */
        template<typename T>
        class BinaryCollectionReader
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Reader.
             * @param id Identifier.
             * @param type Collection type.
             * @param size Collection size.
             */
            BinaryCollectionReader(impl::binary::BinaryReaderImpl* impl, int32_t id, 
                const CollectionType type,  int32_t size) : impl(impl), id(id), type(type), size(size)
            {
                // No-op.
            }

            /**
             * Check whether next element is available for read.
             *
             * @return True if available.
             */
            bool HasNext()
            {
                return impl->HasNextElement(id);
            }

            /**
             * Read next element.
             *
             * @return Next element.
             */
            T GetNext()
            {
                return impl->ReadElement<T>(id);
            }
            
            /**
             * Get collection type.
             *
             * @return Type.
             */
            CollectionType GetType()
            {
                return type;
            }

            /**
             * Get collection size.
             *
             * @return Size or -1 if collection is NULL.
             */
            int32_t GetSize()
            {
                return size;
            }

            /**
             * Whether collection is NULL.
             */
            bool IsNull()
            {
                return size == -1;
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryReaderImpl* impl;  

            /** Identifier. */
            const int32_t id;     
            
            /** Collection type. */
            const CollectionType type;  

            /** Size. */
            const int32_t size;                              
        };    

        /**
         * Binary map reader.
         */
        template<typename K, typename V>
        class BinaryMapReader
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Reader.
             * @param id Identifier.
             * @param type Map type.
             * @param size Map size.
            */
            BinaryMapReader(impl::binary::BinaryReaderImpl* impl, int32_t id, MapType type,
                int32_t size) : impl(impl), id(id), type(type), size(size)
            {
                // No-op.
            }

            /**
             * Check whether next element is available for read.
             *
             * @return True if available.
             */
            bool HasNext()
            {
                return impl->HasNextElement(id);
            }

            /**
             * Read next element.
             *
             * @param key Key.
             * @param val Value.
             */
            void GetNext(K* key, V* val)
            {
                return impl->ReadElement<K, V>(id, key, val);
            }

            /**
             * Get map type.
             *
             * @return Type.
             */
            MapType GetType()
            {
                return type;
            }

            /**
             * Get map size.
             *
             * @return Size or -1 if map is NULL.
             */
            int32_t GetSize()
            {
                return size;
            }

            /**
             * Whether map is NULL.
             */
            bool IsNull()
            {
                return size == -1;
            }
        private:
            /** Implementation delegate. */
            impl::binary::BinaryReaderImpl* impl;  

            /** Identifier. */
            const int32_t id;     

            /** Map type. */
            const MapType type;

            /** Size. */
            const int32_t size;
        };
    }
}

#endif