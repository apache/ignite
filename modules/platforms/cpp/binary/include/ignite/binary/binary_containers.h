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
 * Declares binary reader and writer types for the collections.
 */

#ifndef _IGNITE_BINARY_BINARY_CONTAINERS
#define _IGNITE_BINARY_BINARY_CONTAINERS

#include <stdint.h>

#include <ignite/common/utils.h>

#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/binary/binary_consts.h"

namespace ignite
{
    namespace binary
    {
        /**
         * Binary string array writer.
         *
         * Can be used to write array of strings one by one.
         *
         * Use Write() method to write array string by string, then finilize
         * the writing by calling Close() method. Once the Close() method have
         * been called, instance is not usable and will throw an IgniteError
         * on any subsequent attempt to use it.
         */
        class IGNITE_IMPORT_EXPORT BinaryStringArrayWriter
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Writer implementation.
             * @param id Identifier.
             */
            BinaryStringArrayWriter(impl::binary::BinaryWriterImpl* impl, int32_t id);

            /**
             * Write null-terminated string.
             *
             * @param val Null-terminated character sequence to write.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const char* val);

            /**
             * Write string.
             *
             * @param val String to write.
             * @param len String length in bytes.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const char* val, int32_t len);

            /**
             * Write string.
             *
             * @param val String to write.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const std::string& val)
            {
                Write(val.c_str());
            }

            /**
             * Close the writer.
             *
             * This method should be called to finilize writing
             * of the array.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Close();

        private:
            /** Implementation delegate. */
            impl::binary::BinaryWriterImpl* impl; 

            /** Identifier. */
            const int32_t id;    
        };

        /**
         * Binary array writer.
         *
         * Can be used to write array of values of the specific type one by
         * one.
         *
         * Use Write() method to write array value by value, then finilize
         * the writing by calling Close() method. Once the Close() method have
         * been called, instance is not usable and will throw an IgniteError
         * on any subsequent attempt to use it.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT BinaryArrayWriter
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Writer implementation.
             * @param id Identifier.
             */
            BinaryArrayWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) :
                impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a value.
             *
             * @param val Value to write.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const T& val)
            {
                impl->WriteElement<T>(id, val);
            }

            /**
             * Close the writer.
             *
             * This method should be called to finilize writing
             * of the array.
             *
             * @throw IgniteError if the writer instance is closed already.
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
         *
         * Can be used to write collection of values of the specific type one by
         * one.
         *
         * Use Write() method to write collection value by value, then finilize
         * the writing by calling Close() method. Once the Close() method have
         * been called, instance is not usable and will throw an IgniteError
         * on any subsequent attempt to use it.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT BinaryCollectionWriter
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Writer implementation.
             * @param id Identifier.
             */
            BinaryCollectionWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) :
                impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a value.
             *
             * @param val Value to write.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const T& val)
            {
                impl->WriteElement<T>(id, val);
            }

            /**
             * Close the writer.
             *
             * This method should be called to finilize writing
             * of the collection.
             *
             * @throw IgniteError if the writer instance is closed already.
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
         *
         * Can be used to write map element by element.
         *
         * Use Write() method to write map value by value, then finilize
         * the writing by calling Close() method. Once the Close() method have
         * been called, instance is not usable and will throw an IgniteError
         * on any subsequent attempt to use it.
         */
        template<typename K, typename V>
        class IGNITE_IMPORT_EXPORT BinaryMapWriter
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Writer implementation.
             * @param id Identifier.
             */
            BinaryMapWriter(impl::binary::BinaryWriterImpl* impl, int32_t id) :
                impl(impl), id(id)
            {
                // No-op.
            }

            /**
             * Write a map entry.
             *
             * @param key Key element of the map entry.
             * @param val Value element of the map entry.
             *
             * @throw IgniteError if the writer instance is closed already.
             */
            void Write(const K& key, const V& val)
            {
                impl->WriteElement<K, V>(id, key, val);
            }

            /**
             * Close the writer.
             *
             * This method should be called to finilize writing of the map.
             *
             * @throw IgniteError if the writer instance is closed already.
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
         *
         * Can be used to read array of strings string by string.
         *
         * Use GetNext() method to read array value by value while HasNext()
         * method returns true.
         */
        class IGNITE_IMPORT_EXPORT BinaryStringArrayReader
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Reader implementation.
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
             * @param res Buffer to store data to. 
             * @param len Expected length of string. NULL terminator will be set in case len is 
             *     greater than real string length.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             *
             * @throw IgniteError if there is no element to read.
             */
            int32_t GetNext(char* res, int32_t len);

            /**
             * Get next element.
             *
             * @return String.
             *
             * @throw IgniteError if there is no element to read.
             */
            std::string GetNext()
            {
                int32_t len = GetNext(NULL, 0);

                if (len != -1)
                {
                    ignite::common::FixedSizeArray<char> arr(len + 1);

                    GetNext(arr.GetData(), static_cast<int32_t>(arr.GetSize()));

                    return std::string(arr.GetData());
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
             * Check whether array is NULL.
             *
             * @return True if the array is NULL.
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
         *
         * Can be used to read array of values of the specific type one by one.
         *
         * Use GetNext() method to read array value by value while HasNext()
         * method returns true.
         */
        template<typename T>
        class BinaryArrayReader
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Reader implementation.
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
             *
             * @throw IgniteError if there is no element to read.
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
             * Check whether array is NULL.
             *
             * @return True if the array is NULL.
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
         *
         * Can be used to read collection of values of the specific type
         * one by one.
         *
         * Use GetNext() method to read array value by value while HasNext()
         * method returns true.
         */
        template<typename T>
        class BinaryCollectionReader
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Reader implementation.
             * @param id Identifier.
             * @param type Collection type.
             * @param size Collection size.
             */
            BinaryCollectionReader(impl::binary::BinaryReaderImpl* impl, int32_t id, 
                const CollectionType::Type type,  int32_t size) : impl(impl), id(id), type(type), size(size)
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
             *
             * @throw IgniteError if there is no element to read.
             */
            T GetNext()
            {
                return impl->ReadElement<T>(id);
            }
            
            /**
             * Get collection type.
             *
             * @return Collection type. See CollectionType for the list of
             *     available values and their description.
             */
            CollectionType::Type GetType()
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
             * Check whether collection is NULL.
             *
             * @return True if the collection is NULL.
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
            const CollectionType::Type type;  

            /** Size. */
            const int32_t size;                              
        };    

        /**
         * Binary map reader.
         *
         * Can be used to read map entry by entry.
         *
         * Use GetNext() method to read array value by value while HasNext()
         * method returns true.
         */
        template<typename K, typename V>
        class BinaryMapReader
        {
        public:
            /**
             * Constructor.
             * Internal call. Should not be used by user.
             *
             * @param impl Reader implementation.
             * @param id Identifier.
             * @param type Map type.
             * @param size Map size.
            */
            BinaryMapReader(impl::binary::BinaryReaderImpl* impl, int32_t id, MapType::Type type,
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
             * @param key Pointer to buffer where key element should be stored.
             *     Should not be null.
             * @param val Pointer to buffer where value element should be
             *     stored. Should not be null.
             *
             * @throw IgniteError if there is no element to read.
             */
            void GetNext(K& key, V& val)
            {
                return impl->ReadElement<K, V>(id, key, val);
            }

            /**
             * Get map type.
             *
             * @return Map type. See MapType for the list of available values
             *     and their description.
             */
            MapType::Type GetType()
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
             * Check whether map is NULL.
             *
             * @return True if the map is NULL.
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
            const MapType::Type type;

            /** Size. */
            const int32_t size;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_CONTAINERS
