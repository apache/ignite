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

#ifndef _IGNITE_ODBC_DRIVER_APPLICATION_DATA_BUFFER
#define _IGNITE_ODBC_DRIVER_APPLICATION_DATA_BUFFER

#include <stdint.h>

#include <map>

#include <ignite/guid.h>

#include "ignite/odbc/type_traits.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * User application data buffer.
         */
        class ApplicationDataBuffer
        {
        public:
            /**
             * Default constructor.
             */
            ApplicationDataBuffer();

            /**
             * Constructor.
             *
             * @param type Underlying data type.
             * @param bufferPtr Data buffer pointer.
             * @param buflen Data buffer length.
             * @param reslen Resulting data length.
             */
            ApplicationDataBuffer(type_traits::IgniteSqlType type, void* bufferPtr, int64_t buflen, int64_t* reslen);

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            ApplicationDataBuffer(const ApplicationDataBuffer& other);

            /**
             * Destructor.
             */
            ~ApplicationDataBuffer();
            
            /**
             * Copy assigment operator.
             *
             * @param other Other instance.
             * @return This.
             */
            ApplicationDataBuffer& operator=(const ApplicationDataBuffer& other);

            /**
             * Put in buffer value of type int8_t.
             *
             * @param value Value.
             */
            void PutInt8(int8_t value);

            /**
             * Put in buffer value of type int16_t.
             *
             * @param value Value.
             */
            void PutInt16(int16_t value);

            /**
             * Put in buffer value of type int32_t.
             *
             * @param value Value.
             */
            void PutInt32(int32_t value);

            /**
             * Put in buffer value of type int64_t.
             *
             * @param value Value.
             */
            void PutInt64(int64_t value);

            /**
             * Put in buffer value of type float.
             *
             * @param value Value.
             */
            void PutFloat(float value);

            /**
             * Put in buffer value of type double.
             *
             * @param value Value.
             */
            void PutDouble(double value);

            /**
             * Put in buffer value of type string.
             *
             * @param value Value.
             */
            void PutString(const std::string& value);

            /**
             * Put in buffer value of type GUID.
             *
             * @param value Value.
             */
            void PutGuid(const Guid& value);

            /**
             * Put binary data in buffer.
             *
             * @param data Data pointer.
             * @param len Data length.
             */
            void PutBinaryData(void* data, size_t len);

            /**
             * Put NULL.
             */
            void PutNull();

        private:
            /**
             * Put value of numeric type in the buffer.
             *
             * @param value Numeric value to put.
             */
            template<typename T>
            void PutNum(T value);

            /**
             * Put numeric value to numeric buffer.
             *
             * @param value Numeric value.
             */
            template<typename Tbuf, typename Tin>
            void PutNumToNumBuffer(Tin value);

            /**
             * Put value to string buffer.
             *
             * @param value Value that can be converted to string.
             */
            template<typename CharT, typename Tin>
            void PutValToStrBuffer(const Tin& value);

            /**
             * Put value to string buffer.
             * Specialisation for int8_t.
             * @param value Value that can be converted to string.
             */
            template<typename CharT>
            void PutValToStrBuffer(const int8_t & value);

            /**
             * Put string to string buffer.
             *
             * @param value String value.
             */
            template<typename OutCharT, typename InCharT>
            void PutStrToStrBuffer(const std::basic_string<InCharT>& value);

            /**
             * Put raw data to any buffer.
             *
             * @param data Data pointer.
             * @param len Data length.
             */
            void PutRawDataToBuffer(void *data, size_t len);

            /** Underlying data type. */
            type_traits::IgniteSqlType type;

            /** Buffer pointer. */
            void* buffer;

            /** Buffer length. */
            int64_t buflen;

            /** Result length. */
            int64_t* reslen;
        };

        /** Column binging map type alias. */
        typedef std::map<uint16_t, ApplicationDataBuffer> ColumnBindingMap;
    }
}

#endif