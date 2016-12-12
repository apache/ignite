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

#ifndef _IGNITE_ODBC_APP_APPLICATION_DATA_BUFFER
#define _IGNITE_ODBC_APP_APPLICATION_DATA_BUFFER

#include <stdint.h>

#include <map>

#include <ignite/guid.h>
#include <ignite/date.h>
#include <ignite/timestamp.h>
#include <ignite/common/decimal.h>

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/type_traits.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
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
                 * @param buffer Data buffer pointer.
                 * @param buflen Data buffer length.
                 * @param reslen Resulting data length.
                 * @param offset Pointer to buffer and reslen offset pointer.
                 */
                ApplicationDataBuffer(type_traits::IgniteSqlType type, void* buffer, SqlLen buflen, SqlLen* reslen, size_t** offset = 0);

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
                 * Set pointer to offset pointer.
                 *
                 * @param offset Pointer to offset pointer.
                 */
                void SetPtrToOffsetPtr(size_t** offset)
                {
                    this->offset = offset;
                }

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
                 * @return Number of bytes that have been put in buffer.
                 */
                int32_t PutString(const std::string& value);

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
                 * @return Number of bytes that have been put in buffer.
                 */
                int32_t PutBinaryData(void* data, size_t len);

                /**
                 * Put NULL.
                 */
                void PutNull();

                /**
                 * Put decimal value to buffer.
                 *
                 * @param value Value to put.
                 */
                void PutDecimal(const common::Decimal& value);

                /**
                 * Put date to buffer.
                 *
                 * @param value Value to put.
                 */
                void PutDate(const Date& value);

                /**
                 * Put timestamp to buffer.
                 *
                 * @param value Value to put.
                 */
                void PutTimestamp(const Timestamp& value);

                /**
                 * Get string.
                 *
                 * @return String value of buffer.
                 */
                std::string GetString(size_t maxLen) const;

                /**
                 * Get value of type int8_t.
                 *
                 * @return Integer value of type int8_t.
                 */
                int8_t GetInt8() const;

                /**
                 * Get value of type int16_t.
                 *
                 * @return Integer value of type int16_t.
                 */
                int16_t GetInt16() const;

                /**
                 * Get value of type int32_t.
                 *
                 * @return Integer value of type int32_t.
                 */
                int32_t GetInt32() const;

                /**
                 * Get value of type int64_t.
                 *
                 * @return Integer value of type int64_t.
                 */
                int64_t GetInt64() const;

                /**
                 * Get value of type float.
                 *
                 * @return Integer value of type float.
                 */
                float GetFloat() const;

                /**
                 * Get value of type double.
                 *
                 * @return Value of type double.
                 */
                double GetDouble() const;

                /**
                 * Get value of type GUID.
                 *
                 * @return Value of type Guid.
                 */
                Guid GetGuid() const;

                /**
                 * Get value of type Date.
                 *
                 * @return Value of type Date.
                 */
                Date GetDate() const;

                /**
                 * Get value of type Timestamp.
                 *
                 * @return Value of type Timestamp.
                 */
                Timestamp GetTimestamp() const;

                /**
                 * Get value of type Decimal.
                 *
                 * @param val Result is placed here.
                 */
                void GetDecimal(common::Decimal& val) const;

                /**
                 * Get raw data.
                 *
                 * @return Buffer data.
                 */
                const void* GetData() const;

                /**
                 * Get result data length.
                 *
                 * @return Data length pointer.
                 */
                const SqlLen* GetResLen() const;

                /**
                 * Get buffer size in bytes.
                 *
                 * @return Buffer size.
                 */
                SqlLen GetSize() const
                {
                    return buflen;
                }

            private:
                /**
                 * Get raw data.
                 *
                 * @return Buffer data.
                 */
                void* GetData();

                /**
                 * Get result data length.
                 *
                 * @return Data length pointer.
                 */
                SqlLen* GetResLen();

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

                /**
                 * Get int of type T.
                 *
                 * @return Integer value of specified type.
                 */
                template<typename T>
                T GetNum() const;

                /**
                 * Apply buffer offset to pointer.
                 * Adds offset to pointer if offset pointer is not null.
                 * @param ptr Pointer.
                 * @return Pointer with applied offset.
                 */
                template<typename T>
                T* ApplyOffset(T* ptr) const;

                /** Underlying data type. */
                type_traits::IgniteSqlType type;

                /** Buffer pointer. */
                void* buffer;

                /** Buffer length. */
                SqlLen buflen;

                /** Result length. */
                SqlLen* reslen;

                /** Pointer to implementation pointer to application offset */
                size_t** offset;
            };

            /** Column binging map type alias. */
            typedef std::map<uint16_t, ApplicationDataBuffer> ColumnBindingMap;
        }
    }
}

#endif //_IGNITE_ODBC_APP_APPLICATION_DATA_BUFFER