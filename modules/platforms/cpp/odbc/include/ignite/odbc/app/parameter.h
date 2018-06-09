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

#ifndef _IGNITE_ODBC_APP_PARAMETER
#define _IGNITE_ODBC_APP_PARAMETER

#include <stdint.h>

#include <map>

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include "ignite/odbc/app/application_data_buffer.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            /**
             * Statement parameter.
             */
            class Parameter
            {
            public:
                /**
                 * Default constructor.
                 */
                Parameter();

                /**
                 * Constructor.
                 *
                 * @param buffer Underlying data buffer.
                 * @param sqlType IPD type.
                 * @param columnSize IPD column size.
                 * @param decDigits IPD decimal digits.
                 */
                Parameter(const ApplicationDataBuffer& buffer, int16_t sqlType,
                    size_t columnSize, int16_t decDigits);

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                Parameter(const Parameter& other);

                /**
                 * Destructor.
                 */
                ~Parameter();

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 * @return This.
                 */
                Parameter& operator=(const Parameter& other);

                /**
                 * Write parameter using provided writer.
                 * @param writer Writer.
                 * @param offset Offset for the buffer.
                 * @param idx Index for the array-of-parameters case.
                 */
                void Write(impl::binary::BinaryWriterImpl& writer, int offset = 0, SqlUlen idx = 0) const;

                /**
                 * Get data buffer.
                 *
                 * @return underlying ApplicationDataBuffer instance.
                 */
                ApplicationDataBuffer& GetBuffer();

                /**
                 * Get data buffer.
                 *
                 * @return underlying ApplicationDataBuffer instance.
                 */
                const ApplicationDataBuffer& GetBuffer() const;

                /**
                 * Reset stored at-execution data.
                 */
                void ResetStoredData();

                /**
                 * Check if all the at-execution data has been stored.
                 * @return
                 */
                bool IsDataReady() const;

                /**
                 * Put at-execution data.
                 *
                 * @param data Data buffer pointer.
                 * @param len Data length.
                 */
                void PutData(void* data, SqlLen len);

            private:
                /** Underlying data buffer. */
                ApplicationDataBuffer buffer;

                /** IPD type. */
                int16_t sqlType;

                /** IPD column size. */
                size_t columnSize;

                /** IPD decimal digits. */
                int16_t decDigits;

                /** User provided null data at execution. */
                bool nullData;

                /** Stored at-execution data. */
                std::vector<int8_t> storedData;
            };
        }
    }
}

#endif //_IGNITE_ODBC_APP_PARAMETER
