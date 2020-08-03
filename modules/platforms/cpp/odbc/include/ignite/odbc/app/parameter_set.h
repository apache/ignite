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

#ifndef _IGNITE_ODBC_APP_PARAMETER_SET
#define _IGNITE_ODBC_APP_PARAMETER_SET

#include <stdint.h>

#include <map>

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include "ignite/odbc/app/parameter.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            /**
             * Parameter set.
             */
            class ParameterSet
            {
                /** Parameter binging map type alias. */
                typedef std::map<uint16_t, Parameter> ParameterBindingMap;

                /** Parameter meta vector. */
                typedef std::vector<int8_t> ParameterTypeVector;
            public:
                /**
                 * Default constructor.
                 */
                ParameterSet();

                /**
                 * Destructor.
                 */
                ~ParameterSet()
                {
                    // No-op.
                }

                /**
                 * Set parameters set size.
                 *
                 * @param size Size of the parameter set.
                 */
                void SetParamSetSize(SqlUlen size);

                /**
                 * Bind parameter.
                 * 
                 * @param paramIdx Parameter index.
                 * @param param Parameter.
                 */
                void BindParameter(uint16_t paramIdx, const Parameter& param);

                /**
                 * Unbind specified parameter.
                 *
                 * @param paramIdx Parameter index.
                 */
                void UnbindParameter(uint16_t paramIdx);

                /**
                 * Unbind all parameters.
                 */
                void UnbindAll();

                /**
                 * Get number of binded parameters.
                 *
                 * @return Number of binded parameters.
                 */
                uint16_t GetParametersNumber() const;

                /**
                 * Set parameter binding offset pointer.
                 *
                 * @param ptr Parameter binding offset pointer.
                 */
                void SetParamBindOffsetPtr(int* ptr);

                /**
                 * Get parameter binding offset pointer.
                 *
                 * @return Parameter binding offset pointer.
                 */
                int* GetParamBindOffsetPtr();

                /**
                 * Prepare parameters set for statement execution.
                 */
                void Prepare();

                /**
                 * Check if the data at-execution is needed.
                 *
                 * @return True if the data at execution is needed.
                 */
                bool IsDataAtExecNeeded() const;

                /**
                 * Update parameter types metadata.
                 *
                 * @param meta Types metadata.
                 */
                void UpdateParamsTypes(const ParameterTypeVector& meta);

                /**
                 * Get type id of the parameter.
                 *
                 * @param idx Parameter index.
                 * @param dflt Default value to return if the type can not be found.
                 * @return Type ID of the parameter or dflt, if the type can not be returned.
                 */
                int8_t GetParamType(int16_t idx, int8_t dflt);

                /**
                 * Get expected parameters number.
                 * Using metadata. If metadata was not updated returns zero.
                 *
                 * @return Expected parameters number.
                 */
                uint16_t GetExpectedParamNum();

                /**
                 * Check if the metadata was set for the parameter set.
                 * 
                 * @return True if the metadata was set for the parameter set. 
                 */
                bool IsMetadataSet() const;

                /**
                 * Check if the parameter selected for putting data at-execution.
                 *
                 * @return True if the parameter selected for putting data at-execution.
                 */
                bool IsParameterSelected() const;

                /**
                 * Get parameter by index.
                 *
                 * @param idx Index.
                 * @return Parameter or null, if parameter is not bound.
                 */
                Parameter* GetParameter(uint16_t idx);

                /**
                 * Get selected parameter.
                 *
                 * @return Parameter or null, if parameter is not bound.
                 */
                Parameter* GetSelectedParameter();

                /**
                 * Internally selects next parameter for putting data at-execution.
                 *
                 * @return Parameter if found and null otherwise.
                 */
                Parameter* SelectNextParameter();

                /**
                 * Write only first row of the param set using provided writer.
                 * @param writer Writer.
                 */
                void Write(impl::binary::BinaryWriterImpl& writer) const;

                /**
                 * Write rows of the param set in interval [begin, end) using provided writer.
                 * @param writer Writer.
                 * @param begin Beginng of the interval.
                 * @param end End of the interval.
                 * @param last Last page flag.
                 */
                void Write(impl::binary::BinaryWriterImpl& writer, SqlUlen begin, SqlUlen end, bool last) const;

                /**
                 * Calculate row length.
                 *
                 * @return Row length.
                 */
                int32_t CalculateRowLen() const;

                /**
                 * Get parameter set size.
                 *
                 * @return Number of rows in set.
                 */
                int32_t GetParamSetSize() const;

                /**
                 * Set number of parameters processed in batch.
                 *
                 * @param processed Processed.
                 */
                void SetParamsProcessed(SqlUlen processed) const;

                /**
                 * Number of processed params should be written using provided address.
                 *
                 * @param ptr Pointer.
                 */
                void SetParamsProcessedPtr(SqlUlen* ptr);

                /**
                 * Get pointer to write number of parameters processed in batch.
                 *
                 * @return Pointer to write number of parameters processed in batch.
                 */
                SqlUlen* GetParamsProcessedPtr() const;

                /**
                 * Set pointer to array in which to return the status of each
                 * set of parameters.
                 * @param value Value.
                 */
                void SetParamsStatusPtr(SQLUSMALLINT* value);

                /**
                 * Get pointer to array in which to return the status of each
                 * set of parameters.
                 * @return Value.
                 */
                SQLUSMALLINT* GetParamsStatusPtr() const;

                /**
                 * Set parameter status.
                 * @param idx Parameter index.
                 * @param status Status to set.
                 */
                void SetParamStatus(int64_t idx, SQLUSMALLINT status) const;

            private:
                /**
                 * Write single row of the param set using provided writer.
                 * @param writer Writer.
                 * @param idx Row index.
                 */
                void WriteRow(impl::binary::BinaryWriterImpl& writer, SqlUlen idx) const;

                IGNITE_NO_COPY_ASSIGNMENT(ParameterSet);

                /** Parameters. */
                ParameterBindingMap parameters;

                /** Parameter types. */
                ParameterTypeVector paramTypes;

                /** Offset added to pointers to change binding of parameters. */
                int* paramBindOffset;

                /** Processed parameters. */
                SqlUlen* processedParamRows;

                /** Parameters status. */
                SQLUSMALLINT* paramsStatus;

                /** Parameter set size. */
                SqlUlen paramSetSize;

                /** Current position in parametor set. */
                SqlUlen paramSetPos;

                /** Index of the parameter, which is currently being set. */
                uint16_t currentParamIdx;

                /** Parameter types are set. */
                bool typesSet;
            };
        }
    }
}

#endif //_IGNITE_ODBC_APP_PARAMETER_SET
