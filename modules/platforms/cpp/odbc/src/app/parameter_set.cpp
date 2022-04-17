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

#include "ignite/odbc/app/parameter_set.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            ParameterSet::ParameterSet():
                parameters(),
                paramTypes(),
                paramBindOffset(0),
                processedParamRows(0),
                paramsStatus(0),
                paramSetSize(1),
                paramSetPos(0),
                currentParamIdx(0),
                typesSet(false)
            {
                // No-op.
            }

            void ParameterSet::SetParamSetSize(SqlUlen size)
            {
                paramSetSize = size;
            }

            void ParameterSet::BindParameter(uint16_t paramIdx, const Parameter& param)
            {
                parameters[paramIdx] = param;
            }

            void ParameterSet::UnbindParameter(uint16_t paramIdx)
            {
                parameters.erase(paramIdx);
            }

            void ParameterSet::UnbindAll()
            {
                parameters.clear();
            }

            uint16_t ParameterSet::GetParametersNumber() const
            {
                return static_cast<uint16_t>(parameters.size());
            }

            void ParameterSet::SetParamBindOffsetPtr(int* ptr)
            {
                paramBindOffset = ptr;
            }

            int* ParameterSet::GetParamBindOffsetPtr()
            {
                return paramBindOffset;
            }

            void ParameterSet::Prepare()
            {
                paramTypes.clear();

                typesSet = false;

                paramSetPos = 0;

                for (ParameterBindingMap::iterator it = parameters.begin(); it != parameters.end(); ++it)
                    it->second.ResetStoredData();
            }

            bool ParameterSet::IsDataAtExecNeeded() const
            {
                for (ParameterBindingMap::const_iterator it = parameters.begin(); it != parameters.end(); ++it)
                {
                    if (!it->second.IsDataReady())
                        return true;
                }

                return false;
            }

            void ParameterSet::SetParamsProcessedPtr(SqlUlen* ptr)
            {
                processedParamRows = ptr;
            }

            SqlUlen* ParameterSet::GetParamsProcessedPtr() const
            {
                return processedParamRows;
            }

            void ParameterSet::SetParamsStatusPtr(SQLUSMALLINT* value)
            {
                paramsStatus = value;
            }

            SQLUSMALLINT* ParameterSet::GetParamsStatusPtr() const
            {
                return paramsStatus;
            }

            void ParameterSet::SetParamStatus(int64_t idx, SQLUSMALLINT status) const
            {
                if (idx < 0 || !paramsStatus || idx >= static_cast<int64_t>(paramSetSize))
                    return;

                paramsStatus[idx] = status;
            }

            void ParameterSet::SetParamsProcessed(SqlUlen processed) const
            {
                if (processedParamRows)
                    *processedParamRows = processed;
            }

            void ParameterSet::UpdateParamsTypes(const ParameterTypeVector& meta)
            {
                paramTypes = meta;

                typesSet = true;
            }

            int8_t ParameterSet::GetParamType(int16_t idx, int8_t dflt)
            {
                if (idx > 0 && static_cast<size_t>(idx) <= paramTypes.size())
                    return paramTypes[idx - 1];

                return dflt;
            }

            uint16_t ParameterSet::GetExpectedParamNum()
            {
                return static_cast<uint16_t>(paramTypes.size());
            }

            bool ParameterSet::IsMetadataSet() const
            {
                return typesSet;
            }

            bool ParameterSet::IsParameterSelected() const
            {
                return currentParamIdx != 0;
            }

            Parameter* ParameterSet::GetParameter(uint16_t idx)
            {
                ParameterBindingMap::iterator it = parameters.find(idx);

                if (it != parameters.end())
                    return &it->second;

                return 0;
            }

            Parameter* ParameterSet::GetSelectedParameter()
            {
                return GetParameter(currentParamIdx);
            }

            Parameter* ParameterSet::SelectNextParameter()
            {
                for (ParameterBindingMap::iterator it = parameters.begin(); it != parameters.end(); ++it)
                {
                    uint16_t paramIdx = it->first;
                    Parameter& param = it->second;

                    if (!param.IsDataReady())
                    {
                        currentParamIdx = paramIdx;

                        return &param;
                    }
                }

                return 0;
            }

            void ParameterSet::Write(impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt32(CalculateRowLen());

                WriteRow(writer, 0);
            }

            void ParameterSet::Write(impl::binary::BinaryWriterImpl& writer, SqlUlen begin, SqlUlen end, bool last) const
            {
                int32_t rowLen = CalculateRowLen();

                writer.WriteInt32(rowLen);

                SqlUlen intervalEnd = std::min(paramSetSize, end);

                assert(begin < intervalEnd);

                int32_t intervalLen = static_cast<int32_t>(intervalEnd - begin);

                writer.WriteInt32(intervalLen);
                writer.WriteBool(last);

                if (rowLen)
                {
                    for (SqlUlen i = begin; i < intervalEnd; ++i)
                        WriteRow(writer, i);
                }
            }

            void ParameterSet::WriteRow(impl::binary::BinaryWriterImpl& writer, SqlUlen idx) const
            {
                uint16_t prev = 0;

                int appOffset = paramBindOffset ? *paramBindOffset : 0;

                for (ParameterBindingMap::const_iterator it = parameters.begin(); it != parameters.end(); ++it)
                {
                    uint16_t paramIdx = it->first;
                    const Parameter& param = it->second;

                    while ((paramIdx - prev) > 1)
                    {
                        writer.WriteNull();
                        ++prev;
                    }

                    param.Write(writer, appOffset, idx);

                    prev = paramIdx;
                }
            }

            int32_t ParameterSet::CalculateRowLen() const
            {
                if (!parameters.empty())
                    return static_cast<int32_t>(parameters.rbegin()->first);

                return  0;
            }

            int32_t ParameterSet::GetParamSetSize() const
            {
                return static_cast<int32_t>(paramSetSize);
            }
        }
    }
}
