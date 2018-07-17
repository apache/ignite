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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/row.h"

namespace ignite
{
    namespace odbc
    {
        Row::Row(ignite::impl::interop::InteropUnpooledMemory& pageData) :
            rowBeginPos(0), pos(rowBeginPos), size(0), pageData(pageData),
            stream(&pageData), reader(&stream), columns()
        {
            if (pageData.Length() >= 4)
            {
                Reinit();
            }
        }

        Row::~Row()
        {
            // No-op.
        }

        bool Row::EnsureColumnDiscovered(uint16_t columnIdx)
        {
            if (columns.size() >= columnIdx)
                return true;

            if (columnIdx > GetSize() || columnIdx < 1)
                return false;

            if (columns.empty())
            {
                Column newColumn(reader);

                if (!newColumn.IsValid())
                    return false;

                columns.push_back(newColumn);
            }

            while (columns.size() < columnIdx)
            {
                Column& column = columns.back();

                stream.Position(column.GetEndPosition());

                Column newColumn(reader);

                if (!newColumn.IsValid())
                    return false;

                columns.push_back(newColumn);
            }

            return true;
        }

        app::ConversionResult::Type Row::ReadColumnToBuffer(uint16_t columnIdx, app::ApplicationDataBuffer& dataBuf)
        {
            if (!EnsureColumnDiscovered(columnIdx))
                return app::ConversionResult::AI_FAILURE;

            Column& column = GetColumn(columnIdx);

            return column.ReadToBuffer(reader, dataBuf);
        }

        bool Row::MoveToNext()
        {
            int32_t lastColumnIdx = GetSize();

            if (!EnsureColumnDiscovered(lastColumnIdx))
                return false;

            Column& lastColumn = GetColumn(lastColumnIdx);

            stream.Position(lastColumn.GetEndPosition());

            Reinit();

            return true;
        }

        void Row::Reinit()
        {
            size = stream.ReadInt32();

            rowBeginPos = stream.Position();

            columns.clear();

            columns.reserve(size);

            pos = 0;
        }
    }
}

