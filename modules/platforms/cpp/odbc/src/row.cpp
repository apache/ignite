/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

