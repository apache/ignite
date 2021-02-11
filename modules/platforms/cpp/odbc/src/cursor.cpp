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

#include "ignite/odbc/cursor.h"

namespace ignite
{
    namespace odbc
    {
        Cursor::Cursor(int64_t queryId) :
            queryId(queryId),
            currentPage(),
            currentPagePos(0),
            currentRow()
        {
            // No-op.
        }

        Cursor::~Cursor()
        {
            // No-op.
        }

        bool Cursor::Increment()
        {
            if (currentPage.get() && currentPagePos < currentPage->GetSize())
            {
                ++currentPagePos;

                if (currentPagePos == currentPage->GetSize())
                    currentRow.reset();
                else
                {
                    Row *row = currentRow.get();

                    if (row)
                        row->MoveToNext();
                }
                return true;
            }
            return false;
        }

        bool Cursor::NeedDataUpdate() const
        {
            return !currentPage.get() || (!currentPage->IsLast() &&
                currentPagePos == currentPage->GetSize());
        }

        bool Cursor::HasData() const
        {
            return !currentPage.get() || !currentPage->IsLast() ||
                currentPagePos < currentPage->GetSize();
        }

        bool Cursor::IsClosedRemotely() const
        {
            return currentPage.get() && currentPage->IsLast();
        }

        void Cursor::UpdateData(std::auto_ptr<ResultPage>& newPage)
        {
            currentPage = newPage;

            currentPagePos = 0;

            currentRow.reset(new Row(currentPage->GetData()));
        }

        Row* Cursor::GetRow()
        {
            return currentRow.get();
        }
    }
}

