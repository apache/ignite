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
 
#include "ignite/binary/binary_containers.h"

using namespace ignite::impl::binary;

namespace ignite
{
    namespace binary
    {
        BinaryStringArrayWriter::BinaryStringArrayWriter(BinaryWriterImpl* impl, int32_t id) : 
            impl(impl), id(id)
        {
            // No-op.
        }

        void BinaryStringArrayWriter::Write(const char* val)
        {
            if (val)
                Write(val, static_cast<int32_t>(strlen(val)));
            else
                Write(NULL, -1);
        }

        void BinaryStringArrayWriter::Write(const char* val, int32_t len)
        {
            impl->WriteStringElement(id, val, len);
        }

        void BinaryStringArrayWriter::Close()
        {
            impl->CommitContainer(id);
        }

        BinaryStringArrayReader::BinaryStringArrayReader(impl::binary::BinaryReaderImpl* impl, 
            int32_t id, int32_t size) : impl(impl), id(id), size(size)
        {
            // No-op.
        }

        bool BinaryStringArrayReader::HasNext()
        {
            return impl->HasNextElement(id);
        }

        int32_t BinaryStringArrayReader::GetNext(char* res, int32_t len)
        {
            return impl->ReadStringElement(id, res, len);
        }

        int32_t BinaryStringArrayReader::GetSize() const
        {
            return size;
        }

        bool BinaryStringArrayReader::IsNull() const
        {
            return size == -1;
        }
    }
}