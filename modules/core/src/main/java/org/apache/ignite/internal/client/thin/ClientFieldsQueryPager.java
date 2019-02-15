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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Fields query pager.
 */
class ClientFieldsQueryPager extends GenericQueryPager<List<?>> implements FieldsQueryPager<List<?>> {
    /** Keep binary. */
    private final boolean keepBinary;

    /** Field names. */
    private List<String> fieldNames = new ArrayList<>();

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Constructor. */
    ClientFieldsQueryPager(
        ReliableChannel ch,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<BinaryOutputStream> qryWriter,
        boolean keepBinary,
        ClientBinaryMarshaller marsh
    ) {
        super(ch, qryOp, pageQryOp, qryWriter);

        this.keepBinary = keepBinary;

        serDes = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override Collection<List<?>> readEntries(BinaryInputStream in) {
        if (!hasFirstPage())
            fieldNames = new ArrayList<>(ClientUtils.collection(in, ignored -> (String)serDes.readObject(in, keepBinary)));

        int rowCnt = in.readInt();

        Collection<List<?>> res = new ArrayList<>(rowCnt);

        for (int r = 0; r < rowCnt; r++) {
            List<?> row = new ArrayList<>(fieldNames.size());

            for (int f = 0; f < fieldNames.size(); f++)
                row.add(serDes.readObject(in, keepBinary));

            res.add(row);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<String> getFieldNames() {
        return fieldNames;
    }
}
