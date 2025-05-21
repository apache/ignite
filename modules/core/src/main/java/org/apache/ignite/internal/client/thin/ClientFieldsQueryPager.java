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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.client.thin.TcpClientTransactions.TcpClientTransaction;
import org.jetbrains.annotations.Nullable;

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
        @Nullable TcpClientTransaction tx,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter,
        boolean keepBinary,
        ClientBinaryMarshaller marsh
    ) {
        super(ch, tx, qryOp, pageQryOp, qryWriter);

        this.keepBinary = keepBinary;

        serDes = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override Collection<List<?>> readEntries(PayloadInputChannel payloadCh) {
        BinaryInputStream in = payloadCh.in();

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
