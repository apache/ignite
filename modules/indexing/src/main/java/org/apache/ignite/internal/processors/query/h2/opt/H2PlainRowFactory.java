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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Collection;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.value.Value;

/**
 * Plain row factory.
 */
public class H2PlainRowFactory extends RowFactory {
    /**
     * @param v Value.
     * @return Row.
     */
    public static Row create(Value v) {
        return new H2PlainRowSingle(v);
    }

    /**
     * @param colCnt Requested column count. H2 engine can add extra columns at the end of result set.
     * @param data Values.
     * @return Row.
     * @see ResultInterface#getVisibleColumnCount()
     * @see GridH2ValueMessageFactory#toMessages(Collection, int)
     * @see ResultInterface#currentRow()
     */
    public static Row create(int colCnt, Value... data) {
        switch (colCnt) {
            case 0:
                throw new IllegalStateException("Zero columns row.");

            case 1:
                return new H2PlainRowSingle(data[0]);

            case 2:
                return new H2PlainRowPair(data[0], data[1]);

            default:
                return new H2PlainRow(colCnt, data);
        }
    }

    /** {@inheritDoc} */
    @Override public Row createRow(Value[] data, int memory) {
        return create(data.length, data);
    }
}
