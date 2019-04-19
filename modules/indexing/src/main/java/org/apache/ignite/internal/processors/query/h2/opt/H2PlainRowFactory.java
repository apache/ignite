/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

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
     * @param data Values.
     * @return Row.
     */
    public static Row create(Value... data) {
        switch (data.length) {
            case 0:
                throw new IllegalStateException("Zero columns row.");

            case 1:
                return new H2PlainRowSingle(data[0]);

            case 2:
                return new H2PlainRowPair(data[0], data[1]);

            default:
                return new H2PlainRow(data);
        }
    }

    /** {@inheritDoc} */
    @Override public Row createRow(Value[] data, int memory) {
        return create(data);
    }
}
