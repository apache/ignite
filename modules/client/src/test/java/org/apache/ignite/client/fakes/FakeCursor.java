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

package org.apache.ignite.client.fakes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;

public class FakeCursor implements SqlCursor<List<?>> {

    private final Random random;

    FakeCursor() {
        random = new Random();
    }

    @Override public void close() throws Exception {

    }

    @Override public Iterator<List<?>> iterator() {
        return null;
    }

    @Override public boolean hasNext() {
        return true;
    }

    @Override public List<?> next() {
        List<Object> result = new ArrayList<>();
        result.add(random.nextInt());
        result.add(random.nextLong());
        result.add(random.nextFloat());
        result.add(random.nextDouble());
        result.add(UUID.randomUUID().toString());
        result.add(null);

        return result;
    }

    @Override public SqlQueryType getQueryType() {
        return SqlQueryType.QUERY;
    }
}
