/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Simple factory of SQL queries. Just returns preconfigured queries one by one.
 */
public class PredefinedQueriesSupplier implements Supplier<String> {
    /** */
    private final Collection<String> qrys;

    /** */
    private Iterator<String> it;

    /** */
    public PredefinedQueriesSupplier(Collection<String> qrys) {
        assert !qrys.isEmpty();
        this.qrys = qrys;
        it = qrys.iterator();
    }

    /** {@inheritDoc} */
    @Override public synchronized String get() {
        if (!it.hasNext())
            it = qrys.iterator();

        return it.next();
    }
}
