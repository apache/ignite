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

package org.apache.ignite.internal.management.cache.scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;

/**
 * This format prints cache objects fields in table format.
 */
public class TableCacheScanTaskFormat implements CacheScanTaskFormat {
    /** */
    public static final String NAME = "table";

    /** */
    private List<String> keyTitles;

    /** */
    private List<String> valTitles;

    /** {@inheritDoc} */
    @Override public String name() {
        return NAME;
    }

    /** {@inheritDoc} */
    @Override public List<String> titles(Cache.Entry<Object, Object> first) {
        keyTitles = titles(first.getKey(), DefaultCacheScanTaskFormat.KEY);
        valTitles = titles(first.getValue(), DefaultCacheScanTaskFormat.VALUE);

        List<String> res = new ArrayList<>(keyTitles.size() + valTitles.size());

        res.addAll(keyTitles);
        res.addAll(valTitles);

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<?> row(Cache.Entry<Object, Object> e) {
        List<String> res = new ArrayList<>(keyTitles.size() + valTitles.size());

        res.addAll(columns(e.getKey(), keyTitles, DefaultCacheScanTaskFormat.KEY));
        res.addAll(columns(e.getValue(), valTitles, DefaultCacheScanTaskFormat.VALUE));

        return res;
    }

    /** */
    private List<String> titles(Object o, String dflt) {
        if (o instanceof BinaryObject) {
            BinaryObject b = (BinaryObject)o;

            List<String> flds = new ArrayList<>(b.type().fieldNames());

            Collections.sort(flds);

            return flds;
        }

        return Collections.singletonList(dflt);
    }

    /** */
    private Collection<String> columns(Object o, List<String> titles, String dfltTitle) {
        if (titles.size() == 1 && titles.get(0).equals(dfltTitle))
            return Collections.singletonList(DefaultCacheScanTaskFormat.valueOf(o));

        String[] res = new String[titles.size()];

        if (o instanceof BinaryObject) {
            BinaryObject b = (BinaryObject)o;

            for (int i = 0; i < titles.size(); i++) {
                if (b.hasField(titles.get(i)))
                    res[i] = DefaultCacheScanTaskFormat.valueOf(b.field(titles.get(i)));
            }
        }

        return Arrays.asList(res);
    }
}
