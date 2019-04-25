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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.F;

/**
 * A dialect compatible with the H2 database.
 */
public class H2Dialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols) {
        Collection<String> cols = F.concat(false, keyCols, uniqCols);

        return String.format("MERGE INTO %s (%s) KEY (%s) VALUES(%s)", fullTblName, mkString(cols, ","),
            mkString(keyCols, ","), repeat("?", cols.size(), "", ", ", ""));
    }
}
