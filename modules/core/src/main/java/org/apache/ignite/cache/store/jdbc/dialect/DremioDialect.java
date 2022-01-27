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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;

/**
 * A dialect compatible with the Dremio database.
 */
public class DremioDialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public DremioDialect() {
        // Workaround for known issue with MySQL large result set.
        // See: http://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
        fetchSize = Integer.MIN_VALUE;
    }

    /** {@inheritDoc} */
    @Override public String escape(String ident) {
        return '"' + ident + '"';
    }

    /** {@inheritDoc} */
    @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
        String cols = mkString(keyCols, ",");        

        return String.format("SELECT %1$s FROM (SELECT %1$s, ROW_NUMBER() OVER() AS rn FROM (SELECT %1$s FROM %2$s ORDER BY %1$s) AS tbl) AS tbl WHERE mod(rn, ?) = 0",
                cols, fullTblName);
    }

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
