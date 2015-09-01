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
 * A dialect compatible with the MySQL database.
 */
public class MySQLDialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols,
        Collection<String> uniqCols) {

        Collection<String> cols = F.concat(false, keyCols, uniqCols);

        String updPart = mkString(uniqCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("%s = VALUES(%s)", col, col);
            }
        }, "", ", ", "");

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", fullTblName,
            mkString(cols, ", "), repeat("?", cols.size(), "", ",", ""), updPart);
    }
}