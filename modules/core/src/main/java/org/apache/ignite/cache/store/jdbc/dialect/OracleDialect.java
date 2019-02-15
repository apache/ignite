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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;

/**
 * A dialect compatible with the Oracle database.
 */
public class OracleDialect extends BasicJdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
        String cols = mkString(keyCols, ",");

        return String.format("SELECT %1$s FROM (SELECT %1$s, ROWNUM AS rn FROM (SELECT %1$s FROM %2$s ORDER BY %1$s)) WHERE mod(rn, ?) = 0",
            cols, fullTblName);
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols) {
        Collection<String> cols = F.concat(false, keyCols, uniqCols);

        String colsLst = mkString(cols, ", ");

        String selCols = mkString(cols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("? AS %s", col);
            }
        }, "", ", ", "");

        String match = mkString(keyCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("t.%s=v.%s", col, col);
            }
        }, "(", " AND ", ")");

        String setCols = mkString(uniqCols, new C1<String, String>() {
            @Override public String apply(String col) {
                return String.format("t.%s = v.%s", col, col);
            }
        }, "", ", ", "");

        String valuesCols = mkString(cols, new C1<String, String>() {
            @Override public String apply(String col) {
                return "v." + col;
            }
        }, "", ", ", "");

        return String.format("MERGE INTO %s t" +
            " USING (SELECT %s FROM dual) v" +
            "  ON %s" +
            " WHEN MATCHED THEN" +
            "  UPDATE SET %s" +
            " WHEN NOT MATCHED THEN" +
            "  INSERT (%s) VALUES (%s)", fullTblName, selCols, match, setCols, colsLst, valuesCols);
    }
}
