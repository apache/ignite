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

package org.apache.ignite.internal.ducktest.utils.sql;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;


/**
 * Application executing the set of the SQL statements from the script file.
 */
public class SqlClientApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(jsonNode.get("sql_script_name").asText())) {
            if (is != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                reader.lines()
                        .map(String::trim)
                        .filter(sql -> !sql.isEmpty())
                        .forEach(sql -> client.query(new SqlFieldsQuery(sql)).getAll());
            }
        }
        markFinished();
    }
}
