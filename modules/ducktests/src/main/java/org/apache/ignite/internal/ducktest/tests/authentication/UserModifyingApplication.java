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

package org.apache.ignite.internal.ducktest.tests.authentication;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.rest.GridRestCommand;

/**
 * Simple application that modify users.
 */
public class UserModifyingApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(final JsonNode jsonNode) throws IgniteCheckedException {
        String restKey = jsonNode.get("rest_key").asText();

        String name = jsonNode.get("username").asText();

        String pwd = jsonNode.get("password").asText();

        markInitialized();

        GridRestCommand cmd = GridRestCommand.fromKey(restKey);

        switch (cmd) {
            case ADD_USER:
                client.query(new SqlFieldsQuery(String.format("CREATE USER \"%s\" WITH PASSWORD '%s';", name, pwd))).getAll();
                break;

            case UPDATE_USER:
                client.query(new SqlFieldsQuery(String.format("ALTER USER \"%s\" WITH PASSWORD '%s';", name, pwd))).getAll();
                break;

            case REMOVE_USER:
                client.query(new SqlFieldsQuery(String.format("DROP USER \"%s\";", name))).getAll();
                break;

            default:
                throw new IgniteCheckedException("Unknown operation: " + cmd + ".");
        }

        markFinished();
    }
}
