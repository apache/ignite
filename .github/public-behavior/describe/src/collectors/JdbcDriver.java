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
package collectors;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.Properties;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the connection properties of the JDBC thin driver as the driver gives them to tools: name, whether the property is
 * required, its default and the values it accepts. Descriptions are left out.
 */
public class JdbcDriver implements Collector {
    /** URL with no properties, so the driver reports their defaults. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        DriverPropertyInfo[] props = new IgniteJdbcThinDriver().getPropertyInfo(URL, new Properties());

        for (DriverPropertyInfo p : props) {
            String[] choices = p.choices == null ? new String[0] : p.choices.clone();

            Arrays.sort(choices);

            out.add("Configuration", "JDBC driver property " + p.name, (p.required ? "required" : "optional")
                + (p.value == null ? "" : ", default " + p.value) + (choices.length == 0 ? "" : ", values " + String.join(", ", choices)),
                "", tree.where(ConnectionPropertiesImpl.class, p.name.substring(p.name.lastIndexOf('.') + 1), -1, false));
        }

        if (props.length == 0)
            out.error("The JDBC driver reports no connection properties");
    }
}
