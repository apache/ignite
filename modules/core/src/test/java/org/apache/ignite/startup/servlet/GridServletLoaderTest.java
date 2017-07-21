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

package org.apache.ignite.startup.servlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Servlet loader test.
 *
 * 1. Create folder where all Ignite jar files will be placed.
 * For example: /home/ggdev/apache-tomcat-6.0.14/ignite
 *
 * 2. Add in {@code $TOMCAT_HOME/conf/catalina.properties} for property {@code common.loader}
 * value {@code ,${catalina.home}/ignite/*.jar}
 * For example, {@code common.loader=${catalina.home}/lib,${catalina.home}/lib/*.jar,${catalina.home}/ignite/*.jar}
 *
 * 3. Add in {@code $TOMCAT_HOME/conf/web.xml}
 *          <pre class="snippet">
 *          &lt;servlet&gt;
 *              &lt;servlet-name&gt;Ignite&lt;/servlet-name&gt;
 *              &lt;servlet-class&gt;org.apache.ignite.loaders.servlet.GridServletLoader&lt;/servlet-class&gt;
 *              &lt;init-param&gt;
 *                  &lt;param-name&gt;cfgFilePath&lt;/param-name&gt;
 *                  &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 *              &lt;/init-param&gt;
 *              &lt;load-on-startup&gt;5&lt;/load-on-startup&gt;
 *          &lt;/servlet&gt;</pre>
 *
 * 4. Change ports in {@code $TOMCAT_HOME/conf/server.xml} to 8006, 8084, 8446.
 *
 * 5. Add in {@code $TOMCAT_HOME/bin/catalina.sh} where script {@code start} argument handled
 * {@code JAVA_OPTS="${JAVA_OPTS} "-Dcom.sun.management.jmxremote.port=1097" "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.authenticate=false" "}
 */
@GridCommonTest(group = "Loaders")
public class GridServletLoaderTest extends GridCommonAbstractTest {
    /** */
    public static final int JMX_RMI_CONNECTOR_PORT = 1097;

    /** */
    public static final int WAIT_DELAY = 5000;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked"})
    public void testLoader() throws Exception {
        JMXConnector jmx = null;

        try {
            while (true) {
                try {
                    jmx = getJMXConnector("localhost",
                        Integer.valueOf(GridTestProperties.getProperty("tomcat.jmx.rmi.connector.port")));

                    if (jmx != null)
                        break;
                }
                catch (IOException e) {
                    log().warning("Failed to connect to server (will try again).", e);
                }

                Thread.sleep(WAIT_DELAY);
            }

            assert jmx != null;

            String query = "*:*";

            ObjectName queryName = new ObjectName(query);

            boolean found = false;

            ObjectName kernal = null;

            int i = 0;

            while (found == false) {
                info("Attempt to find GridKernal MBean [num=" + i + ']');

                Set<ObjectName> names = jmx.getMBeanServerConnection().queryNames(queryName, null);

                if (names.isEmpty() == false) {
                    for (ObjectName objectName : names) {
                        info("Found MBean for node: " + objectName);

                        String kernalName = objectName.getKeyProperty("name");

                        if ("GridKernal".equals(kernalName)) {
                            kernal = objectName;

                            found = true;
                        }
                    }
                }

                if (kernal == null) {
                    System.out.println("Node GridKernal MBean was not found.");

                    Thread.sleep(WAIT_DELAY);
                }

                i++;
            }

            UUID nodeId = (UUID)jmx.getMBeanServerConnection().getAttribute(kernal, "LocalNodeId");

            assert nodeId != null : "Failed to get Grid nodeId.";

            info("Found grid node with id: " + nodeId);
        }
        finally {
            if (jmx != null) {
                try {
                    jmx.close();

                    info("JMX connection closed.");
                }
                catch (IOException e) {
                    System.out.println("Failed to close JMX connection (will ignore): " + e.getMessage());
                }
            }
        }
    }

    /**
     * @param host JMX host.
     * @param port JMX port.
     * @return JMX connector.
     * @throws IOException If failed.
     */
    private static JMXConnector getJMXConnector(String host, int port) throws IOException {
        assert host != null;
        assert port > 0;

        JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ':' + port + "/jmxrmi");

        Map<String, Object> props = new HashMap<>();

        props.put(JMXConnectorFactory.PROTOCOL_PROVIDER_PACKAGES, "com.sun.jmx.remote.protocol");

        System.out.println("Try to connect to JMX server [props=" + props + ", url=" + serviceURL + ']');

        return JMXConnectorFactory.connect(serviceURL, props);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        // Wait for 5 minutes.
        return 5 * 60 * 1000;
    }
}