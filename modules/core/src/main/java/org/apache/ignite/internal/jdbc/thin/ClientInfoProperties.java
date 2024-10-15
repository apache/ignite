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

package org.apache.ignite.internal.jdbc.thin;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/** */
public final class ClientInfoProperties extends Properties {
    /** Logger. */
    private static final Logger LOG = Logger.getLogger(ClientInfoProperties.class.getName());

    /** */
    public static final String CLIENT_CONTEXT = "ClientContext";

    /** */
    private static final Set<String> AVAILABLE_PROPERTIES = new HashSet<String>() {{
        add(CLIENT_CONTEXT);
    }};

    /** */
    public void setProperties(Properties props) {
        for (String p: AVAILABLE_PROPERTIES) {
            Object v = props.get(p);

            if (v != null)
                put(p, v);
        }

        for (String p: props.stringPropertyNames())
            checkProperty(p);
    }

    /** {@inheritDoc} */
    @Override public synchronized Object setProperty(String key, String val) {
        if (checkProperty(key))
            return put(key, val);

        return null;
    }

    /** */
    private boolean checkProperty(String name) {
        if (!AVAILABLE_PROPERTIES.contains(name)) {
            LOG.warning("Property '" + name + "' is not supported by the driver.");

            return false;
        }

        return true;
    }

    // TODO: list property names with SESSION_ID -> null
}
