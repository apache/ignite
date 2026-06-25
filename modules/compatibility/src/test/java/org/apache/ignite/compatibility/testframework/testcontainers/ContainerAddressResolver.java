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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.configuration.AddressResolver;

/**
 * Advertises a containerized node by the host-published port so a host-JVM node can reach it on macOS,
 * where container-internal addresses are not routable. External address per bound port is taken from the
 * {@code external.address.<port>} system property (e.g. {@code -Dexternal.address.47500=127.0.0.1:50500}).
 */
public class ContainerAddressResolver implements AddressResolver {
    /** Prefix of the system property that holds the external {@code host:port} for a given bound port. */
    static final String EXT_ADDR_PROP_PREFIX = "external.address.";

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) {
        String ext = System.getProperty(EXT_ADDR_PROP_PREFIX + addr.getPort());

        if (ext == null)
            return Collections.singletonList(addr);

        int sep = ext.lastIndexOf(':');

        return Collections.singletonList(new InetSocketAddress(ext.substring(0, sep), Integer.parseInt(ext.substring(sep + 1))));
    }
}
