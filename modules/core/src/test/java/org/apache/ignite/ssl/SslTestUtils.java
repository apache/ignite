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

package org.apache.ignite.ssl;

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.management.ssl.SslTask;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

/** Runs the {@code --ssl} commands the way control.sh does, and probes what the transports serve. */
public class SslTestUtils {
    /** */
    private SslTestUtils() {
        // No-op.
    }

    /**
     * @param nodes Nodes to reload certificates on, the command submitted from the first one.
     * @return Report of the command.
     */
    public static String reload(IgniteEx... nodes) throws Exception {
        return execute(SslReloadTask.class, nodes);
    }

    /**
     * @param nodes Nodes to report, the command submitted from the first one.
     * @return Report of the command.
     */
    public static String status(IgniteEx... nodes) throws Exception {
        return execute(SslStatusTask.class, nodes);
    }

    /**
     * @param log Logger.
     * @param nodes Nodes to reload certificates on, the command submitted from the first one.
     * @return Report of the failed command, with the whole chain of causes, so that it does not depend on how compute wraps them.
     */
    public static String reloadFailure(IgniteLogger log, IgniteEx... nodes) {
        return X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(nodes), Exception.class, null));
    }

    /**
     * @param probe Context to connect with; discovery asks the client for a certificate it trusts.
     * @param port Port to connect to.
     * @return Certificate the node presents on a new TLS connection to that port.
     */
    public static X509Certificate servedCertificate(SSLContext probe, int port) throws Exception {
        try (SSLSocket sock = (SSLSocket)probe.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port)) {
            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }

    /** @return Discovery port of the node. */
    public static int discoveryPort(IgniteEx node) {
        return ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param task Task of the command.
     * @param nodes Nodes to run on.
     * @return Report of the command.
     */
    private static String execute(Class<? extends SslTask> task, IgniteEx... nodes) throws Exception {
        List<UUID> ids = new ArrayList<>();

        for (IgniteEx node : nodes)
            ids.add(node.localNode().id());

        // Over the whole cluster, as the command itself does: the default facade covers server nodes only.
        return nodes[0].compute(nodes[0].cluster()).execute(task, new VisorTaskArgument<>(ids, new NoArg(), false)).result();
    }
}
