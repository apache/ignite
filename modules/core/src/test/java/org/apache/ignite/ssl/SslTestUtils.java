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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.AbstractSslTask;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

/** Runs the {@code --ssl} commands the way control.sh does, probes what the transports serve, and places the test stores. */
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
     * @param nodes Nodes to reload certificates on, the command submitted from the first one.
     * @return Report of the failed command, with the whole chain of causes, so that it does not depend on how compute wraps them.
     */
    public static String reloadFailure(IgniteEx... nodes) {
        return X.getFullStackTrace(GridTestUtils.assertThrows(null, () -> reload(nodes), Exception.class, null));
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

    /**
     * @param port Port to connect to; the probe presents node01 and trusts both test authorities.
     * @return Subject of the certificate the node presents on a new TLS connection to that port.
     */
    public static String servedSubject(int port) throws Exception {
        return servedCertificate(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port).getSubjectX500Principal().getName();
    }

    /** @return Discovery port of the node. */
    public static int discoveryPort(IgniteEx node) {
        return ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param name Test store, as {@code tests.properties} names it.
     * @param dest File to replace.
     */
    public static void place(String name, Path dest) throws IOException {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * @param task Task of the command.
     * @param nodes Nodes to run on.
     * @return Report of the command.
     */
    private static String execute(Class<? extends AbstractSslTask> task, IgniteEx... nodes) throws Exception {
        List<UUID> ids = Arrays.stream(nodes).map(n -> n.localNode().id()).collect(Collectors.toList());

        ClusterGroup serversAndClients = nodes[0].cluster();

        return nodes[0].compute(serversAndClients).execute(task, new VisorTaskArgument<>(ids, new NoArg(), false)).result();
    }
}
