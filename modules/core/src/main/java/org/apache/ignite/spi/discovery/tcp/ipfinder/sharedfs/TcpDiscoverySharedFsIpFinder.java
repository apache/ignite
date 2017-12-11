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

package org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

/**
 * Shared filesystem-based IP finder. <h1 class="header">Configuration</h1> <h2 class="header">Mandatory</h2> There are
 * no mandatory configuration parameters. <h2 class="header">Optional</h2> <ul> <li>Path (see {@link
 * #setPath(String)})</li> <li>Shared flag (see {@link #setShared(boolean)})</li> </ul> <p> If {@link #getPath()} is not
 * provided, then {@link #DFLT_PATH} will be used and only local nodes will discover each other. To enable discovery
 * over network you must provide a path to a shared directory explicitly. <p> The directory will contain empty files
 * named like the following 192.168.1.136#1001. <p> Note that this finder is shared by default (see {@link
 * org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder#isShared()}.
 */
public class TcpDiscoverySharedFsIpFinder extends TcpDiscoveryIpFinderAdapter {
    /**
     * Default path for discovering of local nodes (testing only). Note that this path is relative to {@code
     * IGNITE_HOME/work} folder if {@code IGNITE_HOME} system or environment variable specified, otherwise it is
     * relative to {@code work} folder under system {@code java.io.tmpdir} folder.
     *
     * @see org.apache.ignite.configuration.IgniteConfiguration#getWorkDirectory()
     */
    public static final String DFLT_PATH = "disco/tcp";

    /** Delimiter to use between address and port tokens in file names. */
    public static final String DELIM = "#";

    /** IPv6 colon delimiter. */
    private static final String COLON_DELIM = ":";

    /** IPv6 colon substitute. */
    private static final String COLON_SUBST = "_";

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** File-system path. */
    private String path = DFLT_PATH;

    /** Folder to keep items in. */
    @GridToStringExclude
    private File folder;

    /** Warning guard. */
    @GridToStringExclude
    private final AtomicBoolean warnGuard = new AtomicBoolean();

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     * Constructor.
     */
    public TcpDiscoverySharedFsIpFinder() {
        setShared(true);
    }

    /**
     * Gets path.
     *
     * @return Shared path.
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets path.
     *
     * @param path Shared path.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySharedFsIpFinder setPath(String path) {
        this.path = path;

        return this;
    }

    /**
     * Initializes folder to work with.
     *
     * @return Folder.
     * @throws org.apache.ignite.spi.IgniteSpiException If failed.
     */
    private File initFolder() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (path == null)
                throw new IgniteSpiException("Shared file system path is null " +
                    "(it should be configured via setPath(..) configuration property).");

            if (path.equals(DFLT_PATH) && warnGuard.compareAndSet(false, true))
                U.warn(log, "Default local computer-only share is used by IP finder.");

            try {
                File tmp;

                if (new File(path).exists())
                    tmp = new File(path);
                else {
                    try {
                        tmp = U.resolveWorkDirectory(ignite.configuration().getWorkDirectory(), path, false);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteSpiException("Failed to resolve directory [path=" + path +
                            ", exception=" + e.getMessage() + ']');
                    }
                }

                if (!tmp.isDirectory())
                    throw new IgniteSpiException("Failed to initialize shared file system path " +
                        "(path must point to folder): " + path);

                if (!tmp.canRead() || !tmp.canWrite())
                    throw new IgniteSpiException("Failed to initialize shared file system path " +
                        "(path must be readable and writable): " + path);

                folder = tmp;
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (folder == null)
                throw new IgniteSpiException("Failed to initialize shared file system folder (check logs for errors).");
        }

        return folder;
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        initFolder();

        Collection<InetSocketAddress> addrs = new LinkedList<>();

        for (String fileName : folder.list()) {
            StringTokenizer st = new StringTokenizer(fileName, DELIM);

            if (st.countTokens() != 2)
                continue;

            String addrStr = st.nextToken();
            String portStr = st.nextToken();

            try {
                int port = Integer.parseInt(portStr);

                addrs.add(new InetSocketAddress(denormalizeAddress(addrStr), port));
            }
            catch (IllegalArgumentException e) {
                U.error(log, "Failed to parse file entry: " + fileName, e);
            }
        }

        return Collections.unmodifiableCollection(addrs);
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initFolder();

        try {
            for (String name : distinctNames(addrs)) {
                File file = new File(folder, name);

                file.createNewFile();
            }
        }
        catch (IOException e) {
            throw new IgniteSpiException("Failed to create file.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        initFolder();

        try {
            for (String name : distinctNames(addrs)) {
                File file = new File(folder, name);

                if (!file.delete())
                    throw new IgniteSpiException("Failed to delete file " + file.getName());
            }
        }
        catch (SecurityException e) {
            throw new IgniteSpiException("Failed to delete file.", e);
        }
    }

    /**
     * Returns set with unique names.
     *
     * @param addresses List of addresses.
     * @return Set of addresses.
     */
    private Iterable<String> distinctNames(Iterable<InetSocketAddress> addresses) {
        Set<String> result = new HashSet<>();

        for (InetSocketAddress address : addresses) {
            result.add(name(address));
        }

        return result;
    }

    /**
     * Creates file name for address.
     *
     * @param addr Node address.
     * @return Name.
     */
    private String name(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        // There is no need to normalize hostname as DNS name specification doesn't allow ':' and '_' chars.
        sb.a(addr.isUnresolved() ? addr.getHostName() : normalizeAddress(addr.getAddress().getHostAddress()))
            .a(DELIM)
            .a(addr.getPort());

        return sb.toString();
    }

    /**
     * Normalizes the host address by substituting colon delimiter with underscore.
     *
     * @param hostAddress Host address.
     * @return Normalized host address that can be safely used in file names.
     */
    private String normalizeAddress(String hostAddress) {
        return hostAddress.replaceAll(COLON_DELIM, COLON_SUBST);
    }

    /**
     * Reverts changes done with {@link TcpDiscoverySharedFsIpFinder#normalizeAddress}.
     *
     * @param hostAddress Host address.
     * @return Standard host address.
     */
    private String denormalizeAddress(String hostAddress) {
        return hostAddress.replaceAll(COLON_SUBST, COLON_DELIM);
    }

    /** {@inheritDoc} */
    @Override public TcpDiscoverySharedFsIpFinder setShared(boolean shared) {
        super.setShared(shared);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoverySharedFsIpFinder.class, this);
    }
}
