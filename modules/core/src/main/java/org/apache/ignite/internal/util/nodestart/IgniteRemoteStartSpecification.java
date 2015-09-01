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

package org.apache.ignite.internal.util.nodestart;

import java.io.File;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Host data.
 */
public class IgniteRemoteStartSpecification {
    /** Hostname. */
    private final String host;

    /** Port number. */
    private final int port;

    /** Username. */
    private final String uname;

    /** Password. */
    private final String passwd;

    /** Private key file. */
    private final File key;

    /** Private key filename. */
    private final String keyName;

    /** Number of nodes to start. */
    private final int nodes;

    /** Ignite installation folder. */
    private String igniteHome;

    /** Configuration path. */
    private String cfg;

    /** Configuration filename. */
    private String cfgName;

    /** Script path. */
    private String script;

    /** Custom logger. */
    private IgniteLogger logger;

    /** Valid flag */
    private boolean valid;

    /**
     * @param host Hostname.
     * @param port Port number.
     * @param uname Username.
     * @param passwd Password (can be {@code null} if private key authentication is used).
     * @param key Private key file path.
     * @param nodes Number of nodes to start.
     * @param igniteHome Ignite installation folder.
     * @param cfg Configuration path.
     * @param script Script path.
     */
    public IgniteRemoteStartSpecification(@Nullable String host, int port, @Nullable String uname,
        @Nullable String passwd, @Nullable File key, int nodes, @Nullable String igniteHome,
        @Nullable String cfg, @Nullable String script) {
        this(host, port, uname, passwd, key, nodes, igniteHome, cfg, script, null);
    }

    /**
     * @param host Hostname.
     * @param port Port number.
     * @param uname Username.
     * @param passwd Password (can be {@code null} if private key authentication is used).
     * @param key Private key file path.
     * @param nodes Number of nodes to start.
     * @param igniteHome Ignite installation folder.
     * @param cfg Configuration path.
     * @param script Script path.
     * @param logger Custom logger.
     */
    public IgniteRemoteStartSpecification(@Nullable String host, int port, @Nullable String uname,
        @Nullable String passwd, @Nullable File key, int nodes, @Nullable String igniteHome,
        @Nullable String cfg, @Nullable String script, @Nullable IgniteLogger logger) {
        assert port > 0;
        assert nodes > 0;

        this.host = !F.isEmpty(host) ? host : null;
        this.port = port;
        this.uname = !F.isEmpty(uname) ? uname : null;
        this.passwd = !F.isEmpty(passwd) ? passwd : null;
        this.key = key;
        this.nodes = nodes;
        this.igniteHome = !F.isEmpty(igniteHome) ? igniteHome : null;
        this.cfg = !F.isEmpty(cfg) ? cfg : null;
        cfgName = cfg == null ? null : shorten(cfg);
        keyName = key == null ? "" : shorten(key.getAbsolutePath());
        this.script = !F.isEmpty(script) ? script : null;
        this.logger = logger;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof IgniteRemoteStartSpecification)) return false;

        IgniteRemoteStartSpecification that = (IgniteRemoteStartSpecification)o;

        return (host == null ? that.host == null : host.equals(that.host)) &&
            (uname == null ? that.uname == null : uname.equals(that.uname)) &&
            (passwd == null ? that.passwd == null : passwd.equals(that.passwd)) &&
            (key == null ? that.key == null : key.equals(that.key)) &&
            (igniteHome == null ? that.igniteHome == null : igniteHome.equals(that.igniteHome)) &&
            (cfg == null ? that.cfg == null : cfg.equals(that.cfg)) &&
            (script == null ? that.script == null : script.equals(that.script)) &&
            port == that.port && nodes == that.nodes;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = host == null ? 0 : host.hashCode();

        res = 31 * res + (uname == null ? 0 : uname.hashCode());
        res = 31 * res + (passwd == null ? 0 : passwd.hashCode());
        res = 31 * res + (key == null ? 0 : key.hashCode());
        res = 31 * res + (igniteHome == null ? 0 : igniteHome.hashCode());
        res = 31 * res + (cfg == null ? 0 : cfg.hashCode());
        res = 31 * res + (script == null ? 0 : script.hashCode());
        res = 31 * res + port;
        res = 31 * res + nodes;

        return res;
    }

    /**
     * Get filename from path.
     *
     * @param path Path.
     * @return Filename.
     */
    private static String shorten(String path) {
        int idx1 = path.lastIndexOf('/');
        int idx2 = path.lastIndexOf('\\');
        int idx = Math.max(idx1, idx2);

        return idx == -1 ? path : path.substring(idx + 1);
    }

    /**
     * @return Hostname.
     */
    public String host() {
        return host;
    }

    /**
     * @return Port number.
     */
    public int port() {
        return port;
    }

    /**
     * @return Username.
     */
    public String username() {
        return uname;
    }

    /**
     * @return Password.
     */
    public String password() {
        return passwd;
    }

    /**
     * @return Private key file path.
     */
    public File key() {
        return key;
    }

    /**
     * @return Private key file name.
     */
    public String keyName() {
        return keyName;
    }

    /**
     * @return Number of nodes to start.
     */
    public int nodes() {
        return nodes;
    }

    /**
     * @return Ignite installation folder.
     */
    public String igniteHome() {
        return igniteHome;
    }

    /**
     * @return Configuration full path.
     */
    public String configuration() {
        return cfg;
    }

    /**
     * @return Configuration path short version - just file name.
     */
    public String configurationName() {
        return cfgName;
    }

    /**
     * @return Script path.
     */
    public String script() {
        return script;
    }

    /**
     * @return Custom logger.
     */
    public IgniteLogger logger() {
        return logger;
    }

    /**
     * @return Valid flag.
     */
    public boolean valid() {
        return valid;
    }

    /**
     * @param valid Valid flag.
     */
    public void valid(boolean valid) {
        this.valid = valid;
    }

    /**
     * Sets correct separator in paths.
     *
     * @param separator Separator.
     */
    public void fixPaths(char separator) {
        if (igniteHome != null)
            igniteHome = igniteHome.replace('\\', separator).replace('/', separator);

        if (script != null)
            script = script.replace('\\', separator).replace('/', separator);

        if (cfg != null)
            cfg = cfg.replace('\\', separator).replace('/', separator);
    }
}