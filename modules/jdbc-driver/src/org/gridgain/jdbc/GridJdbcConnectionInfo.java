/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc;

/**
 * Connection properties.
 */
class GridJdbcConnectionInfo {
    /** URL. */
    private final String url;

    /** Hostname. */
    private String hostname;

    /** Port number. */
    private int port;

    /** Cache name. */
    private String cacheName;

    /**
     * @param url URL.
     */
    GridJdbcConnectionInfo(String url) {
        this.url = url;
    }

    /**
     * @return URL.
     */
    public String url() {
        return url;
    }

    /**
     * @return Hostname.
     */
    public String hostname() {
        return hostname;
    }

    /**
     * @param hostname Hostname.
     */
    public void hostname(String hostname) {
        this.hostname = hostname;
    }

    /**
     * @return Port number.
     */
    public int port() {
        return port;
    }

    /**
     * @param port Port number.
     */
    public void port(int port) {
        this.port = port;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}
