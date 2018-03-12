package org.apache.ignite.internal.jdbc.thin;

import org.apache.ignite.configuration.ClientConnectorConfiguration;

/**
 * Host name and port holder. InetSocketAddress isn't used because host addresses are resolved on each connect attempt.
 */
class HostAndPort {
    /** Host. */
    private String host;

    /** Port. */
    private int port = ClientConnectorConfiguration.DFLT_PORT;

    /**
     * @param host Host name.
     */
    public HostAndPort(String host) {
        this.host = host;
    }

    /**
     * @param host Host name.
     * @param port Port
     */
    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * @return Host name.
     */
    public String host() {
        return host;
    }

    /**
     * @param host Host name.
     */
    public void host(String host) {
        this.host = host;
    }

    /**
     * @return Port.
     */
    public int port() {
        return port;
    }

    /**
     * @param port Port.
     */
    public void port(int port) {
        this.port = port;
    }
}
