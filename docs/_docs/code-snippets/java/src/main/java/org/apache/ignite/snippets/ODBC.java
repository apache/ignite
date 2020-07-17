package org.apache.ignite.snippets;

import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class ODBC {

    void enableODBC() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration();

        clientConnectorCfg.setHost("127.0.0.1");
        clientConnectorCfg.setPort(12345);
        clientConnectorCfg.setPortRange(2);
        clientConnectorCfg.setMaxOpenCursorsPerConnection(512);
        clientConnectorCfg.setSocketSendBufferSize(65536);
        clientConnectorCfg.setSocketReceiveBufferSize(131072);
        clientConnectorCfg.setThreadPoolSize(4);

        cfg.setClientConnectorConfiguration(clientConnectorCfg);
    }
}
