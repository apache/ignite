package org.apache.ignite.cache.store.cassandra.bean;

import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.cassandra.service.CassandraDaemon;

/**
 * Created by Denis_Medvedev1 on 3/31/2016.
 */
public class CassandraLifeCycleBean implements LifecycleBean {

    private CassandraDaemon cassandraDaemon;

    private String jmxPort;
    private String cassandraConfigFile;

    /** */
    private void startEmbededCassandra() {
        try {
            System.setProperty("cassandra.jmx.local.port", jmxPort);
            System.setProperty("cassandra.config", "file:///" + cassandraConfigFile);
            cassandraDaemon = new CassandraDaemon(true);
            cassandraDaemon.init(null);
            cassandraDaemon.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded Cassandra", e);
        }
    }

    /** */
    private void stopEmbededCassandra() {
        cassandraDaemon.deactivate();
    }

    /** */
    public String getJmxPort() {
        return jmxPort;
    }

    /** */
    public void setJmxPort(String jmxPort) {
        this.jmxPort = jmxPort;
    }

    /** */
    public String getCassandraConfigFile() {
        return cassandraConfigFile;
    }

    /** */
    public void setCassandraConfigFile(String cassandraConfigFile) {
        this.cassandraConfigFile = cassandraConfigFile;
    }

    /** */
    @Override
    public void onLifecycleEvent(LifecycleEventType evt) {
        if (evt == LifecycleEventType.BEFORE_NODE_START) {
            startEmbededCassandra();
        } else if (evt == LifecycleEventType.BEFORE_NODE_STOP) {
            stopEmbededCassandra();
        }
    }
}
