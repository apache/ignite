package org.apache.ignite.examples.aep;

import org.apache.ignite.Ignition;

/**
 * @see org.apache.ignite.examples.persistentstore.PersistentStoreExampleNodeStartup
 */
public class ServerNode {
    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.setAepStore("/mnt/mem/" + CacheApiClientExample.class.getSimpleName());
        Ignition.start("examples/config/example-ignite.xml");
        //Ignition.start("examples/config/persistentstore/example-persistent-store.xml");

    }
}
