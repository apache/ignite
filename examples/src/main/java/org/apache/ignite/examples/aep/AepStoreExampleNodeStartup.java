package org.apache.ignite.examples.aep;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

/**
 * @see org.apache.ignite.examples.persistentstore.PersistentStoreExampleNodeStartup
 */
public class AepStoreExampleNodeStartup {
    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.setAepStore(args[0]);
        Ignite ignite = Ignition.start("examples/config/example-ignite.xml");
    }
}
