package org.apache.ignite.snippets.k8s;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class K8s {

    public static void connectThinClient() throws Exception {
        // tag::connectThinClient[]
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("13.86.186.145:10800");
        IgniteClient client = Ignition.startClient(cfg);

        ClientCache<Integer, String> cache = client.getOrCreateCache("test_cache");

        cache.put(1, "first test value");

        System.out.println(cache.get(1));

        client.close();
        // end::connectThinClient[]
    }
}
