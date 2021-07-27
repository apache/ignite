package org.apache.ignite.internal.ducktest.tests.thin_client_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ThinClientConnectAndWaitClient extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();
        
        long stopWait = System.currentTimeMillis() + 10_000;

        while(stopWait > System.currentTimeMillis()){
            TimeUnit.SECONDS.sleep(1);
        }

        markFinished();
    }
}
