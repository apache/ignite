package org.apache.ignite.internal.ducktest.tests.thin_client_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

public class ThinClientConnectAndWaitClient extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();

        long startWait = System.currentTimeMillis();

        long stopWait = startWait + 10_000;

        while(stopWait > System.currentTimeMillis()){
            Thread.sleep(10);
        }

        markFinished();
    }
}
