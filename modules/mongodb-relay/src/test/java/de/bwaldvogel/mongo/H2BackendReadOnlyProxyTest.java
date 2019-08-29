package de.bwaldvogel.mongo;

import de.bwaldvogel.AbstractReadOnlyProxyTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2BackendReadOnlyProxyTest extends AbstractReadOnlyProxyTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return H2Backend.inMemory();
    }

}
