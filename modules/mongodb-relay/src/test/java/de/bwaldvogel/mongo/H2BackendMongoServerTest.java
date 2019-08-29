package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2BackendMongoServerTest extends MongoServerTest {

    @Override
    protected MongoBackend createBackend() throws Exception {
        return H2Backend.inMemory();
    }

}
