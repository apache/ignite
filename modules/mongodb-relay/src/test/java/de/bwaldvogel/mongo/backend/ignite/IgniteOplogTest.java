package de.bwaldvogel.mongo.backend.ignite;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractOplogTest;


public class IgniteOplogTest extends AbstractOplogTest {

    @Override
    protected MongoBackend createBackend() throws Exception {        
        return IgniteBackend.inMemory(new MongoPluginConfiguration());
    }

}
