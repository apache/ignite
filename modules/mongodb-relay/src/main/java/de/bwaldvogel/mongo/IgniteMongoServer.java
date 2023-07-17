package de.bwaldvogel.mongo;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;


public class IgniteMongoServer extends MongoServer {

    private static final Logger log = LoggerFactory.getLogger(IgniteMongoServer.class);

    public static void main(String[] args) throws Exception {
    	MongoPluginConfiguration cfg = new MongoPluginConfiguration();
        final MongoServer mongoServer;
        if (args.length >= 1) {
            String fileName = args[0];
            mongoServer = new IgniteMongoServer(fileName,cfg);
        } else {
            mongoServer = new IgniteMongoServer(cfg);
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down {}", mongoServer);
                mongoServer.shutdownNow();
            }
        });

        mongoServer.bind(cfg.getHost(), cfg.getPort());
        
    }

    public IgniteMongoServer(MongoPluginConfiguration cfg) {
        super(IgniteBackend.inMemory(cfg));
    }

    public IgniteMongoServer(String fileName,MongoPluginConfiguration cfg) {
        super(new IgniteBackend(fileName,cfg));
    }
}
