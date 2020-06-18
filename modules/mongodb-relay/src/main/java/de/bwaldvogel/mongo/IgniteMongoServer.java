package de.bwaldvogel.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import io.netty.util.Signal;

public class IgniteMongoServer extends MongoServer {

    private static final Logger log = LoggerFactory.getLogger(IgniteMongoServer.class);

    public static void main(String[] args) throws Exception {
    	    	
        final MongoServer mongoServer;
        if (args.length >= 1) {
            String fileName = args[0];
            mongoServer = new IgniteMongoServer(fileName);
        } else {
            mongoServer = new IgniteMongoServer();
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down {}", mongoServer);
                mongoServer.shutdownNow();
            }
        });

        mongoServer.bind("0.0.0.0", 27018);
        
    }

    public IgniteMongoServer() {
        super(IgniteBackend.inMemory());
    }

    public IgniteMongoServer(String fileName) {
        super(new IgniteBackend(fileName));
    }
}
