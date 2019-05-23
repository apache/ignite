package de.bwaldvogel.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.h2.H2Backend;

public class H2MongoServer extends MongoServer {

    private static final Logger log = LoggerFactory.getLogger(H2MongoServer.class);

    public static void main(String[] args) throws Exception {

        final MongoServer mongoServer;
        if (args.length == 1) {
            String fileName = args[0];
            mongoServer = new H2MongoServer(fileName);
        } else {
            mongoServer = new H2MongoServer();
        }

        mongoServer.bind("localhost", 27017);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down {}", mongoServer);
                mongoServer.shutdownNow();
            }
        });
    }

    public H2MongoServer() {
        super(H2Backend.inMemory());
    }

    public H2MongoServer(String fileName) {
        super(new H2Backend(fileName));
    }
}
