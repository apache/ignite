package de.bwaldvogel.mongo.backend.h2;

import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class H2Backend extends AbstractMongoBackend {

    private static final Logger log = LoggerFactory.getLogger(H2Backend.class);

    private MVStore mvStore;

    public static H2Backend inMemory() {
        MVStore mvStore = MVStore.open(null);
        return new H2Backend(mvStore);
    }

    public void commit() {
        mvStore.commit();
    }

    public H2Backend(MVStore mvStore) {
        this.mvStore = mvStore;

        for (String mapName : mvStore.getMapNames()) {
            if (mapName.startsWith(H2Database.DATABASES_PREFIX)) {
                String fullName = mapName.substring(H2Database.DATABASES_PREFIX.length());
                String databaseName = fullName.substring(0, fullName.indexOf('.'));

                log.info("opening database '{}'", databaseName);
                try {
                    resolveDatabase(databaseName);
                } catch (MongoServerException e) {
                    log.error("Failed to open {}", e);
                }
            }
        }
    }

    public H2Backend(String fileName) {
        this(openMvStore(fileName));
    }

    private static MVStore openMvStore(String fileName) {
        if (fileName == null) {
            log.info("opening in-memory MVStore");
        } else {
            log.info("opening MVStore in '{}'", fileName);
        }
        return MVStore.open(fileName);
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(String databaseName) {
        return new H2Database(databaseName, this, mvStore);
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        mvStore.close();
    }

    public boolean isInMemory() {
        return mvStore.getFileStore() == null;
    }

    @Override
    public String toString() {
        if (isInMemory()) {
            return getClass().getSimpleName() + "[inMemory]";
        } else {
            return getClass().getSimpleName() + "[" + mvStore.getFileStore().getFileName() + "]";
        }
    }
}
