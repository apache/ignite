package de.bwaldvogel.mongo.backend.ignite;

import java.util.Map;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class IgniteBackend extends AbstractMongoBackend {

    private static final Logger log = LoggerFactory.getLogger(IgniteBackend.class);

    private Ignite mvStore;
    
    private boolean isKeepBinary = true;

    public static IgniteBackend inMemory() {
    	
    	Ignite mvStore = Ignition.start();
       
        return new IgniteBackend(mvStore);
    }

    public void commit() {
    	Transaction tx= mvStore.transactions().tx();
    	if(tx!=null) {
    		tx.commit();
    	}
    }

    public IgniteBackend(Ignite mvStore) {
        this.mvStore = mvStore;

        for (String mapName : mvStore.cacheNames()) {
            if (!mapName.endsWith(IgniteDatabase.INDEX_FLAG) && mapName.indexOf('.')>0) {
                String fullName = mapName;//.substring(IgniteDatabase.INDEX_PREFIX.length());
                //String databaseName = fullName.split("_")[0];
                String databaseName = Utils.firstFragment(fullName);
                log.info("opening database '{}'", databaseName);
                try {
                    resolveDatabase(databaseName);
                } catch (MongoServerException e) {
                    log.error("Failed to open {}", e);
                }
            }
        }
    }

    public IgniteBackend(String fileName) {
        this(openMvStore(fileName));
    }

    private static Ignite openMvStore(String fileName) {
        if (fileName == null) {
            log.info("opening ignite use default config");
        } else {
            log.info("opening ignite use config file '{}'", fileName);
        }
        Ignite mvStore = Ignition.start(fileName);
        return mvStore;
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(String databaseName) {
        return new IgniteDatabase(databaseName, this, mvStore);
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        mvStore.close();
    }

    public boolean isInMemory() {
        return !mvStore.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
    }

    @Override
    public String toString() {
        if (isInMemory()) {
            return getClass().getSimpleName() + "[inMemory]";
        } else {
            return getClass().getSimpleName() + "[" + mvStore.name() + "]";
        }
    }

	public boolean isKeepBinary() {
		return isKeepBinary;
	}

	public void setKeepBinary(boolean isKeepBinary) {
		this.isKeepBinary = isKeepBinary;
	}
	
}
