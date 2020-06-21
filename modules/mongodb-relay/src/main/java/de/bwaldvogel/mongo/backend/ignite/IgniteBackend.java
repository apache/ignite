package de.bwaldvogel.mongo.backend.ignite;

import java.util.Map;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

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
    
    private boolean isKeepBinary = !true;
    
    long oldVersion = System.nanoTime();

    public static IgniteBackend inMemory() {
    	
    	Ignite mvStore = Ignition.start();
    	
        return new IgniteBackend(mvStore);
    }
    
	public void commit() {      
        long newVersion = System.nanoTime();
        log.debug("Committed MVStore (v: {} �� {})", oldVersion, newVersion);
    }

    public IgniteBackend(Ignite mvStore) {
        this.mvStore = mvStore;
        String databaseName = mvStore.name();
        if(databaseName==null || databaseName.isEmpty()) {
        	databaseName = IgniteDatabase.DEFAULT_DB_NAME;
        }
        log.info("opening database '{}'", databaseName);
        try {
            resolveDatabase(databaseName);
        } catch (MongoServerException e) {
            log.error("Failed to open {}", e);
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
    	String gridName = databaseName;
    	if(databaseName!=null && databaseName.equalsIgnoreCase(IgniteDatabase.DEFAULT_DB_NAME)) {
    		gridName = null;
    		databaseName = IgniteDatabase.DEFAULT_DB_NAME;
    	}
    	if(databaseName!=null && databaseName.isEmpty()) {
    		gridName = null;
    		databaseName = IgniteDatabase.DEFAULT_DB_NAME;
    	}    	
    	Ignite mvStore = Ignition.ignite(gridName);
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
