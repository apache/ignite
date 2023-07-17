package de.bwaldvogel.mongo.backend.ignite;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.MongoServerException;


public class IgniteBackend extends AbstractMongoBackend {

    private static final Logger log = LoggerFactory.getLogger(IgniteBackend.class);

    private final Ignite admin; // admin store
    
    private final MongoPluginConfiguration cfg;
    
    private boolean isKeepBinary = true;
    
    long oldVersion = System.nanoTime();

    public static IgniteBackend inMemory(MongoPluginConfiguration cfg) {
    	
    	Ignite mvStore = Ignition.start();
    	
        return new IgniteBackend(mvStore,cfg);
    }
    
	public void commit() {      
        long newVersion = System.nanoTime();
        log.debug("Committed MVStore (old: {} new: {})", oldVersion, newVersion);
    }

    public IgniteBackend(Ignite mvStore,MongoPluginConfiguration cfg) {
        this.admin = mvStore;
        this.cfg = cfg;        
    }

    public IgniteBackend(String fileName,MongoPluginConfiguration cfg) {
        this(openMvStore(fileName),cfg);
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
    	try {
	    	Ignite mvStore = Ignition.ignite(gridName);
	        return new IgniteDatabase(databaseName, this, mvStore, this.getCursorRegistry());
        
    	}
    	catch(Exception e) {
    		throw new MongoServerException(String.format("Database %s not install!",databaseName),e);
    	}
    }
    
    
    protected Set<String> listDatabaseNames() {
        return Ignition.allGrids().stream().map(Ignite::name).collect(Collectors.toSet());
    }

    @Override
    public void close() {
        log.info("closing {}", this);
        
        //Ignition.stopAll(false);
    }

    public boolean isInMemory() {
        return !admin.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
    }

    @Override
    public String toString() {
        if (isInMemory()) {
            return getClass().getSimpleName() + "[inMemory]";
        } else {
            return getClass().getSimpleName() + "[" + admin.name() + "]";
        }
    }

	public boolean isKeepBinary() {
		return isKeepBinary;
	}

	public void setKeepBinary(boolean isKeepBinary) {
		this.isKeepBinary = isKeepBinary;
	}

	public MongoPluginConfiguration getCfg() {
		return cfg;
	}
	
}
