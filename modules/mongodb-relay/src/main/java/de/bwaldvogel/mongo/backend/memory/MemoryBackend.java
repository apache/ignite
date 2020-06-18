package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.backend.AbstractMongoBackend;

public class MemoryBackend extends AbstractMongoBackend {

    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName) {
        return new MemoryDatabase(this, databaseName);
    }

}
