package org.apache.ignite.internal.processors.rest.igfs.model;

import java.io.IOException;
import java.io.InputStream;

public class S3ObjectInputStream extends InputStream {
    private ObjectMetadata metadata;
    private InputStream objectData;

    public S3ObjectInputStream(ObjectMetadata metadata, InputStream objectData) {
        this.metadata = metadata;
        this.objectData = objectData;
    }

    public ObjectMetadata getMetadata() {
        return metadata;
    }

    @Override
    public int read() throws IOException {
        return objectData.read();
    }
}
