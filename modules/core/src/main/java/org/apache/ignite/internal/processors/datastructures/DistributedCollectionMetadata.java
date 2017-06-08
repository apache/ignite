package org.apache.ignite.internal.processors.datastructures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.CollectionConfiguration;

public class DistributedCollectionMetadata extends AtomicDataStructureValue {
    /** */
    private static final long serialVersionUID = 0L;

    private DataStructureType type;

    private CollectionConfiguration cfg;

    private String cacheName;

    public DistributedCollectionMetadata(DataStructureType type, CollectionConfiguration cfg, String cacheName) {
        if (cfg == null)
            System.out.println("???");

        this.type = type;
        this.cfg = cfg;
        this.cacheName = cacheName;
    }

    public DistributedCollectionMetadata() {
    }

    @Override public DataStructureType type() {
        return type;
    }

    public CollectionConfiguration configuration() {
        return cfg;
    }

    public String cacheName() {
        return cacheName;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(type);
        out.writeObject(cfg);
        out.writeObject(cacheName);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = (DataStructureType)in.readObject();
        cfg = (CollectionConfiguration) in.readObject();
        cacheName = (String) in.readObject();
    }
}
