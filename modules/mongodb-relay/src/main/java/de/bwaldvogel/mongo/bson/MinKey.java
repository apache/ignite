package de.bwaldvogel.mongo.bson;

import java.io.ObjectStreamException;

public class MinKey implements Bson {

    private static final long serialVersionUID = 1L;

    private static final MinKey INSTANCE = new MinKey();

    private MinKey() {
    }

    public static MinKey getInstance() {
        return INSTANCE;
    }
    
    protected Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }

}
