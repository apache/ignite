package de.bwaldvogel.mongo.bson;

public class MinKey implements Bson {

    private static final long serialVersionUID = 1L;

    private static final MinKey INSTANCE = new MinKey();

    private MinKey() {
    }

    public static MinKey getInstance() {
        return INSTANCE;
    }

}
