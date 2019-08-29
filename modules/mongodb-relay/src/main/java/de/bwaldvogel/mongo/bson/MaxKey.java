package de.bwaldvogel.mongo.bson;

public class MaxKey implements Bson {

    private static final long serialVersionUID = 1L;

    private static final MaxKey INSTANCE = new MaxKey();

    private MaxKey() {
    }

    public static MaxKey getInstance() {
        return INSTANCE;
    }

}
