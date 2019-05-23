package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Bson;

public class Missing implements Bson {

    private static final long serialVersionUID = 1L;

    private static final Missing INSTANCE = new Missing();

    private Missing() {
    }

    public static Missing getInstance() {
        return INSTANCE;
    }

    public static Object ofNullable(Object value) {
        if (value == null) {
            return getInstance();
        }
        return value;
    }

    public static boolean isNullOrMissing(Object value) {
        return (value == null || value instanceof Missing);
    }
}
