package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;

public interface QueryMatcher {

    boolean matches(Document document, Document query);

    boolean matchesValue(Object queryValue, Object value);

    Integer matchPosition(Document document, Document query);

}
