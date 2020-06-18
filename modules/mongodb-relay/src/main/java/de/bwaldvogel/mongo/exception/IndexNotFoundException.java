package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.bson.Document;

public class IndexNotFoundException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public IndexNotFoundException(Document keys) {
        super(27, "IndexNotFound", "can't find index with key: "
            + keys.toString(true, "{ ", " }"));
    }

}
