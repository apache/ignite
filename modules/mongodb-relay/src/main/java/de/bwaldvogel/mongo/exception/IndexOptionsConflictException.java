package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.backend.Index;

public class IndexOptionsConflictException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public IndexOptionsConflictException(Index<?> index) {
        super(85, "IndexOptionsConflict",
            "Index with name: " + index.getName() + " already exists with different options");
    }

}
