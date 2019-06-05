package de.bwaldvogel.mongo.exception;

public class ImmutableFieldException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public ImmutableFieldException(String message) {
        super(66, "ImmutableField", message);
    }

}
