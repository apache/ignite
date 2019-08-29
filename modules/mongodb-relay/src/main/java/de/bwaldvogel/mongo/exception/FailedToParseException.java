package de.bwaldvogel.mongo.exception;

public class FailedToParseException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public FailedToParseException(String message) {
        super(9, "FailedToParse", message);
    }

}
