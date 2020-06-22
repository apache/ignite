package de.bwaldvogel.mongo.exception;

public class InvalidOptionsException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public InvalidOptionsException(String message) {
        super(ErrorCode.InvalidOptions, message);
    }

}
