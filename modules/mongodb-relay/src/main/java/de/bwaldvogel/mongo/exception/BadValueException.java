package de.bwaldvogel.mongo.exception;

public class BadValueException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public BadValueException(String message) {
        super(2, "BadValue", message);
    }

}
