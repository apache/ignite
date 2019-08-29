package de.bwaldvogel.mongo.exception;

public class KeyConstraintError extends MongoServerError {

    private static final long serialVersionUID = 1L;

    KeyConstraintError(int errorCode, String codeName, String message) {
        super(errorCode, codeName, message);
    }
}
