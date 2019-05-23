package de.bwaldvogel.mongo.exception;

public class TypeMismatchException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public TypeMismatchException(String message) {
        super(14, "TypeMismatch", message);
    }

}
