package de.bwaldvogel.mongo.exception;

public class NamespaceExistsException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public NamespaceExistsException(String message) {
        super(ErrorCode.NamespaceExists, message);
    }

}
