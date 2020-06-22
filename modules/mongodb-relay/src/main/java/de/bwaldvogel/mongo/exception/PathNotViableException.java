package de.bwaldvogel.mongo.exception;

public class PathNotViableException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public PathNotViableException(String message) {
        super(ErrorCode.PathNotViable, message);
    }

}
