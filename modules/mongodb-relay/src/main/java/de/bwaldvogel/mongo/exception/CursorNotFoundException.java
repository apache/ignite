package de.bwaldvogel.mongo.exception;

public class CursorNotFoundException extends MongoServerError {

    private static final long serialVersionUID = 1L;

    public CursorNotFoundException(long cursorId) {
        super(ErrorCode.CursorNotFound, "Cursor id " + cursorId + " does not exists");
    }
}
