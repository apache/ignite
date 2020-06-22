package de.bwaldvogel.mongo.backend;

import java.util.List;
import java.util.NoSuchElementException;

import de.bwaldvogel.mongo.bson.Document;

public class EmptyCursor extends Cursor {

    private static final long EMPTY_CURSOR_ID = 0L;

    private static final EmptyCursor INSTANCE = new EmptyCursor();

    private EmptyCursor() {
        super(EMPTY_CURSOR_ID);
    }

    public static EmptyCursor get() {
        return INSTANCE;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        throw new NoSuchElementException();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "()";
    }
}
