package de.bwaldvogel.mongo.backend;

import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public abstract class Cursor {

    protected final long id;

    protected Cursor(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public abstract boolean isEmpty();

    public abstract List<Document> takeDocuments(int numberToReturn);

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + id + ")";
    }
}
