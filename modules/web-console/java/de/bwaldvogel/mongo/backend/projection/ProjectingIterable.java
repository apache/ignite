package de.bwaldvogel.mongo.backend.projection;

import java.util.Iterator;

import de.bwaldvogel.mongo.bson.Document;

public class ProjectingIterable implements Iterable<Document> {

    private Iterable<Document> iterable;
    private Document fieldSelector;
    private String idField;

    public ProjectingIterable(Iterable<Document> iterable, Document fieldSelector, String idField) {
        this.iterable = iterable;
        this.fieldSelector = fieldSelector;
        this.idField = idField;
    }

    @Override
    public Iterator<Document> iterator() {
        return new ProjectingIterator(iterable.iterator(), fieldSelector, idField);
    }
}
