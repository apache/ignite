package de.bwaldvogel.mongo.backend.projection;

import java.util.Iterator;

import de.bwaldvogel.mongo.bson.Document;

public class ProjectingIterator implements Iterator<Document> {

    private Iterator<Document> iterator;
    private Document fieldSelector;
    private String idField;

    ProjectingIterator(Iterator<Document> iterator, Document fieldSelector, String idField) {
        this.iterator = iterator;
        this.fieldSelector = fieldSelector;
        this.idField = idField;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Document next() {
        Document document = this.iterator.next();
        return Projection.projectDocument(document, fieldSelector, idField);
    }

    @Override
    public void remove() {
        this.iterator.remove();
    }

}
