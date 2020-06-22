package de.bwaldvogel.mongo.backend;

import java.util.Objects;

import de.bwaldvogel.mongo.bson.Document;

public class DocumentWithPosition<P> {

    private final Document document;
    private final P position;

    public DocumentWithPosition(Document document, P position) {
        this.document = Objects.requireNonNull(document);
        this.position = Objects.requireNonNull(position);
    }

    public Document getDocument() {
        return document;
    }

    public P getPosition() {
        return position;
    }

}
