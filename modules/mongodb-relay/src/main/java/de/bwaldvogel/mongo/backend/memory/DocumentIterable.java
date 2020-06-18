package de.bwaldvogel.mongo.backend.memory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import de.bwaldvogel.mongo.bson.Document;

class DocumentIterable implements Iterable<Document> {

    private final List<Document> documents;
    private final boolean reversed;

    DocumentIterable(List<Document> documents) {
        this(documents, false);
    }

    private DocumentIterable(List<Document> documents, boolean reversed) {
        this.documents = documents;
        this.reversed = reversed;
    }

    DocumentIterable reversed() {
        return new DocumentIterable(documents, !reversed);
    }

    @Override
    public Iterator<Document> iterator() {
        if (!reversed) {
            return new DocumentIterator(documents);
        } else {
            return new ReverseDocumentIterator(documents);
        }
    }

    private abstract static class AbstractDocumentIterator implements Iterator<Document> {

        int pos;
        final List<Document> documents;
        Document current;

        AbstractDocumentIterator(List<Document> documents, int pos) {
            this.documents = documents;
            this.pos = pos;
        }

        protected abstract Document getNext();

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = getNext();
            }
            return (current != null);
        }

        @Override
        public Document next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Document document = current;
            current = getNext();
            return document;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static class DocumentIterator extends AbstractDocumentIterator {

        private DocumentIterator(List<Document> documents) {
            super(documents, 0);
        }

        @Override
        protected Document getNext() {
            while (pos < documents.size()) {
                Document document = documents.get(pos++);
                if (document != null) {
                    return document;
                }
            }
            return null;
        }

    }

    private static class ReverseDocumentIterator extends AbstractDocumentIterator {

        private ReverseDocumentIterator(List<Document> documents) {
            super(documents, documents.size() - 1);
        }

        @Override
        protected Document getNext() {
            while (pos >= 0) {
                Document document = documents.get(pos--);
                if (document != null) {
                    return document;
                }
            }
            return null;
        }

    }
}
