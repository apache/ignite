package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.bson.Document;

public class MemoryCollection extends AbstractMongoCollection<Integer> {

    private static final Logger log = LoggerFactory.getLogger(MemoryCollection.class);

    private List<Document> documents = new ArrayList<>();
    private Queue<Integer> emptyPositions = new LinkedList<>();
    private AtomicInteger dataSize = new AtomicInteger();

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        super(databaseName, collectionName, idField);
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        dataSize.addAndGet(sizeDelta);
    }

    @Override
    protected int getDataSize() {
        return dataSize.get();
    }

    @Override
    protected Integer addDocumentInternal(Document document) {
        Integer position = emptyPositions.poll();
        if (position == null) {
            position = Integer.valueOf(documents.size());
        }

        if (position.intValue() == documents.size()) {
            documents.add(document);
        } else {
            documents.set(position.intValue(), document);
        }
        return position;
    }

    @Override
    protected Iterable<Document> matchDocuments(Document query, Iterable<Integer> positions, Document orderBy, int numberToSkip, int numberToReturn) {

        List<Document> matchedDocuments = new ArrayList<>();

        for (Integer position : positions) {
            Document document = getDocument(position);
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        sortDocumentsInMemory(matchedDocuments, orderBy);

        if (numberToSkip > 0) {
            matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    @Override
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
            int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();

        boolean ascending = true;
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == -1) {
                    ascending = false;
                }
            }
        }

        for (Document document : iterateAllDocuments(ascending)) {
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                // already sorted
            } else {
                matchedDocuments.sort(new DocumentComparator(orderBy));
            }
        }

        if (numberToSkip > 0) {
            if (numberToSkip < matchedDocuments.size()) {
                matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
            } else {
                return Collections.emptyList();
            }
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    private static abstract class AbstractDocumentIterator implements Iterator<Document> {

        protected int pos;
        protected final List<Document> documents;
        protected Document current;

        protected AbstractDocumentIterator(List<Document> documents, int pos) {
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

        protected DocumentIterator(List<Document> documents) {
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

        protected ReverseDocumentIterator(List<Document> documents) {
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

    private static class DocumentIterable implements Iterable<Document> {

        private List<Document> documents;

        public DocumentIterable(List<Document> documents) {
            this.documents = documents;
        }

        @Override
        public Iterator<Document> iterator() {
            return new DocumentIterator(documents);
        }

    }

    private static class ReverseDocumentIterable implements Iterable<Document> {

        private List<Document> documents;

        public ReverseDocumentIterable(List<Document> documents) {
            this.documents = documents;
        }

        @Override
        public Iterator<Document> iterator() {
            return new ReverseDocumentIterator(documents);
        }

    }

    private Iterable<Document> iterateAllDocuments(boolean ascending) {
        if (ascending) {
            return new DocumentIterable(documents);
        } else {
            return new ReverseDocumentIterable(documents);
        }
    }

    @Override
    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    @Override
    protected Integer findDocumentPosition(Document document) {
        int position = documents.indexOf(document);
        if (position < 0) {
            return null;
        }
        return Integer.valueOf(position);
    }

    @Override
    protected void removeDocument(Integer position) {
        documents.set(position.intValue(), null);
        emptyPositions.add(position);
    }

    @Override
    protected Document getDocument(Integer position) {
        return documents.get(position.intValue());
    }

    @Override
    protected void handleUpdate(Document document) {
        // noop
    }

}
