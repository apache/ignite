package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.bson.Document;

public class MemoryCollection extends AbstractMongoCollection<Integer> {

    private List<Document> documents = new ArrayList<>();
    private Queue<Integer> emptyPositions = new LinkedList<>();
    private AtomicInteger dataSize = new AtomicInteger();

    public MemoryCollection(MongoDatabase database, String collectionName, String idField) {
        super(database, collectionName, idField);
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
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();

        for (Document document : iterateAllDocuments(orderBy)) {
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        DocumentComparator documentComparator = deriveComparator(orderBy);
        if (documentComparator != null) {
            matchedDocuments.sort(documentComparator);
        }

        return applySkipAndLimit(matchedDocuments, numberToSkip, numberToReturn);
    }

    private Iterable<Document> iterateAllDocuments(Document orderBy) {
        DocumentIterable documentIterable = new DocumentIterable(this.documents);
        if (isNaturalDescending(orderBy)) {
            return documentIterable.reversed();
        } else {
            return documentIterable;
        }
    }

    @Override
    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return documents.isEmpty() || super.isEmpty();
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
    protected Stream<DocumentWithPosition<Integer>> streamAllDocumentsWithPosition() {
        return IntStream.range(0, documents.size())
            .mapToObj(index -> new DocumentWithPosition<>(documents.get(index), index));
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
    protected void handleUpdate(Integer position, Document oldDocument, Document newDocument) {
        // noop
    }

}
