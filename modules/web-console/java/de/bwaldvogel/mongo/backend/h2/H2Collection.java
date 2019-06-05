package de.bwaldvogel.mongo.backend.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

public class H2Collection extends AbstractMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(H2Collection.class);

    private final MVMap<Object, Document> dataMap;
    private final MVMap<String, Object> metaMap;

    private static final String DATA_SIZE_KEY = "dataSize";

    public H2Collection(String databaseName, String collectionName, String idField, MVMap<Object, Document> dataMap, MVMap<String, Object> metaMap) {
        super(databaseName, collectionName, idField);
        this.dataMap = dataMap;
        this.metaMap = metaMap;
        if (!this.metaMap.containsKey(DATA_SIZE_KEY)) {
            this.metaMap.put(DATA_SIZE_KEY, Long.valueOf(0));
        } else {
            log.debug("dataSize of {}: {}", getFullName(), getDataSize());
        }
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        synchronized (metaMap) {
            Number value = (Number) metaMap.get(DATA_SIZE_KEY);
            Long newValue = Long.valueOf(value.longValue() + sizeDelta);
            metaMap.put(DATA_SIZE_KEY, newValue);
        }
    }

    @Override
    protected int getDataSize() {
        Number value = (Number) metaMap.get(DATA_SIZE_KEY);
        return value.intValue();
    }


    @Override
    protected Object addDocumentInternal(Document document) {
        final Object key;
        if (idField != null) {
            key = Utils.getSubdocumentValue(document, idField);
        } else {
            key = UUID.randomUUID();
        }

        Document previous = dataMap.put(Missing.ofNullable(key), document);
        Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
        return key;
    }

    @Override
    public int count() {
        return dataMap.size();
    }

    @Override
    protected Document getDocument(Object position) {
        return dataMap.get(position);
    }

    @Override
    protected void removeDocument(Object position) {
        Document remove = dataMap.remove(position);
        if (remove == null) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Object findDocumentPosition(Document document) {
        for (Entry<Object, Document> entry : dataMap.entrySet()) {
            if (entry.getValue().equals(document)) {
                return entry.getKey();
            }
        }
        return null;
    }


    @Override
    protected Iterable<Document> matchDocuments(Document query, Iterable<Object> positions, Document orderBy, int numberToSkip, int numberToReturn) {

        List<Document> matchedDocuments = new ArrayList<>();

        for (Object position : positions) {
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

        for (Document document : dataMap.values()) {
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // already sorted
                } else if (sortValue == -1) {
                    Collections.reverse(matchedDocuments);
                }
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

    @Override
    protected void handleUpdate(Document document) {
        // noop
    }

}
