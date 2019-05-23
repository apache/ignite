package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.projection.ProjectingIterable;
import de.bwaldvogel.mongo.backend.projection.Projection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public abstract class AbstractMongoCollection<P> implements MongoCollection<P> {
	static final Logger log = LoggerFactory.getLogger(MongoCollection.class);
    private String collectionName;
    private String databaseName;
    private final List<Index<P>> indexes = new ArrayList<>();
    private final QueryMatcher matcher = new DefaultQueryMatcher();
    protected final String idField;

    protected AbstractMongoCollection(String databaseName, String collectionName, String idField) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.idField = idField;
    }

    protected boolean documentMatchesQuery(Document document, Document query) {
        return matcher.matches(document, query);
    }

    private Iterable<Document> queryDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {
        synchronized (indexes) {
            for (Index<P> index : indexes) {
                if (index.canHandle(query)) {
                    Iterable<P> positions = index.getPositions(query);
                    return matchDocuments(query, positions, orderBy, numberToSkip, numberToReturn);
                }
            }
        }

        return matchDocuments(query, orderBy, numberToSkip, numberToReturn);
    }

    protected void sortDocumentsInMemory(List<Document> documents, Document orderBy) {
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // keep it as is
                } else if (sortValue == -1) {
                    Collections.reverse(documents);
                } else {
                    throw new IllegalArgumentException("Illegal sort value: " + sortValue);
                }
            } else {
                documents.sort(new DocumentComparator(orderBy));
            }
        }
    }

    protected abstract Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
                                                           int numberToReturn);

    protected abstract Iterable<Document> matchDocuments(Document query, Iterable<P> positions, Document orderBy,
                                                         int numberToSkip, int numberToReturn);

    protected abstract Document getDocument(P position);

    protected abstract void updateDataSize(int sizeDelta);

    protected abstract int getDataSize();

    protected abstract P addDocumentInternal(Document document);

    @Override
    public synchronized void addDocument(Document document) {

        if (document.get(ID_FIELD) instanceof Collection) {
            throw new BadValueException("can't use an array for _id");
        }

        for (Index<P> index : indexes) {
            index.checkAdd(document, this);
        }

        P position = addDocumentInternal(document);

        for (Index<P> index : indexes) {
            index.add(document, position, this);
        }

        updateDataSize(Utils.calculateSize(document));
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getFullName() {
        return getDatabaseName() + "." + getCollectionName();
    }

    @Override
    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getFullName() + ")";
    }

    @Override
    public void addIndex(Index<P> index) {
        indexes.add(index);
    }
    
    @Override
    public boolean checkIndex(List<IndexKey> index) {
    	for(int i=0;i<indexes.size();i++) {
    		Index<P> idx = indexes.get(i);
    		if(idx.getKeys().equals(index)) {
    			return true;
    		}
    	}
    	return false;
    }
    
    @Override
    public boolean dropIndex(List<IndexKey> index) {
    	for(int i=0;i<indexes.size();i++) {
    		Index<P> idx = indexes.get(i);
    		if(idx.getKeys().equals(index)) {
    			 indexes.remove(i);
    			 return true;
    		}
    	}
    	return false;
    }
    		
    

    private void modifyField(Document document, String modifier, Document update, ArrayFilters arrayFilters,
                             Integer matchPos, boolean isUpsert) {
        Document change = (Document) update.get(modifier);
        UpdateOperator updateOperator = getUpdateOperator(modifier, change);
        FieldUpdates updates = new FieldUpdates(document, updateOperator, idField, isUpsert, matchPos, arrayFilters);
        updates.apply(change, modifier);
    }

    private UpdateOperator getUpdateOperator(String modifier, Document change) {
        final UpdateOperator op;
        try {
            op = UpdateOperator.fromValue(modifier);
        } catch (IllegalArgumentException e) {
            throw new FailedToParseException("Unknown modifier: " + modifier);
        }

        if (op != UpdateOperator.UNSET) {
            for (String key : change.keySet()) {
                if (key.startsWith("$") && !key.startsWith("$[")) {
                    throw new MongoServerError(15896, "Modified field name may not start with $");
                }
            }
        }
        return op;
    }

    private void applyUpdate(Document oldDocument, Document newDocument) {

        if (newDocument.equals(oldDocument)) {
            return;
        }

        Object oldId = oldDocument.get(idField);
        Object newId = newDocument.get(idField);

        if (newId != null && !Utils.nullAwareEquals(oldId, newId)) {
            throw new ImmutableFieldException("After applying the update, the (immutable) field '_id' was found to have been altered to _id: " + newId);
        }

        if (newId == null && oldId != null) {
            newDocument.put(idField, oldId);
        }

        cloneInto(oldDocument, newDocument);
    }

    Object deriveDocumentId(Document selector) {
        Object value = selector.get(idField);
        if (value != null) {
            if (!Utils.containsQueryExpression(value)) {
                return value;
            } else {
                return deriveIdFromExpression(value);
            }
        }
        return new ObjectId();
    }

    private Object deriveIdFromExpression(Object value) {
        Document expression = (Document) value;
        for (String key : expression.keySet()) {
            Object expressionValue = expression.get(key);
            if (key.equals("$in")) {
                Collection<?> list = (Collection<?>) expressionValue;
                if (!list.isEmpty()) {
                    return list.iterator().next();
                }
            }
        }
        // fallback to random object id
        return new ObjectId();
    }

    private Document calculateUpdateDocument(Document oldDocument, Document update, ArrayFilters arrayFilters,
                                             Integer matchPos, boolean isUpsert) {

        int numStartsWithDollar = 0;
        for (String key : update.keySet()) {
            if (key.startsWith("$")) {
                numStartsWithDollar++;
            }
        }

        Document newDocument = new Document(idField, oldDocument.get(idField));

        if (numStartsWithDollar == update.keySet().size()) {
            cloneInto(newDocument, oldDocument);
            for (String key : update.keySet()) {
                modifyField(newDocument, key, update, arrayFilters, matchPos, isUpsert);
            }
        } else if (numStartsWithDollar == 0) {
            applyUpdate(newDocument, update);
        } else {
            throw new MongoServerException("illegal update: " + update);
        }

        return newDocument;
    }

    @Override
    public synchronized Document findAndModify(Document query) {

        boolean returnNew = Utils.isTrue(query.get("new"));

        if (!query.containsKey("remove") && !query.containsKey("update")) {
            throw new FailedToParseException("Either an update or remove=true must be specified");
        }

        Document queryObject = new Document();

        if (query.containsKey("query")) {
            queryObject.put("query", query.get("query"));
        } else {
            queryObject.put("query", new Document());
        }

        if (query.containsKey("sort")) {
            queryObject.put("orderby", query.get("sort"));
        }

        Document lastErrorObject = null;
        Document returnDocument = null;
        int num = 0;
        for (Document document : handleQuery(queryObject, 0, 1)) {
            num++;
            if (Utils.isTrue(query.get("remove"))) {
                removeDocument(document);
                returnDocument = document;
            } else if (query.get("update") != null) {
                Document updateQuery = (Document) query.get("update");

                Integer matchPos = matcher.matchPosition(document, (Document) queryObject.get("query"));

                ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);
                Document oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
                if (returnNew) {
                    returnDocument = document;
                } else {
                    returnDocument = oldDocument;
                }
                lastErrorObject = new Document("updatedExisting", Boolean.TRUE);
                lastErrorObject.put("n", Integer.valueOf(1));
            }
        }
        if (num == 0 && Utils.isTrue(query.get("upsert"))) {
            Document selector = (Document) query.get("query");
            Document updateQuery = (Document) query.get("update");
            ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);
            Document newDocument = handleUpsert(updateQuery, selector, arrayFilters);
            if (returnNew) {
                returnDocument = newDocument;
            } else {
                returnDocument = new Document();
            }
            num++;
        }

        if (query.get("fields") != null) {
            Document fields = (Document) query.get("fields");
            returnDocument = Projection.projectDocument(returnDocument, fields, idField);
        }

        Document result = new Document();
        if (lastErrorObject != null) {
            result.put("lastErrorObject", lastErrorObject);
        }
        result.put("value", returnDocument);
        Utils.markOkay(result);
        return result;
    }

    @Override
    public synchronized Iterable<Document> handleQuery(Document queryObject, int numberToSkip, int numberToReturn,
            Document fieldSelector) {

        final Document query;
        final Document orderBy;

        if (numberToReturn < 0) {
            // actually: request to close cursor automatically
            numberToReturn = -numberToReturn;
        }

        if (queryObject.containsKey("query")) {
            query = (Document) queryObject.get("query");
            orderBy = (Document) queryObject.get("orderby");
        } else if (queryObject.containsKey("$query")) {
            query = (Document) queryObject.get("$query");
            orderBy = (Document) queryObject.get("$orderby");
        } else {
            query = queryObject;
            orderBy = null;
        }

        if (count() == 0) {
            return Collections.emptyList();
        }

        Iterable<Document> objs = queryDocuments(query, orderBy, numberToSkip, numberToReturn);

        if (fieldSelector != null && !fieldSelector.keySet().isEmpty()) {
            return new ProjectingIterable(objs, fieldSelector, idField);
        }

        return objs;
    }

    @Override
    public synchronized Document handleDistinct(Document query) {
        String key = (String) query.get("key");
        Document filter = (Document) query.getOrDefault("query", new Document());
        Set<Object> values = new LinkedTreeSet<>();

        for (Document document : queryDocuments(filter, null, 0, 0)) {
            Object value = Utils.getSubdocumentValue(document, key);
            if (!(value instanceof Missing)) {
                values.add(value);
            }
        }

        Document response = new Document("values", values);
        Utils.markOkay(response);
        return response;
    }

    @Override
    public synchronized void insertDocuments(List<Document> documents) {
        for (Document document : documents) {
            addDocument(document);
        }
    }

    @Override
    public synchronized int deleteDocuments(Document selector, int limit) {
        int n = 0;
        for (Document document : handleQuery(selector, 0, limit)) {
            if (limit > 0 && n >= limit) {
                throw new MongoServerException("internal error: too many elements (" + n + " >= " + limit + ")");
            }
            removeDocument(document);
            n++;
        }
        return n;
    }

    @Override
    public synchronized Document updateDocuments(Document selector, Document updateQuery, ArrayFilters arrayFilters,
                                                 boolean isMulti, boolean isUpsert) {

        if (isMulti) {
            for (String key : updateQuery.keySet()) {
                if (!key.startsWith("$")) {
                    throw new MongoServerError(10158, "multi update only works with $ operators");
                }
            }
        }

        int nMatched = 0;
        int nModified = 0;
        for (Document document : queryDocuments(selector, null, 0, 0)) {
            Integer matchPos = matcher.matchPosition(document, selector);
            Document oldDocument = updateDocument(document, updateQuery, arrayFilters, matchPos);
            if (!Utils.nullAwareEquals(oldDocument, document)) {
                nModified++;
            }
            nMatched++;

            if (!isMulti) {
                break;
            }
        }

        Document result = new Document();

        // insert?
        if (nMatched == 0 && isUpsert) {
            Document newDocument = handleUpsert(updateQuery, selector, arrayFilters);
            result.put("upserted", newDocument.get(idField));
        }

        result.put("n", Integer.valueOf(nMatched));
        result.put("nModified", Integer.valueOf(nModified));
        return result;
    }

    private Document updateDocument(Document document, Document updateQuery,
                                    ArrayFilters arrayFilters, Integer matchPos) {
        synchronized (document) {
            // copy document
            Document oldDocument = new Document();
            cloneInto(oldDocument, document);

            Document newDocument = calculateUpdateDocument(document, updateQuery, arrayFilters, matchPos, false);

            if (!newDocument.equals(oldDocument)) {
                for (Index<P> index : indexes) {
                    index.checkUpdate(oldDocument, newDocument, this);
                }
                for (Index<P> index : indexes) {
                    index.updateInPlace(oldDocument, newDocument, this);
                }

                int oldSize = Utils.calculateSize(oldDocument);
                int newSize = Utils.calculateSize(newDocument);
                updateDataSize(newSize - oldSize);

                // only keep fields that are also in the updated document
                Set<String> fields = new LinkedHashSet<>(document.keySet());
                fields.removeAll(newDocument.keySet());
                for (String key : fields) {
                    document.remove(key);
                }

                // update the fields
                for (String key : newDocument.keySet()) {
                    if (key.contains(".")) {
                        throw new MongoServerException(
                                "illegal field name. must not happen as it must be caught by the driver");
                    }
                    document.put(key, newDocument.get(key));
                }
                handleUpdate(document);
            }
            return oldDocument;
        }
    }

    protected abstract void handleUpdate(Document document);

    private void cloneInto(Document targetDocument, Document sourceDocument) {
        for (String key : sourceDocument.keySet()) {
            targetDocument.put(key, cloneValue(sourceDocument.get(key)));
        }
    }

    private Object cloneValue(Object value) {
        if (value instanceof Document) {
            Document newValue = new Document();
            cloneInto(newValue, (Document) value);
            return newValue;
        } else if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            List<Object> newValue = new ArrayList<>();
            for (Object v : list) {
                newValue.add(cloneValue(v));
            }
            return newValue;
        } else {
            return value;
        }
    }

    private Document handleUpsert(Document updateQuery, Document selector, ArrayFilters arrayFilters) {
        Document document = convertSelectorToDocument(selector);

        Document newDocument = calculateUpdateDocument(document, updateQuery, arrayFilters, null, true);
        if (newDocument.get(idField) == null) {
            newDocument.put(idField, deriveDocumentId(selector));
        }
        addDocument(newDocument);
        return newDocument;
    }

    /**
     * convert selector used in an upsert statement into a document
     */
    Document convertSelectorToDocument(Document selector) {
        Document document = new Document();
        for (String key : selector.keySet()) {
            if (key.startsWith("$")) {
                continue;
            }

            Object value = selector.get(key);
            if (!Utils.containsQueryExpression(value)) {
                Utils.changeSubdocumentValue(document, key, value, (AtomicReference<Integer>) null);
            }
        }
        return document;
    }

    @Override
    public int getNumIndexes() {
        return indexes.size();
    }

    @Override
    public int count(Document query, int skip, int limit) {
        if (query.keySet().isEmpty()) {
            int count = count();
            if (skip > 0) {
                count = Math.max(0, count - skip);
            }
            if (limit > 0) {
                return Math.min(limit, count);
            }
            return count;
        }

        int numberToReturn = (limit >= 0) ? limit : 0;
        int count = 0;
        Iterator<?> it = queryDocuments(query, null, skip, numberToReturn).iterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    @Override
    public Document getStats() {
        int dataSize = getDataSize();

        Document response = new Document("ns", getFullName());
        response.put("count", Integer.valueOf(count()));
        response.put("size", Integer.valueOf(dataSize));

        int averageSize = 0;
        if (count() > 0) {
            averageSize = dataSize / count();
        }
        response.put("avgObjSize", Integer.valueOf(averageSize));
        response.put("storageSize", Integer.valueOf(0));
        response.put("numExtents", Integer.valueOf(0));
        response.put("nindexes", Integer.valueOf(indexes.size()));
        Document indexSizes = new Document();
        for (Index<P> index : indexes) {
            indexSizes.put(index.getName(), Long.valueOf(index.getDataSize()));
        }

        response.put("indexSize", indexSizes);
        Utils.markOkay(response);
        return response;
    }

    @Override
    public synchronized void removeDocument(Document document) {
        P position = null;

        if (!indexes.isEmpty()) {
            for (Index<P> index : indexes) {
                P indexPosition = index.remove(document);
                if (indexPosition == null) {
                    if (index.isSparse()) {
                        continue;
                    } else {
                    	log.warn("Found no position for " + document + " in " + index);
                    	continue;
                        //-throw new IllegalStateException("Found no position for " + document + " in " + index);
                    }
                }
                if (position != null) {
                    Assert.equals(position, indexPosition, () -> "Got different positions for " + document);
                }
                position = indexPosition;
            }
        } 
        
        if (position == null) {
            position = findDocumentPosition(document);
        }

        if (position == null) {
            // not found
            return;
        }

        updateDataSize(-Utils.calculateSize(document));

        removeDocument(position);
    }

    @Override
    public Document validate() {
        Document response = new Document("ns", getFullName());
        response.put("extentCount", Integer.valueOf(0));
        response.put("datasize", Long.valueOf(getDataSize()));
        response.put("nrecords", Integer.valueOf(count()));

        response.put("nIndexes", Integer.valueOf(indexes.size()));
        Document keysPerIndex = new Document();
        for (Index<P> index : indexes) {
            keysPerIndex.put(index.getName(), Long.valueOf(index.getCount()));
        }

        response.put("keysPerIndex", keysPerIndex);
        response.put("valid", Boolean.TRUE);
        response.put("errors", Collections.emptyList());
        Utils.markOkay(response);
        return response;
    }

    @Override
    public void renameTo(String newDatabaseName, String newCollectionName) {
        this.databaseName = newDatabaseName;
        this.collectionName = newCollectionName;
    }

    protected abstract void removeDocument(P position);

    protected abstract P findDocumentPosition(Document document);

}
