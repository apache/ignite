package de.bwaldvogel.mongo.backend.ignite.util;

import java.util.Collection;
import java.util.List;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryFilter;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class BinaryObjectMatch extends DefaultQueryMatcher implements IgniteBiPredicate<Object, BinaryObject>{
	private static final long serialVersionUID = 1L;	
	
	private final Document query;
	private final String idField;
	
	public BinaryObjectMatch(Document query, String idField) {		
		this.query = query;
		this.idField = idField;
	}

	@Override 
	public boolean apply(Object key, BinaryObject other) {		
     	// Document document = DocumentUtil.binaryObjectToDocument(key,other,idField,query.keySet());     	
     	if (this.matches(other, query)) {
            return true;
         }
     	return false;
     }
	
	/**
	 * 预过滤，不确定的情况下返回true
	 * @param document
	 * @param query
	 * @return
	 */
    public boolean matches(BinaryObject document, Document query) {
        for (String key : query.keySet()) {
            Object queryValue = query.get(key);
            validateQueryValue(queryValue, key);
            List<String> keys = splitKey(key);
            if(keys.size()!=1) {
            	return true;
            }
            if (QueryFilter.isQueryFilter(key)) {
                return true;
            }
            if (key.startsWith("$")) {
            	return true;
            }
            if(!document.hasField(key)) {
            	return true;
            }
            Object documentValue = document.field(key);
            if (documentValue instanceof Collection<?>) {
                Collection<?> documentValues = (Collection<?>) documentValue;
                if (queryValue instanceof Document) {
                    Document queryDocument = (Document) queryValue;
                    boolean matches = checkMatchesAnyValue(queryDocument, keys, null, documentValues);
                    if (matches) {
                        return true;
                    }
                    if (isInQuery(queryDocument)) {
                        return checkMatchesValue(queryValue, documentValue);
                    } else {
                        return false;
                    }
                } else if (queryValue instanceof Collection<?>) {
                    return checkMatchesValue(queryValue, documentValues);
                } else if (checkMatchesAnyValue(queryValue, documentValues)) {
                    return true;
                }
            }

            return checkMatchesValue(queryValue, documentValue);
        }

        return true;
    }    

}
