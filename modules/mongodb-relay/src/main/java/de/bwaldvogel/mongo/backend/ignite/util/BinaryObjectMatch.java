package de.bwaldvogel.mongo.backend.ignite.util;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.bson.Document;

public class BinaryObjectMatch implements IgniteBiPredicate<Object, BinaryObject>{
	private static final long serialVersionUID = 1L;
	
	protected final QueryMatcher matcher = new DefaultQueryMatcher();
	private final Document query;
	private final String idField;
	
	public BinaryObjectMatch(Document query, String idField) {		
		this.query = query;
		this.idField = idField;
	}

	@Override public boolean apply(Object key, BinaryObject other) {
		
     	Document document = DocumentUtil.binaryObjectToDocument(key,other,idField,query.keySet());
     	
     	if (matcher.matches(document, query)) {
            return true;
         }
     	return false;
     }
}
