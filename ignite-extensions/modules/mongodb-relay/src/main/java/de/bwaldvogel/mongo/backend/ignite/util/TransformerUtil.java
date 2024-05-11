package de.bwaldvogel.mongo.backend.ignite.util;

import static de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil.objectToDocument;

import java.util.Iterator;
import java.util.function.Function;

import javax.cache.Cache;
import javax.cache.Cache.Entry;

import org.apache.ignite.cache.query.QueryCursor;

import de.bwaldvogel.mongo.backend.CloseableIterator;
import de.bwaldvogel.mongo.bson.Document;


public class TransformerUtil {
	
	public static <E> CloseableIterator<E> map(final Iterator<Cache.Entry<Object, Object>> iterator, Function<Cache.Entry<Object, Object>,E> function){
		 return new CloseableIterator<E>() {
           @Override
           public boolean hasNext() {
               return iterator.hasNext();
           }

           @Override
           public void remove() {
               iterator.remove();
           }

           @Override
           public E next() {
               return function.apply(iterator.next());
           }

           @Override
           public void close() {
               CloseableIterator.closeIterator(iterator);
           }
       };
	}
	
	public static CloseableIterator<Document> map(final QueryCursor<Cache.Entry<Object, Object>> cursor,final String idField){
		final Iterator<Entry<Object, Object>> iterator = cursor.iterator();
		
		return new CloseableIterator<Document>() {
          @Override
          public boolean hasNext() {
              return iterator.hasNext();
          }

          @Override
          public void remove() {
              iterator.remove();
          }

          @Override
          public Document next() {
        	  Cache.Entry<Object, Object> entry = iterator.next();
        	  Document document = objectToDocument(entry.getKey(),entry.getValue(),idField);	 
              return document;
          }

          @Override
          public void close() {
        	  cursor.close();
          }
      };
	}

}
