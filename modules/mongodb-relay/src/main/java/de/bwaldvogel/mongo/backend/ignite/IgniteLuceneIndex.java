package de.bwaldvogel.mongo.backend.ignite;

import java.util.List;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public class IgniteLuceneIndex extends Index<Object>{
	
	private IgniteH2Indexing idxing;
	
	private H2TableDescriptor tableDesc;
	
	private GridLuceneIndex luceneIndex;
	 
	protected IgniteLuceneIndex(String name, List<IndexKey> keys, boolean sparse) {
		super(name, keys, sparse);
		// TODO Auto-generated constructor stub
	}

	public void init(GridKernalContext ctx,String cacheName,String typeName) {
		
		idxing = (IgniteH2Indexing)ctx.query().getIndexing();
		
		tableDesc = idxing.schemaManager().tableForType(idxing.schema(cacheName),cacheName,typeName);
		
		luceneIndex = tableDesc.luceneIndex();
	}

	@Override
	public Object getPosition(Document document) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object remove(Document document) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean canHandle(Document query) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getDataSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateInPlace(Document oldDocument, Document newDocument, Object position,
			MongoCollection<Object> collection) throws KeyConstraintError {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void drop() {
		// TODO Auto-generated method stub
		
	}
}
