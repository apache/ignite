package de.kp.works.janus;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;

public class IgniteKeySlicesIterator implements KeySlicesIterator {

	final Iterator<KeyIterator> iterator;
	
	final Map<SliceQuery, KeyIterator> map;
	
	private KeyIterator entry;

	public IgniteKeySlicesIterator(Map<SliceQuery, KeyIterator> map) {
		this.iterator = map.values().iterator();
		this.map = map;
		if(this.iterator.hasNext()) {
			entry = this.iterator.next();
		}
		else {
			entry = null;
		}
	}

	@Override
	public boolean hasNext() {
		if(entry==null) return false;
		boolean has = entry.hasNext();
		if(has) return has;
		if(iterator.hasNext()) {
			entry = this.iterator.next();
			return hasNext();
		}
		else {
			return false;
		}		
	}

	@Override
	public StaticBuffer next() {
		/*
		 * The key iterator provides hash keys that
		 * are from a column slice request
		 */	
		return entry.next();

	}

	@Override
	public void close() {
	}

	
	@Override
	public Map<SliceQuery, RecordIterator<Entry>> getEntries() {
		Map<SliceQuery, RecordIterator<Entry>> result = new LinkedHashMap<>();
		for(Map.Entry<SliceQuery, KeyIterator> ent: map.entrySet()) {
			result.put(ent.getKey(), ent.getValue().getEntries());
		}
		return result;
	}

}
