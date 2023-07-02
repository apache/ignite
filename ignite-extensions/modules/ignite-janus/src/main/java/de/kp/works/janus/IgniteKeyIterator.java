package de.kp.works.janus;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.util.Iterator;
import java.util.List;

public class IgniteKeyIterator implements KeyIterator {

	final Iterator<Entry> iterator;

	public IgniteKeyIterator(List<Entry> entries) {
		this.iterator = entries.iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public StaticBuffer next() {
		/*
		 * The key iterator provides hash keys that
		 * are from a column slice request
		 */
		Entry entry = iterator.next();
		return entry.getColumn();

	}

	@Override
	public void close() {
	}

	@Override
	public RecordIterator<Entry> getEntries() {

		return new RecordIterator<Entry>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry next() {
				return iterator.next();
			}

			@Override
			public void close() {
			}
		};
	}

}
