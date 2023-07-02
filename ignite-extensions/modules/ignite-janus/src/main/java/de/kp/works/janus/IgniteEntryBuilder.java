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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.janusgraph.diskstorage.StaticBuffer;

public class IgniteEntryBuilder extends AbstractEntryBuilder {

    private final Map<String, IgniteValue> entry = new HashMap<>();

    StaticBuffer hk;
    StaticBuffer rk;
    
    /** HASHKEY SUPPORT **/

    public IgniteEntryBuilder hashKey(final StaticBuffer hashKey) {
    
        this.hk = hashKey;
        entry.put(HASH_KEY, encodeRowKeyAsIgniteValue(hashKey));

        return this;

    }

	public IgniteEntryBuilder hashKeyStart(final StaticBuffer key) {
        entry.put(HASH_KEY_START, encodeRangeKeyAsIgniteValue(key));
        return this;
    }

	public IgniteEntryBuilder hashKeyEnd(final StaticBuffer key) {
        entry.put(HASH_KEY_END, encodeRangeKeyAsIgniteValue(key));
        return this;
    }

	/** RANGE KEY SUPPORT **/
	
	public IgniteEntryBuilder rangeKey(final StaticBuffer rangeKey) {
		
		this.rk = rangeKey;
		
        entry.put(RANGE_KEY, encodeRangeKeyAsIgniteValue(rangeKey));
        return this;
    }

	public IgniteEntryBuilder rangeKeyStart(final StaticBuffer key) {
        entry.put(RANGE_KEY_START, encodeRangeKeyAsIgniteValue(key));
        return this;		
	}

	public IgniteEntryBuilder rangeKeyEnd(final StaticBuffer key) {
        entry.put(RANGE_KEY_END, encodeRangeKeyAsIgniteValue(key));
        return this;		
	}
	
	/** VALUE SUPPORT **/
	
    public IgniteEntryBuilder value(final StaticBuffer value) {
		/*
         * Encode the provided value as [ByteBuffer] as the respective
         * value can be directly decoded into a [StaticBuffer]
         */
        entry.put(BYTE_BUFFER, encodeValueAsIgniteValue(value));
        return this;

    }

    public Map<String, IgniteValue> build() {
        return Collections.unmodifiableMap(entry);
    }

}
