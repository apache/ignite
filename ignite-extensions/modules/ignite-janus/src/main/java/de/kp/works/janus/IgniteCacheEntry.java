package de.kp.works.janus;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.janus.AbstractEntryBuilder;
import de.kp.works.janus.IgniteValue;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Map;

public class IgniteCacheEntry extends AbstractEntryBuilder {

	String hashKey;
	String rangeKey;

	ByteBuffer byteBuffer;

	public IgniteCacheEntry(Map<String, IgniteValue> items) {

		this.hashKey = items.get(HASH_KEY).getS();
		this.rangeKey = items.get(RANGE_KEY).getS();

		if (items.containsKey(BYTE_BUFFER))
			this.byteBuffer = items.get(BYTE_BUFFER).getB();

	}

	public String getHashKey() {
		return hashKey;
	}

	public String getRangeKey() {
		return this.rangeKey;
	}

	public String getCacheKey() {
		String serialized = toString();
		try {
			return MessageDigest.getInstance("MD5").digest(serialized.getBytes("UTF-8")).toString();

		} catch (Exception e) {
			return null;
		}
	}

	public ByteBuffer getBuffer() {
		return byteBuffer;
	}

	@Override
	public String toString() {
		return "hashKey::" + hashKey + ", rangeKey::" + rangeKey + ", byteBuffer:: " + byteBuffer;
	}
}
