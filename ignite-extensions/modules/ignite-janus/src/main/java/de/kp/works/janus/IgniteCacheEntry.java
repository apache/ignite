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
 * 
 * 
 */

import de.kp.works.janus.AbstractEntryBuilder;
import de.kp.works.janus.IgniteValue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class IgniteCacheEntry extends AbstractEntryBuilder {
	static MessageDigest MD5 = null;
	
	static{
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	String hashKey;
	String rangeKey;
	
	byte[] data;

	//ByteBuffer byteBuffer;

	public IgniteCacheEntry(Map<String, IgniteValue> items) {

		this.hashKey = items.get(HASH_KEY).getS();
		this.rangeKey = items.get(RANGE_KEY).getS();

		if (items.containsKey(BYTE_BUFFER))
			this.data = items.get(BYTE_BUFFER).data();

	}

	public String getHashKey() {
		return hashKey;
	}

	public String getRangeKey() {
		return this.rangeKey;
	}

	public String getCacheKey() {
		String serialized = hashKey + ":" + rangeKey;		
		return serialized;
		/**
		try {
			return new String(MD5.digest(serialized.getBytes(StandardCharsets.UTF_8)),StandardCharsets.UTF_8);

		} catch (Exception e) {
			return null;
		}
		*/
	}

	//public ByteBuffer getBuffer() {
	//	return byteBuffer;
	//}
	
	public byte[] data() {
		return data;
	}

	@Override
	public String toString() {
		return "hashKey::" + hashKey + ", rangeKey::" + rangeKey + ", data:: " + new String(data,StandardCharsets.UTF_8);
	}
}
