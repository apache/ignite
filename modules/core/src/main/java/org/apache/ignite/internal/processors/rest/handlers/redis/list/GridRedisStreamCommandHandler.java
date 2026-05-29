/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.handlers.redis.list;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;

import javax.cache.Cache;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener.SESS_STREAM_LAST_ID_META_KEY;

/**
 * Redis List pop command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisStreamCommandHandler implements GridRedisCommandHandler {

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
			XADD,
			XLEN,
			XREAD,
			XRANGE,
			XREVRANGE
    );

    /** Logger. */
    protected final IgniteLogger log;

    /** Kernel context. */
    protected final GridKernalContext ctx;

	protected Map<String,Long> lastIdMap = new ConcurrentHashMap<>();

	protected final String idField = "_id";
	protected final String modifiedField = "_modified";
    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param ctx Kernal context.
     */
    public GridRedisStreamCommandHandler(IgniteLogger log, GridKernalContext ctx) {
        this.log = log;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

	@Override
	public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
		assert msg != null;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        GridRedisCommand cmd = msg.command();
		IgniteCache<Long, BinaryObject> stream = ctx.grid().cache(msg.cacheName());
		if(stream!=null) {
			stream = stream.withKeepBinary();
		}
		if(cmd == XADD) {
			String msgId = msg.aux(2);
			Long key;
			int maxLen = -1;
			int dataPos = 1;
			if(msgId.equalsIgnoreCase("MAXLEN")){
				// XADD stream_name [MAXLEN ~ count] * field value [field value ...]
				if(msg.aux(3).equalsIgnoreCase("~")) {
					msgId = msg.aux(5);
					maxLen = Integer.parseInt(msg.aux(4));
					dataPos += 3;
				}
				else{
					msgId = msg.aux(4);
					maxLen = Integer.parseInt(msg.aux(3));
					dataPos += 2;
				}
			}

			if(msgId.equals("*")){
				IgniteAtomicLong l = ctx.grid().atomicLong(msg.cacheName(), 0, true);
				key = l.incrementAndGet();
				msgId = String.valueOf(key);
			}
			else{
				key = combineToLong(msgId);
			}

			List<String> params = msg.aux();
			BinaryObjectBuilder bb = ctx.grid().binary().builder(msg.cacheName());
			for(int i=dataPos; i<params.size()-1; i+=2) {
				String field = params.get(i);
				String value = params.get(i+1);
				bb.setField(field,value);
			}
			bb.setField(idField,key);
			bb.setField(modifiedField,System.currentTimeMillis());
			stream.put(key,bb.build());
			lastIdMap.put(msg.cacheName(),key);
			if(maxLen>=0){
				int siz = stream.size(CachePeekMode.PRIMARY);
				if(siz>maxLen) {
					IndexQuery<Long,BinaryObject> query = new IndexQuery<>(msg.cacheName());
					query.setLimit(siz-maxLen);
					Set<Long> shouldRemoves = new HashSet<>(siz-maxLen);
					try (QueryCursor<Cache.Entry<Long,BinaryObject>> cursor = stream.query(query)) {
						for (Cache.Entry<Long, BinaryObject> row : cursor) {
							shouldRemoves.add(row.getKey());
						}
					}
					stream.removeAll(shouldRemoves);
				}
			}
			msg.setResponse(GridRedisProtocolParser.toSimpleString(msgId));
			return new GridFinishedFuture<>(msg);
		}
        else if(cmd==XRANGE) {
        	// XRANGE stream_name start end [COUNT count] （- 表示最小ID，+ 表示最大ID）
			List<String> params = msg.auxMKeys();

			IndexQuery<Long,BinaryObject> query = new IndexQuery<>(msg.cacheName());
			int count = 0;
			String countName = stringValue("COUNT",params);
			if(countName!=null) {
				count = Integer.parseInt(countName);
			}
			if(count>0)
				query.setLimit(count);

			String startId = params.get(1);
			String endId = params.get(2);
			Long start = null;
			Long end = null;
			if(!startId.equals("-")){
				start = combineToLong(startId);
			}

			if(!endId.equals("+")){
				end = combineToLong(endId);
			}
			RangeIndexQueryCriterion idFilter = new RangeIndexQueryCriterion(idField,start,end);
			if(startId.charAt(0)!='(')
				idFilter.lowerIncl(true);
			if(endId.charAt(0)!='(')
				idFilter.upperIncl(true);

			query.setCriteria(idFilter);

			Collection<List> data = new ArrayList<>();

			if(stream!=null){

				try (QueryCursor<Cache.Entry<Long,BinaryObject>> cursor = stream.query(query)) {
					for (Cache.Entry<Long,BinaryObject> row : cursor) {
						Long k = row.getKey();
						BinaryObject v = row.getValue();
						Collection<String> result = new ArrayList<>();
						for(String field: v.type().fieldNames()){
							if(field.equals(idField)) continue;
							Object fdata = v.field(field);
							if(fdata!=null) {
								if (field.equals(modifiedField)) {
									Date date = new Date((Long) fdata);
									fdata = format.format(date);
								}
								result.add(field);
								result.add(fdata.toString());
							}
						}
						data.add(List.of(k.toString(),result));
						Long lastId = ses.meta(SESS_STREAM_LAST_ID_META_KEY);
						if(lastId==null || k>lastId) {
							ses.addMeta(SESS_STREAM_LAST_ID_META_KEY, k);
						}
					}
				}
			}

        	msg.setResponse(GridRedisProtocolParser.toArray(data));
        }
		else if(cmd==XREVRANGE ) {
			// XREVRANGE stream_name end start [COUNT count] （- 表示最小ID，+ 表示最大ID）
			List<String> params = msg.auxMKeys();
			// must create index on _key named _key_desc_idx
			IndexQuery<Long,BinaryObject> query = new IndexQuery<>(msg.cacheName(),"_modified_desc_idx");

			int count = 0;
			String countName = stringValue("COUNT",params);
			if(countName!=null) {
				count = Integer.parseInt(countName);
			}
			if(count>0)
				query.setLimit(count);

			String startId = params.get(2);
			String endId = params.get(1);
			Long start = null;
			Long end = null;
			if(!startId.equals("-")){
				start = combineToLong(startId);
			}

			if(!endId.equals("+")){
				end = combineToLong(endId);
			}

			RangeIndexQueryCriterion idFilter = new RangeIndexQueryCriterion(modifiedField,start,end);
			if(startId.charAt(0)!='(')
				idFilter.lowerIncl(true);
			if(endId.charAt(0)!='(')
				idFilter.upperIncl(true);
			query.setCriteria(idFilter);

			Collection<List> data = new ArrayList<>();

			if(stream!=null){

				try (QueryCursor<Cache.Entry<Long,BinaryObject>> cursor = stream.query(query)) {
					for (Cache.Entry<Long,BinaryObject> row : cursor) {
						Long k = row.getKey();
						BinaryObject v = row.getValue();
						Collection<String> result = new ArrayList<>();
						for(String field: v.type().fieldNames()){
							if(field.equals(idField)) continue;
							Object fdata = v.field(field);
							if(fdata!=null) {
								if (field.equals(modifiedField)) {
									Date date = new Date((Long) fdata);
									fdata = format.format(date);
								}
								result.add(field);
								result.add(fdata.toString());
							}
						}
						data.add(List.of(k.toString(),result));
						Long lastId = ses.meta(SESS_STREAM_LAST_ID_META_KEY);
						if(lastId==null || k>lastId) {
							ses.addMeta(SESS_STREAM_LAST_ID_META_KEY, k);
						}
					}
				}
			}

			msg.setResponse(GridRedisProtocolParser.toArray(data));
		}
        else if(cmd == XREAD) {
			// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
			int blockms = -1; // 阻塞0表示永久阻塞
			int count = 1;
			int keyPos = 2;
			List<String> params = msg.auxMKeys();
			for(int i=0;i<params.size();i++){
				if(params.get(i).equalsIgnoreCase("COUNT")){
					count = Integer.parseInt(params.get(i+1));
					keyPos+=2;
				}
				else if(params.get(i).equalsIgnoreCase("BLOCK")){
					blockms = Integer.parseInt(params.get(i+1));
					keyPos+=2;
				}
			}
			String streamName = stringValue("STREAMS",params);
			if(params.size() != keyPos+1){
				msg.setResponse(GridRedisProtocolParser.toGenericError("Wrong number of xread arguments, Ignie redis only support singer stream read."));
				return new GridFinishedFuture<>(msg);
			}
			String id = params.get(keyPos);
			Long idValue = 0L;
			if(id.equals("$")){
				idValue = ses.meta(SESS_STREAM_LAST_ID_META_KEY);
				if(idValue==null) {
					ses.addMeta(SESS_STREAM_LAST_ID_META_KEY,lastIdMap.get(msg.cacheName()));
				}
				if(idValue==null && blockms<0){
					msg.setResponse(GridRedisProtocolParser.nil());
					return new GridFinishedFuture<>(msg);
				}
				idValue = ses.meta(SESS_STREAM_LAST_ID_META_KEY);
			}
			else{
				idValue = combineToLong(id);
			}

			Collection<List> data = new ArrayList<>();

			IndexQuery<Long,BinaryObject> query = new IndexQuery<>(msg.cacheName());
			query.setLimit(count);
			query.setCriteria(new RangeIndexQueryCriterion(idField,idValue,null));
			final int limit = count;
			if(blockms<0){
				try (QueryCursor<Cache.Entry<Long,BinaryObject>> cursor = stream.query(query)) {
					for (Cache.Entry<Long,BinaryObject> row : cursor) {
						Long k = row.getKey();
						BinaryObject v = row.getValue();
						Collection<String> result = new ArrayList<>();
						for(String field: v.type().fieldNames()){
							if(field.equals(idField)) continue;
							Object fdata = v.field(field);
							if(fdata!=null) {
								if (field.equals(modifiedField)) {
									Date date = new Date((Long) fdata);
									fdata = format.format(date);
								}
								result.add(field);
								result.add(fdata.toString());
							}
						}
						data.add(List.of(k.toString(),result));
						ses.addMeta(SESS_STREAM_LAST_ID_META_KEY,k);
					}
				}
				if(data.isEmpty()){
					msg.setResponse(GridRedisProtocolParser.toArray(List.of()));
				}
				else {
					msg.setResponse(GridRedisProtocolParser.toArray(List.of(List.of(streamName, data))));
				}
			}
			else{
				AtomicInteger siz = new AtomicInteger(0);
				ContinuousQuery<Long,BinaryObject> cquery = new ContinuousQuery<>();
				cquery.setInitialQuery(query);
				cquery.setLocalListener((items)->{
					for(var row: items) {
						Long k = row.getKey();
						BinaryObject v = row.getValue();
						Collection<String> result = new ArrayList<>();
						for(String field: v.type().fieldNames()){
							if(field.equals(idField)) continue;
							Object fdata = v.field(field);
							if(fdata!=null) {
								if (field.equals(modifiedField)) {
									Date date = new Date((Long) fdata);
									fdata = format.format(date);
								}
								result.add(field);
								result.add(fdata.toString());
							}
						}
						data.add(List.of(k.toString(),result));
						ses.addMeta(SESS_STREAM_LAST_ID_META_KEY,k);
						siz.incrementAndGet();
						synchronized(siz) {
							if(siz.get()>=limit) {
								siz.notifyAll();
							}
						}
					}

				});

				try (QueryCursor<Cache.Entry<Long,BinaryObject>> cursor = stream.query(cquery)) {
					for (Cache.Entry<Long,BinaryObject> row : cursor) {
						Long k = row.getKey();
						BinaryObject v = row.getValue();
						Collection<String> result = new ArrayList<>();
						for(String field: v.type().fieldNames()){
							if(field.equals(idField)) continue;
							Object fdata = v.field(field);
							if(fdata!=null) {
								if (field.equals(modifiedField)) {
									Date date = new Date((Long) fdata);
									fdata = format.format(date);
								}
								result.add(field);
								result.add(fdata.toString());
							}
						}
						data.add(List.of(k.toString(),result));
						ses.addMeta(SESS_STREAM_LAST_ID_META_KEY,k);
						siz.incrementAndGet();
					}
					try {
						synchronized(siz) {
							if(siz.get()<limit){
								//Thread.sleep(100);
								if (blockms > 0)
									siz.wait(blockms);
								else
									siz.wait();
							}
						}
					} catch (InterruptedException e) {
						//throw new RuntimeException(e);
					}
				}
				if(data.isEmpty()){
					msg.setResponse(GridRedisProtocolParser.nil());
				}
				else {
					msg.setResponse(GridRedisProtocolParser.toArray(List.of(List.of(streamName,data))));
				}
			}
        }
		else if(cmd == XLEN) {
			if(stream==null){
				msg.setResponse(GridRedisProtocolParser.toInteger(0));
			}
			else {
				msg.setResponse(GridRedisProtocolParser.toInteger(stream.size(CachePeekMode.PRIMARY)));
			}
		}

        return new GridFinishedFuture<>(msg);
	}

	/**
	 * 将高位和低位组合成低位固定长度为6的long
	 * @return 组合后的long值
	 */
	public static Long combineToLong(String id) {
		if(id.charAt(0)=='(')
			id = id.substring(1);
		String[] parts = id.split("-");
		if(parts.length==1){
			return Long.valueOf(id);
		}
		String highPart=parts[0];
		String lowPart=parts[1];
		int paddingZeros = 6 - lowPart.length();
		String combined = highPart + String.format("%0" + paddingZeros + "d", 0) + lowPart;
		return Long.valueOf(combined);
	}

}
