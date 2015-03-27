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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.unsafe.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.spi.indexing.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.*;

/**
 * Lucene fulltext index.
 */
public class GridLuceneIndex implements Closeable {
    /** Field name for string representation of value. */
    public static final String VAL_STR_FIELD_NAME = "_gg_val_str__";

    /** Field name for value version. */
    public static final String VER_FIELD_NAME = "_gg_ver__";

    /** Field name for value expiration time. */
    public static final String EXPIRATION_TIME_FIELD_NAME = "_gg_expires__";

    /** */
    private final Marshaller marshaller;

    /** */
    private final String spaceName;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private final IndexWriter writer;

    /** */
    private final String[] idxdFields;

    /** */
    private final boolean storeVal;

    /** */
    private final AtomicLong updateCntr = new GridAtomicLong();

    /** */
    private final GridLuceneDirectory dir;

    /** */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param marshaller Indexing marshaller.
     * @param mem Unsafe memory.
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @param storeVal Store value in index.
     * @throws IgniteCheckedException If failed.
     */
    public GridLuceneIndex(GridKernalContext ctx, Marshaller marshaller, @Nullable GridUnsafeMemory mem,
        @Nullable String spaceName, GridQueryTypeDescriptor type, boolean storeVal) throws IgniteCheckedException {
        this.ctx = ctx;
        this.marshaller = marshaller;
        this.spaceName = spaceName;
        this.type = type;
        this.storeVal = storeVal;

        dir = new GridLuceneDirectory(mem == null ? new GridUnsafeMemory(0) : mem);

        try {
            writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_30, new StandardAnalyzer(
                Version.LUCENE_30)));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        GridQueryIndexDescriptor idx = null;

        for (GridQueryIndexDescriptor descriptor : type.indexes().values()) {
            if (descriptor.type() == GridQueryIndexType.FULLTEXT) {
                idx = descriptor;

                break;
            }
        }

        if (idx != null) {
            Collection<String> fields = idx.fields();

            idxdFields = new String[fields.size() + 1];

            fields.toArray(idxdFields);
        }
        else {
            assert type.valueTextIndex() || type.valueClass() == String.class;

            idxdFields = new String[1];
        }

        idxdFields[idxdFields.length - 1] = VAL_STR_FIELD_NAME;
    }

    /**
     * Stores given data in this fulltext index.
     *
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expires Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    public void store(Object key, Object val, byte[] ver, long expires) throws IgniteCheckedException {
        Document doc = new Document();

        boolean stringsFound = false;

        if (type.valueTextIndex() || type.valueClass() == String.class) {
            doc.add(new Field(VAL_STR_FIELD_NAME, val.toString(), Field.Store.YES, Field.Index.ANALYZED));

            stringsFound = true;
        }

        for (int i = 0, last = idxdFields.length - 1; i < last; i++) {
            Object fieldVal = type.value(idxdFields[i], key, val);

            if (fieldVal != null) {
                doc.add(new Field(idxdFields[i], fieldVal.toString(), Field.Store.YES, Field.Index.ANALYZED));

                stringsFound = true;
            }
        }

        String keyStr = org.apache.commons.codec.binary.Base64.encodeBase64String(marshaller.marshal(key));

        try {
            // Delete first to avoid duplicates.
            writer.deleteDocuments(new Term(KEY_FIELD_NAME, keyStr));

            if (!stringsFound)
                return; // We did not find any strings to be indexed, will not store data at all.

            doc.add(new Field(KEY_FIELD_NAME, keyStr, Field.Store.YES, Field.Index.NOT_ANALYZED));

            if (storeVal && type.valueClass() != String.class)
                doc.add(new Field(VAL_FIELD_NAME, marshaller.marshal(val)));

            doc.add(new Field(VER_FIELD_NAME, ver));

            doc.add(new Field(EXPIRATION_TIME_FIELD_NAME, DateTools.timeToString(expires,
                DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));

            writer.addDocument(doc);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /**
     * Removes entry for given key from this index.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(Object key) throws IgniteCheckedException {
        try {
            writer.deleteDocuments(new Term(KEY_FIELD_NAME, org.apache.commons.codec.binary.Base64.encodeBase64String(marshaller.marshal(key))));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /**
     * Runs lucene fulltext query over this index.
     *
     * @param qry Query.
     * @param filters Filters over result.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(String qry,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        IndexReader reader;

        try {
            long updates = updateCntr.get();

            if (updates != 0) {
                writer.commit();

                updateCntr.addAndGet(-updates);
            }

            reader = IndexReader.open(writer, true);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        IndexSearcher searcher = new IndexSearcher(reader);

        MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_30, idxdFields,
            writer.getAnalyzer());

        // Filter expired items.
        Filter f = new TermRangeFilter(EXPIRATION_TIME_FIELD_NAME, DateTools.timeToString(U.currentTimeMillis(),
            DateTools.Resolution.MILLISECOND), null, false, false);

        TopDocs docs;

        try {
            docs = searcher.search(parser.parse(qry), f, Integer.MAX_VALUE);
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        IgniteBiPredicate<K, V> fltr = null;

        if (filters != null)
            fltr = filters.forSpace(spaceName);

        return new It<>(reader, searcher, docs.scoreDocs, fltr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(writer);
        U.closeQuiet(dir);
    }

    /**
     * Key-value iterator over fulltext search result.
     */
    private class It<K, V> extends GridCloseableIteratorAdapter<IgniteBiTuple<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IndexReader reader;

        /** */
        private final IndexSearcher searcher;

        /** */
        private final ScoreDoc[] docs;

        /** */
        private final IgniteBiPredicate<K, V> filters;

        /** */
        private int idx;

        /** */
        private IgniteBiTuple<K, V> curr;

        /**
         * Constructor.
         *
         * @param reader Reader.
         * @param searcher Searcher.
         * @param docs Docs.
         * @param filters Filters over result.
         * @throws IgniteCheckedException if failed.
         */
        private It(IndexReader reader, IndexSearcher searcher, ScoreDoc[] docs, IgniteBiPredicate<K, V> filters)
            throws IgniteCheckedException {
            this.reader = reader;
            this.searcher = searcher;
            this.docs = docs;
            this.filters = filters;

            findNext();
        }

        /**
         * Filters key using predicates.
         *
         * @param key Key.
         * @param val Value.
         * @return {@code True} if key passes filter.
         */
        private boolean filter(K key, V val) {
            return filters == null || filters.apply(key, val) ;
        }

        /**
         * Finds next element.
         *
         * @throws IgniteCheckedException If failed.
         */
        private void findNext() throws IgniteCheckedException {
            curr = null;

            while (idx < docs.length) {
                Document doc;

                try {
                    doc = searcher.doc(docs[idx++].doc);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                String keyStr = doc.get(KEY_FIELD_NAME);

                ClassLoader ldr = null;

                if (ctx != null && ctx.deploy().enabled())
                    ldr = ctx.cache().internalCache(spaceName).context().deploy().globalLoader();

                K k = marshaller.unmarshal(org.apache.commons.codec.binary.Base64.decodeBase64(keyStr), ldr);

                byte[] valBytes = doc.getBinaryValue(VAL_FIELD_NAME);

                V v = valBytes != null ? marshaller.<V>unmarshal(valBytes, ldr) :
                    type.valueClass() == String.class ?
                    (V)doc.get(VAL_STR_FIELD_NAME): null;

                if (!filter(k, v))
                    continue;

//                byte[] ver = doc.getBinaryValue(VER_FIELD_NAME); TODO rm version

                curr = new IgniteBiTuple<>(k, v);

                break;
            }
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<K, V> onNext() throws IgniteCheckedException {
            IgniteBiTuple<K, V> res = curr;

            findNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            U.closeQuiet(searcher);
            U.closeQuiet(reader);
        }
    }
}
