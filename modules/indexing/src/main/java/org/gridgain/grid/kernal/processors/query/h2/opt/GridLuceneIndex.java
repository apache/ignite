/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.opt;

import org.apache.commons.codec.binary.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.spi.indexing.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.query.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.query.h2.GridH2Indexing.*;

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
    private final IgniteMarshaller marshaller;

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
    private final BitSet keyFields = new BitSet();

    /** */
    private final AtomicLong updateCntr = new GridAtomicLong();

    /** */
    private final GridLuceneDirectory dir;

    /**
     * Constructor.
     *
     * @param marshaller Indexing marshaller.
     * @param mem Unsafe memory.
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @param storeVal Store value in index.
     * @throws GridException If failed.
     */
    public GridLuceneIndex(IgniteMarshaller marshaller, @Nullable GridUnsafeMemory mem,
        @Nullable String spaceName, GridQueryTypeDescriptor type, boolean storeVal) throws GridException {
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
            throw new GridException(e);
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

            for (int i = 0, len = fields.size() ; i < len; i++)
                keyFields.set(i, type.keyFields().containsKey(idxdFields[i]));
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
     * @throws GridException If failed.
     */
    public void store(Object key, Object val, byte[] ver, long expires) throws GridException {
        Document doc = new Document();

        boolean stringsFound = false;

        if (type.valueTextIndex() || type.valueClass() == String.class) {
            doc.add(new Field(VAL_STR_FIELD_NAME, val.toString(), Field.Store.YES, Field.Index.ANALYZED));

            stringsFound = true;
        }

        for (int i = 0, last = idxdFields.length - 1; i < last; i++) {
            Object fieldVal = type.value(keyFields.get(i) ? key : val, idxdFields[i]);

            if (fieldVal != null) {
                doc.add(new Field(idxdFields[i], fieldVal.toString(), Field.Store.YES, Field.Index.ANALYZED));

                stringsFound = true;
            }
        }

        String keyStr = Base64.encodeBase64String(marshaller.marshal(key));

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
            throw new GridException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /**
     * Removes entry for given key from this index.
     *
     * @param key Key.
     * @throws GridException If failed.
     */
    public void remove(Object key) throws GridException {
        try {
            writer.deleteDocuments(new Term(KEY_FIELD_NAME, Base64.encodeBase64String(marshaller.marshal(key))));
        }
        catch (IOException e) {
            throw new GridException(e);
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
     * @throws GridException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(String qry,
        GridIndexingQueryFilter filters) throws GridException {
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
            throw new GridException(e);
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
            throw new GridException(e);
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
         * @throws GridException if failed.
         */
        private It(IndexReader reader, IndexSearcher searcher, ScoreDoc[] docs, IgniteBiPredicate<K, V> filters)
            throws GridException {
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
         * @throws GridException If failed.
         */
        private void findNext() throws GridException {
            curr = null;

            while (idx < docs.length) {
                Document doc;

                try {
                    doc = searcher.doc(docs[idx++].doc);
                }
                catch (IOException e) {
                    throw new GridException(e);
                }

                String keyStr = doc.get(KEY_FIELD_NAME);

                ClassLoader ldr = null; // TODO

                K k = marshaller.unmarshal(Base64.decodeBase64(keyStr), ldr);

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
        @Override protected IgniteBiTuple<K, V> onNext() throws GridException {
            IgniteBiTuple<K, V> res = curr;

            findNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws GridException {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws GridException {
            U.closeQuiet(searcher);
            U.closeQuiet(reader);
        }
    }
}
