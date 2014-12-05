/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.indexing.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridLuceneIndexLoadTest {
    /**
     * @param args Arguments.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public static void main(String ... args) throws IgniteSpiException, FileNotFoundException {
        final IgniteOptimizedMarshaller m = new IgniteOptimizedMarshaller();

        IndexingTypeDescriptor desc = new IndexingTypeDescriptor() {
            @Override public String name() {
                return "StrType";
            }

            @Override public Map<String, Class<?>> valueFields() {
                throw new IllegalStateException();
            }

            @Override public Map<String, Class<?>> keyFields() {
                throw new IllegalStateException();
            }

            @Override public <T> T value(Object obj, String field) {
                throw new IllegalStateException();
            }

            @Override public Map<String, IndexDescriptor> indexes() {
                return Collections.<String, IndexDescriptor>singletonMap("txt_idx", new IndexDescriptor() {
                    @Override public Collection<String> fields() {
                        return Collections.emptyList();
                    }

                    @Override public boolean descending(String field) {
                        return false;
                    }

                    @Override public IndexType type() {
                        return IndexType.FULLTEXT;
                    }
                });
            }

            @Override public Class<?> valueClass() {
                return String.class;
            }

            @Override public Class<?> keyClass() {
                return Integer.class;
            }

            @Override public boolean valueTextIndex() {
                return true;
            }
        };

        GridLuceneIndex idx = new GridLuceneIndex(new IndexingMarshaller() {
            @Override public <T> IndexingEntity<T> unmarshal(byte[] bytes) throws IgniteSpiException {
                try {
                    return new IndexingEntityAdapter<>(m.<T>unmarshal(bytes, null), bytes);
                }
                catch (GridException e) {
                    throw new IgniteSpiException(e);
                }
            }

            @Override public byte[] marshal(IndexingEntity<?> entity) throws IgniteSpiException {
                try {
                    return m.marshal(entity.value());
                }
                catch (GridException e) {
                    throw new IgniteSpiException(e);
                }
            }
        }, null, "spac", desc, false);

        ArrayList<String> ws = words("C:\\Users\\svladykin\\YandexDisk\\www\\CSW07-british-dict");

        byte[] ver = new byte[0];

        Random rnd = new Random();

        long begin = System.currentTimeMillis();

        for (int i = 0, len = 10000000 ; i < len; i++) {
            idx.store(new IndexingEntityAdapter<>(i, null),
                new IndexingEntityAdapter<Object>(sentence(rnd, ws), null),
                ver, 0L);

            if (i % 10000 == 0) {
                long time = System.currentTimeMillis();

                X.println(i + " " + (time - begin) + "ms " + GridLuceneFile.filesCnt.get());

                begin = time;
            }
        }
    }

    /**
     * @param rnd Random.
     * @param ws Words.
     * @return Sentence.
     */
    static String sentence(Random rnd, ArrayList<String> ws) {
        StringBuilder b = new StringBuilder();

        for (int i = 0, wordCnt = 1 + rnd.nextInt(8); i < wordCnt; i++) {
            if (i != 0)
                b.append(' ');

            b.append(ws.get(rnd.nextInt(ws.size())));
        }

        return b.toString();
    }

    /**
     * @param file File.
     * @return Words list.
     * @throws FileNotFoundException If failed.
     */
    static ArrayList<String> words(String file) throws FileNotFoundException {
        Scanner scan = new Scanner(new File(file));

        ArrayList<String> res = new ArrayList<>(270000);

        while (scan.hasNextLine()) {
            String line = scan.nextLine();

            int space = line.indexOf(' ');

            if (space > 0)
                line = line.substring(0, space);

            res.add(line.toLowerCase());
        }

        return res;
    }
}
