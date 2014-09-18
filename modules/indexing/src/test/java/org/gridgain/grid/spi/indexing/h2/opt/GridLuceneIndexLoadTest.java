/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.*;
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
    public static void main(String ... args) throws GridSpiException, FileNotFoundException {
        final GridOptimizedMarshaller m = new GridOptimizedMarshaller();

        GridIndexingTypeDescriptor desc = new GridIndexingTypeDescriptor() {
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

            @Override public Map<String, GridIndexDescriptor> indexes() {
                return Collections.<String, GridIndexDescriptor>singletonMap("txt_idx", new GridIndexDescriptor() {
                    @Override public Collection<String> fields() {
                        return Collections.emptyList();
                    }

                    @Override public boolean descending(String field) {
                        return false;
                    }

                    @Override public GridIndexType type() {
                        return GridIndexType.FULLTEXT;
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

        GridLuceneIndex idx = new GridLuceneIndex(new GridIndexingMarshaller() {
            @Override public <T> GridIndexingEntity<T> unmarshal(byte[] bytes) throws GridSpiException {
                try {
                    return new GridIndexingEntityAdapter<>(m.<T>unmarshal(bytes, null), bytes);
                }
                catch (GridException e) {
                    throw new GridSpiException(e);
                }
            }

            @Override public byte[] marshal(GridIndexingEntity<?> entity) throws GridSpiException {
                try {
                    return m.marshal(entity.value());
                }
                catch (GridException e) {
                    throw new GridSpiException(e);
                }
            }
        }, null, "spac", desc, false);

        ArrayList<String> ws = words("C:\\Users\\svladykin\\YandexDisk\\www\\CSW07-british-dict");

        byte[] ver = new byte[0];

        Random rnd = new Random();

        long begin = System.currentTimeMillis();

        for (int i = 0, len = 10000000 ; i < len; i++) {
            idx.store(new GridIndexingEntityAdapter<>(i, null),
                new GridIndexingEntityAdapter<Object>(sentence(rnd, ws), null),
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
