/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Writer collecting information about object fields.
 */
class GridPortableMetadataCollectingWriter implements GridPortableWriter {
    /** */
    private Map<Integer, List<String>> fieldsMap = new HashMap<>();

    /** */
    private List<String> fields;

    /**
     * @param portable Portable object.
     * @return Information about type fields.
     * @throws IOException In case of error.
     */
    Map<Integer, List<String>> writeAndCollect(GridPortableObject portable) throws IOException {
        fieldsMap = new HashMap<>();

        fields = new ArrayList<>();

        writePortable(portable);

        return fieldsMap;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, String val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID uuid) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws IOException {
        onWrite(fieldName);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(String fieldName, T obj) throws IOException {
        onWrite(fieldName);

        writeObject(obj);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, Map<K, V> map) throws IOException {
        onWrite(fieldName);

        if (map != null)
            writeMap(map);
    }

    /**
     * @param map Map/
     * @throws IOException In case of error.
     */
    private <K, V> void writeMap(Map<K, V> map) throws IOException {
        for (Map.Entry<K, V> e : map.entrySet()) {
            writeObject(e.getKey());
            writeObject(e.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws IOException {
        onWrite(fieldName);

        if (col != null)
            writeCollection(col);
    }

    /**
     * @param col Collection.
     * @throws IOException In case of error.
     */
    private <T> void writeCollection(Collection<T> col) throws IOException {
        for (T obj : col)
            writeObject(obj);
    }

    /**
     * @param portable Portable object.
     * @throws IOException In case of error.
     */
    private void writePortable(GridPortableObject portable) throws IOException {
        List<String> curFields = new ArrayList<>(fields);

        fields = new ArrayList<>();

        portable.writePortable(this);

        fieldsMap.put(portable.typeId(), fields);

        fields = curFields;
    }

    /**
     * @param obj Object.
     * @throws IOException In case of error.
     */
    private void writeObject(Object obj) throws IOException {
        if (obj instanceof GridPortableObject)
            writePortable((GridPortableObject)obj);
        else if (obj instanceof Map)
            writeMap((Map)obj);
        else if (obj instanceof Collection)
            writeCollection((Collection)obj);
    }

    /**
     * @param fieldName Field name.
     * @throws IOException In case of error.
     */
    private void onWrite(String fieldName) throws IOException {
        if (!fields.add(fieldName))
            throw new IOException("Duplicated field:  " + fieldName);
    }
}
