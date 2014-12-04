/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;

/**
 * Communication topic.
 */
public enum GridTopic {
    /** */
    TOPIC_JOB,

    /** */
    TOPIC_JOB_SIBLINGS,

    /** */
    TOPIC_TASK,

    /** */
    TOPIC_CHECKPOINT,

    /** */
    TOPIC_JOB_CANCEL,

    /** */
    TOPIC_TASK_CANCEL,

    /** */
    TOPIC_CLASSLOAD,

    /** */
    TOPIC_EVENT,

    /** Cache topic. */
    TOPIC_CACHE,

    /** */
    TOPIC_COMM_USER,

    /** */
    TOPIC_REST,

    /** */
    TOPIC_REPLICATION,

    /** */
    TOPIC_GGFS,

    /** */
    TOPIC_DATALOAD,

    /** */
    TOPIC_STREAM,

    /** */
    TOPIC_CONTINUOUS,

    /** */
    TOPIC_MONGO,

    /** */
    TOPIC_TIME_SYNC,

    /** */
    TOPIC_HADOOP;

    /** Enum values. */
    private static final GridTopic[] VALS = values();

    /** Default charset to work with strings. */
    private static final Charset DFLT_CHARSET = Charset.forName("UTF-8");

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridTopic fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @param id Topic ID.
     * @return Grid message topic with specified ID.
     */
    public Object topic(IgniteUuid id) {
        return new T1(this, id);
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(IgniteUuid id1, UUID id2) {
        return new T2(this, id1, id2);
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(IgniteUuid id1, long id2) {
        return new T8(this, id1, id2);
    }

    /**
     * NOTE: The method should be used only for cases when there is no any other non-string identifier(s)
     * to use to differentiate topics.
     *
     * @param id Topic ID.
     * @return Grid message topic with specified ID.
     */
    public Object topic(String id) {
        return new T3(this, UUID.nameUUIDFromBytes(id.getBytes(DFLT_CHARSET)));
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(String id1, long id2) {
        return new T6(this, UUID.nameUUIDFromBytes(id1.getBytes(DFLT_CHARSET)), id2);
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @param id3 ID3.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(String id1, int id2, long id3) {
        return new T5(this, UUID.nameUUIDFromBytes(id1.getBytes(DFLT_CHARSET)), id2, id3);
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @param id3 ID3.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(String id1, UUID id2, long id3) {
        return new T4(this, UUID.nameUUIDFromBytes(id1.getBytes(DFLT_CHARSET)), id2, id3);
    }

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     * @param id3 ID3.
     * @param id4 ID4.
     * @return Grid message topic with specified IDs.
     */
    public Object topic(String id1, UUID id2, int id3, long id4) {
        return new T7(this, UUID.nameUUIDFromBytes(id1.getBytes(DFLT_CHARSET)), id2, id3, id4);
    }

    /**
     *
     */
    private static class T1 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private IgniteUuid id;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T1() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id ID.
         */
        private T1(GridTopic topic, IgniteUuid id) {
            this.topic = topic;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T1.class) {
                T1 that = (T1)obj;

                return topic == that.topic && id.equals(that.id);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeGridUuid(out, id);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id = U.readGridUuid(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T1.class, this);
        }
    }

    /**
     *
     */
    private static class T2 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private IgniteUuid id1;

        /** */
        private UUID id2;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T2() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         */
        private T2(GridTopic topic, IgniteUuid id1, UUID id2) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T2.class) {
                T2 that = (T2)obj;

                return topic == that.topic && id1.equals(that.id1) && id2.equals(that.id2);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeGridUuid(out, id1);
            U.writeUuid(out, id2);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readGridUuid(in);
            id2 = U.readUuid(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T2.class, this);
        }
    }

    /**
     *
     */
    private static class T3 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T3() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         */
        private T3(GridTopic topic, UUID id1) {
            this.topic = topic;
            this.id1 = id1;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T3.class) {
                T3 that = (T3)obj;

                return topic == that.topic && id1.equals(that.id1);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T3.class, this);
        }
    }

    /**
     *
     */
    private static class T4 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private UUID id2;

        /** */
        private long id3;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T4() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         * @param id3 ID3.
         */
        private T4(GridTopic topic, UUID id1, UUID id2, long id3) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2.hashCode() + (int)(id3 ^ (id3 >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T4.class) {
                T4 that = (T4)obj;

                return topic == that.topic && id1.equals(that.id1) && id2.equals(that.id2) && id3 == that.id3;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            U.writeUuid(out, id2);
            out.writeLong(id3);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = U.readUuid(in);
            id3 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T4.class, this);
        }
    }

    /**
     *
     */
    private static class T5 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private int id2;

        /** */
        private long id3;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T5() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         * @param id3 ID3.
         */
        private T5(GridTopic topic, UUID id1, int id2, long id3) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2 + (int)(id3 ^ (id3 >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T5.class) {
                T5 that = (T5)obj;

                return topic == that.topic && id1.equals(that.id1) && id2 == that.id2 && id3 == that.id3;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            out.writeInt(id2);
            out.writeLong(id3);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = in.readInt();
            id3 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T5.class, this);
        }
    }

    /**
     *
     */
    private static class T6 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private long id2;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T6() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID 1.
         * @param id2 ID 2.
         */
        private T6(GridTopic topic, UUID id1, long id2) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode () + (int)(id2 ^ (id2 >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T6.class) {
                T6 that = (T6)obj;

                return topic == that.topic && id1.equals(that.id1) && id2 == that.id2;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            out.writeLong(id2);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T6.class, this);
        }
    }

    /**
     *
     */
    private static class T7 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private UUID id1;

        /** */
        private UUID id2;

        /** */
        private int id3;

        /** */
        private long id4;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T7() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         * @param id3 ID3.
         * @param id4 ID4.
         */
        private T7(GridTopic topic, UUID id1, UUID id2, int id3, long id4) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
            this.id4 = id4;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + id2.hashCode() + id3 + (int)(id4 ^ (id4 >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T7.class) {
                T7 that = (T7)obj;

                return topic == that.topic && id1.equals(that.id1) && id2.equals(that.id2) && id3 == that.id3 &&
                    id4 == that.id4;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeUuid(out, id1);
            U.writeUuid(out, id2);
            out.writeInt(id3);
            out.writeLong(id4);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readUuid(in);
            id2 = U.readUuid(in);
            id3 = in.readInt();
            id4 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T7.class, this);
        }
    }

    /**
     *
     */
    private static class T8 implements Externalizable, GridOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** */
        private GridTopic topic;

        /** */
        private IgniteUuid id1;

        /** */
        private long id2;

        /**
         * No-arg constructor needed for {@link Serializable}.
         */
        public T8() {
            // No-op.
        }

        /**
         * @param topic Topic.
         * @param id1 ID1.
         * @param id2 ID2.
         */
        private T8(GridTopic topic, IgniteUuid id1, long id2) {
            this.topic = topic;
            this.id1 = id1;
            this.id2 = id2;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return topic.ordinal() + id1.hashCode() + (int)(id2 ^ (id2 >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj.getClass() == T8.class) {
                T8 that = (T8)obj;

                return topic == that.topic && id1.equals(that.id1) && id2 == that.id2;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(topic.ordinal());
            U.writeGridUuid(out, id1);
            out.writeLong(id2);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topic = fromOrdinal(in.readByte());
            id1 = U.readGridUuid(in);
            id2 = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(T8.class, this);
        }
    }
}
