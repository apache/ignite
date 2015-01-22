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

package org.apache.ignite.client;

import org.apache.ignite.portables.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Test portable object.
 */
@SuppressWarnings("PublicField")
public class GridClientTestPortable implements PortableMarshalAware, Serializable {
    /** */
    public byte b;

    /** */
    public byte bRaw;

    /** */
    public short s;

    /** */
    public short sRaw;

    /** */
    public int i;

    /** */
    public int iRaw;

    /** */
    public long l;

    /** */
    public long lRaw;

    /** */
    public float f;

    /** */
    public float fRaw;

    /** */
    public double d;

    /** */
    public double dRaw;

    /** */
    public char c;

    /** */
    public char cRaw;

    /** */
    public boolean bool;

    /** */
    public boolean boolRaw;

    /** */
    public String str;

    /** */
    public String strRaw;

    /** */
    public UUID uuid;

    /** */
    public UUID uuidRaw;

    /** */
    public Date date;

    /** */
    public Date dateRaw;

    /** */
    public TestEnum e;

    /** */
    public TestEnum eRaw;

    /** */
    public byte[] bArr;

    /** */
    public byte[] bArrRaw;

    /** */
    public short[] sArr;

    /** */
    public short[] sArrRaw;

    /** */
    public int[] iArr;

    /** */
    public int[] iArrRaw;

    /** */
    public long[] lArr;

    /** */
    public long[] lArrRaw;

    /** */
    public float[] fArr;

    /** */
    public float[] fArrRaw;

    /** */
    public double[] dArr;

    /** */
    public double[] dArrRaw;

    /** */
    public char[] cArr;

    /** */
    public char[] cArrRaw;

    /** */
    public boolean[] boolArr;

    /** */
    public boolean[] boolArrRaw;

    /** */
    public String[] strArr;

    /** */
    public String[] strArrRaw;

    /** */
    public UUID[] uuidArr;

    /** */
    public UUID[] uuidArrRaw;

    /** */
    public Date[] dateArr;

    /** */
    public Date[] dateArrRaw;

    /** */
    public TestEnum[] eArr;

    /** */
    public TestEnum[] eArrRaw;

    /** */
    public Object[] objArr;

    /** */
    public Object[] objArrRaw;

    /** */
    public Collection<String> col;

    /** */
    public Collection<String> colRaw;

    /** */
    public Map<Integer, String> map;

    /** */
    public Map<Integer, String> mapRaw;

    /** */
    public GridClientTestPortable portable1;

    /** */
    public GridClientTestPortable portable2;

    /** */
    public GridClientTestPortable portableRaw1;

    /** */
    public GridClientTestPortable portableRaw2;

    /**
     */
    public GridClientTestPortable() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param createInner If {@code true} creates nested object.
     */
    public GridClientTestPortable(int val, boolean createInner) {
        b = (byte)val;
        bRaw = (byte)(val + 1);

        s = (short)val;
        sRaw = (short)(val + 1);

        i = val;
        iRaw = i + 1;

        l = val;
        lRaw = i + 1;

        f = val + 0.5f;
        fRaw = f + 1;

        d = val + 0.5f;
        dRaw = d + 1;

        c = (char)val;
        cRaw = (char)(val + 1);

        bool = true;
        boolRaw = false;

        str = String.valueOf(i);
        strRaw = String.valueOf(iRaw);

        uuid = new UUID(i, i);
        uuidRaw = new UUID(iRaw, iRaw);

        date = new Date(i);
        dateRaw = new Date(iRaw);

        e = enumValue(i);
        eRaw = enumValue(iRaw);

        bArr = new byte[]{b, (byte)(b + 1)};
        bArrRaw = new byte[]{bRaw, (byte)(bRaw + 1)};

        sArr = new short[]{s, (short)(s + 1)};
        sArrRaw = new short[]{sRaw, (short)(sRaw + 1)};

        iArr = new int[]{i, i + 1};
        iArrRaw = new int[]{iRaw, iRaw + 1};

        lArr = new long[]{l, l + 1};
        lArrRaw = new long[]{lRaw, lRaw + 1};

        fArr = new float[]{f, f + 1};
        fArrRaw = new float[]{fRaw, fRaw + 1};

        dArr = new double[]{d, d + 1};
        dArrRaw = new double[]{dRaw, dRaw + 1};

        cArr = new char[]{c, (char)(c + 1)};
        cArrRaw = new char[]{cRaw, (char)(cRaw + 1)};

        boolArr = new boolean[]{true, true};
        boolArrRaw = new boolean[]{true, true};

        strArr = new String[]{str, str + "1"};
        strArrRaw = new String[]{strRaw, strRaw + "1"};

        uuidArr = new UUID[]{uuid, new UUID(uuid.getMostSignificantBits() + 1, uuid.getLeastSignificantBits() + 1)};
        uuidArrRaw = new UUID[]{uuidRaw,
            new UUID(uuidRaw.getMostSignificantBits() + 1, uuidRaw.getLeastSignificantBits() + 1)};

        dateArr = new Date[]{date, new Date(date.getTime() + 1)};
        dateArrRaw = new Date[]{dateRaw, new Date(dateRaw.getTime() + 1)};

        eArr = new TestEnum[]{enumValue(i), enumValue(i + 1)};
        eArrRaw = new TestEnum[]{enumValue(iRaw), enumValue(iRaw + 1)};

        objArr = new Object[]{uuid, new UUID(uuid.getMostSignificantBits() + 1, uuid.getLeastSignificantBits() + 1)};
        objArrRaw = new Object[]{uuidRaw,
            new UUID(uuidRaw.getMostSignificantBits() + 1, uuidRaw.getLeastSignificantBits() + 1)};

        col = Arrays.asList(str, str + "1");
        colRaw = Arrays.asList(strRaw, strRaw + "1");

        map = new HashMap<>();
        map.put(1, str);
        map.put(2, str + "1");

        mapRaw = new HashMap<>();
        mapRaw.put(1, strRaw);
        mapRaw.put(2, strRaw + "1");

        if (createInner) {
            portable1 = new GridClientTestPortable(val + 1, false);
            portable2 = portable1;

            portableRaw1 = new GridClientTestPortable(val + 2, false);
            portableRaw2 = portableRaw1;
        }
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        writer.writeByte("_b", b);
        writer.writeShort("_s", s);
        writer.writeInt("_i", i);
        writer.writeLong("_l", l);
        writer.writeFloat("_f", f);
        writer.writeDouble("_d", d);
        writer.writeChar("_c", c);
        writer.writeBoolean("_bool", bool);
        writer.writeString("_str", str);
        writer.writeUuid("_uuid", uuid);
        writer.writeDate("_date", date);
        writer.writeEnum("_enum", e);
        writer.writeByteArray("_bArr", bArr);
        writer.writeShortArray("_sArr", sArr);
        writer.writeIntArray("_iArr", iArr);
        writer.writeLongArray("_lArr", lArr);
        writer.writeFloatArray("_fArr", fArr);
        writer.writeDoubleArray("_dArr", dArr);
        writer.writeCharArray("_cArr", cArr);
        writer.writeBooleanArray("_boolArr", boolArr);
        writer.writeStringArray("_strArr", strArr);
        writer.writeUuidArray("_uuidArr", uuidArr);
        writer.writeDateArray("_dateArr", dateArr);
        writer.writeEnumArray("_eArr", eArr);
        writer.writeObjectArray("_objArr", objArr);
        writer.writeCollection("_col", col);
        writer.writeMap("_map", map);
        writer.writeObject("_portable1", portable1);
        writer.writeObject("_portable2", portable2);

        PortableRawWriter raw = writer.rawWriter();

        raw.writeByte(bRaw);
        raw.writeShort(sRaw);
        raw.writeInt(iRaw);
        raw.writeLong(lRaw);
        raw.writeFloat(fRaw);
        raw.writeDouble(dRaw);
        raw.writeChar(cRaw);
        raw.writeBoolean(boolRaw);
        raw.writeString(strRaw);
        raw.writeUuid(uuidRaw);
        raw.writeDate(dateRaw);
        raw.writeEnum(eRaw);
        raw.writeByteArray(bArrRaw);
        raw.writeShortArray(sArrRaw);
        raw.writeIntArray(iArrRaw);
        raw.writeLongArray(lArrRaw);
        raw.writeFloatArray(fArrRaw);
        raw.writeDoubleArray(dArrRaw);
        raw.writeCharArray(cArrRaw);
        raw.writeBooleanArray(boolArrRaw);
        raw.writeStringArray(strArrRaw);
        raw.writeUuidArray(uuidArrRaw);
        raw.writeDateArray(dateArrRaw);
        raw.writeEnumArray(eArrRaw);
        raw.writeObjectArray(objArrRaw);
        raw.writeCollection(colRaw);
        raw.writeMap(mapRaw);
        raw.writeObject(portableRaw1);
        raw.writeObject(portableRaw2);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        b = reader.readByte("_b");
        s = reader.readShort("_s");
        i = reader.readInt("_i");
        l = reader.readLong("_l");
        f = reader.readFloat("_f");
        d = reader.readDouble("_d");
        c = reader.readChar("_c");
        bool = reader.readBoolean("_bool");
        str = reader.readString("_str");
        uuid = reader.readUuid("_uuid");
        date = reader.readDate("_date");
        e = reader.readEnum("_enum", TestEnum.class);
        bArr = reader.readByteArray("_bArr");
        sArr = reader.readShortArray("_sArr");
        iArr = reader.readIntArray("_iArr");
        lArr = reader.readLongArray("_lArr");
        fArr = reader.readFloatArray("_fArr");
        dArr = reader.readDoubleArray("_dArr");
        cArr = reader.readCharArray("_cArr");
        boolArr = reader.readBooleanArray("_boolArr");
        strArr = reader.readStringArray("_strArr");
        uuidArr = reader.readUuidArray("_uuidArr");
        dateArr = reader.readDateArray("_dateArr");
        eArr = reader.readEnumArray("_eArr", TestEnum.class);
        objArr = reader.readObjectArray("_objArr");
        col = reader.readCollection("_col");
        map = reader.readMap("_map");
        portable1 = (GridClientTestPortable)reader.readObject("_portable1");
        portable2 = (GridClientTestPortable)reader.readObject("_portable2");

        PortableRawReader raw = reader.rawReader();

        bRaw = raw.readByte();
        sRaw = raw.readShort();
        iRaw = raw.readInt();
        lRaw = raw.readLong();
        fRaw = raw.readFloat();
        dRaw = raw.readDouble();
        cRaw = raw.readChar();
        boolRaw = raw.readBoolean();
        strRaw = raw.readString();
        uuidRaw = raw.readUuid();
        dateRaw = raw.readDate();
        eRaw = raw.readEnum(TestEnum.class);
        bArrRaw = raw.readByteArray();
        sArrRaw = raw.readShortArray();
        iArrRaw = raw.readIntArray();
        lArrRaw = raw.readLongArray();
        fArrRaw = raw.readFloatArray();
        dArrRaw = raw.readDoubleArray();
        cArrRaw = raw.readCharArray();
        boolArrRaw = raw.readBooleanArray();
        strArrRaw = raw.readStringArray();
        uuidArrRaw = raw.readUuidArray();
        dateArrRaw = raw.readDateArray();
        eArrRaw = raw.readEnumArray(TestEnum.class);
        objArrRaw = raw.readObjectArray();
        colRaw = raw.readCollection();
        mapRaw = raw.readMap();
        portableRaw1 = (GridClientTestPortable)raw.readObject();
        portableRaw2 = (GridClientTestPortable)raw.readObject();
    }

    /**
     * @param idx Value index.
     * @return Enum value.
     */
    static TestEnum enumValue(int idx) {
        return TestEnum.values()[idx % TestEnum.values().length];
    }

    /**
     * Test enum.
     */
    private enum TestEnum {
        /** */
        VAL1,

        /** */
        VAL2,

        /** */
        VAl3,

        /** */
        VAL4,

        /** */
        VAL5,

        /** */
        VAL6,

        /** */
        VAL7,

        /** */
        VAL8,

        /** */
        VAL9,

        /** */
        VAL10
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientTestPortable.class, this);
    }
}
