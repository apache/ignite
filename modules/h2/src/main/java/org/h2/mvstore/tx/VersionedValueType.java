/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.value.VersionedValue;
import java.nio.ByteBuffer;

/**
 * The value type for a versioned value.
 */
public class VersionedValueType implements DataType {

    private final DataType valueType;

    public VersionedValueType(DataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        if(obj == null) return 0;
        VersionedValue v = (VersionedValue) obj;
        int res = Constants.MEMORY_OBJECT + 8 + 2 * Constants.MEMORY_POINTER +
                getValMemory(v.getCurrentValue());
        if (v.getOperationId() != 0) {
            res += getValMemory(v.getCommittedValue());
        }
        return res;
    }

    private int getValMemory(Object obj) {
        return obj == null ? 0 : valueType.getMemory(obj);
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        } else if (aObj == null) {
            return -1;
        } else if (bObj == null) {
            return 1;
        }
        VersionedValue a = (VersionedValue) aObj;
        VersionedValue b = (VersionedValue) bObj;
        long comp = a.getOperationId() - b.getOperationId();
        if (comp == 0) {
            return valueType.compare(a.getCurrentValue(), b.getCurrentValue());
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        if (buff.get() == 0) {
            // fast path (no op ids or null entries)
            for (int i = 0; i < len; i++) {
                obj[i] = VersionedValueCommitted.getInstance(valueType.read(buff));
            }
        } else {
            // slow path (some entries may be null)
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        long operationId = DataUtils.readVarLong(buff);
        if (operationId == 0) {
            return VersionedValueCommitted.getInstance(valueType.read(buff));
        } else {
            byte flags = buff.get();
            Object value = (flags & 1) != 0 ? valueType.read(buff) : null;
            Object committedValue = (flags & 2) != 0 ? valueType.read(buff) : null;
            return VersionedValueUncommitted.getInstance(operationId, value, committedValue);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        boolean fastPath = true;
        for (int i = 0; i < len; i++) {
            VersionedValue v = (VersionedValue) obj[i];
            if (v.getOperationId() != 0 || v.getCurrentValue() == null) {
                fastPath = false;
            }
        }
        if (fastPath) {
            buff.put((byte) 0);
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                valueType.write(buff, v.getCurrentValue());
            }
        } else {
            // slow path:
            // store op ids, and some entries may be null
            buff.put((byte) 1);
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        long operationId = v.getOperationId();
        buff.putVarLong(operationId);
        if (operationId == 0) {
            valueType.write(buff, v.getCurrentValue());
        } else {
            Object committedValue = v.getCommittedValue();
            int flags = (v.getCurrentValue() == null ? 0 : 1) | (committedValue == null ? 0 : 2);
            buff.put((byte) flags);
            if (v.getCurrentValue() != null) {
                valueType.write(buff, v.getCurrentValue());
            }
            if (committedValue != null) {
                valueType.write(buff, committedValue);
            }
        }
    }
}
