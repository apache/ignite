package org.apache.ignite.internal.processors.cache.transactions;

/**
 * Created by SBT-Kuznetsov-AL on 18.08.2017.
 */
public class TxThreadId {

    private long value;

    private boolean undefined = false;

    public TxThreadId(long id, boolean undefined) {
        this.value = id;
        this.undefined(undefined);
    }

    public TxThreadId() {
    }

    public void undefined(boolean undefined) {
        this.undefined = undefined;

        if(undefined)
            new Exception("Undefined").printStackTrace();
    }

    public boolean undefined(){
        assert value != 0;

        return undefined;
    }

    public void value(long val) {
        this.value = val;
    }

    public long value() {
        assert value != 0;

        if (!undefined)
            return value;
        else
            throw new RuntimeException("The value is undefined");
    }

    public long valueSafely(){
        return value;
    }

    public TxThreadId copy(){
        return new TxThreadId(value, undefined);
    }

    @Override public boolean equals(Object o) {
        return ((TxThreadId)o).undefined == undefined && ((TxThreadId)o).value == value;
    }
}
