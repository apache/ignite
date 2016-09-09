package org.apache.ignite.tests;

import java.io.Serializable;
import java.util.Date;

public class MyPojo implements Serializable {
    private String field1;
    private int field2;
    private long field3;
    private Date field4;
    private MyPojo ref;

    public MyPojo() {
    }

    public MyPojo(String field1, int field2, long field3, Date field4, MyPojo ref) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.ref = ref;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof MyPojo))
            return false;

        MyPojo myObj = (MyPojo)obj;

        if ((field1 == null && myObj.field1 != null) ||
            (field1 != null && !field1.equals(myObj.field1)))
            return false;

        if ((field4 == null && myObj.field4 != null) ||
            (field4 != null && !field4.equals(myObj.field4)))
            return false;

        return field2 == myObj.field2 && field3 == myObj.field3;
    }

    public void setRef(MyPojo ref) {
        this.ref = ref;
    }

    public MyPojo getRef() {
        return ref;
    }
}
