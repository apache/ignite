/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.test.jaqu;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQTable;
import org.h2.util.New;

/**
 * A data class that contains a column for each data type.
 */
@JQTable(strictTypeMapping = true, version = 1)
public class SupportedTypes {

    static final SupportedTypes SAMPLE = new SupportedTypes();

    @JQColumn(primaryKey = true, autoIncrement = true)
    public Integer id;

    @JQColumn
    private Boolean myBool = false;

    @JQColumn
    private Byte myByte = 2;

    @JQColumn
    private Short myShort;

    @JQColumn
    private Integer myInteger;

    @JQColumn
    private Long myLong;

    @JQColumn
    private Float myFloat = 1.0f;

    @JQColumn
    private Double myDouble;

    @JQColumn
    private BigDecimal myBigDecimal;

    @JQColumn
    private String myString;

    @JQColumn
    private java.util.Date myUtilDate;

    @JQColumn
    private java.sql.Date mySqlDate;

    @JQColumn
    private java.sql.Time mySqlTime;

    @JQColumn
    private java.sql.Timestamp mySqlTimestamp;

    static List<SupportedTypes> createList() {
        List<SupportedTypes> list = New.arrayList();
        for (int i = 0; i < 10; i++) {
            list.add(randomValue());
        }
        return list;
    }

    static SupportedTypes randomValue() {
        Random rand = new Random();
        SupportedTypes s = new SupportedTypes();
        s.myBool = rand.nextBoolean();
        s.myByte = (byte) rand.nextInt(Byte.MAX_VALUE);
        s.myShort = (short) rand.nextInt(Short.MAX_VALUE);
        s.myInteger = rand.nextInt();
        s.myLong = rand.nextLong();
        s.myFloat = rand.nextFloat();
        s.myDouble = rand.nextDouble();
        s.myBigDecimal = new BigDecimal(rand.nextDouble());
        s.myString = Long.toHexString(rand.nextLong());
        s.myUtilDate = new java.util.Date(rand.nextLong());
        s.mySqlDate = new java.sql.Date(rand.nextLong());
        s.mySqlTime = new java.sql.Time(rand.nextLong() / 1_000 * 1_000);
        s.mySqlTimestamp = new java.sql.Timestamp(rand.nextLong());
        return s;
    }

    public boolean equivalentTo(SupportedTypes s) {
        boolean same = true;
        same &= myBool.equals(s.myBool);
        same &= myByte.equals(s.myByte);
        same &= myShort.equals(s.myShort);
        same &= myInteger.equals(s.myInteger);
        same &= myLong.equals(s.myLong);
        same &= myFloat.equals(s.myFloat);
        same &= myDouble.equals(s.myDouble);
        same &= myBigDecimal.equals(s.myBigDecimal);
        Timestamp a = new Timestamp(myUtilDate.getTime());
        same &= a.toString().equals(s.myUtilDate.toString());
        same &= mySqlTimestamp.toString().equals(s.mySqlTimestamp.toString());
        same &= mySqlDate.toString().equals(s.mySqlDate.toString());
        same &= mySqlTime.toString().equals(s.mySqlTime.toString());
        same &= myString.equals(s.myString);
        same &= true;
        return same;
    }

    /**
     * This class demonstrates the table upgrade.
     */
    @JQTable(name = "SupportedTypes", version = 2,
            inheritColumns = true, strictTypeMapping = true)
    public static class SupportedTypes2 extends SupportedTypes {

        public SupportedTypes2() {
            // nothing to do
        }
    }
}
