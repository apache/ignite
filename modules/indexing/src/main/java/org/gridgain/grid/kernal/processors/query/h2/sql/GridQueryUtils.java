/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.h2.table.*;

import java.lang.reflect.*;
import java.util.*;

/**
 *
 */
public class GridQueryUtils {

    /**
     * @param res
     * @param tbl
     */
    public static void extractTables(Set<TableBase> res, Table tbl) {
        if (tbl instanceof TableBase) {
            res.add((TableBase)tbl);

            return;
        }

        if (tbl instanceof TableView) {
            List<Table> tableList = getTables((TableView)tbl);

            for (Table table : tableList) {
                extractTables(res, table);
            }
        }
        else
            throw new IllegalArgumentException("Unknown table type: " + tbl);
    }

    /**
     * @param view Table view.
     * @return List of tables.
     */
    public static List<Table> getTables(TableView view) {
        try {
            Field field = TableView.class.getDeclaredField("tables");

            field.setAccessible(true);

            return (List<Table>)field.get(view);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param fld
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> T getFieldValue(Field fld, Object obj) {
        try {
            return (T)fld.get(obj);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param cls
     * @param fieldName
     * @return
     */
    public static Field getField(Class<?> cls, String fieldName) {
        Field field;
        try {
            field = cls.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

        field.setAccessible(true);

        return field;
    }

    /**
     * @param cls Class.
     * @param obj Object.
     * @param fldName Fld name.
     */
    public static <C> Object getFieldValue(Class<? super C> cls, C obj, String fldName) {
        Field field = getField(cls, fldName);

        return getFieldValue(field, obj);
    }

    /**
     * @param obj Object.
     * @param fldName Fld name.
     */
    public static <T> T getFieldValue(Object obj, String fldName) {
        return (T)getFieldValue((Class<? super Object>)obj.getClass(), obj, fldName);
    }

    /**
     * @param cls Class.
     * @param fldName Fld name.
     */
    public static <T, R> Getter<T, R> getter(Class<T> cls, String fldName) {
        Field field = getField(cls, fldName);

        return new Getter<>(field);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Getter<T, R> {
        /** */
        private final Field fld;

        /**
         * @param fld Fld.
         */
        private Getter(Field fld) {
            this.fld = fld;
        }

        /**
         * @param obj Object.
         * @return Result.
         */
        public R get(T obj) {
            return getFieldValue(fld, obj);
        }
    }
}
