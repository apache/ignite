/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.gridgain.grid.util.typedef.*;
import org.h2.util.*;
import org.h2.value.*;

import java.util.*;

/**
 * Function.
 */
public class GridSqlFunction extends GridSqlElement {
    /** */
    private static final Map<String, GridSqlFunctionType> TYPE_MAP = new HashMap<>();

    /**
     *
     */
    static {
        for (GridSqlFunctionType type : GridSqlFunctionType.values())
            TYPE_MAP.put(type.name(), type);
    }

    /** */
    private final String name;

    /** */
    protected final GridSqlFunctionType type;

    /**  */
    private String castType;

    /**
     * @param type Function type.
     */
    public GridSqlFunction(GridSqlFunctionType type) {
        name = type.functionName();

        this.type = type;

        if (type == GridSqlFunctionType.CONVERT)
            throw new UnsupportedOperationException();
    }

    /**
     * @param name Name.
     */
    public GridSqlFunction(String name) {
        this(TYPE_MAP.get(name));
    }

    /**
     * @param castType Type for {@link GridSqlFunctionType#CAST} function.
     */
    public void setCastType(String castType) {
        this.castType = castType;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(name);

        if (type == GridSqlFunctionType.CASE) {
            if (!children.isEmpty())
                buff.append(" ").append(child().getSQL());

            for (int i = 1, len = children.size() - 1; i < len; i += 2) {
                buff.append(" WHEN ").append(child(i).getSQL());
                buff.append(" THEN ").append(child(i + 1).getSQL());
            }
            if (children.size() % 2 == 0)
                buff.append(" ELSE ").append(child(children.size() - 1).getSQL());

            return buff.append(" END").toString();
        }

        buff.append('(');

        if (type == GridSqlFunctionType.CAST) {
            assert !F.isEmpty(castType) : castType;
            assert children().size() == 1;

            buff.append(child().getSQL()).append(" AS ").append(castType);
        }
        else if (type == GridSqlFunctionType.CONVERT) {
            throw new UnsupportedOperationException("CONVERT");
//            buff.append(args[0].getSQL()).append(',').
//                append(new Column(null, dataType, precision, scale, displaySize).getCreateSQL());
        }
        else if (type == GridSqlFunctionType.EXTRACT) {
            ValueString v = (ValueString)((GridSqlConst)child(0)).value();
            buff.append(v.getString()).append(" FROM ").append(child(1).getSQL());
        }
        else {
            for (GridSqlElement e : children) {
                buff.appendExceptFirst(", ");
                buff.append(e.getSQL());
            }
        }

        return buff.append(')').toString();
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Type.
     */
    public GridSqlFunctionType type() {
        return type;
    }
}
