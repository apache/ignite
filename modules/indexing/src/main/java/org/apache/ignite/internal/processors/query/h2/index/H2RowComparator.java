package org.apache.ignite.internal.processors.query.h2.index;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.ThreadLocalSessionHolder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.value.DataType;
import org.h2.value.Value;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Provide H2 logic of keys comparation.
 */
public class H2RowComparator implements IndexRowComparator {

    private final CacheObjectContext coctx;

    private final GridH2Table table;

    public H2RowComparator(GridH2Table table) {
        this.table = table;
        coctx = table.rowDescriptor().context().cacheObjectContext();
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, Object v, int curType)
        throws IgniteCheckedException {

        if (curType == IndexKeyTypes.UNKNOWN)
            return CANT_BE_COMPARE;

        int objType = InlineIndexKeyTypeRegistry.get(v.getClass()).type();

        int type = Value.getHigherOrder(curType, objType);

        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(type);

        // H2 supports comparison between different types after casting them to single type.
        if (type != objType && type == curType) {
            Value va = DataType.convertToValue(ThreadLocalSessionHolder.getSession(), v, type);

            // TODO: is there other way to invoke inline comparation?
            int c = ((NullableInlineIndexKeyType) t).compare0(pageAddr, off, va.getObject());

            if (c != COMPARE_UNSUPPORTED)
                return c;
        }

        Object v1 = t.get(pageAddr, off, maxSize);

        if (v1 == null)
            return CANT_BE_COMPARE;

        int c = compareValues(wrap(v1, curType), wrap(v, objType));

        return Integer.signum(c);
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexSearchRow left, IndexSearchRow right, int idx) throws IgniteCheckedException {
        Object robject = right.getKey(idx);
        Object lobject = left.getKey(idx);

        int ltype = left.getSchema().getKeyDefinitions()[idx].getIdxType();

        int rtype;

        // Right side of comparison can be set by user in query with type that different for specified schema.
        if (right instanceof IndexSearchRowImpl)
            rtype = DataType.getTypeFromClass(robject.getClass());
        else
            rtype = right.getSchema().getKeyDefinitions()[idx].getIdxType();

        if (lobject == null)
            return CANT_BE_COMPARE;

        int c = compareValues(wrap(lobject, ltype), wrap(robject, rtype));

        return Integer.signum(c);
    }

    /** */
    private Value wrap(Object val, int type) throws IgniteCheckedException {
        return H2Utils.wrap(coctx, val, type);
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) {
        return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);
    }
}
