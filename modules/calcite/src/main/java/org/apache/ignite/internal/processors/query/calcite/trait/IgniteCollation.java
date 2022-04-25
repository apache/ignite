package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Collation with offset and limit. */
public class IgniteCollation extends RelCollationImpl {
    /** SQL offset. */
    private final @Nullable RexNode offset;

    /** SQL limit. */
    private final @Nullable RexNode fetch;

    /** Ctor */
    public IgniteCollation(ImmutableList<RelFieldCollation> fieldCollations, RexNode offset, RexNode fetch) {
        super(fieldCollations);

        this.fetch = fetch;
        this.offset = offset;
    }

    /** */
    public RexNode getOffset() {
        return offset;
    }

    /** */
    public RexNode getFetch() {
        return fetch;
    }

    /** */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass() || !super.equals(o))
            return false;

        IgniteCollation that = (IgniteCollation)o;

        return Objects.equals(offset, that.offset) && Objects.equals(fetch, that.fetch);
    }

    /** */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), offset, fetch);
    }
}
