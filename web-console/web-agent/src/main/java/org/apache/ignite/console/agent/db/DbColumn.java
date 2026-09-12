

package org.apache.ignite.console.agent.db;
import org.apache.ignite.internal.util.typedef.internal.S;
import java.sql.Types;
/**
 * Database table column.
 */
public class DbColumn {
    /** Column name. */
    private final String name;

    /** Column JDBC type. */
    private final int type;

    /** Is this column belongs to primary key. */
    private final boolean key;

    /** Is {@code NULL} allowed for column in database. */
    private final boolean nullable;

    /** Whether column unsigned. */
    private final boolean unsigned;
    
    /** Whether column unsigned. */
    private final String typeName;


	private String comment;

    /**
     * @param name Column name.
     * @param type Column JDBC type.
     * @param key {@code true} if this column belongs to primary key.
     * @param nullable {@code true} if {@code NULL } allowed for column in database.
     * @param unsigned {@code true} if column is unsigned.
     */
    public DbColumn(String name, int type, String typeName,boolean key, boolean nullable, boolean unsigned) {
        this.name = name;
        this.type = type;
        this.typeName = typeName;
        this.key = key;
        this.nullable = nullable;
        this.unsigned = unsigned;
    }

    /**
     * @return Column name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Column JDBC type.
     */
    public int getType() {
        return type;
    }

    /**
     * @return {@code true} if this column belongs to primary key.
     */
    public boolean isKey() {
        return key;
    }

    /**
     * @return {@code true} if {@code NULL } allowed for column in database.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return {@code true} if column is unsigned.
     */
    public boolean isUnsigned() {
        return unsigned;
    }    
    
    public String getTypeName() {
		return typeName;
	}

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DbColumn.class, this);
    }

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
}
