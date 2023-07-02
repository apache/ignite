package org.hawkore.ignite.lucene.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 
 * An immutable sorted list of Columns to be indexed
 *
 */
public class Columns {

    private List<Column> columns;

    /**
     * 
     */
    public Columns() {
        columns = new ArrayList<>();
    }

    /**
     * 
     * @param columns
     */
    public Columns(List<Column> columns) {
        this.columns = columns;
    }

    /**
     * Adds the specified column to current columns
     * @param col
     * @return Columns
     */
    public Columns add(Column col) {
        this.columns.add(col);
        return this;
    }

    /**
     * Adds the specified Columns.columns to current columns
     * 
     * @param columns
     * @return this
     */
    public Columns add(Columns columns) {
        this.columns.addAll(columns.columns);
        return this;
    }

    /**
     * 
     * @return if columns is emply
     */
    public boolean isEmpty() {
        return columns.isEmpty();
    }

    /**
     * Performs the given action for each column 
     * 
     * @param action the action
     */
    public void forEach(Consumer<? super Column> action) {
        columns.forEach(action);
    }

    /**
     * Gets value for field
     * 
     * @param field
     * @return value for field
     */
    public Object valueForField(String field) {
        Optional<Column> col = columns.stream().filter(c -> c.field().equals(field)).findFirst();        
        return !col.isPresent() ? null: col.get().valueOrNull();
    }

    /**
     * Runs the specified function over each column with the specified field
     * 
     * @param field
     * @param func the function
     */
    public void foreachWithMapper(String field, Consumer<Column> func) {
        String mapper = Column.parseMapperName(field);
        columns.forEach(c -> {
            if (c.mapper().equals(mapper))
                func.accept(c);
        });
    }

    /**
     * Returns a new empty columns.
     * @return empty Columns
     */
    public static Columns apply() {
        return new Columns();
    }

    /**
     *  Returns a new Columns composed by the specified Columns.
     * @param columns
     * @return Columns composed by the specified Columns
     */
    public static Columns apply(Column... columns) {
        return new Columns(Arrays.asList(columns));
    }

}
