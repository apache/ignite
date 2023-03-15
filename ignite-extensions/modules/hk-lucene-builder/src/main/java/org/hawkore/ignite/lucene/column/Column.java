package org.hawkore.ignite.lucene.column;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;

import com.google.common.base.MoreObjects;

/**
 * 
 * A cell of column, which in most cases is different from a
 * storage engine field
 * 
 */

public class Column {

    /**
     * the name of the base cell
     */
    private String cell;

    /**
     * the UDT suffix
     */
    private String udt;

    /**
     * the map suffix
     */
    private String map;

    /**
     * the optional value
     */
    private Object value;

    /**
     * The columns mapper name, composed by cell name and UDT names, without map
     * names.
     */
    private String mapper;

    /**
     * The columns field name, composed by cell name, UDT names and map names.
     */
    private String field;

    /** The name suffix for map keys. */
    public static final String MAP_KEY_SUFFIX = "_key";

    /** The name suffix for map values. */
    public static final String MAP_VALUE_SUFFIX = "_value";

    /**
     * UDT SEPARATOR
     */
    public static final String UDT_SEPARATOR = ".";

    /**
     * MAP SEPARATOR
     */
    public static final String MAP_SEPARATOR = "$";

    /**
     * UDT PATTERN
     */
    public static final String UDT_PATTERN = Pattern.quote(UDT_SEPARATOR);

    /**
     * @param cell
     *            the name of the base cell
     */
    public Column(String cell) {
        super();
        this.cell = cell;
    }

    /**
     * 
     * @param cell
     *            the name of the base cell
     * @param udt
     *            the UDT suffix
     * @param map
     *            the map suffix
     * @param value
     *            the optional value
     */
    public Column(String cell, String udt, String map, Object value) {
        super();
        this.cell = cell;
        this.udt = udt;
        this.map = map;
        this.value = value;

        if (StringUtils.isBlank(cell))
            throw new IndexException("Cell name shouldn't be blank");

        /**
         * The columns mapper name, composed by cell name and UDT names, without
         * map names.
         */
        mapper = cell.concat(StringUtils.defaultIfBlank(udt, ""));

        /**
         * The column field name, composed by cell name, UDT names and map
         * names.
         */
        field = mapper.concat(StringUtils.defaultIfBlank(map, ""));
    }

    /**
     * Returns `true` if the value is not defined, `false` otherwise.
     * 
     * @return
     */
    boolean isEmpty() {
        return value == null;
    }

    /**
     * Returns the value, or null if it is not defined
     * 
     * @return value
     */
    public Object valueOrNull() {
        return value;
    }

    /**
     * Returns a copy of this with the specified name appended to the list of
     * UDT names.
     * 
     * @param name
     * @return new Column instance
     */
    public Column withUDTName(String name) {
        String udt = StringUtils.defaultIfBlank(this.udt, "") + UDT_SEPARATOR + name;
        return copy(this.cell, udt, this.map, this.value);
    }

    /**
     * Returns a copy of this with the specified name appended to the list of
     * map names.
     * 
     * @param name
     * @return new Column instance
     */
    public Column withMapName(String name) {
        String map = StringUtils.defaultIfBlank(this.map, "") + MAP_SEPARATOR + name;
        return copy(this.cell, this.udt, map, this.value);
    }

    /**
     * Returns a copy of this with the specified value
     * 
     * @param value
     * @return new Column instance
     */
    public Column withValue(Object value) {
        return copy(this.cell, this.udt, this.map, value);
    }

    /**
     * Returns the name for field
     * 
     * @param field
     * @return name for field
     */
    public String fieldName(String field) {
        return field.concat(StringUtils.defaultIfBlank(map, ""));
    }

    /**
     * Creates a new Column instance with cell name
     * @param cell
     * @return new Column instance
     */
    public static Column apply(String cell) {
        return new Column(cell);
    }

    private Column copy(String cell, String udt, String map, Object value) {
        return new Column(cell, udt, map, value);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("cell", cell)
            .add("field", field)
            .add("value", value)
            .toString();

    }

    /**
     * Extracts cell name from provided name
     * 
     * @param name
     * @return cell name
     */
    public static String parseCellName(String name) {
        int udtSuffixStart = name.indexOf(UDT_SEPARATOR);
        int mapSuffixStart = name.indexOf(MAP_SEPARATOR);
        return udtSuffixStart < 0 ? (mapSuffixStart < 0 ? name : name.substring(0, mapSuffixStart))
            : name.substring(0, udtSuffixStart);
    }

    /**
     * Extracts mapper name from provided name
     * 
     * @param name
     * @return mapper name
     */
    public static String parseMapperName(String name) {
        int mapSuffixStart = name.indexOf(MAP_SEPARATOR);
        return mapSuffixStart < 0 ? name : name.substring(0, mapSuffixStart);
    }

    /**
     * Extracts UDT names from provided name
     *  
     * @param name
     * @return list of UDT names
     */
    public static List<String> parseUdtNames(String name) {
        int udtSuffixStart = name.indexOf(UDT_SEPARATOR);

        int mapSuffixStart = name.indexOf(MAP_SEPARATOR);

        String udtSuffix = mapSuffixStart < 0 ? name.substring(udtSuffixStart + 1)
            : name.substring(udtSuffixStart + 1, mapSuffixStart);

        return udtSuffixStart < 0 ? null : Arrays.asList((String[]) udtSuffix.split(UDT_PATTERN));
    }

    /**
     * @return cell
     */
    public String cell() {
        return cell;
    }

    /**
     * 
     * @return udt
     */
    public String udt() {
        return udt;
    }

    /**
     * 
     * @return map
     */
    public String map() {
        return map;
    }

    /**
     * 
     * @return value
     */
    public Object value() {
        return value;
    }

    /**
     * 
     * @return mapper
     */
    public String mapper() {
        return mapper;
    }

    /**
     * 
     * @return field
     */
    public String field() {
        return field;
    }

}