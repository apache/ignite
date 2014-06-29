/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_VARIANT_HPP_INCLUDED
#define GRID_CLIENT_VARIANT_HPP_INCLUDED

#include <string>
#include <vector>
#include <cstdint>
#include <memory>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridclientdate.hpp>
#include <gridgain/gridportable.hpp>

#include <boost/variant.hpp>

/** Forward declaration of class. */
class GridClientVariantVisitor;
class GridClientVariant;
class GridHashablePortable;
class GridPortableObject;

/**
 * Class that replaces java.lang.Object holder for primitive types and string. It can hold boolean, int_*, float, double,
 * string and byte array values.
 */
class GRIDGAIN_API GridClientVariant {
public:
    /** No-arg constructor - variant with null value is created. */
    GridClientVariant();

    /** Destructor. */
    virtual ~GridClientVariant();

    /**
     * Constructor with byte argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(int8_t val);

    /**
     * Constructor with boolean argument.
     *
     * @param val Value for the variant.
     */
    explicit GridClientVariant(bool val);

    /**
     * Constructor with int16_t argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(int16_t val);

    /**
     * Constructor with int32_t argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(int32_t val);

    /**
     * Constructor with int64_t argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(int64_t val);

    /**
     * Constructor with double argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(double val);

    /**
     * Constructor with float argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(float val);

    /**
     * Constructor with char argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(uint16_t val);

    /**
     * Constructor with string argument. Added const char* explicitly because otherwise it boolean constructor
     * was called.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const char * val);

    /**
     * Constructor with string argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::string& val);

    /**
     * Constructor with wide string argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::wstring& val);

    /**
     * Constructor with byte array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<int8_t>& val);

    /**
     * Constructor with short array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<int16_t>& val);

    /**
     * Constructor with int array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<int32_t>& val);

    /**
     * Constructor with long array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<int64_t>& val);

    /**
     * Constructor with float array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<float>& val);

    /**
     * Constructor with shor array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<double>& val);

    /**
     * Constructor with char array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<uint16_t>& val);

    /**
     * Constructor with bool array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<bool>& val);

    /**
     * Constructor with string array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<std::string>& val);

    /**
     * Constructor with uuid array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<GridClientUuid>& val);

    /**
     * Constructor with date argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const GridClientDate& val);

    /**
     * Constructor with date array argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(const std::vector<GridClientDate>& val);

    /**
     * Constructor with variant vector argument.
     */
    GridClientVariant(const TGridClientVariantSet& val);

    /**
     * Constructor with variant map argument.
     */
    GridClientVariant(const TGridClientVariantMap& val);

    /**
     * Constructor with UUID argument.
     */
    GridClientVariant(const GridClientUuid& val);

    /**
     * Constructor with GridPortableObject argument.
     */
    GridClientVariant(const GridPortableObject& val);

    /**
     * Constructor with GridPortable argument.
     */
    GridClientVariant(GridPortable* val);

    /**
     * Constructor with GridHashablePortable argument.
     */
    GridClientVariant(GridHashablePortable* val);

    /**
     * Copy constructor.
     *
     * @param other Variant to copy value from.
     */
    GridClientVariant(const GridClientVariant& other);

    /**
     * Move constructor.
     *
     * @param other Variant to move value from.
     */
    GridClientVariant(GridClientVariant&& other);

    /**
     * Assignment operator.
     *
     * @param other Variant to take value from.
     */
    GridClientVariant& operator=(const GridClientVariant& other);

    /**
     * Move assignment operator.
     *
     * @param other Variant to move value from.
     */
    GridClientVariant& operator=(GridClientVariant&& other);

    /**
     * Converts value contained in variant, to string.
     *
     * @return String representation of the value held.
     */
    std::string toString() const;

    /**
     * Returns debug string that contains type information and value.
     *
     * @return Type and value information.
     */
    std::string debugString() const;

    /**
     * Assigns this variant a boolean value.
     *
     * @param val New value for the variant.
     */
    void set(bool val);

    /**
     * Checks if this variant holds a byte value.
     *
     * @return <tt>true</tt> if value is of byte type, <tt>false</tt> otherwise.
     */
    bool hasByte() const;

    /**
     * Returns a byte value from this variant.
     *
     * @return Value held in the variant.
     */
    int8_t getByte() const;

    /**
     * Assigns this variant a byte value.
     *
     * @param val New value for the variant.
     */
    void set(int8_t val);

    /**
     * Checks if this variant holds a char value.
     *
     * @return <tt>true</tt> if value is of char type, <tt>false</tt> otherwise.
     */
    bool hasChar() const;

    /**
     * Returns a char value from this variant.
     *
     * @return Value held in the variant.
     */
    uint16_t getChar() const;

    /**
     * Assigns this variant a char value.
     *
     * @param val New value for the variant.
     */
    void set(uint16_t val);

    /**
     * Checks if this variant holds a boolean value.
     *
     * @return <tt>true</tt> if value is of boolean type, <tt>false</tt> otherwise.
     */
    bool hasBool() const;

    /**
     * Returns a boolean value from this variant.
     *
     * @return Value held in the variant.
     */
    bool getBool() const;

    /**
     * Assigns this variant an int16_t value.
     *
     * @param val New value for the variant.
     */
    void set(int16_t val);

    /**
     * Checks if this variant holds an int16_t value.
     *
     * @return <tt>true</tt> if value is of int16_t type, <tt>false</tt> otherwise.
     */
    bool hasShort() const;

    /**
     * Returns an int16_t value from this variant.
     *
     * @return Value held in the variant.
     */
    int16_t getShort() const;

    /**
     * Assigns this variant an int32_t value.
     *
     * @param val New value for the variant.
     */
    void set(int32_t val);

    /**
     * Checks if this variant holds an int32_t value.
     *
     * @return <tt>true</tt> if value is of int32_t type, <tt>false</tt> otherwise.
     */
    bool hasInt() const;

    /**
     * Returns an int16_t value from this variant.
     *
     * @return Value held in the variant.
     */
    int32_t getInt() const;

    /**
     * Assigns this variant an int64_t value.
     *
     * @param val New value for the variant.
     */
    void set(int64_t val);

    /**
     * Checks if this variant holds an int64_t value.
     *
     * @return <tt>true</tt> if value is of int64_t type, <tt>false</tt> otherwise.
     */
    bool hasLong() const;

    /**
     * Returns an int64_t value from this variant.
     *
     * @return Value held in the variant.
     */
    int64_t getLong() const;

    /**
     * Assigns this variant a double value.
     *
     * @param val New value for the variant.
     */
    void set(double val);

    /**
     * Checks if this variant holds a double value.
     *
     * @return <tt>true</tt> if value is of double type, <tt>false</tt> otherwise.
     */
    bool hasDouble() const;

    /**
     * Returns a double value from this variant.
     *
     * @return Value held in the variant.
     */
    double getDouble() const;

    /**
     * Assigns this variant a float value.
     *
     * @param val New value for the variant.
     */
    void set(float val);

    /**
     * Checks if this variant holds a float value.
     *
     * @return <tt>true</tt> if value is of float type, <tt>false</tt> otherwise.
     */
    bool hasFloat() const;

    /**
     * Returns a float value from this variant.
     *
     * @return Value held in the variant.
     */
    float getFloat() const;

    /**
     * Assigns this variant a string value.
     *
     * @param val New value for the variant.
     */
    void set(const std::string& val);

    /**
     * Assigns this variant a string value.
     *
     * @param val New value for the variant.
     */
    void set(const char* val);

    /**
     * Checks if this variant holds a string value.
     *
     * @return <tt>true</tt> if value is of string type, <tt>false</tt> otherwise.
     */
    bool hasString() const;

    /**
     * Returns a string value from this variant.
     *
     * @return Value held in the variant.
     */
    std::string& getString() const;

    /**
     * Assigns this variant a portable value.
     *
     * @param val New value for the variant.
     */
    void set(GridPortable* val);

    /**
     * Checks if this variant holds a portable value.
     *
     * @return <tt>true</tt> if value is of portable, <tt>false</tt> otherwise.
     */
    bool hasPortable() const;

    /**
     * Returns a portable value from this variant.
     *
     * @return Value held in the variant.
     */
    template<typename T>
    T* getPortable() const {
        return static_cast<T*>(getPortable());
    }

    /**
     * Returns a portable value from this variant.
     *
     * @return Value held in the variant.
     */
    GridPortable* getPortable() const;

    /**
     * Checks if this variant holds a hashable portable value.
     *
     * @return <tt>true</tt> if value is of portable, <tt>false</tt> otherwise.
     */
    bool hasHashablePortable() const;

    /**
     * Assigns this variant a hashable portable value.
     *
     * @param val New value for the variant.
     */
    void set(GridHashablePortable* val);

    /**
     * Returns a portable value from this variant.
     *
     * @return Value held in the variant.
     */
    template<typename T>
    T* getHashablePortable() const {
        return static_cast<T*>(getHashablePortable());
    }

    /**
     * Returns a hashable portable value from this variant.
     *
     * @return Value held in the variant.
     */
    GridHashablePortable* getHashablePortable() const;

    /**
     * Checks if this variant holds a portable object value.
     *
     * @return <tt>true</tt> if value is of portable, <tt>false</tt> otherwise.
     */
    bool hasPortableObject() const;

    /**
     * Assigns this variant a portable object value.
     *
     * @param val New value for the variant.
     */
    void set(const GridPortableObject& portableObject);

    /**
     * Returns a hashable portable value from this variant.
     *
     * @return Value held in the variant.
     */
    GridPortableObject& getPortableObject() const;

    template<typename T> 
    T* deserializePortable() const {
        return getPortableObject().deserialize<T>();
    }

    template<typename T> 
    std::unique_ptr<T> deserializePortableUnique() const {
        return getPortableObject().deserializeUnique<T>();
    }

    /**
     * Checks if this variant holds a wide string value.
     *
     * @return <tt>true</tt> if value is of wide string type, <tt>false</tt> otherwise.
     */
    bool hasWideString() const;

    /**
     * Returns a wide string value from this variant.
     *
     * @return Value held in the variant.
     */
    std::wstring& getWideString() const;

    /**
     * Assigns this variant a byte array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<int8_t>& val);

    /**
     * Checks if this variant holds a byte array value.
     *
     * @return <tt>true</tt> if value is of byte array type, <tt>false</tt> otherwise.
     */
    bool hasByteArray() const;

    /**
     * Returns a byte array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<int8_t>& getByteArray() const;

    /**
     * Assigns this variant a short array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<int16_t>& val);

    /**
     * Checks if this variant holds a short array value.
     *
     * @return <tt>true</tt> if value is of short array type, <tt>false</tt> otherwise.
     */
    bool hasShortArray() const;

    /**
     * Returns a byte array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<int16_t>& getShortArray() const;

    /**
     * Assigns this variant a int array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<int32_t>& val);

    /**
     * Checks if this variant holds a int array value.
     *
     * @return <tt>true</tt> if value is of int array type, <tt>false</tt> otherwise.
     */
    bool hasIntArray() const;

    /**
     * Returns a int array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<int32_t>& getIntArray() const;

    /**
     * Assigns this variant a long array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<int64_t>& val);

    /**
     * Checks if this variant holds a long array value.
     *
     * @return <tt>true</tt> if value is of long array type, <tt>false</tt> otherwise.
     */
    bool hasLongArray() const;

    /**
     * Returns a byte array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<int64_t>& getLongArray() const;

    /**
     * Assigns this variant a float array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<float>& val);

    /**
     * Checks if this variant holds a float array value.
     *
     * @return <tt>true</tt> if value is of float array type, <tt>false</tt> otherwise.
     */
    bool hasFloatArray() const;

    /**
     * Returns a float array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<float>& getFloatArray() const;

    /**
     * Assigns this variant a double array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<double>& val);

    /**
     * Checks if this variant holds a double array value.
     *
     * @return <tt>true</tt> if value is of double array type, <tt>false</tt> otherwise.
     */
    bool hasDoubleArray() const;

    /**
     * Returns a double array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<double>& getDoubleArray() const;

    /**
     * Assigns this variant a char array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<uint16_t>& val);

    /**
     * Checks if this variant holds a char array value.
     *
     * @return <tt>true</tt> if value is of char array type, <tt>false</tt> otherwise.
     */
    bool hasCharArray() const;

    /**
     * Returns a char array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<uint16_t>& getCharArray() const;

    /**
     * Assigns this variant a bool array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<bool>& val);

    /**
     * Checks if this variant holds a bool array value.
     *
     * @return <tt>true</tt> if value is of bool array type, <tt>false</tt> otherwise.
     */
    bool hasBoolArray() const;

    /**
     * Returns a bool array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<bool>& getBoolArray() const;

    /**
     * Assigns this variant a string array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<std::string>& val);

    /**
     * Checks if this variant holds a string array value.
     *
     * @return <tt>true</tt> if value is of uuid array type, <tt>false</tt> otherwise.
     */
    bool hasStringArray() const;

    /**
     * Returns a string array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<std::string>& getStringArray() const;

    /**
     * Assigns this variant a uuid array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<GridClientUuid>& val);

    /**
     * Checks if this variant holds a uuid array value.
     *
     * @return <tt>true</tt> if value is of uuid array type, <tt>false</tt> otherwise.
     */
    bool hasUuidArray() const;

    /**
     * Returns a uuid array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<GridClientUuid>& getUuidArray() const;

    /**
     * Assigns this variant a date array value.
     *
     * @param val New value for the variant.
     */
    void set(const std::vector<GridClientDate>& val);

    /**
     * Checks if this variant holds a date array value.
     *
     * @return <tt>true</tt> if value is of date array type, <tt>false</tt> otherwise.
     */
    bool hasDateArray() const;

    /**
     * Returns a date array value from this variant.
     *
     * @return Value held in the variant.
     */
    std::vector<GridClientDate>& getDateArray() const;

    /**
     * Assigns this variant a date value.
     *
     * @param val New value for the variant.
     */
    void set(const GridClientDate& val);

    /**
     * Checks if this variant holds a date value.
     *
     * @return <tt>true</tt> if value is of date type, <tt>false</tt> otherwise.
     */
    bool hasDate() const;

    /**
     * Returns a date value from this variant.
     *
     * @return Value held in the variant.
     */
    GridClientDate& getDate() const;

    /**
     * Checks if this variant holds a variant vector value.
     *
     * @return <tt>true</tt> if value is of variant vector type, <tt>false</tt> otherwise.
     */
    bool hasVariantVector() const;

    /**
     * Returns a variant vector value from this variant.
     *
     * @return Value held in the variant.
     */
    TGridClientVariantSet& getVariantVector() const;

    /**
     * Assigns this variant a variant array value.
     *
     * @param val New value for the variant.
     */
    void set(const TGridClientVariantSet& val);

    /**
     * Checks if this variant holds a variant map value.
     *
     * @return <tt>true</tt> if value is of variant map type, <tt>false</tt> otherwise.
     */
    bool hasVariantMap() const;

    /**
     * Returns a variant map value from this variant.
     *
     * @return Value held in the variant.
     */
    TGridClientVariantMap& getVariantMap() const;

    /**
     * Assigns this variant a variant map value.
     *
     * @param val New value for the variant.
     */
    void set(const TGridClientVariantMap& val);

    /**
     * Assigns this variant a UUID value.
     *
     * @param val New value for the variant.
     */
    void set(const GridClientUuid& val);

    /**
     * Checks if this variant holds a UUID value.
     *
     * @return <tt>true</tt> if value is of UUID type, <tt>false</tt> otherwise.
     */
    bool hasUuid() const;

    /**
     * Returns a UUID value from this variant.
     *
     * @return Value held in the variant.
     */
    GridClientUuid& getUuid() const;

    /**
     * Returns hash code for value from this variant.
     *
     * @return Hash code for value held in this variant.
     */
    int32_t hashCode() const;

    /**
     * Method for visitors.
     *
     * @param visitor Visitor to accept.
     */
    void accept(const GridClientVariantVisitor& visitor) const;

    /**
     * Comparison operator for variant.
     *
     * @param other Variant to compare this variant to.
     * @return <tt>true</tt> if this variant equals to the other, <tt>false</tt> otherwise.
     */
    bool operator==(const GridClientVariant& other) const;

    /**
     * Clears value held (becomes null).
     */
    void clear();

    /**
     * Method to check if this variant holds any value.
     *
     * @return <tt>true</tt> if it has a value set, <tt>false</tt> otherwise.
     */
    bool hasAnyValue() const;

private:
    /** Enum for possible types of values. */
    enum TypeEnum {
        NULL_TYPE,

        BYTE_TYPE,
        SHORT_TYPE,
        INT_TYPE,
        LONG_TYPE,
        FLOAT_TYPE,
        DOUBLE_TYPE,
        CHAR_TYPE,
        BOOL_TYPE,
        STRING_TYPE,
        WIDE_STRING_TYPE,
        UUID_TYPE,
        DATE_TYPE,

        PORTABLE_TYPE,
        HASHABLE_PORTABLE_TYPE,
        PORTABLE_OBJ_TYPE,

        BYTE_ARR_TYPE,
        SHORT_ARR_TYPE,
        INT_ARR_TYPE,
        LONG_ARR_TYPE,
        FLOAT_ARR_TYPE,
        DOUBLE_ARR_TYPE,
        CHAR_ARR_TYPE,
        BOOL_ARR_TYPE,
        STRING_ARR_TYPE,
        UUID_ARR_TYPE,
        DATE_ARR_TYPE,

        VARIANT_ARR_TYPE,
        VARIANT_MAP_TYPE
    };

    union U {
        int8_t byteVal;

        int16_t shortVal;

        int32_t intVal;

        int64_t longVal;

        float floatVal;

        double doubleVal;

        uint16_t charVal;

        bool boolVal;

        std::string* strVal;

        std::wstring* wideStrVal;

        GridClientUuid* uuidVal;

        GridClientDate* dateVal;

        GridPortable* portableVal;

        GridHashablePortable* hashPortableVal;

        GridPortableObject* portableObjVal;

        std::vector<int8_t>* byteArrVal;

        std::vector<int16_t>* shortArrVal;

        std::vector<int32_t>* intArrVal;

        std::vector<int64_t>* longArrVal;

        std::vector<float>* floatArrVal;

        std::vector<double>* doubleArrVal;

        std::vector<uint16_t>* charArrVal;

        std::vector<bool>* boolArrVal;

        std::vector<std::string>* strArrVal;

        std::vector<GridClientUuid>* uuidArrVal;

        std::vector<GridClientDate>* dateArrVal;

        TGridClientVariantSet* variantArrVal;

        TGridClientVariantMap* variantMapVal;
    };

    TypeEnum type;

    U data;

    void copy(const GridClientVariant& other);

    void checkType(TypeEnum expType) const;

    static std::string typeName(TypeEnum type);
};

/**
 * Returns hash code for value from given variant.
 * Function is needed for boost::unordered_map.
 *
 * @return Hash code for value held in this variant.
 */
inline std::size_t hash_value(const GridClientVariant& variant) {
    return variant.hashCode();        
}

/**
 * Prints variant to stream
 *
 * @param out Stream to output variant to.
 * @param m Variant.
 */
inline std::ostream& operator<<(std::ostream &out, const GridClientVariant &m) {
    return out << m.toString();
}

/** Base class for variant visitors. */
class GridClientVariantVisitor {
public:
    /** Virtual destructor. */
    virtual ~GridClientVariantVisitor() {};

    /** */
    virtual void visit(const int8_t) const = 0;

    /** */
    virtual void visit(const int16_t) const = 0;

    /** */
    virtual void visit(const int32_t) const = 0;

    /** */
    virtual void visit(const int64_t) const = 0;

    /** */
    virtual void visit(const float) const = 0;

    /** */
    virtual void visit(const double) const = 0;

    /** */
    virtual void visit(const bool) const = 0;

    /** */
    virtual void visit(const uint16_t) const = 0;

    /** */
    virtual void visit(const std::string&) const = 0;

    /** */
    virtual void visit(const std::wstring&) const = 0;

    /** */
    virtual void visit(const GridClientUuid&) const = 0;

    /** */
    virtual void visit(const GridPortable&) const = 0;

    /** */
    virtual void visit(const GridHashablePortable&) const = 0;

    /** */
    virtual void visit(const GridPortableObject&) const = 0;

    /** */
    virtual void visit(const std::vector<int8_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<int16_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<int32_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<int64_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<float>&) const = 0;

    /** */
    virtual void visit(const std::vector<double>&) const = 0;

    /** */
    virtual void visit(const std::vector<bool>&) const = 0;

    /** */
    virtual void visit(const std::vector<uint16_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<std::string>&) const = 0;

    /** */
    virtual void visit(const std::vector<GridClientUuid>&) const = 0;

    /** */
    virtual void visit(const GridClientDate&) const = 0;

    /** */
    virtual void visit(const std::vector<GridClientDate>&) const = 0;

    /** */
    virtual void visit(const TGridClientVariantSet&) const = 0;

    /** */
    virtual void visit(const TGridClientVariantMap&) const = 0;
};

#endif //GRID_CLIENT_VARIANT_HPP_INCLUDED
