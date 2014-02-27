// @cpp.file.header

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

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclientuuid.hpp>

#include <boost/variant.hpp>

/** Forward declaration of class. */
class GridClientVariantVisitor;

/**
 * Class that replaces java.lang.Object holder for primitive types and string. It can hold boolean, int_*, float, double,
 * string and byte array values.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientVariant {

public:
    /** No-value type. */
    class NullType {
    public:
        /**
         * Comparison operator with another null value.
         *
         * @param other Another null value.
         * @return Always returns <tt>true</tt>.
         */
        bool operator==(const NullType& other) const {
            return true;
        }
    };

private:
    class Impl {
    public:
            /** Boost typedef for holding multiple types. */
        typedef boost::variant<GridClientVariant::NullType, bool, int16_t, int32_t, int64_t, double,
                float, std::string, std::wstring, std::vector<int8_t>, std::vector<GridClientVariant>, GridUuid> TVariantType;

        /** Boost variable. */
        TVariantType var;

        /** Enum for possible types of values. */
        enum TypeEnum {
            BOOL_TYPE = 1,
            SHORT_TYPE,
            INT_TYPE,
            LONG_TYPE,
            DOUBLE_TYPE,
            FLOAT_TYPE,
            STRING_TYPE,
            WIDE_STRING_TYPE,
            BYTE_ARRAY_TYPE,
            VARIANT_VECTOR_TYPE,
            UUID_TYPE
        };
    };

public:

    /** No-arg constructor - variant with null value is created. */
    GridClientVariant();

    /** Destructor. */
    virtual ~GridClientVariant();

    /**
     * Constructor with boolean argument.
     *
     * @param val Value for the variant.
     */
    GridClientVariant(bool val);

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
     * Constructor with variant vector argument.
     */
    GridClientVariant(const std::vector<GridClientVariant>& val);

    /**
     * Constructor with UUID argument.
     */
    GridClientVariant(const GridUuid& val);

    /**
     * Copy constructor.
     *
     * @param other Variant to take value from.
     */
    GridClientVariant(const GridClientVariant& other);

    /**
     * Assignment operator.
     *
     * @param other Variant to take value from.
     */
    GridClientVariant& operator=(const GridClientVariant& other);

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
    std::string getString() const;

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
    std::wstring getWideString() const;

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
    std::vector<int8_t> getByteArray() const;

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
    std::vector<GridClientVariant> getVariantVector() const;

    /**
     * Assigns this variant a UUID value.
     *
     * @param val New value for the variant.
     */
    void set(const GridUuid& val);

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
    GridUuid getUuid() const;

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
     * @return <tt>true</tt> if this variant is less than other, <tt>false</tt> otherwise.
     */
    bool operator<(const GridClientVariant& other) const;

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
    Impl pimpl;
};

/**
 * Print 'null' value to output stream.
 *
 * @param out Stream to print value to.
 * @param src Value to print (ignored).
 */
inline std::ostream& operator <<(std::ostream& out, const GridClientVariant::NullType& src) {
    return out;
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
    virtual void visit(const bool val) const = 0;

    /** */
    virtual void visit(const int16_t) const = 0;

    /** */
    virtual void visit(const int32_t) const = 0;

    /** */
    virtual void visit(const int64_t) const = 0;

    /** */
    virtual void visit(const double) const = 0;

    /** */
    virtual void visit(const float) const = 0;

    /** */
    virtual void visit(const std::string&) const = 0;

    /** */
    virtual void visit(const std::wstring&) const = 0;

    /** */
    virtual void visit(const std::vector<int8_t>&) const = 0;

    /** */
    virtual void visit(const std::vector<GridClientVariant>&) const = 0;

    /** */
    virtual void visit(const GridUuid&) const = 0;
};

#endif //GRID_CLIENT_VARIANT_HPP_INCLUDED
