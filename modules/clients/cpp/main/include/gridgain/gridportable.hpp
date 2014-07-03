/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDPORTABLE_HPP_INCLUDED
#define GRIDPORTABLE_HPP_INCLUDED

#include <string>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridportablereader.hpp>
#include <gridgain/gridportablewriter.hpp>

/**
 * Field ID mapper.
 */
class GRIDGAIN_API GridPortableIdResolver {
public:
    /**
     * Gets field ID for provided field name.
     * If 0 is returned, hash code of lower-case field name will be used.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    virtual int32_t fieldId(int32_t typeId, const char* fieldName) {
        return 0;
    }

    /**
     * Gets field ID for provided field name.
     * If 0 is returned, hash code of lower-case field name will be used.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    virtual int32_t fieldId(int32_t typeId, const std::string& fieldName) {
        return 0;
    }

    /**
     * Destructor.
     */
    virtual ~GridPortableIdResolver() {
    }
};

class GridPortableReader;
class GridPortableWriter;

/**
 * Interface that allows to implement serialization/deserialization logic.
 * Class extending GridPortable must provide default constructor.
 */
class GRIDGAIN_API GridPortable {
public:
     /**
     *
     * Destructor.
     */
    virtual ~GridPortable() {};

    /**
     * @return Type ID.
     */
    virtual int32_t typeId() const = 0;

    /**
     * Writes portable object.
     *
     * @param writer Writer.
     */
    virtual void writePortable(GridPortableWriter& writer) const = 0;

    /**
     * Reads portable object.
     *
     * @param reader Reader.
     */
    virtual void readPortable(GridPortableReader& reader) = 0;
};

/**
 * Extension of GridPortable needed when object is used as cache key,
 * in this case it should provide hasCode function and equality comparison operator.
 */
class GRIDGAIN_API GridHashablePortable : public GridPortable {
public:
    /**
     * @return Hash code.
     */
    virtual int32_t hashCode() const = 0;

    /**
     * Comparison operator.
     *
     * @param other Object to compare this object to.
     * @return <tt>true</tt> if this object equals to the other.
     */
    virtual bool operator==(const GridHashablePortable& other) const = 0;
};

class PortableReadContext;

/**
 * Wrapper for serialized portable object.
 */
class GRIDGAIN_API GridPortableObject {
public:
    /**
     * Copy constructor.
     *
     * @param other Portable object.
     */
    GridPortableObject(const GridPortableObject& other);

    /**
     * Destructor.
     */
    ~GridPortableObject();

    /**
     * @return Portable object type ID.
     */
    int32_t typeId() const;

    /**
     * @return Hash code.
     */
    int32_t hashCode() const;

    /**
     * Gets field value.
     *
     * @param fieldName Field name.
     * @return Field value.
     */
    GridClientVariant field(const std::string& fieldName) const;

    /**
     * Gets fully deserialized instance of portable object.
     *
     * @return Deserialized instance of portable object.
     */
    GridPortable* deserialize() const;

    /**
     * Gets fully deserialized instance of portable object.
     *
     * @return Deserialized instance of portable object.
     */
    template<typename T>
    std::unique_ptr<T> deserializeUnique() const {
        return std::unique_ptr<T>(deserialize<T>());
    }


    /**
     * Gets fully deserialized instance of portable object.
     *
     * @return Deserialized instance of portable object.
     */
    template<typename T>
    T* deserialize() const {
        return static_cast<T*>(deserialize());
    }

    /**
     * Comparison operator.
     *
     * @param other Object to compare this object to.
     * @return <tt>true</tt> if this object equals to the other.
     */
    bool operator==(const GridPortableObject& other) const;

private:
    /**
     * @param ctxPtr Read context.
     * @param start Object start.
     */
    GridPortableObject(boost::shared_ptr<PortableReadContext>& ctxPtr, int32_t start);

    /** */
    class Impl;

    /** */
    Impl* pImpl;
    
    /**
     * @return Object data.
     */
    std::vector<int8_t>* data();

    /**
     * @return Object data start.
     */
    int32_t start();

    friend class GridPortableReaderImpl;
    friend class GridPortableWriterImpl;
    friend class GridPortableMarshaller;
};

#endif // GRIDPORTABLE_HPP_INCLUDED
