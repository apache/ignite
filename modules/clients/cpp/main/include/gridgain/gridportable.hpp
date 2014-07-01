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

class GRIDGAIN_API GridPortableIdResolver {
public:
    virtual boost::optional<int32_t> fieldId(int32_t typeId, const char* fieldName) {
        return boost::optional<int32_t>();
    }

    virtual boost::optional<int32_t> fieldId(int32_t typeId, const std::string& fieldName) {
        return boost::optional<int32_t>();
    }

    virtual ~GridPortableIdResolver() {
    }
};

class GridPortableReader;
class GridPortableWriter;

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortable {
public:
     /**
     *
     * Destructor.
     */
    virtual ~GridPortable() {};

    /**
     * @return Type id.
     */
    virtual int32_t typeId() const = 0;

    virtual void writePortable(GridPortableWriter& writer) const = 0;

    virtual void readPortable(GridPortableReader& reader) = 0;
};

class GRIDGAIN_API GridHashablePortable : public GridPortable {
public:
    virtual int32_t hashCode() const = 0;

    virtual bool operator==(const GridHashablePortable& other) const = 0;
};

class PortableReadContext;

class GRIDGAIN_API GridPortableObject {
public:
    GridPortableObject(const GridPortableObject& other);

    ~GridPortableObject();

    bool userType() const;

    int32_t typeId() const;

    int32_t hashCode() const;

    GridClientVariant field(const std::string& fieldName) const;

    GridPortable* deserialize() const;

    template<typename T>
    std::unique_ptr<T> deserializeUnique() const {
        return std::unique_ptr<T>(deserialize<T>());
    }

    template<typename T>
    T* deserialize() const {
        return static_cast<T*>(deserialize());
    }

    bool operator==(const GridPortableObject& other) const;

private:
    GridPortableObject(boost::shared_ptr<PortableReadContext>& ctxPtr, int32_t start);

    class Impl;

    Impl* pImpl;
    
    std::vector<int8_t>* data();

    int32_t start();

    friend class GridPortableReaderImpl;
    friend class GridPortableWriterImpl;
    friend class GridPortableMarshaller;
};

#endif // GRIDPORTABLE_HPP_INCLUDED
