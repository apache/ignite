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

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridportablereader.hpp>
#include <gridgain/gridportablewriter.hpp>

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
    virtual int hashCode() const = 0;

    virtual bool operator==(const GridHashablePortable& other) const = 0;
};

class GRIDGAIN_API GridPortableObject {
public:
    GridPortableObject(std::vector<int8_t> bytes);

    GridPortableObject(const GridPortableObject& other);

    GridPortableObject(const GridPortableObject&& other);

    int32_t typeId();

    int32_t fieldTypeId(const std::string& fieldName);

    std::string typeName();

    int32_t hashCode();

    std::vector<std::string> fields();

    GridClientVariant field(const std::string& fieldName);

    GridClientVariant deserialize();

    GridPortableObject copy(boost::unordered_map<std::string, GridClientVariant> fields);

private:
    std::vector<int8_t> bytes;
};

class GRIDGAIN_API GridPortableObjectBuilder {
public:
    GridPortableObjectBuilder(int32_t typeId);

    void set(std::string fieldName, const GridClientVariant& val);

    void set(boost::unordered_map<std::string, GridClientVariant> fieldVals);

    GridPortableObject build();
};

#endif // GRIDPORTABLE_HPP_INCLUDED
