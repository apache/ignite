/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <vector>
#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>

#include "gridgain/gridclienthash.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientbytearrayshasheableobject.hpp"
#include "gridgain/impl/hash/gridclientboolhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"

using namespace std;

template<>
int32_t gridHashCode<>(GridClientUuid val) {
    return val.hashCode();
}

template<>
int32_t gridHashCode<>(GridClientVariant val) {
    return val.hashCode();
}

template<>
int32_t gridHashCode<>(uint16_t val) {
    return (int32_t)val;
}

template <class T> void getHashInfo(const T& val, int& hashCode, std::vector<int8_t>& bytes) {
    TGridHasheableObjectPtr simpleHasheable = createHasheable(val);

    bytes.clear();

    hashCode = simpleHasheable->hashCode();

    simpleHasheable->convertToBytes(bytes);
}

namespace {

class GridClientVariantVisitorImpl : public GridClientVariantVisitor {
public:
    GridClientVariantVisitorImpl(int& pHashCode) : hashCode_(pHashCode) {
        hashCode_ = -1;
    }

    virtual void visit(const bool val) const override {
        hashCode_ = gridBoolHash(val);
    }

    virtual void visit(const int8_t val) const override {
        hashCode_ = gridByteHash(val);
    }

    virtual void visit(const uint16_t val) const override {
        hashCode_ = gridHashCode<uint16_t>(val);
    }

    virtual void visit(const int16_t val) const override {
        hashCode_ = gridInt16Hash(val);
    }

    virtual void visit(const int32_t val) const override {
        hashCode_ = gridInt32Hash(val);
    }

    virtual void visit(const int64_t val) const override {
        hashCode_ = gridInt64Hash(val);
    }

    virtual void visit(const double val) const override {
        hashCode_ = gridDoubleHash(val);
    }

    virtual void visit(const float val) const override {
        hashCode_ = gridFloatHash(val);
    }

    virtual void visit(const string& val) const override {
        hashCode_ = gridStringHash(val);
    }

    virtual void visit(const std::wstring& val) const override {
        hashCode_ = gridWStringHash(val);
    }

    virtual void visit(const vector<int8_t>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<bool>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int16_t>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<uint16_t>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int32_t>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int64_t>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<float>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<double>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<string>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<GridClientUuid>& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const TGridClientVariantSet& val) const override {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const GridClientUuid& uuid) const override {
        hashCode_ = uuid.hashCode();
    }

    virtual void visit(const TGridClientVariantMap& vmap) const override {
        hashCode_ = 0;

        BOOST_FOREACH(TGridClientVariantMap::value_type pair, vmap) {
            GridClientVariant key = pair.first;
            GridClientVariant value = pair.second;

            hashCode_ += hash_value(key) ^ hash_value(value);
        }
    }

    virtual void visit(const GridPortable&) const override {
        throw runtime_error("Can not calculate hash code for GridClientVariant holding GridPortable.");
    }

    virtual void visit(const GridHashablePortable& val) const override {
        hashCode_ = val.hashCode();
    }

    virtual void visit(const GridPortableObject& val) const override {
        hashCode_ = val.hashCode();
    }

private:
    int32_t& hashCode_;
};

}

void GridClientVariantHasheableObject::init(const GridClientVariant& var) {
    GridClientVariantVisitorImpl vis(hashCode_);

    var.accept(vis);
}

GridClientVariantHasheableObject::GridClientVariantHasheableObject(const GridClientVariant& var) {
    init(var);
}
