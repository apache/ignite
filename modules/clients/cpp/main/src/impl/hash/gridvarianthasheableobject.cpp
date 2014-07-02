/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <vector>
#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>

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
int32_t gridHashCode<>(const GridClientUuid& val) {
    return val.hashCode();
}

template<>
int32_t gridHashCode<>(const GridClientDate& val) {
    return val.hashCode();
}

template<>
int32_t gridHashCode<>(const GridClientVariant& val) {
    return val.hashCode();
}

template<>
int32_t gridHashCode<>(const uint16_t& val) {
    return (int32_t)val;
}

namespace {

class GridClientVariantVisitorImpl : public GridClientVariantVisitor {
public:
    GridClientVariantVisitorImpl(int& pHashCode) : hashCode_(pHashCode) {
        hashCode_ = -1;
    }

    virtual void visit(const bool val) const {
        hashCode_ = gridBoolHash(val);
    }

    virtual void visit(const int8_t val) const {
        hashCode_ = gridByteHash(val);
    }

    virtual void visit(const uint16_t val) const {
        hashCode_ = gridHashCode<uint16_t>(val);
    }

    virtual void visit(const int16_t val) const {
        hashCode_ = gridInt16Hash(val);
    }

    virtual void visit(const int32_t val) const {
        hashCode_ = gridInt32Hash(val);
    }

    virtual void visit(const int64_t val) const {
        hashCode_ = gridInt64Hash(val);
    }

    virtual void visit(const double val) const {
        hashCode_ = gridDoubleHash(val);
    }

    virtual void visit(const float val) const {
        hashCode_ = gridFloatHash(val);
    }

    virtual void visit(const string& val) const {
        hashCode_ = gridStringHash(val);
    }

    virtual void visit(const std::wstring& val) const {
        hashCode_ = gridWStringHash(val);
    }

    virtual void visit(const vector<int8_t>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<bool>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int16_t>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<uint16_t>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int32_t>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<int64_t>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<float>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<double>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<string>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const vector<GridClientUuid>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const TGridClientVariantSet& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const TGridClientVariantPair& pair) const {
        hashCode_ = pair.first.hashCode() ^ pair.second.hashCode();
    }

    virtual void visit(const GridClientUuid& uuid) const {
        hashCode_ = uuid.hashCode();
    }

    virtual void visit(const GridClientDate& val) const {
        hashCode_ = val.hashCode();
    }

    virtual void visit(const std::vector<GridClientDate>& val) const {
        hashCode_ = gridCollectionHash(val);
    }

    virtual void visit(const TGridClientVariantMap& vmap) const {
        hashCode_ = 0;

        BOOST_FOREACH(TGridClientVariantMap::value_type pair, vmap) {
            const GridClientVariant& key = pair.first;
            const GridClientVariant& value = pair.second;

            hashCode_ += hash_value(key) ^ hash_value(value);
        }
    }

    virtual void visit(const GridPortable&) const {
        throw runtime_error("Can not calculate hash code for GridClientVariant holding GridPortable.");
    }

    virtual void visit(const GridHashablePortable& val) const {
        hashCode_ = val.hashCode();
    }

    virtual void visit(const GridPortableObject& val) const {
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
