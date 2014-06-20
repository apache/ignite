/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <string>
#include <iostream>
#include <sstream>

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"

using namespace std;

GridClientVariant::GridClientVariant() : type(NULL_TYPE) {
}

GridClientVariant::~GridClientVariant() {
    clear();
}

GridClientVariant::GridClientVariant(const GridClientVariant& other) {
    copy(other);
}

GridClientVariant& GridClientVariant::operator=(const GridClientVariant& other) {
    if (this != &other) {
        clear();

        copy(other);
    }

    return *this;
}

void GridClientVariant::copy(const GridClientVariant& other) {
    this->type = other.type;

    switch(other.type) {
        case NULL_TYPE:
            break;

        case BYTE_TYPE:
        case SHORT_TYPE:
        case INT_TYPE:
        case LONG_TYPE:
        case FLOAT_TYPE:
        case DOUBLE_TYPE:
        case CHAR_TYPE:
        case BOOL_TYPE:
        case PORTABLE_TYPE:
        case HASHABLE_PORTABLE_TYPE:
            data = other.data;

            break;

        case STRING_TYPE:
            data.strVal = new string(*other.data.strVal);

            break;

        case WIDE_STRING_TYPE:
            data.wideStrVal = new wstring(*other.data.wideStrVal);

            break;

        case UUID_TYPE:
            data.uuidVal = new GridClientUuid(*other.data.uuidVal);

            break;

        case PORTABLE_OBJ_TYPE:
            data.portableObjVal = new GridPortableObject(*other.data.portableObjVal);

            break;

        case BYTE_ARR_TYPE:
            data.byteArrVal = new vector<int8_t>(*other.data.byteArrVal);

            break;

        case SHORT_ARR_TYPE:
            data.shortArrVal = new vector<int16_t>(*other.data.shortArrVal);

            break;

        case INT_ARR_TYPE:
            data.intArrVal = new vector<int32_t>(*other.data.intArrVal);

            break;

        case LONG_ARR_TYPE:
            data.longArrVal = new vector<int64_t>(*other.data.longArrVal);

            break;

        case FLOAT_ARR_TYPE:
            data.floatArrVal = new vector<float>(*other.data.floatArrVal);

            break;

        case DOUBLE_ARR_TYPE:
            data.doubleArrVal = new vector<double>(*other.data.doubleArrVal);

            break;

        case CHAR_ARR_TYPE:
            data.charArrVal = new vector<uint16_t>(*other.data.charArrVal);

            break;

        case BOOL_ARR_TYPE:
            data.boolArrVal = new vector<bool>(*other.data.boolArrVal);

            break;

        case STRING_ARR_TYPE:
            data.strArrVal = new vector<string>(*other.data.strArrVal);

            break;

        case UUID_ARR_TYPE:
            data.uuidArrVal = new vector<GridClientUuid>(*other.data.uuidArrVal);

            break;

        case VARIANT_ARR_TYPE:
            data.variantArr = new TGridClientVariantSet(*other.data.variantArr);

            break;

        case VARIANT_MAP_TYPE:
            data.variantMap = new TGridClientVariantMap(*other.data.variantMap);

            break;

        default:
            assert(false);
    }
}

GridClientVariant::GridClientVariant(bool val) : type(BOOL_TYPE) {
    data.boolVal = val;
}

GridClientVariant::GridClientVariant(int16_t val) : type(SHORT_TYPE) {
    data.shortVal = val;
}

GridClientVariant::GridClientVariant(int8_t val) : type(BYTE_TYPE) {
    data.byteVal = val;
}

GridClientVariant::GridClientVariant(uint16_t val) : type(CHAR_TYPE) {
    data.charVal = val;
}

GridClientVariant::GridClientVariant(int32_t val) : type(INT_TYPE) {
    data.intVal = val;
}

GridClientVariant::GridClientVariant(int64_t val) : type(LONG_TYPE) {
    data.longVal = val;
}

GridClientVariant::GridClientVariant(double val)  : type(DOUBLE_TYPE) {
    data.doubleVal = val;
}

GridClientVariant::GridClientVariant(float val)  : type(FLOAT_TYPE) {
    data.floatVal = val;
}

GridClientVariant::GridClientVariant(const char* val) : type(STRING_TYPE) {
    data.strVal = new string(val);
}

GridClientVariant::GridClientVariant(const string& val) : type(STRING_TYPE) {
    data.strVal = new string(val);
}

GridClientVariant::GridClientVariant(const std::wstring& val) : type(WIDE_STRING_TYPE) {
    data.wideStrVal = new wstring(val);
}

GridClientVariant::GridClientVariant(const vector<bool>& val) : type(BOOL_ARR_TYPE) {
    data.boolArrVal = new vector<bool>(val);
}

GridClientVariant::GridClientVariant(const vector<int16_t>& val) : type(SHORT_ARR_TYPE) {
    data.shortArrVal = new vector<int16_t>(val);
}

GridClientVariant::GridClientVariant(const vector<int8_t>& val) : type(BYTE_ARR_TYPE) {
    data.byteArrVal = new vector<int8_t>(val);
}

GridClientVariant::GridClientVariant(const vector<uint16_t>& val) : type(CHAR_ARR_TYPE) {
    data.charArrVal = new vector<uint16_t>(val);
}

GridClientVariant::GridClientVariant(const vector<int32_t>& val) : type(INT_ARR_TYPE) {
    data.intArrVal = new vector<int32_t>(val);
}

GridClientVariant::GridClientVariant(const vector<int64_t>& val) : type(LONG_ARR_TYPE) {
    data.longArrVal = new vector<int64_t>(val);
}

GridClientVariant::GridClientVariant(const vector<float>& val) : type(FLOAT_ARR_TYPE) {
    data.floatArrVal = new vector<float>(val);
}

GridClientVariant::GridClientVariant(const vector<double>& val) : type(DOUBLE_ARR_TYPE) {
    data.doubleArrVal = new vector<double>(val);
}

GridClientVariant::GridClientVariant(const vector<string>& val) : type(STRING_ARR_TYPE) {
    data.strArrVal = new vector<string>(val);
}

GridClientVariant::GridClientVariant(const vector<GridClientUuid>& val) : type(UUID_ARR_TYPE) {
    data.uuidArrVal = new vector<GridClientUuid>(val);
}

GridClientVariant::GridClientVariant(const TGridClientVariantMap& val) : type(VARIANT_MAP_TYPE) {
    data.variantMapVal = new TGridClientVariantMap(val);
}

GridClientVariant::GridClientVariant(const TGridClientVariantSet& val) : type(VARIANT_ARR_TYPE) {
    data.variantArrVal = new TGridClientVariantSet(val);
}

GridClientVariant::GridClientVariant(GridPortable* val)  : type(PORTABLE_TYPE) {
    data.portableVal = val;
}

GridClientVariant::GridClientVariant(GridHashablePortable* val) : type(HASHABLE_PORTABLE_TYPE) {
    data.hashPortableVal = val;
}

GridClientVariant::GridClientVariant(const GridPortableObject& val) : type(PORTABLE_OBJ_TYPE) {
    data.portableObjVal = new GridPortableObject(val);
}

GridClientVariant::GridClientVariant(const GridClientUuid& val)  {
    set(val);
}

void GridClientVariant::set(GridPortable* val) {
    pimpl.var = NullType();
    portable = val;
    hashablePortable = false;
}

void GridClientVariant::set(GridHashablePortable* val) {
    pimpl.var = NullType();
    portable = val;
    hashablePortable = true;
}

bool GridClientVariant::hasPortable() const {
    return portable != nullptr;
}

bool GridClientVariant::hasHashablePortable() const {
    return portable != nullptr && hashablePortable;
}

void GridClientVariant::set(bool pBool) {
    pimpl.var = pBool;
    resetPortable();
}

bool GridClientVariant::hasBool() const {
    return pimpl.var.which() == Impl::BOOL_TYPE;
}

bool GridClientVariant::getBool() const {
    return boost::get<bool>(pimpl.var);
}

void GridClientVariant::set(int16_t value) {
    pimpl.var = value;
    resetPortable();
}

bool GridClientVariant::hasShort() const {
    return pimpl.var.which() == Impl::SHORT_TYPE;
}

int16_t GridClientVariant::getShort() const {
    return boost::get<int16_t>(pimpl.var);
}

void GridClientVariant::set(int32_t value) {
    pimpl.var = value;
    resetPortable();
}

bool GridClientVariant::hasInt() const {
    return pimpl.var.which() == Impl::INT_TYPE;
}

int32_t GridClientVariant::getInt() const {
    return boost::get<int32_t>(pimpl.var);
}

void GridClientVariant::set(int64_t value) {
    pimpl.var = value;
    portable = nullptr;
}

bool GridClientVariant::hasLong() const {
    return pimpl.var.which() == Impl::LONG_TYPE;
}

int64_t GridClientVariant::getLong() const {
    return boost::get<int64_t>(pimpl.var);
}

void GridClientVariant::set(double val) {
    pimpl.var = val;
    portable = nullptr;
}

bool GridClientVariant::hasDouble() const {
    return pimpl.var.which() == Impl::DOUBLE_TYPE;
}

double GridClientVariant::getDouble() const {
    return boost::get<double>(pimpl.var);
}

void GridClientVariant::set(float val) {
    pimpl.var = val;
    portable = nullptr;
}

bool GridClientVariant::hasFloat() const {
    return pimpl.var.which() == Impl::FLOAT_TYPE;
}

float GridClientVariant::getFloat() const {
    return boost::get<float>(pimpl.var);
}

void GridClientVariant::set(const char* pText) {
    pimpl.var = std::string(pText);
    portable = nullptr;
}

void GridClientVariant::set(const string& pText) {
    pimpl.var = pText;
    portable = nullptr;
}

bool GridClientVariant::hasString() const {
    return pimpl.var.which() == Impl::STRING_TYPE;
}

string GridClientVariant::getString() const {
    return boost::get<std::string>(pimpl.var);
}

bool GridClientVariant::hasWideString() const {
    return pimpl.var.which() == Impl::WIDE_STRING_TYPE;
}

wstring GridClientVariant::getWideString() const {
    return boost::get<std::wstring>(pimpl.var);
}

void GridClientVariant::set(const vector<int8_t>& pBuf) {
    pimpl.var = pBuf;
    portable = nullptr;
}

bool GridClientVariant::hasByteArray() const {
    return pimpl.var.which() == Impl::BYTE_ARRAY_TYPE;
}

vector<int8_t> GridClientVariant::getByteArray() const {
    return boost::get<std::vector<int8_t> >(pimpl.var);
}

bool GridClientVariant::hasVariantVector() const {
    return pimpl.var.which() == Impl::VARIANT_VECTOR_TYPE;
}

bool GridClientVariant::hasVariantMap() const {
    return pimpl.var.which() == Impl::VARIANT_MAP_TYPE;
}

std::vector<GridClientVariant> GridClientVariant::getVariantVector() const {
    return boost::get<std::vector<GridClientVariant>>(pimpl.var);
}

boost::unordered_map<GridClientVariant, GridClientVariant> GridClientVariant::getVariantMap() const {
    return boost::get<boost::unordered_map<GridClientVariant, GridClientVariant>>(pimpl.var);
}

void GridClientVariant::set(const GridClientUuid& val) {
    pimpl.var = val;
    resetPortable();
}

bool GridClientVariant::hasUuid() const {
    return pimpl.var.which() == Impl::UUID_TYPE;
}

GridClientUuid GridClientVariant::getUuid() const {
    return boost::get<GridClientUuid>(pimpl.var);
}

GridPortable* GridClientVariant::getPortable() const {
    if (!hasPortable())
        throw std::exception("GridClientVariant does not hold GridPortable.");

    return portable;
}

bool GridClientVariant::hasByte() const {
    return false; // TODO
}

int8_t GridClientVariant::getByte() const {
    return 0; // TODO
}

void GridClientVariant::set(int8_t val) {
    // TODO
}

bool GridClientVariant::hasChar() const {
    return false; //  TODO
}

uint16_t GridClientVariant::getChar() const {
    return 0; // TODO
}

void GridClientVariant::set(uint16_t val) {
    // TODO
}

int32_t GridClientVariant::hashCode() const {
    return hash_value(*this);
}

string GridClientVariant::toString() const {
    if (Impl::STRING_TYPE == pimpl.var.which()) {
        return getString();
    }
    else if (hasPortable()) {
        return "GridPortable";
    }
    else {
        ostringstream os;

        switch (pimpl.var.which()) {
            case Impl::BOOL_TYPE:
                os << getBool();

                break;

            case Impl::SHORT_TYPE:
                os << getShort();

                break;

            case Impl::INT_TYPE:
                os << getInt();

                break;

            case Impl::LONG_TYPE:
                os << getLong();

                break;

            case Impl::DOUBLE_TYPE:
                os << getDouble();

                break;

            case Impl::FLOAT_TYPE:
                os << getFloat();

                break;

            case Impl::STRING_TYPE:
                os << getString();

                break;

            case Impl::WIDE_STRING_TYPE:
                os << getWideString();

                break;

            case Impl::UUID_TYPE:
                os << getUuid().uuid();

                break;
        }

        return os.str();
    }
}

string GridClientVariant::debugString() const {
    ostringstream os;

    os << "GridClientVariant [type=";

    if (!hasPortable()) {
        switch (pimpl.var.which()) {
        case Impl::BOOL_TYPE:
            os << "bool, value=" << getBool();

            break;

        case Impl::SHORT_TYPE:
            os << "short, value=" << getShort();

            break;

        case Impl::INT_TYPE:
            os << "int, value=" << getInt();

            break;

        case Impl::LONG_TYPE:
            os << "long, value=" << getLong();

            break;

        case Impl::DOUBLE_TYPE:
            os << "double, value=" << getDouble();

            break;

        case Impl::FLOAT_TYPE:
            os << "float, value=" << getFloat();

            break;

        case Impl::STRING_TYPE:
            os << "string, value=" << getString();

            break;

        case Impl::WIDE_STRING_TYPE:
            os << "wstring, value=" << getWideString();

            break;

        case Impl::BYTE_ARRAY_TYPE:
            os << "byte[], length=" << getByteArray().size();

            break;

        case Impl::VARIANT_VECTOR_TYPE:
            os << "variant[], length=" << getVariantVector().size();

            break;

        case Impl::UUID_TYPE:
            os << "GridClientUuid, value=" << getUuid().uuid();

            break;

        default:
            os << "UNKNOWN";

            break;
        }
    }
    else
        os << "GridPortable";

    os << ']';

    return os.str();
}

class VariantVisitorImpl : public boost::static_visitor<> {
public:
    VariantVisitorImpl(const GridClientVariantVisitor& vis) :
        visitor(vis) {}

    void operator()(bool val) const {
        visitor.visit(val);
    }

    void operator()(int16_t val) const {
        visitor.visit(val);
    }

    void operator()(int32_t val) const {
        visitor.visit(val);
    }

    void operator()(int64_t val) const {
        visitor.visit(val);
    }

    void operator()(float val) const {
        visitor.visit(val);
    }

    void operator()(double val) const {
        visitor.visit(val);
    }

    void operator()(const string& val) const {
        visitor.visit(val);
    }

    void operator()(const wstring& val) const {
        visitor.visit(val);
    }

    void operator()(const vector<int8_t>& val) const {
        visitor.visit(val);
    }

    void operator()(const vector<GridClientVariant>& val) const {
        visitor.visit(val);
    }

    void operator()(const boost::unordered_map<GridClientVariant, GridClientVariant>& val) const {
        visitor.visit(val);
    }

    void operator()(const GridClientUuid& val) const {
        visitor.visit(val);
    }

    void operator()(const GridClientVariant::NullType&) const {
    }

private:
    const GridClientVariantVisitor& visitor;
};

void GridClientVariant::accept(const GridClientVariantVisitor& visitor) const {
    VariantVisitorImpl visitorImpl(visitor);

    boost::apply_visitor(visitorImpl, pimpl.var);
}

bool GridClientVariant::operator==(const GridClientVariant& other) const {
    if (hasPortable()) {
        if (!hashablePortable)
            throw std::exception("Can not compare GridClientVariant holding GridPortable, GridHahshablePortable must be used instead.");

        if (!other.hasPortable() || portable->typeId() != other.getPortable()->typeId())
            return false;

        if (!other.hashablePortable)
            throw std::exception("Can not compare GridClientVariant holding GridPortable, GridHahshablePortable must be used instead.");

        return *static_cast<GridHashablePortable*>(portable) == *static_cast<GridHashablePortable*>(other.portable);
    }
    else {
        if (other.hasPortable())
            return false;

        return pimpl.var == other.pimpl.var;
    }
}

bool GridClientVariant::hasAnyValue() const {
    return pimpl.var.which() != 0 || portable != nullptr;
}

void GridClientVariant::clear() {
    const static Impl::TVariantType emptyVar;

    pimpl.var = emptyVar;
    portable = nullptr;
}

std::size_t hash_value(GridClientVariant const& x) {
    return GridClientVariantHasheableObject(x).hashCode();
}
