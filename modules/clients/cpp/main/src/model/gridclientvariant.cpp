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

using namespace std;

GridClientVariant::GridClientVariant(){
    pimpl.var = NullType();
}

GridClientVariant::~GridClientVariant(){
}

GridClientVariant::GridClientVariant(const GridClientVariant& other) : pimpl(other.pimpl){
}

GridClientVariant& GridClientVariant::operator=(const GridClientVariant& rhs) {
    if (this != &rhs)
        pimpl.var = rhs.pimpl.var;

    return *this;
}

GridClientVariant::GridClientVariant(bool b) {
    pimpl.var = b;
}

GridClientVariant::GridClientVariant(int16_t s) {
    pimpl.var = s;
}

GridClientVariant::GridClientVariant(int32_t i) {
    pimpl.var = i;
}

GridClientVariant::GridClientVariant(int64_t l) {
    pimpl.var = l;
}

GridClientVariant::GridClientVariant(double d) {
    pimpl.var = d;
}

GridClientVariant::GridClientVariant(float f) {
    pimpl.var = f;
}

GridClientVariant::GridClientVariant(const char * s) {
    pimpl.var = std::string(s);
}

GridClientVariant::GridClientVariant(const string& s) {
    pimpl.var = s;
}

GridClientVariant::GridClientVariant(const std::wstring& s) {
    pimpl.var = s;
}

GridClientVariant::GridClientVariant(const vector<int8_t>& b) {
    pimpl.var = b;
}

GridClientVariant::GridClientVariant(const std::vector<GridClientVariant>& v)  {
    pimpl.var = v;
}

GridClientVariant::GridClientVariant(const GridClientUuid& val)  {
    pimpl.var = val;
}

void GridClientVariant::set(bool pBool) {
    pimpl.var = pBool;
}

bool GridClientVariant::hasBool() const {
    return pimpl.var.which() == Impl::BOOL_TYPE;
}

bool GridClientVariant::getBool() const {
    return boost::get<bool>(pimpl.var);
}

void GridClientVariant::set(int16_t value) {
    pimpl.var = value;
}

bool GridClientVariant::hasShort() const {
    return pimpl.var.which() == Impl::SHORT_TYPE;
}

int16_t GridClientVariant::getShort() const {
    return boost::get<int16_t>(pimpl.var);
}

void GridClientVariant::set(int32_t value) {
    pimpl.var = value;
}

bool GridClientVariant::hasInt() const {
    return pimpl.var.which() == Impl::INT_TYPE;
}

int32_t GridClientVariant::getInt() const {
    return boost::get<int32_t>(pimpl.var);
}

void GridClientVariant::set(int64_t value) {
    pimpl.var = value;
}

bool GridClientVariant::hasLong() const {
    return pimpl.var.which() == Impl::LONG_TYPE;
}

int64_t GridClientVariant::getLong() const {
    return boost::get<int64_t>(pimpl.var);
}

void GridClientVariant::set(double val) {
    pimpl.var = val;
}

bool GridClientVariant::hasDouble() const {
    return pimpl.var.which() == Impl::DOUBLE_TYPE;
}

double GridClientVariant::getDouble() const {
    return boost::get<double>(pimpl.var);
}

void GridClientVariant::set(float val) {
    pimpl.var = val;
}

bool GridClientVariant::hasFloat() const {
    return pimpl.var.which() == Impl::FLOAT_TYPE;
}

float GridClientVariant::getFloat() const {
    return boost::get<float>(pimpl.var);
}

void GridClientVariant::set(const char* pText) {
    pimpl.var = std::string(pText);
}

void GridClientVariant::set(const string& pText) {
    pimpl.var = pText;
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

std::vector<GridClientVariant> GridClientVariant::getVariantVector() const {
    return boost::get<std::vector<GridClientVariant> >(pimpl.var);
}

void GridClientVariant::set(const GridClientUuid& val) {
    pimpl.var = val;
}

bool GridClientVariant::hasUuid() const {
    return pimpl.var.which() == Impl::UUID_TYPE;
}

GridClientUuid GridClientVariant::getUuid() const {
    return boost::get<GridClientUuid>(pimpl.var);
}

string GridClientVariant::toString() const {
    if (Impl::STRING_TYPE == pimpl.var.which()) {
        return getString();
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

bool GridClientVariant::operator<(const GridClientVariant& varImpl) const {
    return pimpl.var.which() == varImpl.pimpl.var.which() ?
        toString() < varImpl.toString() : pimpl.var.which() < varImpl.pimpl.var.which();
}

bool GridClientVariant::operator == (const GridClientVariant& varImpl) const {
    return pimpl.var == varImpl.pimpl.var;
}

bool GridClientVariant::hasAnyValue() const {
    return pimpl.var.which() != 0;
}

void GridClientVariant::clear() {
    const static Impl::TVariantType emptyVar;

    pimpl.var = emptyVar;
}
