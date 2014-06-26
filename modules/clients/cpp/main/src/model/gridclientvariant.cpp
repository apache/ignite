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

GridClientVariant& GridClientVariant::operator=(GridClientVariant&& other) {
    if (this != &other) {
        clear();

        type = other.type;
        data = other.data;

        other.type = NULL_TYPE;
    }

    return *this;
}

GridClientVariant::GridClientVariant(GridClientVariant&& other) : type(other.type), data(other.data) {
    other.type = NULL_TYPE;
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

        case DATE_TYPE:
            data.dateVal = new GridClientDate(*other.data.dateVal);

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

        case DATE_ARR_TYPE:
            data.dateArrVal = new vector<boost::optional<GridClientDate>>(*other.data.dateArrVal);

            break;

        case VARIANT_ARR_TYPE:
            data.variantArrVal = new TGridClientVariantSet(*other.data.variantArrVal);

            break;

        case VARIANT_MAP_TYPE:
            data.variantMapVal = new TGridClientVariantMap(*other.data.variantMapVal);

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

GridClientVariant::GridClientVariant(const GridClientUuid& val) : type(UUID_TYPE) {
    data.uuidVal = new GridClientUuid(val);
}

GridClientVariant::GridClientVariant(const GridClientDate& val) : type(DATE_TYPE) {
    data.dateVal = new GridClientDate(val);
}

GridClientVariant::GridClientVariant(const std::vector<boost::optional<GridClientDate>>& val) : type(DATE_ARR_TYPE) {
    data.dateArrVal = new std::vector<boost::optional<GridClientDate>>(val);
}

void GridClientVariant::set(GridPortable* val) {
    clear();

    data.portableVal = val;
    type = PORTABLE_TYPE;
}

void GridClientVariant::set(GridHashablePortable* val) {
    clear();

    data.hashPortableVal = val;
    type = HASHABLE_PORTABLE_TYPE;
}

bool GridClientVariant::hasPortable() const {
    return type == PORTABLE_TYPE;
}

bool GridClientVariant::hasHashablePortable() const {
    return type == HASHABLE_PORTABLE_TYPE;
}

void GridClientVariant::set(const GridPortableObject& val) {
    clear();

    data.portableObjVal = new GridPortableObject(val);
    type = PORTABLE_OBJ_TYPE;
}

GridPortableObject& GridClientVariant::getPortableObject() const {
    checkType(PORTABLE_OBJ_TYPE);

    return *data.portableObjVal;
}

bool GridClientVariant::hasPortableObject() const {
    return type == PORTABLE_OBJ_TYPE;
}

string GridClientVariant::typeName(TypeEnum type) {
    switch(type) {
        case NULL_TYPE: return "Null";

        case BYTE_TYPE: return "Byte";

        case SHORT_TYPE: return "Short";

        case INT_TYPE: return "Int";

        case LONG_TYPE: return "Long";

        case FLOAT_TYPE: return "Float";

        case DOUBLE_TYPE: return "Double";

        case CHAR_TYPE: return "Char";

        case BOOL_TYPE: return "Bool";

        case PORTABLE_TYPE: return "Portable";

        case HASHABLE_PORTABLE_TYPE: return "HashablePortable";

        case STRING_TYPE: return "String";

        case WIDE_STRING_TYPE: return "WideString";

        case UUID_TYPE: return "Uuid";

        case DATE_TYPE: return "Date";

        case DATE_ARR_TYPE: return "DateArray";

        case PORTABLE_OBJ_TYPE: return "PortableObject";

        case BYTE_ARR_TYPE: return "ByteArray";

        case SHORT_ARR_TYPE: return "ShortArray";

        case INT_ARR_TYPE: return "IntArray";

        case LONG_ARR_TYPE: return "LongArray";

        case FLOAT_ARR_TYPE: return "FloatArray";

        case DOUBLE_ARR_TYPE: return "DoubleArray";

        case CHAR_ARR_TYPE: return "CharArray";

        case BOOL_ARR_TYPE: return "BoolArray";

        case STRING_ARR_TYPE: return "StringArray";

        case UUID_ARR_TYPE: return "UuidArray";

        case VARIANT_ARR_TYPE: return "VariantArray";

        case VARIANT_MAP_TYPE: return "VariantMap";
    }

    assert(false);
    
    return "Unknown";
}

void GridClientVariant::checkType(TypeEnum expType) const {
    if (type != expType) {
        string msg = "Varinat contains unexpected type [exp=" + typeName(expType) + ", actual=" + typeName(type) + "]";
        
        throw runtime_error(msg);
    }
}

void GridClientVariant::set(bool val) {
    clear();

    data.boolVal = val;
    type = BOOL_TYPE;
}

bool GridClientVariant::hasBool() const {
    return type == BOOL_TYPE;
}

bool GridClientVariant::getBool() const {
    checkType(BOOL_TYPE);

    return data.boolVal;
}

void GridClientVariant::set(int16_t val) {
    clear();

    data.shortVal = val;
    type = SHORT_TYPE;
}

bool GridClientVariant::hasShort() const {
    return type == SHORT_TYPE;
}

int16_t GridClientVariant::getShort() const {
    checkType(SHORT_TYPE);

    return data.shortVal;
}

void GridClientVariant::set(int8_t val) {
    clear();

    data.byteVal = val;
    type = BYTE_TYPE;
}

bool GridClientVariant::hasByte() const {
    return type == BYTE_TYPE;
}

int8_t GridClientVariant::getByte() const {
    checkType(BYTE_TYPE);

    return data.byteVal;
}

void GridClientVariant::set(uint16_t val) {
    clear();

    data.charVal = val;
    type = CHAR_TYPE;
}

bool GridClientVariant::hasChar() const {
    return type == CHAR_TYPE;
}

uint16_t GridClientVariant::getChar() const {
    checkType(CHAR_TYPE);

    return data.charVal;
}

void GridClientVariant::set(int32_t val) {
    clear();

    data.intVal = val;
    type = INT_TYPE;
}

bool GridClientVariant::hasInt() const {
    return type == INT_TYPE;
}

int32_t GridClientVariant::getInt() const {
    checkType(INT_TYPE);

    return data.intVal;
}

void GridClientVariant::set(int64_t val) {
    clear();

    data.longVal = val;
    type = LONG_TYPE;
}

bool GridClientVariant::hasLong() const {
    return type == LONG_TYPE;
}

int64_t GridClientVariant::getLong() const {
    checkType(LONG_TYPE);

    return data.longVal;
}

void GridClientVariant::set(double val) {
    clear();

    data.doubleVal = val;
    type = DOUBLE_TYPE;
}

bool GridClientVariant::hasDouble() const {
    return type == DOUBLE_TYPE;
}

double GridClientVariant::getDouble() const {
    checkType(DOUBLE_TYPE);

    return data.doubleVal;
}

void GridClientVariant::set(float val) {
    clear();

    data.floatVal = val;
    type = FLOAT_TYPE;
}

bool GridClientVariant::hasFloat() const {
    return type == FLOAT_TYPE;
}

float GridClientVariant::getFloat() const {
    checkType(FLOAT_TYPE);

    return data.floatVal;
}

void GridClientVariant::set(const char* val) {
    clear();

    data.strVal = new string(val);
    type = STRING_TYPE;
}

void GridClientVariant::set(const string& val) {
    clear();

    data.strVal = new string(val);
    type = STRING_TYPE;
}

bool GridClientVariant::hasString() const {
    return type == STRING_TYPE;
}

string& GridClientVariant::getString() const {
    checkType(STRING_TYPE);

    return *data.strVal;
}

bool GridClientVariant::hasWideString() const {
    return type == WIDE_STRING_TYPE;
}

wstring& GridClientVariant::getWideString() const {
    checkType(WIDE_STRING_TYPE);

    return *data.wideStrVal;
}

void GridClientVariant::set(const vector<int8_t>& val) {
    clear();

    data.byteArrVal = new vector<int8_t>(val);
    type = BYTE_ARR_TYPE;
}

bool GridClientVariant::hasByteArray() const {
    return type == BYTE_ARR_TYPE;
}

vector<int8_t>& GridClientVariant::getByteArray() const {
    checkType(BYTE_ARR_TYPE);

    return *data.byteArrVal;
}

void GridClientVariant::set(const vector<bool>& val) {
    clear();

    data.boolArrVal = new vector<bool>(val);
    type = BOOL_ARR_TYPE;
}

bool GridClientVariant::hasBoolArray() const {
    return type == BOOL_ARR_TYPE;
}

vector<bool>& GridClientVariant::getBoolArray() const {
    checkType(BOOL_ARR_TYPE);

    return *data.boolArrVal;
}

void GridClientVariant::set(const vector<int16_t>& val) {
    clear();

    data.shortArrVal = new vector<int16_t>(val);
    type = SHORT_ARR_TYPE;
}

bool GridClientVariant::hasShortArray() const {
    return type == SHORT_ARR_TYPE;
}

vector<int16_t>& GridClientVariant::getShortArray() const {
    checkType(SHORT_ARR_TYPE);

    return *data.shortArrVal;
}

void GridClientVariant::set(const vector<int32_t>& val) {
    clear();

    data.intArrVal = new vector<int32_t>(val);
    type = INT_ARR_TYPE;
}

bool GridClientVariant::hasIntArray() const {
    return type == INT_ARR_TYPE;
}

vector<int32_t>& GridClientVariant::getIntArray() const {
    checkType(INT_ARR_TYPE);

    return *data.intArrVal;
}

void GridClientVariant::set(const vector<int64_t>& val) {
    clear();

    data.longArrVal = new vector<int64_t>(val);
    type = LONG_ARR_TYPE;
}

bool GridClientVariant::hasLongArray() const {
    return type == LONG_ARR_TYPE;
}

vector<int64_t>& GridClientVariant::getLongArray() const {
    checkType(LONG_ARR_TYPE);

    return *data.longArrVal;
}

void GridClientVariant::set(const vector<float>& val) {
    clear();

    data.floatArrVal = new vector<float>(val);
    type = FLOAT_ARR_TYPE;
}

bool GridClientVariant::hasFloatArray() const {
    return type == FLOAT_ARR_TYPE;
}

vector<float>& GridClientVariant::getFloatArray() const {
    checkType(FLOAT_ARR_TYPE);

    return *data.floatArrVal;
}

void GridClientVariant::set(const vector<double>& val) {
    clear();

    data.doubleArrVal = new vector<double>(val);
    type = DOUBLE_ARR_TYPE;
}

bool GridClientVariant::hasDoubleArray() const {
    return type == DOUBLE_ARR_TYPE;
}

vector<double>& GridClientVariant::getDoubleArray() const {
    checkType(DOUBLE_ARR_TYPE);

    return *data.doubleArrVal;
}

void GridClientVariant::set(const vector<uint16_t>& val) {
    clear();

    data.charArrVal = new vector<uint16_t>(val);
    type = CHAR_ARR_TYPE;
}

bool GridClientVariant::hasCharArray() const {
    return type == CHAR_ARR_TYPE;
}

vector<uint16_t>& GridClientVariant::getCharArray() const {
    checkType(CHAR_ARR_TYPE);

    return *data.charArrVal;
}

void GridClientVariant::set(const vector<string>& val) {
    clear();

    data.strArrVal = new vector<string>(val);
    type = STRING_ARR_TYPE;
}

bool GridClientVariant::hasStringArray() const {
    return type == STRING_ARR_TYPE;
}

vector<string>& GridClientVariant::getStringArray() const {
    checkType(STRING_ARR_TYPE);

    return *data.strArrVal;
}

void GridClientVariant::set(const vector<GridClientUuid>& val) {
    clear();

    data.uuidArrVal = new vector<GridClientUuid>(val);
    type = UUID_ARR_TYPE;
}

bool GridClientVariant::hasUuidArray() const {
    return type == UUID_ARR_TYPE;
}

vector<GridClientUuid>& GridClientVariant::getUuidArray() const {
    checkType(UUID_ARR_TYPE);

    return *data.uuidArrVal;
}

bool GridClientVariant::hasVariantVector() const {
    return type == VARIANT_ARR_TYPE;
}

TGridClientVariantSet& GridClientVariant::getVariantVector() const {
    checkType(VARIANT_ARR_TYPE);

    return *data.variantArrVal;
}

void GridClientVariant::set(const TGridClientVariantSet& val) {
    clear();

    data.variantArrVal = new TGridClientVariantSet(val);
    type = VARIANT_ARR_TYPE;
}

bool GridClientVariant::hasVariantMap() const {
    return type == VARIANT_MAP_TYPE;
}

TGridClientVariantMap& GridClientVariant::getVariantMap() const {
    checkType(VARIANT_MAP_TYPE);

    return *data.variantMapVal;
}

void GridClientVariant::set(const TGridClientVariantMap& val) {
    clear();

    data.variantMapVal = new TGridClientVariantMap(val);
    type = VARIANT_MAP_TYPE;
}

void GridClientVariant::set(const GridClientUuid& val) {
    clear();

    data.uuidVal = new GridClientUuid(val);
    type = UUID_TYPE;
}

bool GridClientVariant::hasUuid() const {
    return type == UUID_TYPE;
}

GridClientUuid& GridClientVariant::getUuid() const {
    checkType(UUID_TYPE);

    return *data.uuidVal;
}

void GridClientVariant::set(const GridClientDate& val) {
    clear();

    data.dateVal = new GridClientDate(val);
    type = DATE_TYPE;
}

bool GridClientVariant::hasDate() const {
    return type == DATE_TYPE;
}

GridClientDate& GridClientVariant::getDate() const {
    checkType(DATE_TYPE);

    return *data.dateVal;
}

void GridClientVariant::set(const vector<boost::optional<GridClientDate>>& val) {
    clear();

    data.dateArrVal = new vector<boost::optional<GridClientDate>>(val);
    type = DATE_ARR_TYPE;
}

bool GridClientVariant::hasDateArray() const {
    return type == DATE_ARR_TYPE;
}

vector<boost::optional<GridClientDate>>& GridClientVariant::getDateArray() const {
    checkType(DATE_ARR_TYPE);

    return *data.dateArrVal;
}

GridPortable* GridClientVariant::getPortable() const {
    checkType(PORTABLE_TYPE);

    return data.portableVal;
}

GridHashablePortable* GridClientVariant::getHashablePortable() const {
    checkType(HASHABLE_PORTABLE_TYPE);

    return data.hashPortableVal;
}

int32_t GridClientVariant::hashCode() const {
    return GridClientVariantHasheableObject(*this).hashCode();
}

string GridClientVariant::toString() const {
    ostringstream os;

    switch(type) {
        case NULL_TYPE: os << "Null"; break;

        case BYTE_TYPE: os << (int64_t)data.byteVal; break;

        case SHORT_TYPE: os << (int64_t)data.shortVal; break;

        case INT_TYPE: os << (int64_t)data.intVal; break;

        case LONG_TYPE: os << data.longVal; break;

        case FLOAT_TYPE: os << data.floatVal; break;

        case DOUBLE_TYPE: os << data.doubleVal; break;

        case CHAR_TYPE: os << (int64_t)data.charVal; break;;

        case BOOL_TYPE: os << (data.boolVal ? "true" : "false"); break;

        case PORTABLE_TYPE: os << "[Portable [typeId=" << data.portableVal->typeId() << "]]"; break;

        case HASHABLE_PORTABLE_TYPE: os << "[HashablePortable [typeId=" << data.portableVal->typeId() << "]]"; break;

        case STRING_TYPE: os << *data.strVal; break;

        case WIDE_STRING_TYPE: os << "[wstring]"; break;

        case UUID_TYPE: os << *data.uuidVal; break;

        case DATE_TYPE: os << *data.dateVal; break;

        case PORTABLE_OBJ_TYPE: os << "[PortableObject [typeId=" << data.portableObjVal->typeId() << "]]"; break;

        case BYTE_ARR_TYPE: os << "[ByteArray]"; break;

        case SHORT_ARR_TYPE: os << "[ShortArray]"; break;

        case INT_ARR_TYPE: os << "[IntArray]"; break;

        case LONG_ARR_TYPE: os << "[LongArray]"; break;

        case FLOAT_ARR_TYPE: os << "[FloatArray]"; break;

        case DOUBLE_ARR_TYPE: os << "[DoubleArray]"; break;

        case CHAR_ARR_TYPE: os << "[CharArray]"; break;

        case BOOL_ARR_TYPE: os << "[BoolArray]"; break;

        case STRING_ARR_TYPE: os << "[StringArray]"; break;

        case UUID_ARR_TYPE: os << "[UuidArray]"; break;

        case DATE_ARR_TYPE: os << "[DateArray]"; break;

        case VARIANT_ARR_TYPE: os << "[VariantArray]"; break;

        case VARIANT_MAP_TYPE: os << "[VariantMap]"; break;
    }
    
    return os.str();
}

string GridClientVariant::debugString() const {
    ostringstream os;

    os << "GridClientVariant [type=" << typeName(type);

    switch(type) {
        case NULL_TYPE: os << "Null"; break;

        case BYTE_TYPE: os << ", val=" << (int64_t)data.byteVal; break;

        case SHORT_TYPE: os << ", val=" << (int64_t)data.shortVal; break;

        case INT_TYPE: os << ", val=" << (int64_t)data.intVal; break;

        case LONG_TYPE: os << ", val=" << data.longVal; break;

        case FLOAT_TYPE: os << ", val=" << data.floatVal; break;

        case DOUBLE_TYPE: os << ", val=" << data.doubleVal; break;

        case CHAR_TYPE: os << ", val=" << (int64_t)data.charVal; break;;

        case BOOL_TYPE: os << ", val=" << (data.boolVal ? "true" : "false"); break;

        case PORTABLE_TYPE: os << ", typeId=" << data.portableVal->typeId(); break;

        case HASHABLE_PORTABLE_TYPE: os << "typeId=" << data.portableVal->typeId(); break;

        case STRING_TYPE: os << ", val=" << *data.strVal; break;

        case WIDE_STRING_TYPE: os << ", size=" << data.wideStrVal->size(); break;

        case UUID_TYPE: os << ", val=" << *data.uuidVal; break;

        case DATE_TYPE: os << ", val=" << *data.dateVal; break;

        case PORTABLE_OBJ_TYPE: os << "typeId=" << data.portableObjVal->typeId(); break;

        case BYTE_ARR_TYPE: os << ", size=" << data.byteArrVal->size(); break;

        case SHORT_ARR_TYPE: os << ", size=" << data.shortArrVal->size(); break;

        case INT_ARR_TYPE:  os << ", size=" << data.intArrVal->size(); break;

        case LONG_ARR_TYPE:  os << ", size=" << data.longArrVal->size(); break;

        case FLOAT_ARR_TYPE:  os << ", size=" << data.floatArrVal->size(); break;

        case DOUBLE_ARR_TYPE:  os << ", size=" << data.doubleArrVal->size(); break;

        case CHAR_ARR_TYPE:  os << ", size=" << data.charArrVal->size(); break;

        case BOOL_ARR_TYPE:  os << ", size=" << data.boolArrVal->size(); break;

        case STRING_ARR_TYPE:  os << ", size=" << data.strArrVal->size(); break;

        case UUID_ARR_TYPE:  os << ", size=" << data.uuidArrVal->size(); break;
      
        case DATE_ARR_TYPE:  os << ", size=" << data.dateArrVal->size(); break;

        case VARIANT_ARR_TYPE:  os << ", size=" << data.variantArrVal->size(); break;

        case VARIANT_MAP_TYPE:  os << ", size=" << data.variantMapVal->size(); break;
    }

    os << ']';

    return os.str();
}

void GridClientVariant::accept(const GridClientVariantVisitor& visitor) const {
    switch(type) {
        case NULL_TYPE: return;

        case BYTE_TYPE: visitor.visit(data.byteVal); return;

        case SHORT_TYPE: visitor.visit(data.shortVal); return;

        case INT_TYPE: visitor.visit(data.intVal); return;

        case LONG_TYPE: visitor.visit(data.longVal); return;

        case FLOAT_TYPE: visitor.visit(data.floatVal); return;

        case DOUBLE_TYPE: visitor.visit(data.doubleVal); return;

        case CHAR_TYPE: visitor.visit(data.charVal); return;

        case BOOL_TYPE: visitor.visit(data.boolVal); return;

        case PORTABLE_TYPE: visitor.visit(*data.portableVal); return;

        case HASHABLE_PORTABLE_TYPE: visitor.visit(*data.hashPortableVal); return;

        case STRING_TYPE: visitor.visit(*data.strVal); return;

        case WIDE_STRING_TYPE: visitor.visit(*data.wideStrVal); return;

        case UUID_TYPE: visitor.visit(*data.uuidVal); return;

        case DATE_TYPE: visitor.visit(*data.dateVal); return;

        case PORTABLE_OBJ_TYPE: visitor.visit(*data.portableObjVal); return;

        case BYTE_ARR_TYPE: visitor.visit(*data.byteArrVal); return;

        case SHORT_ARR_TYPE: visitor.visit(*data.shortArrVal); return;

        case INT_ARR_TYPE: visitor.visit(*data.intArrVal); return;

        case LONG_ARR_TYPE: visitor.visit(*data.longArrVal); return;

        case FLOAT_ARR_TYPE: visitor.visit(*data.floatArrVal); return;

        case DOUBLE_ARR_TYPE: visitor.visit(*data.doubleArrVal); return;

        case CHAR_ARR_TYPE: visitor.visit(*data.charArrVal); return;

        case BOOL_ARR_TYPE: visitor.visit(*data.boolArrVal); return;

        case STRING_ARR_TYPE: visitor.visit(*data.strArrVal); return;

        case UUID_ARR_TYPE: visitor.visit(*data.uuidArrVal); return;

        case DATE_ARR_TYPE: visitor.visit(*data.dateArrVal); return;

        case VARIANT_ARR_TYPE: visitor.visit(*data.variantArrVal); return;

        case VARIANT_MAP_TYPE: visitor.visit(*data.variantMapVal); return;
    }

    assert(false);
}

bool GridClientVariant::operator==(const GridClientVariant& other) const {
    if (type != other.type)
        return false;
    
    switch(type) {
        case NULL_TYPE: return true;

        case BYTE_TYPE: return data.byteVal == other.data.byteVal;

        case SHORT_TYPE: return data.shortVal == other.data.shortVal;

        case INT_TYPE: return data.intVal == other.data.intVal;

        case LONG_TYPE: return data.longVal == other.data.longVal;

        case FLOAT_TYPE: return data.floatVal == other.data.floatVal;

        case DOUBLE_TYPE: return data.doubleVal == other.data.doubleVal;

        case CHAR_TYPE: return data.charVal == other.data.charVal;

        case BOOL_TYPE: return data.boolVal == other.data.boolVal;

        case PORTABLE_TYPE:
            throw runtime_error("GridPortable does not support comparison.");

        case HASHABLE_PORTABLE_TYPE: 
            if (data.hashPortableVal->typeId() != other.data.hashPortableVal->typeId())
                return false;

            return *data.hashPortableVal == *other.data.hashPortableVal;

        case STRING_TYPE: return *data.strVal == *other.data.strVal;

        case WIDE_STRING_TYPE: return *data.wideStrVal == *other.data.wideStrVal;

        case UUID_TYPE: return *data.uuidVal == *other.data.uuidVal;

        case DATE_TYPE: return *data.dateVal == *other.data.dateVal;

        case PORTABLE_OBJ_TYPE: return *data.portableObjVal == *other.data.portableObjVal;

        case BYTE_ARR_TYPE: return *data.byteArrVal == *other.data.byteArrVal;

        case SHORT_ARR_TYPE: return *data.shortArrVal == *other.data.shortArrVal;

        case INT_ARR_TYPE: return *data.intArrVal == *other.data.intArrVal;

        case LONG_ARR_TYPE: return *data.longArrVal == *other.data.longArrVal;

        case FLOAT_ARR_TYPE: return *data.floatArrVal == *other.data.floatArrVal;

        case DOUBLE_ARR_TYPE: return *data.doubleArrVal == *other.data.doubleArrVal;

        case CHAR_ARR_TYPE: return *data.charArrVal == *other.data.charArrVal;

        case BOOL_ARR_TYPE: return *data.boolArrVal == *other.data.boolArrVal;

        case STRING_ARR_TYPE: return *data.strArrVal == *other.data.strArrVal;

        case UUID_ARR_TYPE: return *data.uuidArrVal == *other.data.uuidArrVal;

        case DATE_ARR_TYPE: return *data.dateArrVal == *other.data.dateArrVal;

        case VARIANT_ARR_TYPE: return *data.variantArrVal == *other.data.variantArrVal;

        case VARIANT_MAP_TYPE: return *data.variantMapVal == *other.data.variantMapVal;
    }

    assert(false);
    
    return false;
}

bool GridClientVariant::hasAnyValue() const {
    return type != NULL_TYPE;
}

void GridClientVariant::clear() {
    switch(type) {
        case NULL_TYPE:
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
            break;

        case STRING_TYPE:
            delete data.strVal;

            break;

        case WIDE_STRING_TYPE:
            delete data.wideStrVal;

            break;

        case UUID_TYPE:
            delete data.uuidVal;

            break;

        case DATE_TYPE:
            delete data.dateVal;

            break;

        case PORTABLE_OBJ_TYPE:
            delete data.portableObjVal;

            break;

        case BYTE_ARR_TYPE:
            delete data.byteArrVal;

            break;

        case SHORT_ARR_TYPE:
            delete data.shortArrVal;

            break;

        case INT_ARR_TYPE:
            delete data.intArrVal;

            break;

        case LONG_ARR_TYPE:
            delete data.longArrVal;

            break;

        case FLOAT_ARR_TYPE:
            delete data.floatArrVal;

            break;

        case DOUBLE_ARR_TYPE:
            delete data.doubleArrVal;

            break;

        case CHAR_ARR_TYPE:
            delete data.charArrVal;

            break;

        case BOOL_ARR_TYPE:
            delete data.boolArrVal;

            break;

        case STRING_ARR_TYPE:
            delete data.strArrVal;

            break;

        case UUID_ARR_TYPE:
            delete data.uuidArrVal;

            break;

        case DATE_ARR_TYPE:
            delete data.dateArrVal;

            break;

        case VARIANT_ARR_TYPE:
            delete data.variantArrVal;

            break;

        case VARIANT_MAP_TYPE:
            delete data.variantMapVal;

            break;
    }

    type = NULL_TYPE;
}
