/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <map>

#include "gridgain/impl/utils/gridclientbyteutils.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientobjectwrapperconvertor.hpp"
#include "gridgain/impl/marshaller/protobuf/ClientMessages.pb.h"
#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/impl/hash/gridclientwidestringhasheableobject.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientprotobufmarshaller.hpp"

using namespace org::gridgain::grid::kernal::processors::rest::client::message;
using namespace std;

static bool getBoolValue(const string& binary, GridClientVariant& var) {
    assert(binary.size() == 1);

    var.set(binary[0] == 1 ? true : false);

    return true;
}

static bool getBytesValue(const string& binary, GridClientVariant& var) {
    std::vector<int8_t> v(binary.size());

    std::copy(binary.begin(), binary.end(), v.begin());

    var.set(v);

    return true;
}

static bool getByteValue(const string& binary, GridClientVariant& var) {
    assert(binary.size() == 1);

    return getBytesValue(binary, var);

    return true;
}

static bool getInt16Value(const string& binary, GridClientVariant& var) {
    int16_t res;

    GridClientByteUtils::bytesToValue((const int8_t*) binary.data(), binary.size(), res);

    var.set(res);

    return true;
}

static bool getInt32Value(const string& binary, GridClientVariant& var) {
    int32_t res;

    GridClientByteUtils::bytesToValue((const int8_t*) binary.data(), binary.size(), res);

    var.set(res);

    return true;
}

static bool getInt64Value(const string& binary, GridClientVariant& var) {
    int64_t res;

    GridClientByteUtils::bytesToValue((const int8_t*) binary.data(), binary.size(), res);

    var.set(res);

    return true;
}

static bool getFloatValue(const string& binary, GridClientVariant& var) {
    int32_t res;

    GridClientByteUtils::bytesToValue((const int8_t*) binary.data(), binary.size(), res);

    float floatVal = GridFloatHasheableObject::intBitsToFloat(res);

    var.set(floatVal);

    return true;
}

static bool getDoubleValue(const string& binary, GridClientVariant& var) {
    int64_t res;

    GridClientByteUtils::bytesToValue((const int8_t*) binary.data(), binary.size(), res);

    double doubleVal = GridDoubleHasheableObject::longBitsToDouble(res);

    var.set(doubleVal);

    return true;
}

static bool doUnwrapSimpleType(const ObjectWrapper& objWrapper, GridClientVariant& var);

static bool getTaskTesult(const string& binary, GridClientVariant& var) {
    ProtoTaskBean tb;

    if (!tb.ParseFromString(binary))
        return false;

    if (!tb.has_resultbean() || !tb.has_finished())
        return false;

    return doUnwrapSimpleType(tb.resultbean(), var);
}

static bool doUnwrapSimpleType(const ObjectWrapper& objWrapper, GridClientVariant& var) {
    assert(objWrapper.has_binary());

    GG_LOG_DEBUG("Unwrap simple type: %s", objWrapper.DebugString().c_str());

    string binary = objWrapper.binary();

    bool unwrapRes = false;

    switch (objWrapper.type()) {
        case NONE:
            return true;
        case BOOL:
            return getBoolValue(binary, var);

        case BYTE:
            return getByteValue(binary, var);

        case BYTES:
            return getBytesValue(binary, var);

        case INT32:
            return getInt32Value(binary, var);

        case INT64:
            return getInt64Value(binary, var);

        case SHORT:
            return getInt16Value(binary, var);

        case STRING:
            var.set(binary);
            return true;

        case DOUBLE:
            return getDoubleValue(binary, var);

        case FLOAT:
            return getFloatValue(binary, var);

        case TASK_BEAN:
            return getTaskTesult(binary, var);

        default: // Non-simple type

            break;
    }

    return unwrapRes;
}

bool GridClientObjectWrapperConvertor::unwrapSimpleType(const ObjectWrapper& objWrapper, GridClientVariant& var) {
    return doUnwrapSimpleType(objWrapper, var);
}

namespace {

    class GridClientVariantVisitorImpl : public GridClientVariantVisitor {
    public:
        GridClientVariantVisitorImpl(ObjectWrapper& wrapper) : objWrapper(wrapper) {
        }

        typedef vector<int8_t> TByteVector;

        virtual void visit(const bool pValue) const {
            TByteVector bytes;

            bytes.push_back(pValue ? 1 : 0);
            serialize(BOOL, bytes);
        }

        virtual void visit(const int16_t pShort) const {
            int8_t bytes[sizeof(pShort)];

            memset(&bytes[0], 0, sizeof(pShort));
            GridClientByteUtils::valueToBytes(pShort, &bytes[0], sizeof(pShort));
            serialize(SHORT, bytes, sizeof(pShort));
        }

        virtual void visit(const int32_t pInt) const {
            int8_t bytes[sizeof(pInt)];

            memset(&bytes[0], 0, sizeof(pInt));
            GridClientByteUtils::valueToBytes(pInt, &bytes[0], sizeof(pInt));
            serialize(INT32, bytes, sizeof(pInt));
        }

        virtual void visit(const int64_t pLong) const {
            int8_t bytes[sizeof(pLong)];

            memset(&bytes[0], 0, sizeof(pLong));
            GridClientByteUtils::valueToBytes(pLong, &bytes[0], sizeof(pLong));
            serialize(INT64, bytes, sizeof(pLong));
        }

        virtual void visit(const float pFloat) const {
            int32_t intBits = GridFloatHasheableObject::floatToIntBits(pFloat);
            int8_t bytes[sizeof(intBits)];

            memset(&bytes[0], 0, sizeof(intBits));
            GridClientByteUtils::valueToBytes(intBits, &bytes[0], sizeof(intBits));
            serialize(INT32, bytes, sizeof(intBits));
        }

        virtual void visit(const double pDouble) const {
            int64_t longBits = GridDoubleHasheableObject::doubleToLongBits(pDouble);
            int8_t bytes[sizeof(longBits)];

            memset(&bytes[0], 0, sizeof(longBits));
            GridClientByteUtils::valueToBytes(longBits, &bytes[0], sizeof(longBits));
            serialize(INT64, bytes, sizeof(longBits));
        }

        virtual void visit(const string& pText) const {
            TByteVector bytes(pText.size());

            copy(pText.begin(), pText.end(), bytes.begin());
            serialize(STRING, bytes);
        }

        virtual void visit(const wstring& pText) const {
            TByteVector bytes;

            GridWideStringHasheableObject(pText).convertToBytes(bytes);
            serialize(STRING, bytes);
        }

        virtual void visit(const vector<int8_t>& bytes) const {
            serialize(BYTES, bytes);
        }

        virtual void visit(const vector<GridClientVariant>& vvec) const {
            Collection c;
            ProtobufCollInserter collIns(c);

            std::for_each(vvec.begin(), vvec.end(), collIns);

            objWrapper.set_type(COLLECTION);

            int8_t * pBuffer;
            unsigned long bufferLength;

            GridClientProtobufMarshaller::marshalMsg(c, pBuffer, bufferLength);
            objWrapper.set_binary(pBuffer, bufferLength);
            delete[] pBuffer;
        }

        virtual void visit(const GridClientUuid& uuid) const {
            TByteVector bytes;

            uuid.convertToBytes(bytes);
            serialize(UUID, bytes);
        }

        void serialize(const ObjectWrapperType& type, const TByteVector& bytes) const {
            objWrapper.set_type(type);

            if (bytes.size() > 0)
                objWrapper.set_binary(bytes.data(), bytes.size());
            else
                objWrapper.set_binary((void*) NULL, 0);
        }

        void serialize(const ObjectWrapperType& type, int8_t * bytes, int byteslen) const {
            objWrapper.set_type(type);

            if (byteslen > 0)
                objWrapper.set_binary(bytes, byteslen);
            else
                objWrapper.set_binary((void*) NULL, 0);
        }

        ObjectWrapper& objWrapper;
    };

}

bool GridClientObjectWrapperConvertor::wrapSimpleType(const GridClientVariant& var, ObjectWrapper& objWrapper) {
    objWrapper.set_type(NONE);
    GridClientVariantVisitorImpl visitor(objWrapper);
    objWrapper.set_binary((void*) NULL, 0);
    var.accept(visitor);

    return objWrapper.type() != NONE;
}

