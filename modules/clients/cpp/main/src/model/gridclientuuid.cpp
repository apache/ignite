/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <sstream>
#include <iterator>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/thread.hpp>

#include "gridgain/gridclientuuid.hpp"

using namespace std;

static boost::uuids::random_generator uuidGen;
static boost::mutex uuidGenMux;

GridUuid::GridUuid() {
    memset(pimpl.uuid_.data, 0, pimpl.uuid_.size());
}

GridUuid::GridUuid(const char* str)  {
    if (str != NULL) {
        std::stringstream ss;

        ss << str;

        ss >> pimpl.uuid_;
    }
    else
        memset(pimpl.uuid_.data, 0, pimpl.uuid_.size());
}

GridUuid::GridUuid(const string& str)  {
    std::stringstream ss;

    ss << str;

    ss >> pimpl.uuid_;
}

GridUuid::GridUuid(const GridUuid& other) {
    pimpl.uuid_ = other.pimpl.uuid_;
}

GridUuid GridUuid::randomUuid() {
    GridUuid ret;

    {
        boost::lock_guard<boost::mutex> g(uuidGenMux);

        ret.pimpl.uuid_ = uuidGen();
    }

    return ret;
}

GridUuid& GridUuid::operator=(const GridUuid& rhs){
    if (this != &rhs)
        pimpl.uuid_ = rhs.pimpl.uuid_;

    return *this;
}

GridUuid::~GridUuid(){
}

std::string const GridUuid::uuid() const {
    std::stringstream ss;

    ss << pimpl.uuid_;

    return ss.str();
}

int64_t GridUuid::leastSignificantBits() const {
    int64_t lsb = pimpl.uuid_.data[8];

    for (int i = 9 ; i < 16 ; i++) {
        lsb <<= 8 ;
        lsb |= pimpl.uuid_.data[i];
    }

    return lsb;
}

int64_t GridUuid::mostSignificantBits() const {
    int64_t msb = pimpl.uuid_.data[0];

    for (int i = 1 ; i < 8 ; i++) {
        msb <<= 8 ;
        msb |= pimpl.uuid_.data[i];
    }

    return msb;
}

int32_t GridUuid::hashCode() const {
    int32_t hashCode = (int32_t)((mostSignificantBits() >> 32) ^
            mostSignificantBits() ^ (leastSignificantBits() >> 32) ^ leastSignificantBits());

    return hashCode;
}

void GridUuid::convertToBytes(vector<int8_t>& bytes) const {
    back_insert_iterator<vector<int8_t>> inserter=back_inserter(bytes);

    reverse_copy(&(pimpl.uuid_.data[0]), &(pimpl.uuid_.data[pimpl.uuid_.size()/2]), inserter);

    reverse_copy(&(pimpl.uuid_.data[pimpl.uuid_.size()/2]), &(pimpl.uuid_.data[pimpl.uuid_.size()]), inserter);
}

void GridUuid::rawBytes(vector<int8_t>& bytes) const {
    bytes.resize(pimpl.uuid_.size());

    copy(pimpl.uuid_.data, pimpl.uuid_.data + pimpl.uuid_.size(), bytes.begin());
}

GridUuid GridUuid::fromBytes(const string& bytes) {
    GridUuid ret;

    ::memcpy(ret.pimpl.uuid_.data, bytes.c_str(), ret.pimpl.uuid_.size());

    return ret;
}

bool GridUuid::operator < (const GridUuid& other) const {
    if (mostSignificantBits() < other.mostSignificantBits())
        return true;

    if (mostSignificantBits() > other.mostSignificantBits())
        return false;

    return leastSignificantBits() < other.leastSignificantBits();
}

bool GridUuid::operator == (const GridUuid& other) const {
    return pimpl.uuid_ == other.pimpl.uuid_;
}
