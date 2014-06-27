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

GridClientUuid::GridClientUuid() {
    memset(pimpl.uuid_.data, 0, pimpl.uuid_.size());
}

GridClientUuid::GridClientUuid(const char* str)  {
    if (str != NULL) {
        std::stringstream ss;

        ss << str;

        ss >> pimpl.uuid_;
    }
    else
        memset(pimpl.uuid_.data, 0, pimpl.uuid_.size());
}

GridClientUuid::GridClientUuid(const string& str)  {
    std::stringstream ss;

    ss << str;

    ss >> pimpl.uuid_;
}

GridClientUuid::GridClientUuid(int64_t mostSignificantBits, int64_t leastSignificantBits) {
    pimpl.uuid_.data[7] = (mostSignificantBits >> 0) & 0xFF;
    pimpl.uuid_.data[6] = (mostSignificantBits >> 8) & 0xFF;
    pimpl.uuid_.data[5] = (mostSignificantBits >> 16) & 0xFF;
    pimpl.uuid_.data[4] = (mostSignificantBits >> 24) & 0xFF;
    pimpl.uuid_.data[3] = (mostSignificantBits >> 32) & 0xFF;
    pimpl.uuid_.data[2] = (mostSignificantBits >> 40) & 0xFF;
    pimpl.uuid_.data[1] = (mostSignificantBits >> 48) & 0xFF;
    pimpl.uuid_.data[0] = (mostSignificantBits >> 56) & 0xFF;

    pimpl.uuid_.data[15] = (leastSignificantBits >> 0) & 0xFF;
    pimpl.uuid_.data[14] = (leastSignificantBits >> 8) & 0xFF;
    pimpl.uuid_.data[13] = (leastSignificantBits >> 16) & 0xFF;
    pimpl.uuid_.data[12] = (leastSignificantBits >> 24) & 0xFF;
    pimpl.uuid_.data[11] = (leastSignificantBits >> 32) & 0xFF;
    pimpl.uuid_.data[10] = (leastSignificantBits >> 40) & 0xFF;
    pimpl.uuid_.data[9] = (leastSignificantBits >> 48) & 0xFF;
    pimpl.uuid_.data[8] = (leastSignificantBits >> 56) & 0xFF;
}


GridClientUuid::GridClientUuid(const GridClientUuid& other) {
    pimpl.uuid_ = other.pimpl.uuid_;
}

GridClientUuid GridClientUuid::randomUuid() {
    GridClientUuid ret;

    {
        boost::lock_guard<boost::mutex> g(uuidGenMux);

        ret.pimpl.uuid_ = uuidGen();
    }

    return ret;
}

GridClientUuid& GridClientUuid::operator=(const GridClientUuid& rhs){
    if (this != &rhs)
        pimpl.uuid_ = rhs.pimpl.uuid_;

    return *this;
}

GridClientUuid::~GridClientUuid(){
}

std::string const GridClientUuid::uuid() const {
    std::stringstream ss;

    ss << pimpl.uuid_;

    return ss.str();
}

int64_t GridClientUuid::leastSignificantBits() const {
    int64_t lsb = pimpl.uuid_.data[8];

    for (int i = 9 ; i < 16 ; i++) {
        lsb <<= 8 ;
        lsb |= pimpl.uuid_.data[i];
    }

    return lsb;
}

int64_t GridClientUuid::mostSignificantBits() const {
    int64_t msb = pimpl.uuid_.data[0];

    for (int i = 1 ; i < 8 ; i++) {
        msb <<= 8 ;
        msb |= pimpl.uuid_.data[i];
    }

    return msb;
}

int32_t GridClientUuid::hashCode() const {
    int32_t hashCode = (int32_t)((mostSignificantBits() >> 32) ^
            mostSignificantBits() ^ (leastSignificantBits() >> 32) ^ leastSignificantBits());

    return hashCode;
}

void GridClientUuid::convertToBytes(vector<int8_t>& bytes) const {
    back_insert_iterator<vector<int8_t>> inserter=back_inserter(bytes);

    reverse_copy(&(pimpl.uuid_.data[0]), &(pimpl.uuid_.data[pimpl.uuid_.size()/2]), inserter);

    reverse_copy(&(pimpl.uuid_.data[pimpl.uuid_.size()/2]), &(pimpl.uuid_.data[pimpl.uuid_.size()]), inserter);
}

void GridClientUuid::rawBytes(vector<int8_t>& bytes) const {
    bytes.resize(pimpl.uuid_.size());

    copy(pimpl.uuid_.data, pimpl.uuid_.data + pimpl.uuid_.size(), bytes.begin());
}

GridClientUuid GridClientUuid::fromBytes(const string& bytes) {
    GridClientUuid ret;

    ::memcpy(ret.pimpl.uuid_.data, bytes.c_str(), ret.pimpl.uuid_.size());

    return ret;
}

bool GridClientUuid::operator < (const GridClientUuid& other) const {
    if (mostSignificantBits() < other.mostSignificantBits())
        return true;

    if (mostSignificantBits() > other.mostSignificantBits())
        return false;

    return leastSignificantBits() < other.leastSignificantBits();
}

bool GridClientUuid::operator == (const GridClientUuid& other) const {
    return pimpl.uuid_ == other.pimpl.uuid_;
}
