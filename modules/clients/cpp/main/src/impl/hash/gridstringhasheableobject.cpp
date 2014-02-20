// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"

// Public constructor.
GridStringHasheableObject::GridStringHasheableObject(const std::string& s) {
    str = s;
}

// Calculate the hash code by the demand.
int32_t GridStringHasheableObject::hashCode() const {
    int len = str.length();
    int32_t hash = 0;
    const char* val = str.c_str();

    for (int i = 0; i < len; i++)
        hash = 31 * hash + val[i];

    return hash;
}

void GridStringHasheableObject::convertToBytes(std::vector<int8_t>& bytes) const {
    bytes.clear();

    for (size_t i = 0; i< str.length(); i++) {
        int8_t symb = (int8_t&) str[i];

        bytes.push_back(symb);
    }
}
