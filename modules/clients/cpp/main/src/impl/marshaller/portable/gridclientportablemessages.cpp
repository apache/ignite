/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/marshaller/portable/gridclientportablemessages.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

void GridClientCacheRequest::writePortable(GridPortableWriter& writer) const {
    assert(cacheName != 0);

    GridClientPortableMessage::writePortable(writer);

    GridPortableWriterImpl& raw = (GridPortableWriterImpl&)writer.rawWriter();

    raw.writeInt32(op);

    raw.writeString(*cacheName);

    raw.writeInt32(cacheFlagsOn);

    raw.writeVariantEx(key ? *key : nullVariant);
    raw.writeVariantEx(val ? *val : nullVariant);
    raw.writeVariantEx(val2 ? *val2 : nullVariant);

    if (vals) {
        raw.writeInt32(vals->size());

        for (auto iter = vals->begin(); iter != vals->end(); ++iter) {
            const GridClientVariant& key = iter->first;
            const GridClientVariant& val = iter->second;

            raw.writeVariantEx(key);
            raw.writeVariantEx(val);
        }
    }
    else 
        raw.writeInt32(0);
}

