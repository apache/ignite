/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.caches.impl;

import java.util.ArrayList;
import java.util.List;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import io.stuart.caches.AclCache;
import io.stuart.consts.AclConst;
import io.stuart.consts.CacheConst;
import io.stuart.entities.auth.MqttAcl;
import io.stuart.enums.Access;
import io.stuart.enums.Authority;
import io.stuart.enums.Target;
import io.stuart.utils.RowUtil;

public class AclCacheImpl implements AclCache {

    private static final String sql;

    private final Ignite ignite;

    private final IgniteCache<Long, MqttAcl> cache;

    private final IgniteAtomicSequence sequence;

    static {
        StringBuilder sb = new StringBuilder();

        sb.append(" select topic, access, authority from MqttAcl where ");
        sb.append(" (target = ? and type = ?) or (target = ? and type = ?) or ");
        sb.append(" (target = ? and type = ?) or (target = ? and type = ?) ");
        sb.append(" order by seq desc ");

        sql = sb.toString();
    }

    public AclCacheImpl(Ignite ignite, CacheConfiguration<Long, MqttAcl> cfg) {
        // set ignite instance
        this.ignite = ignite;

        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);

        // get or create sequence
        this.sequence = ignite.atomicSequence(CacheConst.ACL_SEQ, 0, true);
    }

    @Override
    public void init() {
        if (cache == null || cache.size() > 0 || sequence.get() > 0) {
            return;
        }

        // mqtt acl
        MqttAcl acl = new MqttAcl();

        // get seq
        Long seq = sequence.incrementAndGet();

        // set all # sub/pub allow
        acl.setSeq(seq);
        acl.setTarget(AclConst.ALL);
        acl.setType(Target.All.value());
        acl.setTopic(AclConst.POUND);
        acl.setAccess(Access.SubPub.value());
        acl.setAuthority(Authority.Allow.value());

        cache.put(seq, acl);

        // get seq
        seq = sequence.incrementAndGet();

        // set all eq(#) sub deny
        acl.setSeq(seq);
        acl.setTarget(AclConst.ALL);
        acl.setType(Target.All.value());
        acl.setTopic(AclConst.EQ_POUND);
        acl.setAccess(Access.Sub.value());
        acl.setAuthority(Authority.Deny.value());

        cache.put(seq, acl);

        // get seq
        seq = sequence.incrementAndGet();

        // set all $SYS/# sub deny
        acl.setSeq(seq);
        acl.setTarget(AclConst.ALL);
        acl.setType(Target.All.value());
        acl.setTopic(AclConst.SYS_SLASH_POUND);
        acl.setAccess(Access.Sub.value());
        acl.setAuthority(Authority.Deny.value());

        cache.put(seq, acl);

        // get seq
        seq = sequence.incrementAndGet();

        // set 127.0.0.1 # sub/pub allow
        acl.setSeq(seq);
        acl.setTarget(AclConst.LOCAL_ADDRESS);
        acl.setType(Target.IpAddr.value());
        acl.setTopic(AclConst.POUND);
        acl.setAccess(Access.SubPub.value());
        acl.setAuthority(Authority.Allow.value());

        cache.put(seq, acl);

        // get seq
        seq = sequence.incrementAndGet();

        // set 127.0.0.1 $SYS/# sub/pub allow
        acl.setSeq(seq);
        acl.setTarget(AclConst.LOCAL_ADDRESS);
        acl.setType(Target.IpAddr.value());
        acl.setTopic(AclConst.SYS_SLASH_POUND);
        acl.setAccess(Access.SubPub.value());
        acl.setAuthority(Authority.Allow.value());

        cache.put(seq, acl);
    }

    @Override
    public long add(MqttAcl acl) {
        if (acl == null) {
            // error
            return CacheConst.ERROR;
        }

        // get seq
        long seq = sequence.incrementAndGet();

        // set seq
        acl.setSeq(seq);

        // save mqtt acl
        cache.put(seq, acl);

        // seq
        return seq;
    }

    @Override
    public int delete(Long seq) {
        if (seq != null && cache.remove(seq)) {
            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int update(MqttAcl acl) {
        if (acl == null) {
            // error
            return CacheConst.ERROR;
        }

        // get seq
        Long seq = acl.getSeq();
        // get old mqtt acl
        MqttAcl old = cache.get(seq);

        if (old != null) {
            // set create account
            acl.setCreateAccount(old.getCreateAccount());
            // set create time
            acl.setCreateTime(old.getCreateTime());

            // save mqtt acl
            cache.put(seq, acl);

            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int reorder(List<MqttAcl> acls) {
        if (acls == null || acls.isEmpty()) {
            // has no data
            return CacheConst.NONE;
        }

        IgniteTransactions transactions = ignite.transactions();

        try (Transaction tx = transactions.txStart()) {
            acls.forEach(val -> {
                cache.put(val.getSeq(), val);
            });

            tx.commit();
        }

        // data size
        return acls.size();
    }

    @Override
    public List<MqttAcl> get(String username, String ipAddr, String clientId) {
        // result
        List<MqttAcl> result = new ArrayList<>();
        // mqtt acl
        MqttAcl acl = null;

        Object[] args = new Object[8];
        args[0] = username;
        args[1] = Target.Username.value();
        args[2] = ipAddr;
        args[3] = Target.IpAddr.value();
        args[4] = clientId;
        args[5] = Target.ClientId.value();
        args[6] = AclConst.ALL;
        args[7] = Target.All.value();

        SqlFieldsQuery query = new SqlFieldsQuery(sql);

        try (QueryCursor<List<?>> cursor = cache.query(query.setArgs(args))) {
            for (List<?> row : cursor) {
                acl = new MqttAcl();

                acl.setTopic(RowUtil.getStr(row.get(0)));
                acl.setAccess(RowUtil.getInt(row.get(1)));
                acl.setAuthority(RowUtil.getInt(row.get(2)));

                result.add(acl);
            }
        }

        return result;
    }

    @Override
    public List<Object[]> query() {
        // result
        List<Object[]> result = new ArrayList<>();
        // object array
        Object[] obj = null;
        // mqtt acl
        MqttAcl acl = null;

        SqlQuery<Long, MqttAcl> query = new SqlQuery<>(MqttAcl.class, "order by seq desc");

        try (QueryCursor<Entry<Long, MqttAcl>> cursor = cache.query(query)) {
            for (Entry<Long, MqttAcl> entry : cursor) {
                // initialize object array
                obj = new Object[7];
                // get mqtt acl
                acl = entry.getValue();

                obj[0] = acl.getSeq();
                obj[1] = acl.getTarget();
                obj[2] = acl.getType();
                obj[3] = acl.getTopic();
                obj[4] = acl.getAccess();
                obj[5] = acl.getAuthority();
                obj[6] = acl.getSeq();

                result.add(obj);
            }
        }

        return result;
    }

}
