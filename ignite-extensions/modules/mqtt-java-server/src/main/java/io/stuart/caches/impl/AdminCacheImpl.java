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

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.AdminCache;
import io.stuart.consts.CacheConst;
import io.stuart.entities.auth.MqttAdmin;
import io.stuart.utils.AesUtil;
import io.stuart.utils.RowUtil;

public class AdminCacheImpl implements AdminCache {

    private final IgniteCache<String, MqttAdmin> cache;

    public AdminCacheImpl(Ignite ignite, CacheConfiguration<String, MqttAdmin> cfg) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public void init() {
        if (cache == null || cache.size() > 0) {
            return;
        }

        // initialize system administrator
        MqttAdmin admin = new MqttAdmin();
        // set account
        admin.setAccount(CacheConst.SYS_ADMIN_ACCOUNT);
        // set password
        admin.setPassword(AesUtil.encryptBase64(CacheConst.SYS_ADMIN_PASSWORD));

        cache.put(CacheConst.SYS_ADMIN_ACCOUNT, admin);
    }

    @Override
    public int add(MqttAdmin admin) {
        if (admin == null) {
            // error
            return CacheConst.ERROR;
        }

        // get account
        String account = admin.getAccount();
        // get password
        String password = admin.getPassword();

        if (StringUtils.isBlank(account) || StringUtils.isBlank(password)) {
            // error
            return CacheConst.ERROR;
        }

        if (cache.containsKey(account)) {
            // existed
            return CacheConst.EXISTED;
        } else {
            // set password
            admin.setPassword(AesUtil.encryptBase64(password));

            // save mqtt admin
            cache.put(account, admin);

            // succeeded
            return CacheConst.SUCCEEDED;
        }
    }

    @Override
    public int delete(String account) {
        if (StringUtils.isBlank(account) || CacheConst.SYS_ADMIN_ACCOUNT.equals(account)) {
            // error
            return CacheConst.ERROR;
        }

        if (cache.remove(account)) {
            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int update(MqttAdmin admin) {
        if (admin == null) {
            // error
            return CacheConst.ERROR;
        }

        // get account
        String account = admin.getAccount();
        // get old mqtt admin
        MqttAdmin old = cache.get(account);

        if (old != null) {
            // set old password
            admin.setPassword(old.getPassword());
            // set create account
            admin.setCreateAccount(old.getCreateAccount());
            // set create time
            admin.setCreateTime(old.getCreateTime());

            // save mqtt admin
            cache.put(account, admin);

            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int update(MqttAdmin admin, String oldPasswd, String newPasswd) {
        if (admin == null) {
            // error
            return CacheConst.ERROR;
        }

        if (StringUtils.isBlank(oldPasswd) || StringUtils.isBlank(newPasswd) || oldPasswd.equals(newPasswd)) {
            // error
            return CacheConst.ERROR;
        }

        // get account
        String account = admin.getAccount();
        // get old mqtt admin
        MqttAdmin old = get(account);

        if (old != null) {
            if (!AesUtil.encryptBase64(oldPasswd).equals(old.getPassword())) {
                // error
                return CacheConst.ERROR;
            }

            // set new password
            admin.setPassword(AesUtil.encryptBase64(newPasswd));
            // set create account
            admin.setCreateAccount(old.getCreateAccount());
            // set create time
            admin.setCreateTime(old.getCreateTime());

            // save mqtt admin
            cache.put(account, admin);

            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public MqttAdmin get(String account) {
        if (StringUtils.isNotBlank(account)) {
            return cache.get(account);
        } else {
            return null;
        }
    }

    @Override
    public int count(String account) {
        int count = 0;

        SqlFieldsQuery query = new SqlFieldsQuery("select count(*) from MqttAdmin where account like ?");

        String arg = null;

        if (StringUtils.isBlank(account)) {
            arg = "%";
        } else {
            arg = "%" + account + "%";
        }

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query.setArgs(arg))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttAdmin> query(String account, Integer pageNum, Integer pageSize) {
        // result
        List<MqttAdmin> result = new ArrayList<>();
        // mqtt admin
        MqttAdmin admin = null;

        SqlQuery<String, MqttAdmin> query = new SqlQuery<>(MqttAdmin.class, "account like ? order by createTime limit ? offset ?");

        Object[] args = new Object[3];
        Integer ps = null;

        if (StringUtils.isBlank(account)) {
            args[0] = "%";
        } else {
            args[0] = "%" + account + "%";
        }

        if (pageSize == null || pageSize <= 0) {
            ps = CacheConst.DEF_PAGE_SIZE;
        } else {
            ps = pageSize;
        }

        args[1] = ps;

        if (pageNum == null || pageNum <= 0) {
            args[2] = (CacheConst.DEF_PAGE_NUM - 1) * ps;
        } else {
            args[2] = (pageNum - 1) * ps;
        }

        try (QueryCursor<Entry<String, MqttAdmin>> cursor = cache.query(query.setArgs(args))) {
            for (Entry<String, MqttAdmin> entry : cursor) {
                // get mqtt admin
                admin = entry.getValue();
                // set password
                admin.setPassword(null);

                // add to result list
                result.add(admin);
            }
        }

        return result;
    }

}
