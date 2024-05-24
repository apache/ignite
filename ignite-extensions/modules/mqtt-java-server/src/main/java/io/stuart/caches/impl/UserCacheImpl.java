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
import io.stuart.caches.UserCache;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.entities.auth.MqttAdmin;
import io.stuart.entities.auth.MqttUser;
import io.stuart.utils.AesUtil;
import io.stuart.utils.RowUtil;

public class UserCacheImpl implements UserCache {

    private final IgniteCache<String, MqttUser> cache;

    private final AdminCache adminCache;

    public UserCacheImpl(Ignite ignite, CacheConfiguration<String, MqttUser> cfg, AdminCache adminCache) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);

        // set admin cache
        this.adminCache = adminCache;
    }

    @Override
    public int add(MqttUser user) {
        if (user == null) {
            // error
            return CacheConst.ERROR;
        }

        // get username
        String username = user.getUsername();
        // get password
        String password = user.getPassword();

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            // error
            return CacheConst.ERROR;
        }

        if (cache.containsKey(username)) {
            // existed
            return CacheConst.EXISTED;
        } else {
            // set new password
            user.setPassword(Config.getAes().encryptBase64(password));

            // save mqtt user
            cache.put(username, user);

            // succeeded
            return CacheConst.SUCCEEDED;
        }
    }

    @Override
    public int delete(String username) {
        if (StringUtils.isBlank(username)) {
            // error
            return CacheConst.ERROR;
        }

        if (cache.remove(username)) {
            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int update(MqttUser user) {
        if (user == null) {
            // error
            return CacheConst.ERROR;
        }

        // get username
        String username = user.getUsername();
        // get old mqtt user
        MqttUser old = cache.get(username);

        if (old != null) {
            // set old password
            user.setPassword(old.getPassword());
            // set create account
            user.setCreateAccount(old.getCreateAccount());
            // set create time
            user.setCreateTime(old.getCreateTime());

            // save mqtt user
            cache.put(username, user);

            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public int update(MqttUser user, String adminAccount, String adminPasswd) {
        if (user == null) {
            // error
            return CacheConst.ERROR;
        }

        // get mqtt admin
        MqttAdmin admin = adminCache.get(adminAccount);

        if (admin == null || StringUtils.isBlank(adminPasswd) || !AesUtil.encryptBase64(adminPasswd).equals(admin.getPassword())) {
            // error
            return CacheConst.ERROR;
        }

        // get username
        String username = user.getUsername();
        // get old mqtt user
        MqttUser old = cache.get(username);

        if (old != null) {
            // set new password
            user.setPassword(Config.getAes().encryptBase64(user.getPassword()));
            // set create account
            user.setCreateAccount(old.getCreateAccount());
            // set create time
            user.setCreateTime(old.getCreateTime());

            // save mqtt user
            cache.put(username, user);

            // succeeded
            return CacheConst.SUCCEEDED;
        } else {
            // not existed
            return CacheConst.NONE;
        }
    }

    @Override
    public MqttUser get(String username) {
        if (StringUtils.isNotBlank(username)) {
            return cache.get(username);
        } else {
            return null;
        }
    }

    @Override
    public int count(String username) {
        int count = 0;

        SqlFieldsQuery query = new SqlFieldsQuery("select count(*) from MqttUser where username like ?");

        String arg = null;

        if (StringUtils.isBlank(username)) {
            arg = "%";
        } else {
            arg = "%" + username + "%";
        }

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query.setArgs(arg))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttUser> query(String username, Integer pageNum, Integer pageSize) {
        // result
        List<MqttUser> result = new ArrayList<>();
        // mqtt user
        MqttUser user = null;

        SqlQuery<String, MqttUser> query = new SqlQuery<>(MqttUser.class, "username like ? order by createTime limit ? offset ?");

        Object[] args = new Object[3];
        Integer ps = null;

        if (StringUtils.isBlank(username)) {
            args[0] = "%";
        } else {
            args[0] = "%" + username + "%";
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

        try (QueryCursor<Entry<String, MqttUser>> cursor = cache.query(query.setArgs(args))) {
            for (Entry<String, MqttUser> entry : cursor) {
                // get mqtt user
                user = entry.getValue();
                // set password
                user.setPassword(null);

                // add to result list
                result.add(user);
            }
        }

        return result;
    }

}
