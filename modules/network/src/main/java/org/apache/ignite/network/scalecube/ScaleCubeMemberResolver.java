/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.network.scalecube;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.scalecube.cluster.Member;
import org.apache.ignite.network.NetworkMember;

import static java.util.Objects.requireNonNull;

/**
 * Resolver for scalecube specific member.
 */
public class ScaleCubeMemberResolver {
    /** Map of public network member by its unique name. */
    private final Map<String, NetworkMember> directMemberMap = new ConcurrentHashMap<>();

    /** Map of scalecube member by its public member. */
    private final Map<NetworkMember, Member> reverseMemberMap = new ConcurrentHashMap<>();

    /**
     * Getting the existed member by scalecube member or create new one.
     *
     * @param member ScaleCube specific member.
     * @return Public network member instance.
     */
    public NetworkMember resolveNetworkMember(Member member) {
        String alias = member.alias();

        NetworkMember networkMember = directMemberMap.get(alias);

        if (networkMember != null)
            return networkMember;

        networkMember = directMemberMap.computeIfAbsent(alias, NetworkMember::new);

        reverseMemberMap.put(networkMember, member);

        return networkMember;
    }

    /**
     * @param member Public network member.
     * @return ScaleCube specific member.
     */
    public Member resolveMember(NetworkMember member) {
        return requireNonNull(reverseMemberMap.get(member));
    }
}
