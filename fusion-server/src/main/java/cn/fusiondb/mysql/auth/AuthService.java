/*
 * Copyright 2020 FusionLab, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package cn.fusiondb.mysql.auth;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiliu on 2020/12/15
 */
public class AuthService {
    private static LoadingCache<String, Boolean> userIpCache =
            CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, Boolean>() {
                        @Override
                        public Boolean load(String s) throws Exception {
                            return false;
                        }
                    });

    public static AuthResult checkWhiteList(String user, Set<String> addrs, Set<String> ipWhiteList) {
        for (String addr : addrs) {
            String userAndIp = user + "@" + addr;
            try {
                if (!userIpCache.get(userAndIp)) {
                    boolean addrInWhiteList = false;
                    for (String ip : ipWhiteList) {
                        if (matchIP(addr, ip)) {
                            addrInWhiteList = true;
                            userIpCache.put(userAndIp, true);
                            break;
                        }
                    }
                    if (!addrInWhiteList) {
                        return new AuthResult(AuthStatus.ERROR, "proxy or source address is not in whitelist: " + addr);
                    }
                }
            } catch (Exception e) {
                return new AuthResult(AuthStatus.ERROR, "load cache occurs exceptions");
            }
        }
        return new AuthResult(AuthStatus.OK, null);
    }

    private static boolean matchIP(final String srcIP, final String dstIP) {
        if (srcIP == null || srcIP.length() <= 0 || dstIP == null || dstIP.length() <= 0) {
            return false;
        }
        if (dstIP.equals("*")) {
            return true;
        }
        String[] srcIpSegment = srcIP.split("\\.");
        String[] dstIpSegment = dstIP.split("\\.");
        if (srcIpSegment.length < 4 || dstIpSegment.length < 4) {
            return false;
        }
        if ( (srcIpSegment[0].equals(dstIpSegment[0]) || dstIpSegment[0].equals("*"))
                && (srcIpSegment[1].equals(dstIpSegment[1]) || dstIpSegment[1].equals("*"))
                && (srcIpSegment[2].equals(dstIpSegment[2]) || dstIpSegment[2].equals("*"))
                && (srcIpSegment[3].equals(dstIpSegment[3]) || dstIpSegment[3].equals("*"))) {
            return true;
        }
        return false;
    }
}
