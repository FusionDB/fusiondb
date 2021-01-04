/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package cn.fusiondb.mysql;

import cn.fusiondb.mysql.audit.AuditLog;
import cn.fusiondb.mysql.audit.AuditService;
import cn.fusiondb.mysql.auth.AuthResult;
import cn.fusiondb.mysql.auth.AuthService;
import cn.fusiondb.mysql.auth.AuthStatus;
import com.facebook.airlift.log.Logger;
import com.google.api.client.util.Strings;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// MySQL protocol util
public class MysqlProto {
    private static final Logger logger = Logger.get(MysqlProto.class);
    private static final AuditLog auditLog = AuditService.getAuditLog();
    private static final Map<String, String> userList = new ConcurrentHashMap();
    private static final Set<String> hostIpWhiteList = new HashSet<>();
    private ImmutableSet<String> hostnameWhiteList; // TODO

    private static void init(byte[] randomString) {
        hostIpWhiteList.add("*");
        hostIpWhiteList.add("fusion-server-0");
        hostIpWhiteList.add("localhost");
        hostIpWhiteList.add("192.168.0.100");
        userList.put("root", new String(MysqlPassword.makeScrambledPassword("root123")));
        userList.put("admin", new String(MysqlPassword.makeScrambledPassword("admin123")));
    }

    // scramble: data receive from server.
    // randomString: data send by server in plugin data field
    private static boolean authenticate(ConnectContext context, byte[] scramble, byte[] randomString, String user) {
        // TODO Auth Grant
        init(randomString);
        // parser catalog name by user
        String[] split = user.split("@");
        if (split.length == 2) {
            context.setCatalog(split[1]);
            user = split[0];
        } else {
            logger.warn("Current login user = %s, not set catalog", user);
        }

        String usePass = scramble.length == 0 ? "NO" : "YES";

        if (user == null || user.isEmpty()) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, "", usePass);
            logger.warn(" user == null");
            return false;
        }

        String userPassword = userList.get(user);
        if (userPassword == null) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, user, usePass);
            return false;
        }

        byte[] mysqlPass = MysqlPassword.getSaltFromPassword(userPassword.getBytes());

        // when the length of password is zero, the user has no password
        if ((scramble.length == mysqlPass.length)
                && (scramble.length == 0 || MysqlPassword.checkScramble(scramble, randomString, mysqlPass))) {
            // authenticate success
            context.setUser(user);
        } else {
            // password check failed.
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, user, usePass);
            return false;
        }

        String sourceAddrs = context.getMysqlChannel().getRemote().split(":")[0];
        long startTimestamp = System.currentTimeMillis();
        if (!getUsernameWithoutTenantFromFullUsername(user).equalsIgnoreCase("root")) {
            Set<String> addrs = new HashSet<>();
            addrs.add(sourceAddrs);

            AuthResult whiteListResult = AuthService.checkWhiteList(user, addrs, hostIpWhiteList);
            if (whiteListResult.getStatus() != AuthStatus.OK) {
                // whitelist check failed.
                ErrorReport.report(ErrorCode.ERR_IP_ACCESS_DENIED, sourceAddrs, usePass);
                String message = String.format("Login denied, ip not in whitelist: %s", hostIpWhiteList.toString());
                auditLog.log(startTimestamp, user, sourceAddrs, "login", 0, message);
                return false;
            }
        }

        auditLog.log(startTimestamp, user, sourceAddrs, "login", 0, "mysql authenticate successfully");
        return true;
    }

    private static String getUsernameWithoutTenantFromFullUsername(String user) {
        // TODO root user
        return user;
    }

    // send response packet(OK/EOF/ERR).
    // before call this function, should set information in state of ConnectContext
    public static void sendResponsePacket(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        MysqlPacket packet = context.getState().toResponsePacket();

        // send response packet to client
        serializer.reset();
        packet.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());
    }

    /**
     * negotiate with client, use MySQL protocol
     * server ---handshake---> client
     * server <--- authenticate --- client
     * server --- response(OK/ERR) ---> client
     * Exception:
     * IOException:
     */
    public static boolean negotiate(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        context.getState().setOk();

        // Server send handshake packet to client.
        serializer.reset();
        MysqlHandshakePacket handshakePacket = new MysqlHandshakePacket(context.getConnectionId());
        handshakePacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        // Server receive authenticate packet from client.
        ByteBuffer handshakeResponse = channel.fetchOnePacket();
        if (handshakeResponse == null) {
            // receive response failed.
            logger.warn(" receive handshake response failed");
            return false;
        }

        MysqlAuthPacket authPacket = new MysqlAuthPacket();
        if (!authPacket.readFrom(handshakeResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            logger.warn(" err not suppored auth mode");
            return false;
        }

        // check capability
        if (!MysqlCapability.isCompatible(context.getServerCapability(), authPacket.getCapability())) {
            // TODO: client return capability can not support
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            logger.warn(" err not suppored auth mode");
            return false;
        }
        // change the capability of serializer
        context.setCapability(context.getServerCapability());
        serializer.setCapability(context.getCapability());

        // NOTE: when we behind proxy, we need random string sent by proxy.
        byte[] randomString = handshakePacket.getAuthPluginData();

        // check authenticate
        if (!authenticate(context, authPacket.getAuthResponse(), randomString, authPacket.getUser())) {
            sendResponsePacket(context);
            logger.warn("auth false, user = %s catalog = %s database = %s", authPacket.getUser(), context.getCatalog(), authPacket.getDb());
            return false;
        }

        // set database
        String db = authPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            try {
                context.setDatabase(db);
            } catch (Exception e) {
                sendResponsePacket(context);
                return false;
            }
        }
        return true;
    }

    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    public static int readInt1(ByteBuffer buffer) {
        return readByte(buffer) & 0XFF;
    }

    public static int readInt2(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8);
    }

    public static int readInt3(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16);
    }

    public static int readInt4(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16) | ((readByte(buffer) & 0XFF) << 24);
    }

    public static long readInt6(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt2(buffer)) << 32);
    }

    public static long readInt8(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt4(buffer)) << 32);
    }

    public static long readVInt(ByteBuffer buffer) {
        int b = readInt1(buffer);

        if (b < 251) {
            return b;
        }
        if (b == 252) {
            return readInt2(buffer);
        }
        if (b == 253) {
            return readInt3(buffer);
        }
        if (b == 254) {
            return readInt8(buffer);
        }
        if (b == 251) {
            throw new NullPointerException();
        }
        return 0;
    }

    public static byte[] readFixedString(ByteBuffer buffer, int len) {
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readEofString(ByteBuffer buffer) {
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readLenEncodedString(ByteBuffer buffer) {
        long length = readVInt(buffer);
        byte[] buf = new byte[(int) length];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readNulTerminateString(ByteBuffer buffer) {
        int oldPos = buffer.position();
        int nullPos = oldPos;
        for (nullPos = oldPos; nullPos < buffer.limit(); ++nullPos) {
            if (buffer.get(nullPos) == 0) {
                break;
            }
        }
        byte[] buf = new byte[nullPos - oldPos];
        buffer.get(buf);
        // skip null byte.
        buffer.get();
        return buf;
    }

}
