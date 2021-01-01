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

import com.google.common.collect.Lists;

import java.nio.channels.SocketChannel;
import java.util.List;

// When one client connect in, we create a connect context for it.
// We store session information here. Meanwhile ConnectScheduler all
// connect with its connection id.
// Use `volatile` to make the reference change atomic.
public class ConnectContext {
    private static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<ConnectContext>();
    //  del
    // private volatile TUniqueId queryId;
    // id for this connection
    private volatile int connectionId;
    // mysql net
    private volatile MysqlChannel mysqlChannel;
    // state
    private volatile QueryState state;
    // the protocol capability which server say it can support
    private volatile MysqlCapability serverCapability;
    // the protocol capability after server and client negotiate
    private volatile MysqlCapability capability;
    // Indicate if this client is killed.
    private volatile boolean isKilled;
    // Db
    private volatile String currentDb = "";
    // catalog
    private volatile String currentCatalog = "";
    // User
    private volatile String user;
    // Serializer used to pack MySQL packet.
    private volatile MysqlSerializer serializer;
    // Variables belong to this session.
    //  del
    // private volatile SessionVariable sessionVariable;
    // Scheduler this connection belongs to
    private volatile ConnectScheduler connectScheduler;
    // Executor
    private volatile StmtExecutor executor;
    // Command this connection is processing.
    private volatile MysqlCommand command;
    // Timestamp in millisecond last command starts at
    private volatile long startTime;
    // Cache thread info for this connection.
    private volatile ThreadInfo threadInfo;

    // Catalog: put catalog here is convenient for unit test,
    // because catalog is singleton, hard to mock
    // private Catalog catalog;
    private boolean isSend;

    // coordinator node ip:port
    private String server;

    // private AuditBuilder auditBuilder;

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public void setIsSend(boolean isSend) {
        this.isSend = isSend;
    }

    public boolean isSend() {
        return this.isSend;
    }

    public ConnectContext(SocketChannel channel) {
        state = new QueryState();
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        mysqlChannel = new MysqlChannel(channel);
        serializer = MysqlSerializer.newInstance();
        command = MysqlCommand.COM_SLEEP;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public QueryState getState() {
        return state;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) {
        currentDb = db;
    }

    public String getCatalog() {
        return currentCatalog;
    }

    public void setCatalog(String currentCatalog) {
        this.currentCatalog = currentCatalog;
    }

    public void setExecutor(StmtExecutor executor) {
        this.executor = executor;
    }

    public void cleanup() {
        mysqlChannel.close();
    }

    public boolean isKilled() {
        return isKilled;
    }

    // Set kill flag to true;
    public void setKilled() {
        isKilled = true;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        if (isKilled) {
            return;
        }
        if (killConnection) {
            isKilled = true;
            // Close channel to break connection with client
            mysqlChannel.close();
        }
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel();
        }
    }

//    public void checkTimeout(long now) {
//        if (startTime <= 0) {
//            return;
//        }
//
//        long delta = now - startTime;
//        boolean killFlag = false;
//        boolean killConnection = false;
//        if (command == MysqlCommand.COM_SLEEP) {
//            if (delta > sessionVariable.getWaitTimeoutS() * 1000) {
//                // Need kill this connection.
//                killFlag = true;
//                killConnection = true;
//            }
//        } else {
//            if (delta > sessionVariable.getQueryTimeoutS() * 1000) {
//                // Only kill
//                killFlag = true;
//            }
//        }
//        if (killFlag) {
//            kill(killConnection);
//        }
//    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo() {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        return threadInfo;
    }

    public class ThreadInfo {
        public List<String>  toRow(long nowMs) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(user);
            row.add(mysqlChannel.getRemote());
            row.add(currentDb);
            row.add(command.toString());
            row.add("" + (nowMs - startTime) / 1000);
            row.add("");
            row.add("");
            return row;
        }
    }
}
