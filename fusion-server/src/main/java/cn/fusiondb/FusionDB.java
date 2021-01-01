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
package cn.fusiondb;

import cn.fusiondb.mysql.ConnectScheduler;
import cn.fusiondb.mysql.MysqlServer;
import com.facebook.airlift.configuration.ConfigurationLoader;
import com.facebook.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Created by xiliu on 2020/9/24
 */
public class FusionDB {
    private static final Logger logger = Logger.get(MysqlServer.class);
    private static Map<String, String> config = null;

    public static void main(String[] args) {
        String configFile = System.getProperty("config");
        if (configFile != null) {
            try {
                config = ConfigurationLoader.loadPropertiesFrom(configFile);
            } catch (IOException var10) {
                throw new UncheckedIOException(var10);
            }
        }
        System.setProperty("fdb.foreground", "yes");
        System.setProperty("http-server.http.port", config.get("http-server.http.port"));
        String[] startArgs = new String[args.length + 1];
        startArgs[0] = "start";
        System.arraycopy(args, 0, startArgs, 1, args.length);

        // start mysql server
        // int maxConectNum = config.getAsInt("mysql.max_connections", 1024);
        // int mysqlPort = config.getAsInt("mysql.port", 8306);
        int maxConectNum = 1024;
        int mysqlPort = 8306;
        ConnectScheduler scheduler = new ConnectScheduler(maxConectNum);
        MysqlServer mysqlServer = new MysqlServer(mysqlPort, scheduler);
        if (!mysqlServer.start()) {
            logger.error("mysql server start failed");
            System.exit(-1);
        } else {
            logger.info("mysql server start successful, max_connections: "
                    + maxConectNum + " port: " + mysqlPort);
        }
    }

    public void start() {
        String configFile = System.getProperty("config");
        if (configFile != null) {
            try {
                config = ConfigurationLoader.loadPropertiesFrom(configFile);
            } catch (IOException var10) {
                throw new UncheckedIOException(var10);
            }
        }
        System.setProperty("fdb.foreground", "yes");
        System.setProperty("http-server.http.port", config.get("http-server.http.port"));
        int maxConectNum = 1024;
        int mysqlPort = 8306;
        ConnectScheduler scheduler = new ConnectScheduler(maxConectNum);
        MysqlServer mysqlServer = new MysqlServer(mysqlPort, scheduler);
        if (!mysqlServer.start()) {
            logger.error("mysql server start failed");
            System.exit(-1);
        } else {
            logger.info("mysql server start successful, max_connections: "
                    + maxConectNum + " port: " + mysqlPort);
        }
    }
}