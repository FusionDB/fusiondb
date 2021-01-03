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

package cn.fusiondb.client;

import cn.fusiondb.mysql.ConnectContext;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.log.LoggingConfiguration;
import com.facebook.presto.cli.QueryPreprocessorException;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static cn.fusiondb.client.ClientSession.stripTransactionId;
import static cn.fusiondb.client.QueryPreprocessor.preprocessQuery;
import static com.facebook.presto.sql.parser.StatementSplitter.isEmptyStatement;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.*;

/**
 * Created by xiliu on 2020/11/24
 */
public class QueryExecutor {
    private static final Logger log = Logger.get(QueryExecutor.class);
    private static final Duration EXIT_DELAY = new Duration(3, SECONDS);
    private static ConnectContext context;
    private static LoadingCache<String, String> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, MINUTES)
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(final String key) {
                    return getCoordinatorHostAndPort();
                }
            });

    @Inject
    public ClientConfig clientConfig = new ClientConfig();

    public boolean run(ConnectContext context, String query) throws ExecutionException {
        String server = requireNonNull(cache.get("server"), "coordinator server is null");
        ClientSession session = clientConfig.toClientSession(server, context.getUser(), context.getCatalog(), context.getDatabase());
        this.context = context;
        boolean hasQuery = true;

        initializeLogging(clientConfig.logLevelsFile);

        if (hasQuery) {
            query += ";";
        }

        // abort any running query if the CLI is terminated
        AtomicBoolean exiting = new AtomicBoolean();
        ThreadInterruptor interruptor = new ThreadInterruptor();
        CountDownLatch exited = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            exiting.set(true);
            interruptor.interrupt();
            awaitUninterruptibly(exited, EXIT_DELAY.toMillis(), MILLISECONDS);
        }));

        try (QueryRunner queryRunner = new QueryRunner(
                session,
                clientConfig.debug,
                Optional.ofNullable(clientConfig.socksProxy),
                Optional.ofNullable(clientConfig.httpProxy),
                Optional.ofNullable(clientConfig.keystorePath),
                Optional.ofNullable(clientConfig.keystorePassword),
                Optional.ofNullable(clientConfig.truststorePath),
                Optional.ofNullable(clientConfig.truststorePassword),
                Optional.ofNullable(clientConfig.accessToken),
                Optional.ofNullable(context.getUser()),
                clientConfig.password ? Optional.of(getPassword()) : Optional.empty(),
                Optional.ofNullable(clientConfig.krb5Principal),
                Optional.ofNullable(clientConfig.krb5RemoteServiceName),
                Optional.ofNullable(clientConfig.krb5ConfigPath),
                Optional.ofNullable(clientConfig.krb5KeytabPath),
                Optional.ofNullable(clientConfig.krb5CredentialCachePath),
                !clientConfig.krb5DisableRemoteServiceHostnameCanonicalization)) {
            return executeCommand(queryRunner, query, clientConfig.outputFormat, clientConfig.ignoreErrors);
        } finally {
            exited.countDown();
            interruptor.close();
        }
    }

    private static String getCoordinatorHostAndPort() {
        OkHttpClient client = new OkHttpClient();
        String httpUrl = String.format("http://%s:%s%s", "localhost", System.getProperty("http-server.http.port"), "/v1/info/coordinator");
        // rest request /v1/info/coordinator
        ObjectMapper mapper = new ObjectMapper();
        Request request = new Request.Builder()
                .url(httpUrl)
                .build();
        String server = null;
        try (Response response = client.newCall(request).execute()) {
            JsonNode coordinatorInfo = mapper.readTree(response.body().string());
            if (coordinatorInfo.get("internalNodes").size() > 0) {
                server = coordinatorInfo.get("internalNodes").get(0).get("hostAndPort").asText();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return server;
    }

    private String getPassword() {
        // TODO user by context
        checkState(clientConfig.user != null, "Username must be specified along with password");
        String defaultPassword = System.getenv("PRESTO_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        java.io.Console console = System.console();
        if (console == null) {
            throw new RuntimeException("No console from which to read password");
        }
        char[] password = console.readPassword("Password: ");
        if (password != null) {
            return new String(password);
        }
        return "";
    }

    private static boolean executeCommand(QueryRunner queryRunner, String query, ClientConfig.OutputFormat outputFormat, boolean ignoreErrors) {
        boolean success = true;
        StatementSplitter splitter = new StatementSplitter(query);
        for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
            if (!isEmptyStatement(split.statement())) {
                if (!process(queryRunner, split.statement(), outputFormat, () -> {
                }, false)) {
                    if (!ignoreErrors) {
                        return false;
                    }
                    success = false;
                }
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
            return false;
        }
        return success;
    }

    private static boolean process(QueryRunner queryRunner, String sql, ClientConfig.OutputFormat outputFormat, Runnable schemaChanged, boolean interactive) {
        String finalSql;
        try {
            finalSql = preprocessQuery(
                    Optional.ofNullable(queryRunner.getSession().getCatalog()),
                    Optional.ofNullable(queryRunner.getSession().getSchema()),
                    sql);
        } catch (QueryPreprocessorException e) {
            System.err.println(e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace();
            }
            return false;
        }

        try (Query query = queryRunner.startQuery(finalSql)) {
            boolean success = query.renderOutput(context, System.out, outputFormat, interactive);

            ClientSession session = queryRunner.getSession();

            // update catalog and schema if present
            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                session = ClientSession.builder(session)
                        .withCatalog(query.getSetCatalog().orElse(session.getCatalog()))
                        .withSchema(query.getSetSchema().orElse(session.getSchema()))
                        .build();
                schemaChanged.run();
            }

            // update transaction ID if necessary
            if (query.isClearTransactionId()) {
                session = stripTransactionId(session);
            }

            ClientSession.Builder builder = ClientSession.builder(session);

            if (query.getStartedTransactionId() != null) {
                builder = builder.withTransactionId(query.getStartedTransactionId());
            }

            // update session properties if present
            if (!query.getSetSessionProperties().isEmpty() || !query.getResetSessionProperties().isEmpty()) {
                Map<String, String> sessionProperties = new HashMap<>(session.getProperties());
                sessionProperties.putAll(query.getSetSessionProperties());
                sessionProperties.keySet().removeAll(query.getResetSessionProperties());
                builder = builder.withProperties(sessionProperties);
            }

            // update session roles
            if (!query.getSetRoles().isEmpty()) {
                Map<String, SelectedRole> roles = new HashMap<>(session.getRoles());
                roles.putAll(query.getSetRoles());
                builder = builder.withRoles(roles);
            }

            // update prepared statements if present
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.withPreparedStatements(preparedStatements);
            }

            session = builder.build();
            queryRunner.setSession(session);

            return success;
        } catch (RuntimeException e) {
            if (queryRunner.isDebug()) {
                e.printStackTrace();
            }
            throw new RuntimeException("Error running command: " + e.getMessage());
//            return false;
        }
    }

    private static void initializeLogging(String logLevelsFile) {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;

        try {
            LoggingConfiguration config = new LoggingConfiguration();

            if (logLevelsFile == null) {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                config.setConsoleEnabled(false);
            } else {
                config.setLevelsFile(logLevelsFile);
            }

            Logging logging = Logging.initialize();
            logging.configure(config);
        } finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
