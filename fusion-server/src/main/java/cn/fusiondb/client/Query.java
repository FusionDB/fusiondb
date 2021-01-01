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
import cn.fusiondb.mysql.MysqlServer;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cli.*;
import com.facebook.presto.client.*;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.security.SelectedRole;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.fusesource.jansi.Ansi;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Created by xiliu on 2020/12/9
 */
public class Query implements Closeable {
    private static final Logger logger = Logger.get(Query.class);
    private static final Signal SIGINT = new Signal("INT");

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;
    private final boolean debug;
    private ConnectContext context;

    public Query(StatementClient client, boolean debug)
    {
        this.client = requireNonNull(client, "client is null");
        this.debug = debug;
    }

    public Optional<String> getSetCatalog()
    {
        return client.getSetCatalog();
    }

    public Optional<String> getSetSchema()
    {
        return client.getSetSchema();
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    public Map<String, SelectedRole> getSetRoles()
    {
        return client.getSetRoles();
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return client.getAddedPreparedStatements();
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return client.getDeallocatedPreparedStatements();
    }

    public String getStartedTransactionId()
    {
        return client.getStartedTransactionId();
    }

    public boolean isClearTransactionId()
    {
        return client.isClearTransactionId();
    }

    public boolean renderOutput(ConnectContext context, PrintStream out, ClientConfig.OutputFormat outputFormat, boolean interactive)
    {
        this.context = context;
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, signal -> {
            if (ignoreUserInterrupt.get() || client.isClientAborted()) {
                return;
            }
            client.close();
            clientThread.interrupt();
        });
        try {
            return renderQueryOutput(out, outputFormat, interactive);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private boolean renderQueryOutput(PrintStream out, ClientConfig.OutputFormat outputFormat, boolean interactive)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = interactive ? out : System.err;
        WarningsPrinter warningsPrinter = new Query.PrintStreamWarningsPrinter(System.err);

        if (interactive) {
            statusPrinter = new StatusPrinter(client, out, debug);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            processInitialStatusUpdates(warningsPrinter);
        }

        // if running or finished
        if (client.isRunning() || (client.isFinished() && client.finalStatusInfo().getError() == null)) {
            QueryStatusInfo results = client.isRunning() ? client.currentStatusInfo() : client.finalStatusInfo();
            if (results.getUpdateType() != null) {
                renderUpdate(errorChannel, results);
            }
            else if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return false;
            }
            else {
                renderResults(outputFormat, interactive, results.getColumns());
            }
        }

        checkState(!client.isRunning());

        if (statusPrinter != null) {
            // Print all warnings at the end of the query
            new Query.PrintStreamWarningsPrinter(System.err).print(client.finalStatusInfo().getWarnings(), true, true);
            statusPrinter.printFinalInfo();
        }
        else {
            // Print remaining warnings separated
            warningsPrinter.print(client.finalStatusInfo().getWarnings(), true, true);
        }

        if (client.isClientAborted()) {
            errorChannel.println("Query aborted by user");
            return false;
        }
        if (client.isClientError()) {
            errorChannel.println("Query is gone (server restarted?)");
            return false;
        }

        verify(client.isFinished());
        if (client.finalStatusInfo().getError() != null) {
            renderFailure();
            return false;
        }

        return true;
    }

    private void processInitialStatusUpdates(WarningsPrinter warningsPrinter)
    {
        while (client.isRunning() && (client.currentData().getData() == null)) {
            warningsPrinter.print(client.currentStatusInfo().getWarnings(), true, false);
            client.advance();
        }
        List<PrestoWarning> warnings;
        if (client.isRunning()) {
            warnings = client.currentStatusInfo().getWarnings();
        }
        else {
            warnings = client.finalStatusInfo().getWarnings();
        }
        warningsPrinter.print(warnings, false, true);
    }

    private void renderUpdate(PrintStream out, QueryStatusInfo results)
    {
        String status = results.getUpdateType();
        if (results.getUpdateCount() != null) {
            long count = results.getUpdateCount();
            status += format(": %s row%s", count, (count != 1) ? "s" : "");
        }
        out.println(status);
        discardResults();
    }

    private void discardResults()
    {
        try (OutputHandler handler = new OutputHandler(new NullPrinter())) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void renderResults(ClientConfig.OutputFormat outputFormat, boolean interactive, List<Column> columns)
    {
        try {
            doRenderResults(context, outputFormat, interactive, columns);
        }
        catch (QueryAbortedException e) {
//            System.out.println("(query aborted by user)");
            logger.warn("(query aborted by user)");
            client.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void doRenderResults(ConnectContext context, ClientConfig.OutputFormat format, boolean interactive, List<Column> columns)
            throws IOException
    {
        List<String> fieldNames = Lists.transform(columns, Column::getName);
        if (interactive) {
            pageOutput(context, format, fieldNames);
        }
        else {
            sendOutput(context, format, fieldNames);
        }
    }

    private void pageOutput(ConnectContext context, ClientConfig.OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        try (Pager pager = Pager.create();
             ThreadInterruptor clientThread = new ThreadInterruptor();
             OutputHandler handler = createOutputHandler(context, format, fieldNames)) {
            if (!pager.isNullPager()) {
                // ignore the user pressing ctrl-C while in the pager
                ignoreUserInterrupt.set(true);
                pager.getFinishFuture().thenRun(() -> {
                    ignoreUserInterrupt.set(false);
                    client.close();
                    clientThread.interrupt();
                });
            }
            handler.processRows(client);
        }
        catch (RuntimeException | IOException e) {
            if (client.isClientAborted() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
        }
    }

    private void sendOutput(ConnectContext context, ClientConfig.OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(context, format, fieldNames)) {
            handler.processRows(client);
        }
    }

    private static OutputHandler createOutputHandler(ConnectContext context, ClientConfig.OutputFormat format,List<String> fieldNames)
    {
        return new OutputHandler(createOutputPrinter(context, format, fieldNames));
    }

    private static OutputPrinter createOutputPrinter(ConnectContext context, ClientConfig.OutputFormat format,  List<String> fieldNames)
    {
        switch (format) {
            case MYSQL:
                return new MySQLPrinter(context, fieldNames);
            case NULL:
                return new NullPrinter();
        }
        throw new RuntimeException(format + " not supported");
    }

    @Override
    public void close()
    {
        client.close();
    }

    public void renderFailure()
    {
        StringBuilder stringBuilder = new StringBuilder();

        QueryStatusInfo results = client.finalStatusInfo();
        QueryError error = results.getError();
        checkState(error != null);

        String message = String.format("Query %s failed: %s%n", results.getId(), error.getMessage());
        stringBuilder.append(message);
        if (debug && (error.getFailureInfo() != null)) {
            String stacktrace = ExceptionUtils.getStackTrace(error.getFailureInfo().toException());
            stringBuilder.append(stacktrace);
        }
        if (error.getErrorLocation() != null) {
            stringBuilder.append(renderErrorLocation(client.getQuery(), error.getErrorLocation()).toString());
        }

        throw new RuntimeException(stringBuilder.toString());
    }

    private static StringBuilder renderErrorLocation(String query, ErrorLocation location)
    {
        StringBuilder stringBuilder = new StringBuilder();
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (REAL_TERMINAL) {
            Ansi ansi = Ansi.ansi();

            ansi.fg(Ansi.Color.CYAN);
            for (int i = 1; i < location.getLineNumber(); i++) {
                ansi.a(lines.get(i - 1)).newline();
            }
            ansi.a(good);

            ansi.fg(Ansi.Color.RED);
            ansi.a(bad).newline();
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                ansi.a(lines.get(i)).newline();
            }

            ansi.reset();
            stringBuilder.append(ansi).append("\n");
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            stringBuilder.append(prefix + errorLine).append("\n");
            stringBuilder.append(padding + "^").append("\n");
        }
        return stringBuilder;
    }

    private static class PrintStreamWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private final PrintStream printStream;

        PrintStreamWarningsPrinter(PrintStream printStream)
        {
            super(OptionalInt.empty());
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            warnings.stream()
                    .forEach(printStream::println);
        }

        @Override
        protected void printSeparator()
        {
            printStream.println();
        }
    }
}
