package io.deephaven.examples;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.impl.*;
import io.deephaven.client.impl.BarrageSessionFactoryConfig.Factory;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.*;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A basic example of the Deephaven client API. This example covers:
 *
 * <ul>
 *     <li>Connecting to a Deephaven server</li>
 *     <li>Defining an {@link InMemoryAppendOnlyInputTable} on the server</li>
 *     <li>Exposing the table to the Deephaven UI</li>
 *     <li>Adding data to the table on the server</li>
 *     <li>Retrieving data from a table on the server</li>
 * </ul>
 * <p>
 * <b>Note: When running this example, the JVM must be started with the
 * {@code --add-opens=java.base/java.nio=ALL-UNNAMED} command line option.</b>
 * <p>
 * This example uses Deephaven's <a href="https://github.com/deephaven/barrage">Barrage</a> protocol, which is an
 * extension of <a href="https://arrow.apache.org/docs/format/Flight.html">Apache Arrow Flight</a>. Barrage is required
 * for the {@link BarrageSession#snapshot} call at the end of this example, which retrieves a table from the server and
 * reconstitutes it locally as a usable Deephaven {@link Table}.
 */
public class BasicJavaClientExample {
    public static void main(String[] args) throws Exception {

        // The pre-shared key used for authentication. (This is defined when starting the Deephaven server instance.)
        final String pskString = "my-secret-key";

        // ClientConfig describes the configuration to connect to the host
        final ClientConfig config = ClientConfig.builder()
                .target(DeephavenTarget.of(URI.create("dh+plain://localhost:10000")))
                .build();

        // SessionConfig describes the configuration needed to create a session
        final SessionConfig sessionConfig = SessionConfig.builder()
//                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + pskString)
                .build();

        // Create a BufferAllocator. (This is used by the underlying Flight implementation.)
        final BufferAllocator bufferAllocator = new RootAllocator();

        // Create a scheduler thread pool. This is used by the Flight session.
        final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(4);

        // Create a FlightSessionFactory. This stitches together the above components to create the real, live
        // API session with the server.
        final Factory factory = BarrageSessionFactoryConfig.builder()
                .clientConfig(config)
                .allocator(bufferAllocator)
                .scheduler(threadPool)
                .build()
                .factory();

        // Create the Flight session and retrieve the underlying client API session.
        try (
                final BarrageSession barrageSession = factory.newBarrageSession(sessionConfig);
                final Session clientSession = barrageSession.session()) {

            // Define a new table to publish to the server. This table has three columns: an integer column and two
            // string columns.
            final NewTable myTable = NewTable.of(
                    Column.ofInt("MyIntCol", 1, 1, 2, 3, 5, 8, 13, 21, 34),
                    Column.of("MyStrCol", String.class, "This", "is", "an", "example", "table", "created", "on", "the", "client"),
                    Column.of("MyStrCol2", String.class, "Row1", "Row2", "Row3", "Row4", "Row5", "Row6", "Row7", "Row8", "Row9")
            );

            // Push the table to the server. This returns a TableHandle, which is a reference to the table on the server
            // that can be used to run queries against the table from the client.
            final TableHandle myTableHandle = barrageSession.putExport(myTable, bufferAllocator);

            // Publish the table on the server. This makes the table accessible via the Deephaven UI.
            clientSession.publish("my_table", myTableHandle).get();

            // For a modifiable table, define an append-only "InputTable" on the server. The client can send data to the server to append to this.
            final TableSpec inputTableSpec = InMemoryAppendOnlyInputTable.of(TableHeader.of(
                    ColumnHeader.ofInt("MyIntCol"),
                    ColumnHeader.of("MyStrCol", String.class),
                    ColumnHeader.of("MyStrCol2", String.class)
            ));

            // Instantiate the InputTable on the server by 'executing' the inputTableSpec.
            final TableHandle inputTableHandle = clientSession.execute(inputTableSpec);

            // Publish the input table on the server -- this makes the table accessible via the UI.
            clientSession.publish("my_input_table", inputTableHandle);

            // Define (client-side) a table of new rows to add to the InputTable on the server
            final NewTable dataToAdd = NewTable.of(
                    Column.ofInt("MyIntCol", 1, 20, 300, 4000),
                    Column.of("MyStrCol", String.class, "ABC", "DEF", "ABC", "DEF"),
                    Column.of("MyStrCol2", String.class, "Row1", "Row2", "Row3", "Row4")
            );

            // Send the 'dataToAdd' to the server and append it to the input table.
            barrageSession.addToInputTable(inputTableHandle, dataToAdd, bufferAllocator).get(5, TimeUnit.SECONDS);

            // Using the TableHandle ('inputTableHandle') as a starting point, we can run queries to create new tables:
            final TicketTable inputTableTicket = inputTableHandle.ticketId().table();
            // The TicketTable allows the exact same API operations as the Table object we use on the server (including
            // from a Code Studio).
            final TableSpec tableSpec = inputTableTicket.aggBy(List.of(
                    Aggregation.AggSum("MyIntCol"),
                    Aggregation.AggDistinct("MyStrCol2"),
                    Aggregation.AggCount("NRows")
            ), "MyStrCol");
            // Execute the TableSpec to create the new table:
            final TableHandle myAggregatedTable = clientSession.execute(tableSpec);

            // As with inputTableHandle, we must publish the table on the server for the UI to show it.
            // (This is not necessary if the table will only be used from this client.)
            clientSession.publish("my_aggregated_table", myAggregatedTable);


            // We can also run arbitrary Python code on the server, such as to create new UDFs for use in queries.
            // (For Groovy workers, specify "groovy" instead of "python" as the argument to the console() method.)
            final Changes changes = clientSession.console("python").get().executeCode("\n" +
                    "def my_function(my_int) -> int:\n" +
                    "    return my_int * 2\n" +
                    "\n" +
                    "my_updated_input_table = my_input_table.update('MyIntColDoubled = my_function(MyIntCol)')"
            );
            if (changes.errorMessage().isPresent()) {
                throw new RuntimeException(changes.errorMessage().get());
            }

            // Get a TableHandle for my my_updated_input_table, accessing it by its name in the scope. We will use this
            // to pull the table over to the client.
            // (We could also use the TicketTable to execute additional queries against my_updated_input_table.)
            final TicketTable myUpdatedInputTable_ticket = TicketTable.fromQueryScopeField("my_updated_input_table");
            final TableHandle myUpdatedTableHandle_handle = clientSession.of(myUpdatedInputTable_ticket);

            // Pull myTable over from the server and print out its contents.
            System.out.println("Printing 'my_updated_input_table' locally:");
            pullDataToClient(barrageSession, myUpdatedTableHandle_handle);

            // Do the same for myAggregatedTable
            System.out.println("Printing 'myAggregatedTable' locally:");
            pullDataToClient(barrageSession, myAggregatedTable);

            // Create a timetable on the server, add a cycling key column (0–4), and apply lastBy so
            // the result has exactly 5 rows. The first 5 ticks produce adds; every subsequent tick
            // modifies the matching row -- giving us a mix of adds and modifies to observe.
            final TableHandle timeTableHandle = clientSession.execute(TimeTable.of(Duration.ofSeconds(1)));
            final TableHandle listenable = timeTableHandle
                    .update("Key = (int)(i % 5)", "TotalRowsCount = i")
                    .lastBy("Key");
            clientSession.publish("my_time_table", listenable);

            System.out.println("\nSubscribing to lastBy timetable (ticks every 1 second).");
            subscribeToTable(barrageSession, listenable);
        } finally {
            // Shut down the thread pool, allowing the client application to exit. (Otherwise the threads will keep the client app alive.)
            threadPool.shutdown();
        }

        System.out.println("Done!");
    }

    /**
     * This method demonstrates using a {@link BarrageSession#snapshot Barrage snapshot} to pull data from the server
     * to this client. After receiving the snapshot, this method prints the data two ways:
     * <ol>
     *     <li>Using {@link TableTools#show TableTools.show()} to automatically print the table to STDOUT</li>
     *     <li>By extracting the data from the table row-by-row using the table's {@link Table#getRowSet() row set} and
     *     {@link Table#getColumnSource column sources}, then printing the values manually with {@code System.out.println}.</li>
     * </ol>
     *
     * @param barrageSession   The Barrage session to use.
     * @param tableHandle The TableHandle for the table whose data will be pulled from the server.
     * @throws InterruptedException If interrupted while pulling the Barrage snapshot
     */
    private static void pullDataToClient(BarrageSession barrageSession, TableHandle tableHandle) throws InterruptedException, ExecutionException {
        // Take a snapshot of a table on the server and pull it back to the client.
        // For the example in this file this will work great is fine -- but for tables with millions of rows, this may
        // take some time and use a lot of memory!
        final BarrageSnapshot snapshot = barrageSession.snapshot(tableHandle);
        final Table localTableRetrievedFromServer = snapshot.entireTable().get();

        // Print the table to STDOUT with TableTools.show():
        System.out.println("Printing table with TableTools.show():");
        TableTools.show(localTableRetrievedFromServer);
        System.out.println();

        // We can also extract data from the table row-by-row, for example to use in other Java code.
        // See https://deephaven.io/core/groovy/docs/how-to-guides/extract-table-value/ for additional examples.
        final String[] columnNames = localTableRetrievedFromServer.getDefinition().getColumnNamesArray();
        final Map<String, ? extends ColumnSource<?>> columnSources = localTableRetrievedFromServer.getColumnSourceMap();


        System.out.println("Printing table using RowSet and ColumnSources:");
        int rowIdx = 0;
        for (RowSet.Iterator iterator = localTableRetrievedFromServer.getRowSet().iterator(); iterator.hasNext(); ) {
            // Iterate over the RowSet to get the "row keys". Row keys are used to access data in a ColumnSource.
            final long nextRowKey = iterator.nextLong();
            System.out.println("Data for row " + (rowIdx++) + ":");
            for (final String colName : columnNames) {
                System.out.println(colName + ": " + columnSources.get(colName).get(nextRowKey));
            }
            System.out.println();
        }
    }

    /**
     * Demonstrates a live Barrage subscription to a server-side table, with a listener that prints incremental updates.
     * <p>
     * Unlike {@link #pullDataToClient}, which takes a one-time snapshot, this method sets up a persistent subscription:
     * the local {@link Table} returned by {@link BarrageSubscription#entireTable()} is kept in sync with the server
     * and an {@link InstrumentedTableUpdateListenerAdapter} is attached to print every add, modify, and remove as it
     * arrives.
     * <p>
     * Because the Java client has no default {@link ExecutionContext}, one must be built from scratch. We reuse the
     * live table's own update graph (managed by the Barrage client) rather than creating a new one.
     *
     * @param barrageSession The Barrage session to use.
     * @param tableHandle    The TableHandle for the table to subscribe to.
     * @throws InterruptedException If interrupted while waiting for the initial snapshot.
     * @throws ExecutionException   If the subscription future completes exceptionally.
     */
    private static void subscribeToTable(BarrageSession barrageSession, TableHandle tableHandle)
            throws Exception {

        // Subscribe to the table. Unlike snapshot(), this creates a live local Table that receives
        // incremental updates from the server as they occur.
        final BarrageSubscription subscription = barrageSession.subscribe(tableHandle);

        // In a Java client there is no default ExecutionContext, so we must build one from scratch.
        // We need an update graph to build the context, so we borrow the one from the BarrageSession
        // by peeking at it before the subscription table is materialized.
        // We use a PeriodicUpdateGraph with a unique name; the Barrage client will attach the
        // subscription table to this same graph when entireTable() is called inside the open context.
        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .newQueryLibrary()
                .newQueryScope()
                .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
                .setUpdateGraph(PeriodicUpdateGraph.newBuilder("BarrageClientGraph").existingOrBuild())
                .setQueryCompiler(QueryCompilerImpl.create(
                        Files.createTempDirectory("qc_").toFile(),
                        ClassLoader.getSystemClassLoader()))
                .build();

        // The ExecutionContext must be open for the entirety of the subscription lifecycle.
        try (final SafeCloseable ignored = executionContext.open()) {

            // Block until the initial table image has been fully received from the server.
            // After this point 'liveTable' is a ticking local copy that updates automatically.
            final Table liveTable = subscription.entireTable().get();

            final String[] columnNames = liveTable.getDefinition().getColumnNamesArray();
            final Map<String, ? extends ColumnSource<?>> columnSources = liveTable.getColumnSourceMap();

            final InstrumentedTableUpdateListenerAdapter listener =
                    new InstrumentedTableUpdateListenerAdapter("subscribeToTable listener", liveTable, false) {
                        @Override
                        public void onUpdate(final TableUpdate upstream) {
                            if (upstream.added().isNonempty()) {
                                System.out.println("=== Added " + upstream.added().size() + " row(s) ===");
                                printRows(upstream.added(), columnNames, columnSources);
                            }
                            if (upstream.modified().isNonempty()) {
                                System.out.println("=== Modified " + upstream.modified().size() + " row(s) ===");
                                printRows(upstream.modified(), columnNames, columnSources);
                            }
                            if (upstream.removed().isNonempty()) {
                                // Row keys for removed rows refer to the *pre-update* row set,
                                // so the data is no longer in the column sources at this point.
                                System.out.println("=== Removed " + upstream.removed().size() + " row(s) ===");
                            }

                            System.out.println("Entire table:");
                            TableTools.show(liveTable);
                        }
                    };

            liveTable.addUpdateListener(listener);
            System.out.println("Listening for updates. Press Enter to stop...");

            // Block until the user presses Enter, then cleanly remove the listener.
            //noinspection ResultOfMethodCallIgnored
            System.in.read();

            liveTable.removeUpdateListener(listener);
            System.out.println("Unsubscribed.");
        }
    }

    /**
     * Iterates over a {@link RowSet} and prints each row's column values using the provided column sources.
     *
     * @param rowSet        The set of row keys to print.
     * @param columnNames   The column names, in order.
     * @param columnSources A map from column name to its {@link ColumnSource}.
     */
    private static void printRows(
            final RowSet rowSet,
            final String[] columnNames,
            final Map<String, ? extends ColumnSource<?>> columnSources) {
        for (final RowSet.Iterator it = rowSet.iterator(); it.hasNext(); ) {
            final long rowKey = it.nextLong();
            System.out.println("  Row key " + rowKey + ":");
            for (final String colName : columnNames) {
                System.out.println("    " + colName + ": " + columnSources.get(colName).get(rowKey));
            }
        }
    }

}