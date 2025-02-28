package io.deephaven.examples;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.impl.*;
import io.deephaven.client.impl.BarrageSessionFactoryConfig.Factory;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.*;
import io.deephaven.uri.DeephavenTarget;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.net.URI;
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
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + pskString)
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
        final BarrageSnapshot snapshot = barrageSession.snapshot(tableHandle, BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS);
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
}