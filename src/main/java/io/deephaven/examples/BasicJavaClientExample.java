package io.deephaven.examples;

import io.deephaven.client.impl.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

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

        // Create a BufferAllocator. (This is used by the underlying Flight implementation.)
        final BufferAllocator bufferAllocator = new RootAllocator();

        // Create the ManagedChannel. This defines the underlying connection to the Deephaven server.
        final ManagedChannel managedChannel = ManagedChannelBuilder.forTarget("localhost:10000")
                .usePlaintext() // Use '.useTransportSecurity()' if TLS is enabled on the server
                .build();

        // Create a scheduler thread pool. This is used by the Flight session.
        final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(4);

        // Create a FlightSessionFactory. This stitches together the above components to create the real, live
        // API session with the server.
        final BarrageSessionFactory barrageSessionFactory =
                DaggerDeephavenBarrageRoot.create().factoryBuilder()
                        .managedChannel(managedChannel)
                        .scheduler(threadPool)
                        .allocator(bufferAllocator)
                        .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + pskString)
                        .build();

        // Create the Flight session and retrieve the underlying client API session.
        try (final BarrageSession barrageSession = barrageSessionFactory.newBarrageSession();
             final Session clientSession = barrageSession.session()
        ) {

            // Define an append-only "InputTable" on the server. The client can send data to the server to append to this.
            final TableSpec inputTableSpec = InMemoryAppendOnlyInputTable.of(TableHeader.of(
                    ColumnHeader.ofInt("MyIntCol"),
                    ColumnHeader.of("MyStrCol", String.class)
            ));

            // Instantiate the InputTable on the server by 'executing' the inputTableSpec.
            final TableHandle inputTableHandle = clientSession.execute(inputTableSpec);

            // Publish the input table on the server -- this makes the table accessible via the UI.
            clientSession.publish("myTable", inputTableHandle);

            // Define (client-side) a table of new rows to add to the InputTable on the server
            final NewTable dataToAdd = NewTable.of(
                    Column.ofInt("MyIntCol", 1, 20, 300, 4000),
                    Column.of("MyStrCol", String.class, "Row1", "Row", "Row3", "Row4")
            );

            // Send the 'dataToAdd' to the server and append it to the input table.
            // Note that this
            barrageSession.addToInputTable(inputTableHandle,
                    dataToAdd,
                    bufferAllocator
            ).get(5, TimeUnit.SECONDS);


            // Take a snapshot of the server-side version of the input table we created, and pull it back to the client.
            // For our example this is fine, but for tables with millions of rows, this may take some time and use a
            // lot of memory!
            final BarrageSnapshot snapshot = barrageSession.snapshot(inputTableHandle, BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS);
            final Table tableFromServer = snapshot.entireTable().get();

            // Print the table to STDOUT:
            TableTools.show(tableFromServer);
        } finally {
            // Shut down the thread pool, allowing the client application to exit. (Otherwise the threads will keep the client app alive.)
            threadPool.shutdown();
        }

        System.out.println("Done!");
    }
}