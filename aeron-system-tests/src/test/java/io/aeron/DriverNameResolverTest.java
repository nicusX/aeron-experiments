/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.concurrent.status.CountersReader.*;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class DriverNameResolverTest
{
    private static final SleepingMillisIdleStrategy SLEEP_50_MS = new SleepingMillisIdleStrategy(50);
    private final String baseDir = CommonContext.getAeronDirectoryName();
    private final Map<String, TestMediaDriver> drivers = new TreeMap<>();
    private final Map<String, Aeron> clients = new TreeMap<>();

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(clients.values());
        CloseHelper.closeAll(drivers.values());

        for (final TestMediaDriver driver : drivers.values())
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    public void shouldInitializeWithDefaultsAndHaveResolverCounters()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context()
            .resolverName("A")
            .resolverInterface("0.0.0.0:0")), testWatcher));
        startClients();

        final int neighborsCounterId = awaitNeighborsCounterId("A");
        assertNotEquals(neighborsCounterId, NULL_VALUE);
    }

    @Test
    @Timeout(10)
    public void shouldSeeNeighbor()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");

        awaitCounterValue("A", aNeighborsCounterId, 1);
        awaitCounterValue("B", bNeighborsCounterId, 1);
    }

    @Test
    @Timeout(20)
    public void shouldSeeNeighborsViaGossip()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8051"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");
        final int cNeighborsCounterId = awaitNeighborsCounterId("C");

        awaitCounterValue("A", aNeighborsCounterId, 2);
        awaitCounterValue("B", bNeighborsCounterId, 2);
        awaitCounterValue("C", cNeighborsCounterId, 2);
    }

    @Test
    @Timeout(15)
    public void shouldSeeNeighborsViaGossipAsLateJoiningDriver()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");
        final int cNeighborsCounterId = awaitNeighborsCounterId("C");

        awaitCounterValue("A", aNeighborsCounterId, 2);
        awaitCounterValue("B", bNeighborsCounterId, 2);
        awaitCounterValue("C", cNeighborsCounterId, 2);

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-D")
            .resolverName("D")
            .resolverInterface("0.0.0.0:8053")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int dNeighborsCounterId = awaitNeighborsCounterId("D");

        awaitCounterValue("D", dNeighborsCounterId, 3);
        awaitCounterValue("A", aNeighborsCounterId, 3);
        awaitCounterValue("B", bNeighborsCounterId, 3);
        awaitCounterValue("C", cNeighborsCounterId, 3);
    }

    @Test
    @Timeout(10)
    public void shouldResolveDriverNameAndAllowConnection()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");

        awaitCounterValue("A", aNeighborsCounterId, 1);
        awaitCounterValue("B", bNeighborsCounterId, 1);

        final int aCacheEntriesCounterId = awaitCacheEntriesCounterId("A");

        awaitCounterValue("A", aCacheEntriesCounterId, 1);

        try (Subscription subscription = clients.get("B").addSubscription("aeron:udp?endpoint=localhost:24325", 1);
            Publication publication = clients.get("A").addPublication("aeron:udp?endpoint=B:24325", 1))
        {
            while (!publication.isConnected() || !subscription.isConnected())
            {
                Tests.sleep(50);
            }
        }
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldTimeoutAllNeighborsAndCacheEntries()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");

        awaitCounterValue("A", aNeighborsCounterId, 1);
        awaitCounterValue("B", bNeighborsCounterId, 1);

        final int aCacheEntriesCounterId = awaitCacheEntriesCounterId("A");

        awaitCounterValue("A", aCacheEntriesCounterId, 1);

        closeDriver("B");

        awaitCounterValue("A", aNeighborsCounterId, 0);
        awaitCounterValue("A", aCacheEntriesCounterId, 0);
    }

    @SlowTest
    @Test
    @Timeout(30)
    public void shouldTimeoutNeighborsAndCacheEntriesThatAreSeenViaGossip()
    {
        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-A")
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-B")
            .resolverName("B")
            .resolverInterface("0.0.0.0:8051")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));

        addDriver(TestMediaDriver.launch(setDefaults(new MediaDriver.Context())
            .aeronDirectoryName(baseDir + "-C")
            .resolverName("C")
            .resolverInterface("0.0.0.0:8052")
            .resolverBootstrapNeighbor("localhost:8050"), testWatcher));
        startClients();

        final int aNeighborsCounterId = awaitNeighborsCounterId("A");
        final int bNeighborsCounterId = awaitNeighborsCounterId("B");
        final int cNeighborsCounterId = awaitNeighborsCounterId("C");

        awaitCounterValue("A", aNeighborsCounterId, 2);
        awaitCounterValue("B", bNeighborsCounterId, 2);
        awaitCounterValue("C", cNeighborsCounterId, 2);

        final int aCacheEntriesCounterId = awaitCacheEntriesCounterId("A");
        final int bCacheEntriesCounterId = awaitCacheEntriesCounterId("B");
        awaitCounterValue("A", aCacheEntriesCounterId, 2);
        awaitCounterValue("B", bCacheEntriesCounterId, 2);

        closeDriver("B");

        awaitCounterValue("A", aNeighborsCounterId, 1);
        awaitCounterValue("A", aCacheEntriesCounterId, 1);
        awaitCounterValue("C", bNeighborsCounterId, 1);
        awaitCounterValue("C", bCacheEntriesCounterId, 1);
    }

    private void closeDriver(final String index)
    {
        clients.get(index).close();
        clients.remove(index);
        drivers.get(index).close();
        drivers.get(index).context().deleteDirectory();
        drivers.remove(index);
    }

    private static MediaDriver.Context setDefaults(final MediaDriver.Context context)
    {
        context
            .errorHandler(Tests::onError)
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);

        return context;
    }

    private int awaitNeighborsCounterId(final String name)
    {
        final AtomicBuffer metaDataBuffer = clients.get(name).countersReader().metaDataBuffer();

        while (true)
        {
            for (int offset = 0, counterId = 0, capacity = metaDataBuffer.capacity();
                offset < capacity;
                offset += METADATA_LENGTH, counterId++)
            {
                final int recordStatus = metaDataBuffer.getIntVolatile(offset);
                if (RECORD_ALLOCATED == recordStatus)
                {
                    final int typeId = metaDataBuffer.getInt(offset + TYPE_ID_OFFSET);
                    if (AeronCounters.NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID == typeId)
                    {
                        return counterId;
                    }
                }
                else if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }
            }

            Tests.sleep(1);
        }
    }

    private int awaitCacheEntriesCounterId(final String name)
    {
        final AtomicBuffer metaDataBuffer = clients.get(name).countersReader().metaDataBuffer();

        while (true)
        {
            for (int offset = 0, counterId = 0, capacity = metaDataBuffer.capacity();
                offset < capacity;
                offset += METADATA_LENGTH, counterId++)
            {
                final int recordStatus = metaDataBuffer.getIntVolatile(offset);
                if (RECORD_ALLOCATED == recordStatus)
                {
                    final int typeId = metaDataBuffer.getInt(offset + TYPE_ID_OFFSET);
                    if (AeronCounters.NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID == typeId)
                    {
                        return counterId;
                    }
                }
                else if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }
            }

            Tests.sleep(1);
        }
    }

    private void awaitCounterValue(final String name, final int counterId, final long expectedValue)
    {
        final Aeron aeron = clients.get(name);
        final CountersReader countersReader = aeron.countersReader();
        final Supplier<String> messageSupplier =
            () -> "Counter value: " + countersReader.getCounterValue(counterId) + ", expected: " + expectedValue;

        while (countersReader.getCounterValue(counterId) != expectedValue)
        {
            Tests.wait(SLEEP_50_MS, messageSupplier);
            if (aeron.isClosed())
            {
                fail(messageSupplier.get());
            }
        }
    }

    private void startClients()
    {
        drivers.forEach(
            (name, driver) ->
            {
                if (!clients.containsKey(name))
                {
                    clients.put(name, Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(driver.aeronDirectoryName())
                        .errorHandler(Tests::onError)));
                }
            });
    }

    private void addDriver(final TestMediaDriver testMediaDriver)
    {
        final String name = testMediaDriver.context().resolverName();
        drivers.put(name, testMediaDriver);
    }
}
