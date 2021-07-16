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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_SNAPSHOT;
import static io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordingLogTest
{
    private static final File TEMP_DIR = new File(SystemUtil.tmpDirName());

    @BeforeEach
    public void before()
    {
        IoUtil.delete(new File(TEMP_DIR, RecordingLog.RECORDING_LOG_FILE_NAME), true);
    }

    @AfterEach
    public void after()
    {
        IoUtil.delete(new File(TEMP_DIR, RecordingLog.RECORDING_LOG_FILE_NAME), false);
    }

    @Test
    public void shouldCreateNewIndex()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(0, recordingLog.entries().size());
        }
    }

    @Test
    public void shouldAppendAndThenReloadLatestSnapshot()
    {
        final RecordingLog.Entry entry = new RecordingLog.Entry(
            1, 3, 2, 777, 4, NULL_VALUE, ENTRY_TYPE_SNAPSHOT, true, 0);

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(
                entry.recordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                777,
                entry.timestamp,
                SERVICE_ID);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());

            final RecordingLog.Entry snapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
            assertNotNull(snapshot);
            assertEquals(entry, snapshot);
        }
    }

    @Test
    public void shouldIgnoreIncompleteSnapshotInRecoveryPlan()
    {
        final int serviceCount = 1;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(1L, 1L, 0, 777L, 0, 0);
            recordingLog.appendSnapshot(2L, 1L, 0, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(3L, 1L, 0, 888L, 0, 0);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(3, recordingLog.entries().size());

            final AeronArchive mockArchive = mock(AeronArchive.class);
            final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(mockArchive, serviceCount,
                Aeron.NULL_VALUE);
            assertEquals(2, recoveryPlan.snapshots.size());
            assertEquals(SERVICE_ID, recoveryPlan.snapshots.get(0).serviceId);
            assertEquals(2L, recoveryPlan.snapshots.get(0).recordingId);
            assertEquals(0, recoveryPlan.snapshots.get(1).serviceId);
            assertEquals(1L, recoveryPlan.snapshots.get(1).recordingId);
        }
    }

    @Test
    public void shouldIgnoreInvalidMidSnapshotInRecoveryPlan()
    {
        final int serviceCount = 1;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(1, 1L, 0, 777L, 0, 0);
            recordingLog.appendSnapshot(2, 1L, 0, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(3, 1L, 0, 888L, 0, 0);
            recordingLog.appendSnapshot(4, 1L, 0, 888L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(5, 1L, 0, 999L, 0, 0);
            recordingLog.appendSnapshot(6, 1L, 0, 999L, 0, SERVICE_ID);

            recordingLog.invalidateEntry(1L, 2);
            recordingLog.invalidateEntry(1L, 3);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final AeronArchive mockArchive = mock(AeronArchive.class);
            final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(mockArchive, serviceCount,
                Aeron.NULL_VALUE);
            assertEquals(2, recoveryPlan.snapshots.size());
            assertEquals(SERVICE_ID, recoveryPlan.snapshots.get(0).serviceId);
            assertEquals(6L, recoveryPlan.snapshots.get(0).recordingId);
            assertEquals(0, recoveryPlan.snapshots.get(1).serviceId);
            assertEquals(5L, recoveryPlan.snapshots.get(1).recordingId);
        }
    }

    @Test
    public void shouldIgnoreInvalidTermInRecoveryPlan()
    {
        final int serviceCount = 1;
        final long removedLeadershipTerm = 11L;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(0L, 9L, 444, 0);
            recordingLog.appendTerm(0L, 10L, 666, 0);
            recordingLog.appendSnapshot(1L, 10L, 666, 777L, 0, 0);
            recordingLog.appendSnapshot(2L, 10L, 666, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(3L, 10L, 666, 888L, 0, 0);
            recordingLog.appendSnapshot(4L, 10L, 666, 888L, 0, SERVICE_ID);
            recordingLog.appendTerm(0L, removedLeadershipTerm, 999, 0);

            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();

            assertNotNull(lastTerm);
            assertEquals(999L, lastTerm.termBaseLogPosition);

            recordingLog.invalidateEntry(removedLeadershipTerm, 6);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final AeronArchive mockArchive = mock(AeronArchive.class);
            when(mockArchive.listRecording(anyLong(), any())).thenReturn(1);

            final RecordingLog.RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(mockArchive, serviceCount,
                Aeron.NULL_VALUE);
            assertEquals(0L, recoveryPlan.log.recordingId);
            assertEquals(10L, recoveryPlan.log.leadershipTermId);
            assertEquals(666, recoveryPlan.log.termBaseLogPosition);

            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();
            assertNotNull(lastTerm);
            assertEquals(0L, lastTerm.recordingId);
            assertEquals(0L, recordingLog.findLastTermRecordingId());
            assertTrue(recordingLog.isUnknown(removedLeadershipTerm));
            assertEquals(NULL_VALUE, recordingLog.getTermTimestamp(removedLeadershipTerm));

            assertThrows(ClusterException.class, () -> recordingLog.getTermEntry(removedLeadershipTerm));
            assertThrows(ClusterException.class, () -> recordingLog.commitLogPosition(removedLeadershipTerm, 99L));
        }
    }

    @Test
    public void shouldAppendAndThenCommitTermPosition()
    {
        final long newPosition = 9999L;
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final long recordingId = 1L;
            final long leadershipTermId = 1111L;
            final long logPosition = 2222L;
            final long timestamp = 3333L;

            recordingLog.appendTerm(recordingId, leadershipTermId, logPosition, timestamp);
            recordingLog.commitLogPosition(leadershipTermId, newPosition);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());

            final RecordingLog.Entry actualEntry = recordingLog.entries().get(0);
            assertEquals(newPosition, actualEntry.logPosition);
        }
    }

    @Test
    public void shouldRemoveEntry()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final RecordingLog.Entry entryOne = new RecordingLog.Entry(
                1L, 3, 2, NULL_POSITION, 4, 0, ENTRY_TYPE_TERM, true, 0);
            recordingLog.appendTerm(
                entryOne.recordingId, entryOne.leadershipTermId, entryOne.termBaseLogPosition, entryOne.timestamp);

            final RecordingLog.Entry entryTwo = new RecordingLog.Entry(
                1L, 4, 3, NULL_POSITION, 5, 0, ENTRY_TYPE_TERM, true, 0);
            recordingLog.appendTerm(
                entryTwo.recordingId, entryTwo.leadershipTermId, entryTwo.termBaseLogPosition, entryTwo.timestamp);

            recordingLog.removeEntry(entryTwo.leadershipTermId, recordingLog.nextEntryIndex() - 1);
            assertEquals(1, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(1, recordingLog.entries().size());
            assertEquals(2, recordingLog.nextEntryIndex());
        }
    }

    @Test
    public void shouldCorrectlyOrderSnapshots()
    {
        final ArrayList<RecordingLog.Snapshot> snapshots = new ArrayList<>();
        final ArrayList<RecordingLog.Entry> entries = new ArrayList<>();

        addRecordingLogEntry(entries, ConsensusModule.Configuration.SERVICE_ID, 0, ENTRY_TYPE_TERM);
        addRecordingLogEntry(entries, 2, 4, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, 1, 5, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, 0, 6, ENTRY_TYPE_SNAPSHOT);
        addRecordingLogEntry(entries, ConsensusModule.Configuration.SERVICE_ID, 7, ENTRY_TYPE_SNAPSHOT);

        RecordingLog.addSnapshots(snapshots, entries, 3, entries.size() - 1);

        assertEquals(4, snapshots.size());
        assertEquals(ConsensusModule.Configuration.SERVICE_ID, snapshots.get(0).serviceId);
        assertEquals(0, snapshots.get(1).serviceId);
        assertEquals(1, snapshots.get(2).serviceId);
        assertEquals(2, snapshots.get(3).serviceId);
    }

    @Test
    public void shouldInvalidateLatestSnapshot()
    {
        final long termBaseLogPosition = 0L;
        final long logIncrement = 640L;
        long leadershipTermId = 7L;
        long logPosition = 0L;
        long timestamp = 1000L;

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(1, leadershipTermId, termBaseLogPosition, timestamp);

            timestamp += 1;
            logPosition += logIncrement;

            recordingLog.appendSnapshot(
                2, leadershipTermId, termBaseLogPosition, logPosition, timestamp, 0);
            recordingLog.appendSnapshot(
                3, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

            timestamp += 1;
            logPosition += logIncrement;

            recordingLog.appendSnapshot(
                4, leadershipTermId, termBaseLogPosition, logPosition, timestamp, 0);
            recordingLog.appendSnapshot(
                5, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

            leadershipTermId++;
            recordingLog.appendTerm(1, leadershipTermId, logPosition, timestamp);

            assertTrue(recordingLog.invalidateLatestSnapshot());
            assertEquals(6, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(6, recordingLog.entries().size());

            assertTrue(recordingLog.entries().get(0).isValid);
            assertTrue(recordingLog.entries().get(1).isValid);
            assertTrue(recordingLog.entries().get(2).isValid);
            assertFalse(recordingLog.entries().get(3).isValid);
            assertFalse(recordingLog.entries().get(4).isValid);
            assertTrue(recordingLog.entries().get(5).isValid);

            final RecordingLog.Entry latestServiceSnapshot = recordingLog.getLatestSnapshot(0);
            assertSame(recordingLog.entries().get(1), latestServiceSnapshot);

            final RecordingLog.Entry latestCmSnapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
            assertSame(recordingLog.entries().get(2), latestCmSnapshot);

            assertTrue(recordingLog.invalidateLatestSnapshot());
            assertEquals(6, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(6, recordingLog.entries().size());

            assertTrue(recordingLog.entries().get(0).isValid);
            assertFalse(recordingLog.entries().get(1).isValid);
            assertFalse(recordingLog.entries().get(2).isValid);
            assertFalse(recordingLog.entries().get(3).isValid);
            assertFalse(recordingLog.entries().get(4).isValid);
            assertTrue(recordingLog.entries().get(5).isValid);

            assertFalse(recordingLog.invalidateLatestSnapshot());
            assertSame(recordingLog.entries().get(5), recordingLog.getTermEntry(leadershipTermId));
        }
    }

    @Test
    void shouldRecoverSnapshotsMidLogMarkedInvalid()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(1L, 1L, 10, 555L, 0, 0);
            recordingLog.appendSnapshot(2L, 1L, 10, 555L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(3L, 1L, 10, 777L, 0, 0);
            recordingLog.appendSnapshot(4L, 1L, 10, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(5L, 1L, 10, 888L, 0, 0);
            recordingLog.appendSnapshot(6L, 1L, 10, 888L, 0, SERVICE_ID);

            recordingLog.invalidateEntry(1L, 2);
            recordingLog.invalidateEntry(1L, 3);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(7L, 1L, 10, 777L, 555, 0);
            recordingLog.appendSnapshot(8L, 1L, 10, 777L, -999, SERVICE_ID);

            assertEquals(6, recordingLog.entries().size());
            assertTrue(recordingLog.entries().get(2).isValid);
            assertEquals(7L, recordingLog.entries().get(2).recordingId);
            assertTrue(recordingLog.entries().get(3).isValid);
            assertEquals(8L, recordingLog.entries().get(3).recordingId);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(6, recordingLog.entries().size());
            assertTrue(recordingLog.entries().get(2).isValid);
            assertEquals(7L, recordingLog.entries().get(2).recordingId);
            assertTrue(recordingLog.entries().get(3).isValid);
            assertEquals(8L, recordingLog.entries().get(3).recordingId);
        }
    }

    @Test
    void shouldRecoverSnapshotsLastInLogMarkedWithInvalid()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(-10, 1L, 0, 777L, 0, 0);
            recordingLog.appendSnapshot(-11, 1L, 0, 777L, 0, SERVICE_ID);

            recordingLog.appendTerm(1, 2L, 10, 0);
            recordingLog.appendSnapshot(-12, 2L, 10, 888L, 0, 0);
            recordingLog.appendSnapshot(-13, 2L, 10, 888L, 0, SERVICE_ID);

            recordingLog.appendTerm(1, 3L, 20, 0);
            recordingLog.appendSnapshot(-14, 3L, 20, 999L, 0, 0);
            recordingLog.appendSnapshot(-15, 3L, 20, 999L, 0, SERVICE_ID);

            recordingLog.invalidateLatestSnapshot();
            recordingLog.invalidateLatestSnapshot();
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(1, 2L, 10, 888L, 0, 0);
            recordingLog.appendSnapshot(2, 2L, 10, 888L, 0, SERVICE_ID);

            recordingLog.appendSnapshot(3, 3L, 20, 999L, 0, 0);
            recordingLog.appendSnapshot(4, 3L, 20, 999L, 0, SERVICE_ID);

            assertEquals(8, recordingLog.entries().size());
            assertTrue(recordingLog.entries().get(2).isValid);
            assertEquals(1L, recordingLog.entries().get(3).recordingId);
            assertTrue(recordingLog.entries().get(3).isValid);
            assertEquals(2L, recordingLog.entries().get(4).recordingId);
            assertTrue(recordingLog.entries().get(4).isValid);
            assertEquals(3L, recordingLog.entries().get(6).recordingId);
            assertTrue(recordingLog.entries().get(5).isValid);
            assertEquals(4L, recordingLog.entries().get(7).recordingId);
        }
    }

    @Test
    void shouldNotAllowInvalidateOfSnapshotWithoutParentTerm()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(-10, 1L, 0, 777L, 0, 0);
            recordingLog.appendSnapshot(-11, 1L, 0, 777L, 0, SERVICE_ID);

            final ClusterException ex = assertThrows(ClusterException.class, recordingLog::invalidateLatestSnapshot);
            assertEquals("ERROR - no matching term for snapshot: leadershipTermId=1", ex.getMessage());
        }
    }

    @Test
    void shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(10L, 0L, 0, 0);
            recordingLog.appendTerm(10L, 1L, 500, 0);
            recordingLog.appendSnapshot(0, 1L, 500, 777L, 0, 0);
            recordingLog.appendSnapshot(1, 1L, 500, 777L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(2, 1L, 500, 888L, 0, 0);
            recordingLog.appendSnapshot(3, 1L, 500, 888L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(4, 1L, 500, 999L, 0, 0);
            recordingLog.appendSnapshot(5, 1L, 500, 999L, 0, SERVICE_ID);
            recordingLog.appendTerm(10L, 2L, 1000, 5);

            assertTrue(recordingLog.invalidateLatestSnapshot());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendSnapshot(6, 2L, 500, 999L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(7, 1L, 501, 999L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(8, 1L, 500, 998L, 0, SERVICE_ID);
            recordingLog.appendSnapshot(9, 1L, 500, 999L, 0, 42);

            assertEquals(new RecordingLog.Entry(8, 1, 500, 998, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 11),
                recordingLog.entries().get(6));
            assertEquals(new RecordingLog.Entry(4, 1, 500, 999, 0, 0, ENTRY_TYPE_SNAPSHOT, false, 6),
                recordingLog.entries().get(7));
            assertEquals(new RecordingLog.Entry(5, 1, 500, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, false, 7),
                recordingLog.entries().get(8));
            assertEquals(new RecordingLog.Entry(7, 1, 501, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 10),
                recordingLog.entries().get(9));
            assertEquals(new RecordingLog.Entry(9, 1, 500, 999, 0, 42, ENTRY_TYPE_SNAPSHOT, true, 12),
                recordingLog.entries().get(10));
            assertEquals(new RecordingLog.Entry(10, 2, 1000, NULL_POSITION, 5, NULL_VALUE, ENTRY_TYPE_TERM, true, 8),
                recordingLog.entries().get(11));
            assertEquals(new RecordingLog.Entry(6, 2, 500, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 9),
                recordingLog.entries().get(12));
            final RecordingLog.Entry latestSnapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
            assertNotNull(latestSnapshot);
            assertEquals(6L, latestSnapshot.recordingId);
        }
    }

    @Test
    void shouldAppendTermWithLeadershipTermIdOutOfOrder()
    {
        final List<RecordingLog.Entry> sortedEntries = asList(
            new RecordingLog.Entry(0, 0, 0, 700, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 0),
            new RecordingLog.Entry(0, 1, 700, 2048, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 3),
            new RecordingLog.Entry(0, 2, 2048, 5000, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 1),
            new RecordingLog.Entry(0, 3, 5000, NULL_POSITION, 100, NULL_VALUE, ENTRY_TYPE_TERM, true, 2));

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(0, 0, 0, 0);
            recordingLog.appendTerm(0, 2, 2048, 0);
            recordingLog.appendTerm(0, 3, 5000, 100);
            recordingLog.appendTerm(0, 1, 700, 0);

            assertEquals(4, recordingLog.nextEntryIndex());
            final List<RecordingLog.Entry> entries = recordingLog.entries();
            assertEquals(sortedEntries, entries);

            assertSame(entries.get(0), recordingLog.getTermEntry(0));
            assertSame(entries.get(1), recordingLog.getTermEntry(1));
            assertSame(entries.get(2), recordingLog.getTermEntry(2));
            assertSame(entries.get(3), recordingLog.getTermEntry(3));
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(sortedEntries, recordingLog.entries());
        }
    }

    @Test
    void shouldAppendSnapshotWithLeadershipTermIdOutOfOrder()
    {
        final List<RecordingLog.Entry> sortedEntries = asList(
            new RecordingLog.Entry(3, 1, 0, 200, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 0),
            new RecordingLog.Entry(10, 1, 0, 56, 42, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 1),
            new RecordingLog.Entry(3, 2, 200, 2048, 555, NULL_VALUE, ENTRY_TYPE_TERM, true, 2),
            new RecordingLog.Entry(11, 2, 200, 250, 100, 1, ENTRY_TYPE_SNAPSHOT, true, 4),
            new RecordingLog.Entry(100, 2, 200, 250, 100, 0, ENTRY_TYPE_SNAPSHOT, true, 5),
            new RecordingLog.Entry(3, 3, 2048, NULL_POSITION, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 3));

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(3, 1, 0, 0);
            recordingLog.appendSnapshot(10, 1, 0, 56, 42, SERVICE_ID);
            recordingLog.invalidateEntry(1, 1);

            recordingLog.commitLogPosition(1, 200);
            recordingLog.appendTerm(3, 2, 200, 555);

            recordingLog.commitLogPosition(2, 2048);
            recordingLog.appendTerm(3, 3, 2048, 0);
            recordingLog.appendSnapshot(11, 2, 200, 250, 100, 1);
            recordingLog.appendSnapshot(10, 1, 0, 56, 42, SERVICE_ID);
            recordingLog.appendSnapshot(100, 2, 200, 250, 100, 0);

            final List<RecordingLog.Entry> entries = recordingLog.entries();
            assertEquals(sortedEntries, entries);
            assertEquals(6, recordingLog.nextEntryIndex());

            assertSame(entries.get(0), recordingLog.getTermEntry(1));
            assertNull(recordingLog.findTermEntry(0));
            assertSame(entries.get(2), recordingLog.getTermEntry(2));
            assertSame(entries.get(5), recordingLog.getTermEntry(3));
            final RecordingLog.Entry latestSnapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
            assertSame(entries.get(1), latestSnapshot);
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            assertEquals(sortedEntries, recordingLog.entries());
        }
    }

    @Test
    void appendTermShouldRejectNullValueAsRecordingId()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final ClusterException exception = assertThrows(ClusterException.class,
                () -> recordingLog.appendTerm(NULL_VALUE, 0, 0, 0));
            assertEquals("ERROR - invalid recordingId=-1", exception.getMessage());
            assertEquals(0, recordingLog.entries().size());
        }
    }

    @Test
    void appendSnapshotShouldRejectNullValueAsRecordingId()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final ClusterException exception = assertThrows(ClusterException.class,
                () -> recordingLog.appendSnapshot(NULL_VALUE, 0, 0, 0, 0, 0));
            assertEquals("ERROR - invalid recordingId=-1", exception.getMessage());
            assertEquals(0, recordingLog.entries().size());
        }
    }

    @Test
    void appendTermShouldNotAcceptDifferentRecordingIds()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(42, 0, 0, 0);

            final ClusterException exception = assertThrows(ClusterException.class,
                () -> recordingLog.appendTerm(21, 1, 0, 0));
            assertEquals("ERROR - invalid TERM recordingId=21, expected recordingId=42", exception.getMessage());
            assertEquals(1, recordingLog.entries().size());
        }

        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            final ClusterException exception = assertThrows(ClusterException.class,
                () -> recordingLog.appendTerm(-5, -5, -5, -5));
            assertEquals("ERROR - invalid TERM recordingId=-5, expected recordingId=42", exception.getMessage());
            assertEquals(1, recordingLog.entries().size());
        }
    }

    @Test
    void appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId()
    {
        try (RecordingLog recordingLog = new RecordingLog(TEMP_DIR))
        {
            recordingLog.appendTerm(8, 0, 0, 0);
            recordingLog.appendTerm(8, 1, 1, 1);

            recordingLog.invalidateEntry(0, 0);
            recordingLog.appendTerm(8, 0, 100, 100);

            final ClusterException exception = assertThrows(ClusterException.class,
                () -> recordingLog.appendTerm(8, 1, 5, 5));
            assertEquals("ERROR - duplicate TERM entry for leadershipTermId=1", exception.getMessage());
            assertEquals(3, recordingLog.entries().size());
        }
    }

    private static void addRecordingLogEntry(
        final ArrayList<RecordingLog.Entry> entries, final int serviceId, final int recordingId, final int entryType)
    {
        entries.add(new RecordingLog.Entry(
            recordingId, 1, 1440, 2880, 0L, serviceId, entryType, true, entries.size()));
    }
}