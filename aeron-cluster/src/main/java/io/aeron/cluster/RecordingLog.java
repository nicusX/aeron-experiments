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
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.driver.Configuration.FILE_PAGE_SIZE_DEFAULT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Comparator.comparingLong;
import static java.util.Comparator.reverseOrder;
import static org.agrona.BitUtil.*;

/**
 * A log of recordings which make up the history of a Raft log across leadership terms. Entries are in order.
 * <p>
 * The log is made up of entries of leadership terms or snapshots to roll up state as of a log position within a
 * leadership term.
 * <p>
 * The latest state is made up of a the latest snapshot followed by any leadership term logs which follow. It is
 * possible that a snapshot is taken mid term and therefore the latest state is the snapshot plus the log of messages
 * which got appended to the log after the snapshot was taken.
 * <p>
 * Record layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Log Position at beginning of term                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |        Log Position when entry was created or committed       |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |               Timestamp when entry was created                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Service ID when a Snapshot                   |
 *  +---------------------------------------------------------------+
 *  |R|               Entry Type (Log or Snapshot)                  |
 *  +---------------------------------------------------------------+
 *  |                                                               |
 *  |                                                              ...
 * ...                Repeats to the end of the log                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 * <p>The reserved bit on the entry type indicates whether the entry was marked invalid.</p>
 */
public final class RecordingLog implements AutoCloseable
{
    /**
     * Representation of the entry in the {@link RecordingLog}.
     */
    public static final class Entry
    {
        /**
         * Identity of the recording in the archive.
         */
        public final long recordingId;

        /**
         * Identity of the leadership term.
         */
        public final long leadershipTermId;

        /**
         * The log position at the base of the leadership term.
         */
        public final long termBaseLogPosition;

        /**
         * Position the log has reached for the entry.
         */
        public final long logPosition;

        /**
         * Timestamp for the cluster clock in the time units configured for the cluster.
         */
        public final long timestamp;

        /**
         * Identity of the service associated with the entry.
         */
        public final int serviceId;

        /**
         * Type, or classification, of the entry, e.g. {@link #ENTRY_TYPE_TERM} or {@link #ENTRY_TYPE_SNAPSHOT}.
         */
        public final int type;

        /**
         * Index of the entry in the recording log.
         */
        public final int entryIndex;

        /**
         * Flag to indicate if the entry is invalid and thus should be ignored.
         */
        public final boolean isValid;

        /**
         * A new entry in the recording log.
         *
         * @param recordingId         of the entry in an archive.
         * @param leadershipTermId    of this entry.
         * @param termBaseLogPosition position of the log over leadership terms at the beginning of this term.
         * @param logPosition         position reached when the entry was created
         * @param timestamp           of this entry.
         * @param serviceId           service id for snapshot.
         * @param type                of the entry as a log of a term or a snapshot.
         * @param isValid             indicates if the entry is valid, {@link RecordingLog#invalidateEntry(long, int)}
         *                            marks it invalid.
         * @param entryIndex          of the entry on disk.
         */
        public Entry(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long timestamp,
            final int serviceId,
            final int type,
            final boolean isValid,
            final int entryIndex)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.timestamp = timestamp;
            this.serviceId = serviceId;
            this.type = type;
            this.entryIndex = entryIndex;
            this.isValid = isValid;
        }

        Entry invalidate()
        {
            return new Entry(
                recordingId,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                timestamp,
                serviceId,
                type,
                false,
                entryIndex);
        }

        Entry logPosition(final long logPosition)
        {
            return new Entry(
                recordingId,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                timestamp,
                serviceId,
                type,
                isValid,
                entryIndex);
        }

        long serviceId()
        {
            return serviceId;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final Entry entry = (Entry)o;

            return recordingId == entry.recordingId &&
                leadershipTermId == entry.leadershipTermId &&
                termBaseLogPosition == entry.termBaseLogPosition &&
                logPosition == entry.logPosition &&
                timestamp == entry.timestamp &&
                serviceId == entry.serviceId &&
                type == entry.type &&
                entryIndex == entry.entryIndex &&
                isValid == entry.isValid;
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            int result = (int)(recordingId ^ (recordingId >>> 32));
            result = 31 * result + (int)(leadershipTermId ^ (leadershipTermId >>> 32));
            result = 31 * result + (int)(termBaseLogPosition ^ (termBaseLogPosition >>> 32));
            result = 31 * result + (int)(logPosition ^ (logPosition >>> 32));
            result = 31 * result + (int)(timestamp ^ (timestamp >>> 32));
            result = 31 * result + serviceId;
            result = 31 * result + type;
            result = 31 * result + entryIndex;
            result = 31 * result + (isValid ? 1 : 0);
            return result;
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Entry{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", timestamp=" + timestamp +
                ", serviceId=" + serviceId +
                ", type=" + type +
                ", isValid=" + isValid +
                ", entryIndex=" + entryIndex +
                '}';
        }
    }

    /**
     * Representation of a snapshot entry in the {@link RecordingLog}.
     */
    public static final class Snapshot
    {
        /**
         * Identity of the recording in the archive for the snapshot.
         */
        public final long recordingId;

        /**
         * Identity of the leadership term.
         */
        public final long leadershipTermId;

        /**
         * The log position at the base of the leadership term.
         */
        public final long termBaseLogPosition;

        /**
         * Position the log has reached for the snapshot.
         */
        public final long logPosition;

        /**
         * Timestamp for the cluster clock in the time units configured for the cluster at time of the snapshot.
         */
        public final long timestamp;

        /**
         * Identity of the service associated with the snapshot.
         */
        public final int serviceId;

        /**
         * A snapshot entry in the {@link RecordingLog}.
         *
         * @param recordingId         of the entry in an archive.
         * @param leadershipTermId    in which the snapshot was taken.
         * @param termBaseLogPosition position of the log over leadership terms at the beginning of this term.
         * @param logPosition         position reached when the entry was snapshot was taken.
         * @param timestamp           as which the snapshot was taken.
         * @param serviceId           which the snapshot belongs to.
         */
        public Snapshot(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long timestamp,
            final int serviceId)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.timestamp = timestamp;
            this.serviceId = serviceId;
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Snapshot{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", timestamp=" + timestamp +
                ", serviceId=" + serviceId +
                '}';
        }
    }

    /**
     * Representation of a log entry in the {@link RecordingLog}.
     */
    public static final class Log
    {
        /**
         * Identity of the recording in the archive for the log.
         */
        public final long recordingId;

        /**
         * Identity of the leadership term.
         */
        public final long leadershipTermId;

        /**
         * The log position at the base of the leadership term.
         */
        public final long termBaseLogPosition;

        /**
         * Position the log has reached for the term which can be {@link AeronArchive#NULL_POSITION} when not committed.
         */
        public final long logPosition;

        /**
         * Start position of the recording captured in the archive.
         */
        public final long startPosition;

        /**
         * Stop position of the recording captured in the archive.
         */
        public final long stopPosition;

        /**
         * Initial term identity of the stream captured for the recording.
         */
        public final int initialTermId;

        /**
         * Transport term buffer length of the stream captured for the recording.
         */
        public final int termBufferLength;

        /**
         * Transport MTU length of the stream captured for the recording.
         */
        public final int mtuLength;

        /**
         * Transport session identity of the stream captured for the recording.
         */
        public final int sessionId;

        /**
         * Construct a representation of a log entry in the {@link RecordingLog}.
         *
         * @param recordingId         for the recording in an archive.
         * @param leadershipTermId    identity for the leadership term.
         * @param termBaseLogPosition log position at the base of the leadership term.
         * @param logPosition         position the log has reached for the term.
         * @param startPosition       of the recording captured in the archive.
         * @param stopPosition        of the recording captured in the archive.
         * @param initialTermId       of the stream captured for the recording.
         * @param termBufferLength    of the stream captured for the recording.
         * @param mtuLength           of the stream captured for the recording.
         * @param sessionId           of the stream captured for the recording.
         */
        public Log(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
            this.initialTermId = initialTermId;
            this.termBufferLength = termBufferLength;
            this.mtuLength = mtuLength;
            this.sessionId = sessionId;
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Log{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", startPosition=" + startPosition +
                ", stopPosition=" + stopPosition +
                ", initialTermId=" + initialTermId +
                ", termBufferLength=" + termBufferLength +
                ", mtuLength=" + mtuLength +
                ", sessionId=" + sessionId +
                '}';
        }
    }

    /**
     * The snapshots and steps to recover the state of a cluster.
     */
    public static final class RecoveryPlan
    {
        /**
         * The last, i.e. most recent, leadership term identity for the log.
         */
        public final long lastLeadershipTermId;

        /**
         * The last, i.e. most recent, leadership term base log position.
         */
        public final long lastTermBaseLogPosition;

        /**
         * The position reached for local appended log.
         */
        public final long appendedLogPosition;

        /**
         * The position reached for the local appended log for which the commit position is known.
         */
        public final long committedLogPosition;

        /**
         * The most recent snapshots for the consensus module and services to accelerate recovery.
         */
        public final ArrayList<Snapshot> snapshots;

        /**
         * The appended local log details.
         */
        public final Log log;

        /**
         *
         * @param lastLeadershipTermId    the last, i.e. most recent, leadership term identity for the log.
         * @param lastTermBaseLogPosition last, i.e. most recent, leadership term base log position.
         * @param appendedLogPosition     reached for local appended log.
         * @param committedLogPosition    reached for the local appended log for which the commit position is known.
         * @param snapshots               most recent snapshots for the consensus module and services.
         * @param log                     appended local log details.
         */
        public RecoveryPlan(
            final long lastLeadershipTermId,
            final long lastTermBaseLogPosition,
            final long appendedLogPosition,
            final long committedLogPosition,
            final ArrayList<Snapshot> snapshots,
            final Log log)
        {
            this.lastLeadershipTermId = lastLeadershipTermId;
            this.lastTermBaseLogPosition = lastTermBaseLogPosition;
            this.appendedLogPosition = appendedLogPosition;
            this.committedLogPosition = committedLogPosition;
            this.snapshots = snapshots;
            this.log = log;
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "RecoveryPlan{" +
                "lastLeadershipTermId=" + lastLeadershipTermId +
                ", lastTermBaseLogPosition=" + lastTermBaseLogPosition +
                ", appendedLogPosition=" + appendedLogPosition +
                ", committedLogPosition=" + committedLogPosition +
                ", snapshots=" + snapshots +
                ", log=" + log +
                '}';
        }
    }

    /**
     * Filename for the history of leadership log terms and snapshot recordings.
     */
    public static final String RECORDING_LOG_FILE_NAME = "recording.log";

    /**
     * The log entry is for a recording of messages within a leadership term to the log.
     */
    public static final int ENTRY_TYPE_TERM = 0;

    /**
     * The log entry is for a recording of a snapshot of state taken as of a position in the log.
     */
    public static final int ENTRY_TYPE_SNAPSHOT = 1;

    /**
     * The flag used to determine if the entry has been marked with invalid.
     */
    public static final int ENTRY_TYPE_INVALID_FLAG = 1 << 31;

    /**
     * The offset at which the recording id for the entry is stored.
     */
    public static final int RECORDING_ID_OFFSET = 0;

    /**
     * The offset at which the leadership term id for the entry is stored.
     */
    public static final int LEADERSHIP_TERM_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the log position as of the beginning of the term for the entry is stored.
     */
    public static final int TERM_BASE_LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the log position is stored.
     */
    public static final int LOG_POSITION_OFFSET = TERM_BASE_LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the timestamp for the entry is stored.
     */
    public static final int TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the service id is recorded.
     */
    public static final int SERVICE_ID_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the type of the entry is stored.
     */
    public static final int ENTRY_TYPE_OFFSET = SERVICE_ID_OFFSET + SIZE_OF_INT;

    /**
     * The length of each entry in the recording log (not the recordings in the archive).
     */
    static final int ENTRY_LENGTH = BitUtil.align(ENTRY_TYPE_OFFSET + SIZE_OF_INT, CACHE_LINE_LENGTH);

    private static final Comparator<Entry> ENTRY_COMPARATOR =
        comparingLong((Entry e) -> e.leadershipTermId)
        .thenComparingInt((e) -> e.type)
        .thenComparingLong((e) -> e.logPosition)
        .thenComparing(Entry::serviceId, reverseOrder());

    private long termRecordingId = NULL_VALUE;
    private int nextEntryIndex;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(FILE_PAGE_SIZE_DEFAULT).order(LITTLE_ENDIAN);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final ArrayList<Entry> entriesCache = new ArrayList<>();
    private final Long2LongHashMap cacheIndexByLeadershipTermIdMap = new Long2LongHashMap(NULL_VALUE);
    private final IntArrayList invalidSnapshots = new IntArrayList();

    /**
     * Create a log that appends to an existing log or creates a new one.
     *
     * @param parentDir in which the log will be created.
     */
    public RecordingLog(final File parentDir)
    {
        final File logFile = new File(parentDir, RECORDING_LOG_FILE_NAME);
        final boolean isNewFile = !logFile.exists();

        try
        {
            fileChannel = FileChannel.open(logFile.toPath(), CREATE, READ, WRITE);

            if (isNewFile)
            {
                syncDirectory(parentDir);
            }
            else
            {
                reload();
            }
        }
        catch (final IOException ex)
        {
            throw new ClusterException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(fileChannel);
    }

    /**
     * Force the file to backing storage. Same as calling {@link FileChannel#force(boolean)} with true.
     *
     * @param fileSyncLevel as defined by {@link ConsensusModule.Configuration#FILE_SYNC_LEVEL_PROP_NAME}.
     */
    public void force(final int fileSyncLevel)
    {
        if (fileSyncLevel > 0)
        {
            try
            {
                fileChannel.force(fileSyncLevel > 1);
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    /**
     * List of currently loaded entries.
     *
     * @return the list of currently loaded entries.
     */
    public List<Entry> entries()
    {
        return entriesCache;
    }

    /**
     * Get the next index to be used when appending an entry to the log.
     *
     * @return the next index to be used when appending an entry to the log.
     */
    public int nextEntryIndex()
    {
        return nextEntryIndex;
    }

    /**
     * Reload the recording log from disk.
     */
    public void reload()
    {
        entriesCache.clear();
        cacheIndexByLeadershipTermIdMap.clear();
        invalidSnapshots.clear();
        cacheIndexByLeadershipTermIdMap.compact();

        nextEntryIndex = 0;
        byteBuffer.clear();

        try
        {
            long filePosition = 0;
            while (true)
            {
                final int bytesRead = fileChannel.read(byteBuffer, filePosition);
                if (byteBuffer.remaining() == 0)
                {
                    byteBuffer.flip();
                    captureEntriesFromBuffer(byteBuffer, buffer, entriesCache);
                    byteBuffer.clear();
                }

                if (bytesRead <= 0)
                {
                    if (byteBuffer.position() > 0)
                    {
                        byteBuffer.flip();
                        captureEntriesFromBuffer(byteBuffer, buffer, entriesCache);
                        byteBuffer.clear();
                    }

                    break;
                }

                filePosition += bytesRead;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        entriesCache.sort(ENTRY_COMPARATOR);
        for (int i = 0, size = entriesCache.size(); i < size; i++)
        {
            final Entry entry = entriesCache.get(i);

            if (isValidTerm(entry))
            {
                cacheIndexByLeadershipTermIdMap.put(entry.leadershipTermId, i);
            }

            if (ENTRY_TYPE_SNAPSHOT == entry.type && !entry.isValid)
            {
                invalidSnapshots.add(i);
            }
        }
    }

    /**
     * Find the last recording id used for a leadership term. If not found then {@link RecordingPos#NULL_RECORDING_ID}.
     *
     * @return the last leadership term recording id or {@link RecordingPos#NULL_RECORDING_ID} if not found.
     */
    public long findLastTermRecordingId()
    {
        final Entry lastTerm = findLastTerm();
        return null != lastTerm ? lastTerm.recordingId : RecordingPos.NULL_RECORDING_ID;
    }

    /**
     * Find the last leadership term in the recording log.
     *
     * @return the last leadership term in the recording log.
     */
    public Entry findLastTerm()
    {
        for (int i = entriesCache.size() - 1; i >= 0; i--)
        {
            final Entry entry = entriesCache.get(i);
            if (isValidTerm(entry))
            {
                return entry;
            }
        }

        return null;
    }

    /**
     * Get the term {@link Entry} for a given leadership term id.
     *
     * @param leadershipTermId to get {@link Entry} for.
     * @return the {@link Entry} if found or throw {@link IllegalArgumentException} if no entry exists for term.
     */
    public Entry getTermEntry(final long leadershipTermId)
    {
        final int index = (int)cacheIndexByLeadershipTermIdMap.get(leadershipTermId);
        if (NULL_VALUE != index)
        {
            return entriesCache.get(index);
        }

        throw new ClusterException("unknown leadershipTermId=" + leadershipTermId);
    }

    /**
     * Find the term {@link Entry} for a given leadership term id.
     *
     * @param leadershipTermId to get {@link Entry} for.
     * @return the {@link Entry} if found or null if not found.
     */
    public Entry findTermEntry(final long leadershipTermId)
    {
        final int index = (int)cacheIndexByLeadershipTermIdMap.get(leadershipTermId);
        if (NULL_VALUE != index)
        {
            return entriesCache.get(index);
        }

        return null;
    }

    /**
     * Get the latest snapshot {@link Entry} in the log.
     *
     * @param serviceId for the snapshot.
     * @return the latest snapshot {@link Entry} in the log or null if no snapshot exists.
     */
    public Entry getLatestSnapshot(final int serviceId)
    {
        for (int i = entriesCache.size() - 1; i >= 0; i--)
        {
            final Entry entry = entriesCache.get(i);
            if (isValidSnapshot(entry) && ConsensusModule.Configuration.SERVICE_ID == entry.serviceId)
            {
                if (ConsensusModule.Configuration.SERVICE_ID == serviceId)
                {
                    return entry;
                }

                final int serviceSnapshotIndex = i - (serviceId + 1);
                if (serviceSnapshotIndex >= 0)
                {
                    final Entry snapshot = entriesCache.get(serviceSnapshotIndex);
                    if (isValidSnapshot(snapshot) && serviceId == snapshot.serviceId)
                    {
                        return snapshot;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Invalidate the last snapshot taken by the cluster so that on restart it can revert to the previous one.
     *
     * @return true if the latest snapshot was found and marked as invalid so it will not be reloaded.
     */
    public boolean invalidateLatestSnapshot()
    {
        int index = -1;
        for (int i = entriesCache.size() - 1; i >= 0; i--)
        {
            final Entry entry = entriesCache.get(i);
            if (isValidSnapshot(entry) && ConsensusModule.Configuration.SERVICE_ID == entry.serviceId)
            {
                if (!cacheIndexByLeadershipTermIdMap.containsKey(entry.leadershipTermId))
                {
                    throw new ClusterException(
                        "no matching term for snapshot: leadershipTermId=" + entry.leadershipTermId);
                }

                index = i;
                break;
            }
        }

        if (index >= 0)
        {
            int serviceId = ConsensusModule.Configuration.SERVICE_ID;
            for (int i = index; i >= 0; i--)
            {
                final Entry entry = entriesCache.get(i);
                if (isValidSnapshot(entry) && entry.serviceId == serviceId)
                {
                    invalidateEntry(entry.leadershipTermId, entry.entryIndex);
                    serviceId++;
                }
                else
                {
                    break;
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Get the {@link Entry#timestamp} for a term.
     *
     * @param leadershipTermId to get {@link Entry#timestamp} for.
     * @return the timestamp or {@link io.aeron.Aeron#NULL_VALUE} if not found.
     */
    public long getTermTimestamp(final long leadershipTermId)
    {
        final int index = (int)cacheIndexByLeadershipTermIdMap.get(leadershipTermId);
        if (NULL_VALUE != index)
        {
            return entriesCache.get(index).timestamp;
        }

        return NULL_VALUE;
    }

    /**
     * Create a recovery plan for the cluster so that when the steps are replayed the plan will bring the cluster
     * back to the latest stable state.
     *
     * @param archive               to lookup recording descriptors.
     * @param serviceCount          of services that may have snapshots.
     * @param replicatedRecordingId leader's recordingId used for replicating
     * @return a new {@link RecoveryPlan} for the cluster.
     */
    public RecoveryPlan createRecoveryPlan(
        final AeronArchive archive, final int serviceCount, final long replicatedRecordingId)
    {
        final ArrayList<Snapshot> snapshots = new ArrayList<>();
        final MutableReference<Log> logRef = new MutableReference<>();
        planRecovery(snapshots, logRef, entriesCache, archive, serviceCount, replicatedRecordingId);

        long lastLeadershipTermId = NULL_VALUE;
        long lastTermBaseLogPosition = 0;
        long committedLogPosition = 0;
        long appendedLogPosition = 0;

        final int snapshotStepsSize = snapshots.size();
        if (snapshotStepsSize > 0)
        {
            final Snapshot snapshot = snapshots.get(0);

            lastLeadershipTermId = snapshot.leadershipTermId;
            lastTermBaseLogPosition = snapshot.termBaseLogPosition;
            appendedLogPosition = snapshot.logPosition;
            committedLogPosition = snapshot.logPosition;
        }

        final Log log = logRef.get();
        if (null != log)
        {
            lastLeadershipTermId = log.leadershipTermId;
            lastTermBaseLogPosition = log.termBaseLogPosition;
            appendedLogPosition = log.stopPosition;
            committedLogPosition = NULL_POSITION != log.logPosition ? log.logPosition : committedLogPosition;
        }

        return new RecoveryPlan(
            lastLeadershipTermId,
            lastTermBaseLogPosition,
            appendedLogPosition,
            committedLogPosition,
            snapshots,
            log);
    }

    /**
     * Create a recovery plan that has only snapshots. Used for dynamicJoin snapshot load.
     *
     * @param snapshots to construct plan from.
     * @return a new {@link RecoveryPlan} for the cluster.
     */
    public static RecoveryPlan createRecoveryPlan(final ArrayList<RecordingLog.Snapshot> snapshots)
    {
        long lastLeadershipTermId = NULL_VALUE;
        long lastTermBaseLogPosition = 0;
        long committedLogPosition = 0;
        long appendedLogPosition = 0;

        final int snapshotStepsSize = snapshots.size();
        if (snapshotStepsSize > 0)
        {
            final Snapshot snapshot = snapshots.get(0);

            lastLeadershipTermId = snapshot.leadershipTermId;
            lastTermBaseLogPosition = snapshot.termBaseLogPosition;
            appendedLogPosition = snapshot.logPosition;
            committedLogPosition = snapshot.logPosition;
        }

        return new RecoveryPlan(
            lastLeadershipTermId,
            lastTermBaseLogPosition,
            appendedLogPosition,
            committedLogPosition,
            snapshots,
            null);
    }

    /**
     * Is the given leadershipTermId unknown for the log?
     *
     * @param leadershipTermId to check.
     * @return true if term has not yet been appended otherwise false.
     */
    public boolean isUnknown(final long leadershipTermId)
    {
        return NULL_VALUE == cacheIndexByLeadershipTermIdMap.get(leadershipTermId);
    }

    /**
     * Append a log entry for a leadership term. Terms must be appended in ascending order.
     *
     * @param recordingId         of the log in the archive.
     * @param leadershipTermId    for the appended term.
     * @param termBaseLogPosition for the beginning of the term.
     * @param timestamp           at the beginning of the appended term.
     */
    public void appendTerm(
        final long recordingId, final long leadershipTermId, final long termBaseLogPosition, final long timestamp)
    {
        validateTermRecordingId(recordingId);

        long logPosition = NULL_POSITION;

        if (!entriesCache.isEmpty())
        {
            if (cacheIndexByLeadershipTermIdMap.containsKey(leadershipTermId))
            {
                throw new ClusterException("duplicate TERM entry for leadershipTermId=" + leadershipTermId);
            }

            final long previousLeadershipTermId = leadershipTermId - 1;
            if (cacheIndexByLeadershipTermIdMap.containsKey(previousLeadershipTermId))
            {
                commitLogPosition(previousLeadershipTermId, termBaseLogPosition);
            }

            final Entry nextTermEntry = findTermEntry(leadershipTermId + 1);
            if (null != nextTermEntry)
            {
                logPosition = nextTermEntry.termBaseLogPosition;
            }
        }

        final int index = append(
            ENTRY_TYPE_TERM,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            NULL_VALUE);

        cacheIndexByLeadershipTermIdMap.put(leadershipTermId, index);
    }

    /**
     * Append a log entry for a snapshot. Snapshots must be for the current term.
     *
     * @param recordingId         in the archive for the snapshot.
     * @param leadershipTermId    for the current term
     * @param termBaseLogPosition at the beginning of the leadership term.
     * @param logPosition         within the current term or accumulated length for the log.
     * @param timestamp           at which the snapshot was taken.
     * @param serviceId           for which the snapshot is recorded.
     */
    public void appendSnapshot(
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int serviceId)
    {
        validateRecordingId(recordingId);

        if (!restoreInvalidSnapshot(
            recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId))
        {
            append(
                ENTRY_TYPE_SNAPSHOT,
                recordingId,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                timestamp,
                serviceId);
        }
    }

    /**
     * Commit the log position reached in a leadership term.
     *
     * @param leadershipTermId for committing the term position reached.
     * @param logPosition      reached in the leadership term.
     */
    public void commitLogPosition(final long leadershipTermId, final long logPosition)
    {
        final int index = (int)cacheIndexByLeadershipTermIdMap.get(leadershipTermId);
        if (NULL_VALUE == index)
        {
            throw new ClusterException("unknown leadershipTermId=" + leadershipTermId);
        }

        final Entry entry = entriesCache.get(index);
        if (entry.logPosition != logPosition)
        {
            commitEntryLogPosition(entry.entryIndex, logPosition);
            entriesCache.set(index, entry.logPosition(logPosition));
        }
    }

    /**
     * Invalidate an entry in the log so it is no longer valid. Be careful that the recording log is not left in an
     * invalid state for recovery.
     *
     * @param leadershipTermId to match for validation.
     * @param entryIndex       reached in the leadership term.
     * @see #invalidateLatestSnapshot()
     */
    public void invalidateEntry(final long leadershipTermId, final int entryIndex)
    {
        Entry invalidEntry = null;

        for (int i = entriesCache.size() - 1; i >= 0; i--)
        {
            final Entry entry = entriesCache.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
            {
                invalidEntry = entry.invalidate();
                entriesCache.set(i, invalidEntry);

                if (ENTRY_TYPE_TERM == entry.type)
                {
                    cacheIndexByLeadershipTermIdMap.remove(leadershipTermId);
                }
                else if (ENTRY_TYPE_SNAPSHOT == entry.type)
                {
                    invalidSnapshots.add(i);
                }

                break;
            }
        }

        if (null == invalidEntry)
        {
            throw new ClusterException("unknown entry index: " + entryIndex);
        }

        final int invalidEntryType = ENTRY_TYPE_INVALID_FLAG | invalidEntry.type;
        buffer.putInt(0, invalidEntryType, LITTLE_ENDIAN);
        persistToStorage(entryIndex, ENTRY_TYPE_OFFSET, SIZE_OF_INT);
    }

    /**
     * Remove an entry in the log and reload the caches.
     *
     * @param leadershipTermId to match for validation.
     * @param entryIndex       reached in the leadership term.
     */
    void removeEntry(final long leadershipTermId, final int entryIndex)
    {
        int index = -1;

        for (int i = entriesCache.size() - 1; i >= 0; i--)
        {
            final Entry entry = entriesCache.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
            {
                index = entry.entryIndex;
                break;
            }
        }

        if (-1 == index)
        {
            throw new ClusterException("unknown entry index: " + entryIndex);
        }

        buffer.putInt(0, NULL_VALUE, LITTLE_ENDIAN);
        persistToStorage(index, ENTRY_TYPE_OFFSET, SIZE_OF_INT);

        reload();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "RecordingLog{" +
            "entries=" + entriesCache +
            ", cacheIndex=" + cacheIndexByLeadershipTermIdMap +
            '}';
    }

    static void addSnapshots(
        final ArrayList<Snapshot> snapshots,
        final ArrayList<Entry> entries,
        final int serviceCount,
        final int snapshotIndex)
    {
        final Entry snapshot = entries.get(snapshotIndex);
        snapshots.add(new Snapshot(
            snapshot.recordingId,
            snapshot.leadershipTermId,
            snapshot.termBaseLogPosition,
            snapshot.logPosition,
            snapshot.timestamp,
            snapshot.serviceId));

        for (int i = 1; i <= serviceCount; i++)
        {
            if ((snapshotIndex - i) < 0)
            {
                throw new ClusterException("snapshot missing for service at index " + i + " in " + entries);
            }

            final Entry entry = entries.get(snapshotIndex - i);

            if (ENTRY_TYPE_SNAPSHOT == entry.type &&
                entry.leadershipTermId == snapshot.leadershipTermId &&
                entry.logPosition == snapshot.logPosition)
            {
                snapshots.add(entry.serviceId + 1, new Snapshot(
                    entry.recordingId,
                    entry.leadershipTermId,
                    entry.termBaseLogPosition,
                    entry.logPosition,
                    entry.timestamp,
                    entry.serviceId));
            }
        }
    }

    private static void validateRecordingId(final long recordingId)
    {
        if (NULL_VALUE == recordingId)
        {
            throw new ClusterException("invalid recordingId=" + recordingId);
        }
    }

    private void validateTermRecordingId(final long recordingId)
    {
        validateRecordingId(recordingId);

        if (recordingId != termRecordingId)
        {
            if (NULL_VALUE == termRecordingId)
            {
                termRecordingId = recordingId;
            }
            else
            {
                throw new ClusterException(
                    "invalid TERM recordingId=" + recordingId + ", expected recordingId=" + termRecordingId);
            }
        }
    }

    private boolean restoreInvalidSnapshot(
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int serviceId)
    {
        for (int i = invalidSnapshots.size() - 1; i >= 0; i--)
        {
            final int entryCacheIndex = invalidSnapshots.getInt(i);
            final Entry entry = entriesCache.get(entryCacheIndex);

            if (matchesEntry(entry, leadershipTermId, termBaseLogPosition, logPosition, serviceId))
            {
                final Entry validatedEntry = new Entry(
                    recordingId,
                    leadershipTermId,
                    termBaseLogPosition,
                    logPosition,
                    timestamp,
                    serviceId,
                    ENTRY_TYPE_SNAPSHOT,
                    true,
                    entry.entryIndex);

                writeEntryToBuffer(validatedEntry, buffer);
                persistToStorage(entry.entryIndex, 0, ENTRY_LENGTH);

                entriesCache.set(entryCacheIndex, validatedEntry);
                invalidSnapshots.fastUnorderedRemove(i);

                return true;
            }
        }

        return false;
    }

    private int append(
        final int entryType,
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int serviceId)
    {
        final Entry entry = new Entry(
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            serviceId,
            entryType,
            true,
            nextEntryIndex);

        writeEntryToBuffer(entry, buffer);
        persistToStorage(entry.entryIndex, 0, ENTRY_LENGTH);

        nextEntryIndex++;

        final ArrayList<Entry> entries = this.entriesCache;
        final int size = entries.size();
        int index = size;

        for (int i = size - 1; i >= 0; i--)
        {
            final Entry e = entries.get(i);
            if (e.leadershipTermId > leadershipTermId ||
                (leadershipTermId == e.leadershipTermId &&
                ENTRY_TYPE_SNAPSHOT == e.type &&
                e.logPosition > logPosition))
            {
                index--;
            }
            else
            {
                break;
            }
        }

        if (index < size)
        {
            entries.add(null);
            for (int i = size - 1; i >= index; i--)
            {
                entries.set(i + 1, entries.get(i));
            }
            entries.set(index, entry);

            final Long2LongHashMap.EntryIterator entryIterator = cacheIndexByLeadershipTermIdMap.entrySet().iterator();
            while (entryIterator.hasNext())
            {
                entryIterator.next();
                if (entryIterator.getLongKey() > leadershipTermId)
                {
                    entryIterator.setValue(entryIterator.getLongValue() + 1);
                }
            }

            for (int i = invalidSnapshots.size() - 1; i >= 0; i--)
            {
                final int snapshotIndex = invalidSnapshots.getInt(i);
                if (snapshotIndex >= index)
                {
                    invalidSnapshots.set(i, snapshotIndex + 1);
                }
                else
                {
                    break;
                }
            }
        }
        else
        {
            entries.add(entry);
        }

        return index;
    }

    static void writeEntryToBuffer(final Entry entry, final UnsafeBuffer buffer)
    {
        buffer.putLong(RECORDING_ID_OFFSET, entry.recordingId, LITTLE_ENDIAN);
        buffer.putLong(LEADERSHIP_TERM_ID_OFFSET, entry.leadershipTermId, LITTLE_ENDIAN);
        buffer.putLong(TERM_BASE_LOG_POSITION_OFFSET, entry.termBaseLogPosition, LITTLE_ENDIAN);
        buffer.putLong(LOG_POSITION_OFFSET, entry.logPosition, LITTLE_ENDIAN);
        buffer.putLong(TIMESTAMP_OFFSET, entry.timestamp, LITTLE_ENDIAN);
        buffer.putInt(SERVICE_ID_OFFSET, entry.serviceId, LITTLE_ENDIAN);
        buffer.putInt(ENTRY_TYPE_OFFSET, entry.type, LITTLE_ENDIAN);
    }

    private void persistToStorage(final int entryIndex, final int offset, final int length)
    {
        byteBuffer.limit(length).position(0);
        final long position = (entryIndex * (long)ENTRY_LENGTH) + offset;

        try
        {
            if (length != fileChannel.write(byteBuffer, position))
            {
                throw new ClusterException("failed to write field atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void captureEntriesFromBuffer(
        final ByteBuffer byteBuffer, final UnsafeBuffer buffer, final ArrayList<Entry> entries)
    {
        for (int i = 0, length = byteBuffer.limit(); i < length; i += ENTRY_LENGTH)
        {
            final int entryType = buffer.getInt(i + ENTRY_TYPE_OFFSET);
            if (NULL_VALUE != entryType)
            {
                final int type = entryType & ~ENTRY_TYPE_INVALID_FLAG;
                final boolean isValid = (entryType & ENTRY_TYPE_INVALID_FLAG) == 0;

                final Entry entry = new Entry(
                    buffer.getLong(i + RECORDING_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + LEADERSHIP_TERM_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TERM_BASE_LOG_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + LOG_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TIMESTAMP_OFFSET, LITTLE_ENDIAN),
                    buffer.getInt(i + SERVICE_ID_OFFSET, LITTLE_ENDIAN),
                    type,
                    isValid,
                    nextEntryIndex);

                if (ENTRY_TYPE_TERM == type)
                {
                    validateTermRecordingId(entry.recordingId);
                }

                entries.add(entry);
            }

            ++nextEntryIndex;
        }
    }

    private static void syncDirectory(final File dir)
    {
        try (FileChannel fileChannel = FileChannel.open(dir.toPath()))
        {
            fileChannel.force(true);
        }
        catch (final IOException ignore)
        {
        }
    }

    private void commitEntryLogPosition(final int entryIndex, final long value)
    {
        buffer.putLong(0, value, LITTLE_ENDIAN);
        persistToStorage(entryIndex, LOG_POSITION_OFFSET, SIZE_OF_LONG);
    }

    private static void planRecovery(
        final ArrayList<Snapshot> snapshots,
        final MutableReference<Log> logRef,
        final ArrayList<Entry> entries,
        final AeronArchive archive,
        final int serviceCount,
        final long replicatedRecordingId)
    {
        if (entries.isEmpty())
        {
            if (Aeron.NULL_VALUE != replicatedRecordingId)
            {
                final RecordingExtent recordingExtent = new RecordingExtent();
                if (archive.listRecording(replicatedRecordingId, recordingExtent) == 0)
                {
                    throw new ClusterException("unknown recording id: " + replicatedRecordingId);
                }

                logRef.set(new Log(
                    replicatedRecordingId,
                    NULL_VALUE,
                    0,
                    0,
                    recordingExtent.startPosition,
                    recordingExtent.stopPosition,
                    recordingExtent.initialTermId,
                    recordingExtent.termBufferLength,
                    recordingExtent.mtuLength,
                    recordingExtent.sessionId));
            }

            return;
        }

        int logIndex = -1;
        int snapshotIndex = -1;

        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (-1 == snapshotIndex && isValidSnapshot(entry) &&
                entry.serviceId == ConsensusModule.Configuration.SERVICE_ID)
            {
                snapshotIndex = i;
            }
            else if (-1 == logIndex &&
                entry.isValid &&
                ENTRY_TYPE_TERM == entry.type &&
                NULL_VALUE != entry.recordingId)
            {
                logIndex = i;
            }
            else if (-1 != snapshotIndex && -1 != logIndex)
            {
                break;
            }
        }

        if (-1 != snapshotIndex)
        {
            addSnapshots(snapshots, entries, serviceCount, snapshotIndex);
        }

        if (-1 != logIndex)
        {
            final Entry entry = entries.get(logIndex);
            final RecordingExtent recordingExtent = new RecordingExtent();
            if (archive.listRecording(entry.recordingId, recordingExtent) == 0)
            {
                throw new ClusterException("unknown recording id: " + entry.recordingId);
            }

            final long startPosition = -1 == snapshotIndex ?
                recordingExtent.startPosition : snapshots.get(0).logPosition;

            logRef.set(new Log(
                entry.recordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                entry.logPosition,
                startPosition,
                recordingExtent.stopPosition,
                recordingExtent.initialTermId,
                recordingExtent.termBufferLength,
                recordingExtent.mtuLength,
                recordingExtent.sessionId));
        }
    }

    private static boolean isValidSnapshot(final Entry entry)
    {
        return entry.isValid && ENTRY_TYPE_SNAPSHOT == entry.type;
    }

    private static boolean isValidTerm(final Entry entry)
    {
        return ENTRY_TYPE_TERM == entry.type && entry.isValid;
    }

    private static boolean matchesEntry(
        final Entry entry,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final int serviceId)
    {
        return entry.leadershipTermId == leadershipTermId &&
            entry.termBaseLogPosition == termBaseLogPosition &&
            entry.logPosition == logPosition &&
            entry.serviceId == serviceId;
    }
}
