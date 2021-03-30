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
package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ListRecordingsSessionTest
{
    private static final long CAPACITY = 1024 * 1024;
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final long[] recordingIds = new long[3];
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final EpochClock clock = mock(EpochClock.class);

    private Catalog catalog;
    private final long correlationId = 1;
    private final ControlResponseProxy controlResponseProxy = mock(ControlResponseProxy.class);
    private final ControlSession controlSession = mock(ControlSession.class);
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();

    @BeforeEach
    public void before()
    {
        catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, null);
        recordingIds[0] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1, "channelG", "channelG?tag=f", "sourceA");
        recordingIds[1] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 2, "channelH", "channelH?tag=f", "sourceV");
        recordingIds[2] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 3, "channelK", "channelK?tag=f", "sourceB");
    }

    @AfterEach
    public void after()
    {
        CloseHelper.close(catalog);
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void shouldSendAllDescriptors()
    {
        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            0,
            3,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer
        );

        final MutableLong counter = new MutableLong(0);
        when(controlSession.sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy)))
            .then(verifySendDescriptor(counter));

        session.doWork();
        verify(controlSession, times(3)).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));
    }

    @Test
    public void shouldSend2Descriptors()
    {
        final int fromId = 1;
        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            fromId,
            2,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer);

        final MutableLong counter = new MutableLong(fromId);
        when(controlSession.sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy)))
            .then(verifySendDescriptor(counter));

        session.doWork();
        verify(controlSession, times(2)).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));
    }

    @Test
    public void shouldResendDescriptorWhenSendFails()
    {
        final long fromRecordingId = 1;
        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            fromRecordingId,
            1,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer);

        when(controlSession.sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy))).thenReturn(0);
        session.doWork();
        verify(controlSession, times(1)).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));

        final MutableLong counter = new MutableLong(fromRecordingId);
        when(controlSession.sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy)))
            .then(verifySendDescriptor(counter));

        session.doWork();
        verify(controlSession, times(2)).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));
    }

    @Test
    public void shouldSendTwoDescriptorsThenRecordingUnknown()
    {
        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            1,
            3,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer);

        final MutableLong counter = new MutableLong(1);
        when(controlSession.sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy)))
            .then(verifySendDescriptor(counter));

        session.doWork();

        verify(controlSession, times(2)).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));
        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(3L), eq(controlResponseProxy));
    }

    @Test
    public void shouldSendRecordingUnknownOnFirst()
    {
        final ListRecordingsSession session = new ListRecordingsSession(
            correlationId,
            3,
            3,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer);

        session.doWork();

        verify(controlSession, never()).sendDescriptor(eq(correlationId), any(), eq(controlResponseProxy));
        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(3L), eq(controlResponseProxy));
    }

    private Answer<Object> verifySendDescriptor(final MutableLong counter)
    {
        return (invocation) ->
        {
            final UnsafeBuffer buffer = invocation.getArgument(1);

            recordingDescriptorDecoder.wrap(
                buffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            final int i = counter.intValue();
            assertEquals(recordingIds[i], recordingDescriptorDecoder.recordingId());
            counter.set(i + 1);

            return buffer.getInt(0);
        };
    }
}