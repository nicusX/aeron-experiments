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

import io.aeron.*;
import io.aeron.archive.client.ArchiveException;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.CountedErrorHandler;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Consumes an {@link Image} and records data to file using a {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    enum State
    {
        INIT, RECORDING, INACTIVE, STOPPED
    }

    private final boolean isAutoStop;
    private volatile boolean isAborted = false;
    private final long correlationId;
    private final long recordingId;
    private long progressEventPosition;
    private final int blockLengthLimit;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private final String originalChannel;
    private final ControlSession controlSession;
    private final CountedErrorHandler countedErrorHandler;
    private State state = State.INIT;
    private String errorMessage = null;
    private int errorCode = ArchiveException.GENERIC;

    RecordingSession(
        final long correlationId,
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final String originalChannel,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final Archive.Context ctx,
        final ControlSession controlSession,
        final boolean isAutoStop)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;
        this.controlSession = controlSession;
        this.isAutoStop = isAutoStop;
        this.countedErrorHandler = ctx.countedErrorHandler();
        this.progressEventPosition = image.joinPosition();

        blockLengthLimit = Math.min(image.termBufferLength(), ctx.fileIoMaxLength());
        recordingWriter = new RecordingWriter(recordingId, startPosition, segmentLength, image, ctx);
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return recordingId;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return state == State.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        recordingWriter.close();
        CloseHelper.close(countedErrorHandler, position);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            state = State.INACTIVE;
        }

        if (State.INIT == state)
        {
            workCount += init();
        }

        if (State.RECORDING == state)
        {
            workCount += record();
        }

        if (State.INACTIVE == state)
        {
            recordingWriter.close();
            state(State.STOPPED);
            workCount++;

            if (null != recordingEventsProxy)
            {
                recordingEventsProxy.stopped(recordingId, image.joinPosition(), position.getWeak());
            }
        }

        return workCount;
    }

    void abortClose()
    {
        recordingWriter.close();
    }

    long correlationId()
    {
        return correlationId;
    }

    Counter recordingPosition()
    {
        return position;
    }

    long recordedPosition()
    {
        if (position.isClosed())
        {
            return NULL_POSITION;
        }

        return position.get();
    }

    Subscription subscription()
    {
        return image.subscription();
    }

    ControlSession controlSession()
    {
        return controlSession;
    }

    boolean isAutoStop()
    {
        return isAutoStop;
    }

    void sendPendingError(final ControlResponseProxy controlResponseProxy)
    {
        if (null != errorMessage && !controlSession.isDone())
        {
            controlSession.attemptErrorResponse(correlationId, errorCode, errorMessage, controlResponseProxy);
        }
    }

    private int init()
    {
        try
        {
            recordingWriter.init();
        }
        catch (final Throwable ex)
        {
            errorMessage = ex.getClass().getName() + ": " + ex.getMessage();
            recordingWriter.close();
            state(State.STOPPED);
            LangUtil.rethrowUnchecked(ex);
        }

        if (null != recordingEventsProxy)
        {
            recordingEventsProxy.started(
                recordingId,
                image.joinPosition(),
                image.sessionId(),
                image.subscription().streamId(),
                originalChannel,
                image.sourceIdentity());
        }

        state(State.RECORDING);

        return 1;
    }

    private int record()
    {
        try
        {
            final int workCount = image.blockPoll(recordingWriter, blockLengthLimit);
            if (workCount > 0)
            {
                this.position.setOrdered(recordingWriter.position());
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                state(State.INACTIVE);
            }

            if (null != recordingEventsProxy)
            {
                final long recordedPosition = recordingWriter.position();
                if (progressEventPosition < recordedPosition)
                {
                    if (recordingEventsProxy.progress(recordingId, image.joinPosition(), recordedPosition))
                    {
                        progressEventPosition = recordedPosition;
                    }
                }
            }

            return workCount;
        }
        catch (final ArchiveException ex)
        {
            countedErrorHandler.onError(ex);
            errorMessage = ex.getMessage();
            errorCode = ex.errorCode();
            state(State.INACTIVE);
        }
        catch (final Throwable ex)
        {
            countedErrorHandler.onError(ex);
            errorMessage = ex.getClass().getName() + ": " + ex.getMessage();
            state(State.INACTIVE);
        }

        return 1;
    }

    private void state(final State newState)
    {
        //System.out.println("RecordingSession: " + state + " -> " + newState);
        state = newState;
    }
}
