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

import io.aeron.Publication;
import io.aeron.security.Authenticator;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.aeron.archive.ControlSession.State.DONE;
import static io.aeron.archive.ControlSession.State.INACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ControlSessionTest
{
    private static final long CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private final ControlSessionDemuxer mockDemuxer = mock(ControlSessionDemuxer.class);
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final Publication mockControlPublication = mock(Publication.class);
    private final ControlResponseProxy mockProxy = mock(ControlResponseProxy.class);
    private final ControlSessionProxy mockSessionProxy = mock(ControlSessionProxy.class);
    private final Authenticator mockAuthenticator = mock(Authenticator.class);
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private ControlSession session;

    @BeforeEach
    public void before()
    {
        session = new ControlSession(
            1,
            2,
            CONNECT_TIMEOUT_MS,
            null,
            mockDemuxer,
            mockControlPublication,
            mockConductor,
            cachedEpochClock,
            mockProxy,
            mockAuthenticator,
            mockSessionProxy);
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);

        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(INACTIVE, session.state());
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationFailsToSend()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);

        session.doWork();
        session.sendOkResponse(1L, mockProxy);
        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(INACTIVE, session.state());
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }
}