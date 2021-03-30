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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.SessionProxy;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.security.NullCredentialsSupplier.NULL_CREDENTIAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;

public class ArchiveAuthenticationTest
{
    private static final int RECORDED_STREAM_ID = 1033;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(ArchiveSystemTests.TERM_LENGTH)
        .build();

    private static final String CREDENTIALS_STRING = "username=\"admin\"|password=\"secret\"";
    private static final String CHALLENGE_STRING = "I challenge you!";
    private static final String PRINCIPAL_STRING = "I am THE Principal!";

    private final byte[] encodedCredentials = CREDENTIALS_STRING.getBytes();
    private final byte[] encodedChallenge = CHALLENGE_STRING.getBytes();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    private final String aeronDirectoryName = CommonContext.generateRandomDirName();

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);

        archive.context().deleteDirectory();
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldBeAbleToRecordWithDefaultCredentialsAndAuthenticator()
    {
        launchArchivingMediaDriver(null);
        connectClient(null);

        createRecording();
    }

    @Test
    @Timeout(10)
    public void shouldBeAbleToRecordWithAuthenticateOnConnectRequestWithCredentials()
    {
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
        {
            public byte[] encodedCredentials()
            {
                return encodedCredentials;
            }

            public byte[] onChallenge(final byte[] encodedChallenge)
            {
                fail();
                return null;
            }
        });

        final Authenticator authenticator = spy(new Authenticator()
        {
            public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                authenticatorSessionId.value = sessionId;
                assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
            }

            public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                fail();
            }

            public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                sessionProxy.authenticate(PRINCIPAL_STRING.getBytes());
            }

            public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                fail();
            }
        });

        launchArchivingMediaDriver(() -> authenticator);
        connectClient(credentialsSupplier);

        assertEquals(aeronArchive.controlSessionId(), authenticatorSessionId.value);

        createRecording();
    }

    @Test
    @Timeout(10)
    public void shouldBeAbleToRecordWithAuthenticateOnChallengeResponse()
    {
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
        {
            public byte[] encodedCredentials()
            {
                return NULL_CREDENTIAL;
            }

            public byte[] onChallenge(final byte[] encodedChallenge)
            {
                assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                return encodedCredentials;
            }
        });

        final Authenticator authenticator = spy(new Authenticator()
        {
            boolean challengeSuccessful = false;

            public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                authenticatorSessionId.value = sessionId;
                assertEquals(0, encodedCredentials.length);
            }

            public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                assertEquals(sessionId, authenticatorSessionId.value);
                assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
                challengeSuccessful = true;
            }

            public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                sessionProxy.challenge(encodedChallenge);
            }

            public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                if (challengeSuccessful)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.authenticate(PRINCIPAL_STRING.getBytes());
                }
            }
        });

        launchArchivingMediaDriver(() -> authenticator);
        connectClient(credentialsSupplier);

        assertEquals(aeronArchive.controlSessionId(), authenticatorSessionId.value);

        createRecording();
    }

    @Test
    @Timeout(10)
    public void shouldNotBeAbleToConnectWithRejectOnConnectRequest()
    {
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
        {
            public byte[] encodedCredentials()
            {
                return NULL_CREDENTIAL;
            }

            public byte[] onChallenge(final byte[] encodedChallenge)
            {
                assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                return encodedCredentials;
            }
        });

        final Authenticator authenticator = spy(new Authenticator()
        {
            public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                authenticatorSessionId.value = sessionId;
                assertEquals(0, encodedCredentials.length);
            }

            public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                fail();
            }

            public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                sessionProxy.reject();
            }

            public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                fail();
            }
        });

        launchArchivingMediaDriver(() -> authenticator);

        try
        {
            connectClient(credentialsSupplier);
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.AUTHENTICATION_REJECTED, ex.errorCode());
            return;
        }

        fail("should have seen exception");
    }

    @Test
    @Timeout(10)
    public void shouldNotBeAbleToConnectWithRejectOnChallengeResponse()
    {
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
        {
            public byte[] encodedCredentials()
            {
                return NULL_CREDENTIAL;
            }

            public byte[] onChallenge(final byte[] encodedChallenge)
            {
                assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                return encodedCredentials;
            }
        });

        final Authenticator authenticator = spy(new Authenticator()
        {
            boolean challengeRespondedTo = false;

            public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                authenticatorSessionId.value = sessionId;
                assertEquals(0, encodedCredentials.length);
            }

            public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
            {
                assertEquals(sessionId, authenticatorSessionId.value);
                assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
                challengeRespondedTo = true;
            }

            public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                sessionProxy.challenge(encodedChallenge);
            }

            public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                if (challengeRespondedTo)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.reject();
                }
            }
        });

        launchArchivingMediaDriver(() -> authenticator);

        try
        {
            connectClient(credentialsSupplier);
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.AUTHENTICATION_REJECTED, ex.errorCode());
            return;
        }

        fail("should have seen exception");
    }

    private void connectClient(final CredentialsSupplier credentialsSupplier)
    {
        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .credentialsSupplier(credentialsSupplier)
                .aeron(aeron));
    }

    private void launchArchivingMediaDriver(final AuthenticatorSupplier authenticatorSupplier)
    {
        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            testWatcher);

        archive = Archive.launch(
            new Archive.Context()
                .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
                .aeronDirectoryName(aeronDirectoryName)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "archive"))
                .errorHandler(Tests::onError)
                .fileSyncLevel(0)
                .authenticatorSupplier(authenticatorSupplier)
                .threadingMode(ArchiveThreadingMode.SHARED));
    }

    private void createRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = ArchiveSystemTests.awaitRecordingCounterId(counters, publication.sessionId());

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            awaitPosition(counters, counterId, currentPosition);
        }

        aeronArchive.stopRecording(subscriptionId);
    }
}
