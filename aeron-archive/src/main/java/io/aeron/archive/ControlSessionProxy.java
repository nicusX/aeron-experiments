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

import io.aeron.security.SessionProxy;

import static io.aeron.archive.codecs.ControlResponseCode.OK;

class ControlSessionProxy implements SessionProxy
{
    private final ControlResponseProxy controlResponseProxy;
    private ControlSession controlSession;

    ControlSessionProxy(final ControlResponseProxy controlResponseProxy)
    {
        this.controlResponseProxy = controlResponseProxy;
    }

    ControlSessionProxy controlSession(final ControlSession controlSession)
    {
        this.controlSession = controlSession;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return controlSession.sessionId();
    }

    /**
     * {@inheritDoc}
     */
    public boolean challenge(final byte[] encodedChallenge)
    {
        if (controlResponseProxy.sendChallenge(
            controlSession.sessionId(),
            controlSession.correlationId(),
            encodedChallenge,
            controlSession))
        {
            controlSession.challenged();
            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean authenticate(final byte[] encodedPrincipal)
    {
        if (controlResponseProxy.sendResponse(
            controlSession.sessionId(),
            controlSession.correlationId(),
            controlSession.sessionId(),
            OK,
            null,
            controlSession))
        {
            controlSession.authenticate(encodedPrincipal);
            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void reject()
    {
        controlSession.reject();
    }
}
