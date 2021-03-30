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
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.buffer.RawLog;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.SystemUtil;

import static io.aeron.ChannelUri.INVALID_TAG;
import static io.aeron.CommonContext.*;

final class PublicationParams
{
    long lingerTimeoutNs;
    long entityTag = ChannelUri.INVALID_TAG;
    int termLength;
    int mtuLength;
    int initialTermId = 0;
    int termId = 0;
    int termOffset = 0;
    int sessionId = 0;
    boolean hasPosition = false;
    boolean hasSessionId = false;
    boolean isSessionIdTagged = false;
    boolean signalEos = true;
    boolean isSparse;
    boolean spiesSimulateConnection;

    PublicationParams()
    {
    }

    static PublicationParams getPublicationParams(
        final ChannelUri channelUri,
        final MediaDriver.Context ctx,
        final DriverConductor driverConductor,
        final boolean isExclusive,
        final boolean isIpc)
    {
        final PublicationParams params = new PublicationParams(ctx, isIpc);

        params.getEntityTag(channelUri, driverConductor);
        params.getSessionId(channelUri, driverConductor);
        params.getTermBufferLength(channelUri);
        params.getMtuLength(channelUri);
        params.getLingerTimeoutNs(channelUri);
        params.getEos(channelUri);
        params.getSparse(channelUri, ctx);
        params.getSpiesSimulateConnection(channelUri, ctx);

        int count = 0;

        final String initialTermIdStr = channelUri.get(INITIAL_TERM_ID_PARAM_NAME);
        count = initialTermIdStr != null ? count + 1 : count;

        final String termIdStr = channelUri.get(TERM_ID_PARAM_NAME);
        count = termIdStr != null ? count + 1 : count;

        final String termOffsetStr = channelUri.get(TERM_OFFSET_PARAM_NAME);
        count = termOffsetStr != null ? count + 1 : count;

        if (count > 0)
        {
            if (!isExclusive)
            {
                throw new IllegalArgumentException("params: " + INITIAL_TERM_ID_PARAM_NAME + " " + TERM_ID_PARAM_NAME +
                    " " + TERM_OFFSET_PARAM_NAME + " are not supported for concurrent publications");
            }
            if (count < 3)
            {
                throw new IllegalArgumentException("params must be used as a complete set: " +
                    INITIAL_TERM_ID_PARAM_NAME + " " + TERM_ID_PARAM_NAME + " " + TERM_OFFSET_PARAM_NAME);
            }

            params.initialTermId = Integer.parseInt(initialTermIdStr);
            params.termId = Integer.parseInt(termIdStr);
            params.termOffset = Integer.parseInt(termOffsetStr);

            if (params.termOffset > params.termLength)
            {
                throw new IllegalArgumentException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " > " +
                    TERM_LENGTH_PARAM_NAME + "=" + params.termLength);
            }

            if (params.termOffset < 0 || params.termOffset > LogBufferDescriptor.TERM_MAX_LENGTH)
            {
                throw new IllegalArgumentException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " out of range");
            }

            if ((params.termOffset & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
            {
                throw new IllegalArgumentException(
                    TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " must be a multiple of FRAME_ALIGNMENT");
            }

            if (params.termId - params.initialTermId < 0)
            {
                throw new IllegalStateException(
                    "difference greater than 2^31 - 1: " + INITIAL_TERM_ID_PARAM_NAME + "=" +
                    params.initialTermId + " when " + TERM_ID_PARAM_NAME + "=" + params.termId);
            }

            params.hasPosition = true;
        }

        return params;
    }

    private PublicationParams(final MediaDriver.Context context, final boolean isIpc)
    {
        termLength = isIpc ? context.ipcTermBufferLength() : context.publicationTermBufferLength();
        mtuLength = isIpc ? context.ipcMtuLength() : context.mtuLength();
        lingerTimeoutNs = context.publicationLingerTimeoutNs();
        isSparse = context.termBufferSparseFile();
    }

    private void getEntityTag(final ChannelUri channelUri, final DriverConductor driverConductor)
    {
        final String tagParam = channelUri.entityTag();
        if (null != tagParam)
        {
            final long entityTag = Long.parseLong(tagParam);
            validateEntityTag(entityTag, driverConductor);
            this.entityTag = entityTag;
        }
    }

    private void getTermBufferLength(final ChannelUri channelUri)
    {
        final String termLengthParam = channelUri.get(TERM_LENGTH_PARAM_NAME);
        if (null != termLengthParam)
        {
            final int termLength = (int)SystemUtil.parseSize(TERM_LENGTH_PARAM_NAME, termLengthParam);
            LogBufferDescriptor.checkTermLength(termLength);
            validateTermLength(this, termLength);
            this.termLength = termLength;
        }
    }

    private void getMtuLength(final ChannelUri channelUri)
    {
        final String mtuParam = channelUri.get(MTU_LENGTH_PARAM_NAME);
        if (null != mtuParam)
        {
            final int mtuLength = (int)SystemUtil.parseSize(MTU_LENGTH_PARAM_NAME, mtuParam);
            Configuration.validateMtuLength(mtuLength);
            validateMtuLength(this, mtuLength);
            this.mtuLength = mtuLength;
        }
    }

    static void validateMtuForMaxMessage(final PublicationParams params)
    {
        final int termLength = params.termLength;
        final int maxMessageLength = FrameDescriptor.computeMaxMessageLength(termLength);

        if (params.mtuLength > maxMessageLength)
        {
            throw new IllegalStateException("MTU greater than max message length for term length: mtu=" +
                params.mtuLength + " maxMessageLength=" + maxMessageLength + " termLength=" + termLength);
        }
    }

    static void validateTermLength(final PublicationParams params, final int explicitTermLength)
    {
        if (params.isSessionIdTagged && explicitTermLength != params.termLength)
        {
            throw new IllegalArgumentException(
                TERM_LENGTH_PARAM_NAME + "=" + explicitTermLength + " does not match session-id tag value");
        }
    }

    static void validateMtuLength(final PublicationParams params, final int explicitMtuLength)
    {
        if (params.isSessionIdTagged && explicitMtuLength != params.mtuLength)
        {
            throw new IllegalArgumentException(
                MTU_LENGTH_PARAM_NAME + "=" + explicitMtuLength + " does not match session-id tag value");
        }
    }

    static void confirmMatch(
        final ChannelUri channelUri,
        final PublicationParams params,
        final RawLog rawLog,
        final int existingSessionId)
    {
        final int mtuLength = LogBufferDescriptor.mtuLength(rawLog.metaData());
        if (channelUri.containsKey(MTU_LENGTH_PARAM_NAME) && mtuLength != params.mtuLength)
        {
            throw new IllegalStateException("existing publication has different MTU length: existing=" +
                mtuLength + " requested=" + params.mtuLength);
        }

        if (channelUri.containsKey(TERM_LENGTH_PARAM_NAME) && rawLog.termLength() != params.termLength)
        {
            throw new IllegalStateException("existing publication has different term length: existing=" +
                rawLog.termLength() + " requested=" + params.termLength);
        }

        if (channelUri.containsKey(SESSION_ID_PARAM_NAME) && params.sessionId != existingSessionId)
        {
            throw new IllegalStateException("existing publication has different session id: existing=" +
                existingSessionId + " requested=" + params.sessionId);
        }
    }

    static void validateSpiesSimulateConnection(
        final PublicationParams params, final boolean existingSpiesSimulateConnection)
    {
        if (params.spiesSimulateConnection != existingSpiesSimulateConnection)
        {
            throw new IllegalStateException("existing publication has different spiesSimulateConnection: existing=" +
                existingSpiesSimulateConnection + " requested=" + params.spiesSimulateConnection);
        }
    }

    static void validateMtuForSndbuf(
        final PublicationParams params, final int channelSocketSndbufLength, final MediaDriver.Context ctx)
    {
        if (0 != channelSocketSndbufLength && params.mtuLength > channelSocketSndbufLength)
        {
            throw new IllegalStateException(
                "MTU greater than SO_SNDBUF for channel: mtu=" + params.mtuLength +
                " so-sndbuf=" + channelSocketSndbufLength);
        }
        else if (0 == channelSocketSndbufLength && params.mtuLength > ctx.osDefaultSocketSndbufLength())
        {
            throw new IllegalStateException(
                "MTU greater than SO_SNDBUF for channel: mtu=" + params.mtuLength +
                " so-sndbuf=" + ctx.osDefaultSocketSndbufLength() + " (OS default)");
        }
    }

    private void getLingerTimeoutNs(final ChannelUri channelUri)
    {
        final String lingerParam = channelUri.get(LINGER_PARAM_NAME);
        if (null != lingerParam)
        {
            lingerTimeoutNs = SystemUtil.parseDuration(LINGER_PARAM_NAME, lingerParam);
            Configuration.validatePublicationLingerTimeoutNs(lingerTimeoutNs, lingerTimeoutNs);
        }
    }

    private void getSessionId(final ChannelUri channelUri, final DriverConductor driverConductor)
    {
        final String sessionIdStr = channelUri.get(SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            isSessionIdTagged = ChannelUri.isTagged(sessionIdStr);
            if (isSessionIdTagged)
            {
                final NetworkPublication publication = driverConductor.findNetworkPublicationByTag(
                    ChannelUri.getTag(sessionIdStr));

                if (null == publication)
                {
                    throw new IllegalArgumentException(
                        SESSION_ID_PARAM_NAME + "=" + sessionIdStr + " must reference a network publication");
                }

                sessionId = publication.sessionId();
                mtuLength = publication.mtuLength();
                termLength = publication.termBufferLength();
            }
            else
            {
                sessionId = Integer.parseInt(sessionIdStr);
            }

            hasSessionId = true;
        }
    }

    private void getEos(final ChannelUri channelUri)
    {
        final String eosStr = channelUri.get(EOS_PARAM_NAME);
        if (null != eosStr)
        {
            signalEos = "true".equals(eosStr);
        }
    }

    private void getSparse(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String sparseStr = channelUri.get(SPARSE_PARAM_NAME);
        isSparse = null != sparseStr ? "true".equals(sparseStr) : ctx.termBufferSparseFile();
    }

    private void getSpiesSimulateConnection(final ChannelUri channelUri, final MediaDriver.Context ctx)
    {
        final String sscStr = channelUri.get(SPIES_SIMULATE_CONNECTION_PARAM_NAME);
        spiesSimulateConnection = null != sscStr ? "true".equals(sscStr) : ctx.spiesSimulateConnection();
    }

    private static void validateEntityTag(final long entityTag, final DriverConductor driverConductor)
    {
        if (INVALID_TAG == entityTag)
        {
            throw new IllegalArgumentException(INVALID_TAG + " tag is reserved");
        }

        if (null != driverConductor.findNetworkPublicationByTag(entityTag) ||
            null != driverConductor.findIpcPublicationByTag(entityTag))
        {
            throw new IllegalArgumentException(entityTag + " entityTag already in use");
        }
    }

    public String toString()
    {
        return "PublicationParams{" +
            "lingerTimeoutNs=" + lingerTimeoutNs +
            ", entityTag=" + entityTag +
            ", termLength=" + termLength +
            ", mtuLength=" + mtuLength +
            ", initialTermId=" + initialTermId +
            ", termId=" + termId +
            ", termOffset=" + termOffset +
            ", sessionId=" + sessionId +
            ", hasPosition=" + hasPosition +
            ", hasSessionId=" + hasSessionId +
            ", isSessionIdTagged=" + isSessionIdTagged +
            ", isSparse=" + isSparse +
            ", signalEos=" + signalEos +
            ", spiesSimulateConnection=" + spiesSimulateConnection +
            '}';
    }
}
