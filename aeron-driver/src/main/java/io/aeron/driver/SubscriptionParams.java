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
import io.aeron.CommonContext;
import io.aeron.driver.media.UdpChannel;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;

import static io.aeron.CommonContext.*;

final class SubscriptionParams
{
    int initialTermId = 0;
    int termId = 0;
    int termOffset = 0;
    int sessionId = 0;
    boolean hasJoinPosition = false;
    boolean hasSessionId = false;
    boolean isReliable = true;
    boolean isRejoin = true;
    boolean isSparse = true;
    boolean isTether = true;
    InferableBoolean group = InferableBoolean.INFER;
    int initialWindowLength;

    static SubscriptionParams getSubscriptionParams(final ChannelUri channelUri, final MediaDriver.Context context)
    {
        final SubscriptionParams params = new SubscriptionParams();

        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            params.sessionId = Integer.parseInt(sessionIdStr);
            params.hasSessionId = true;
        }

        int count = 0;

        final String initialTermIdStr = channelUri.get(INITIAL_TERM_ID_PARAM_NAME);
        count = initialTermIdStr != null ? count + 1 : count;

        final String termIdStr = channelUri.get(TERM_ID_PARAM_NAME);
        count = termIdStr != null ? count + 1 : count;

        final String termOffsetStr = channelUri.get(TERM_OFFSET_PARAM_NAME);
        count = termOffsetStr != null ? count + 1 : count;

        if (count > 0)
        {
            if (count < 3)
            {
                throw new IllegalArgumentException("params must be used as a complete set: " +
                    INITIAL_TERM_ID_PARAM_NAME + " " +
                    TERM_ID_PARAM_NAME + " " +
                    TERM_OFFSET_PARAM_NAME);
            }

            params.initialTermId = Integer.parseInt(initialTermIdStr);
            params.termId = Integer.parseInt(termIdStr);
            params.termOffset = Integer.parseInt(termOffsetStr);

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

            params.hasJoinPosition = true;
        }

        final String reliableStr = channelUri.get(RELIABLE_STREAM_PARAM_NAME);
        params.isReliable = null != reliableStr ? "true".equals(reliableStr) : context.reliableStream();

        final String rejoinStr = channelUri.get(REJOIN_PARAM_NAME);
        params.isRejoin = null != rejoinStr ? "true".equals(rejoinStr) : context.rejoinStream();

        final String tetherStr = channelUri.get(TETHER_PARAM_NAME);
        params.isTether = null != tetherStr ? "true".equals(tetherStr) : context.tetherSubscriptions();

        final String sparseStr = channelUri.get(SPARSE_PARAM_NAME);
        params.isSparse = null != sparseStr ? "true".equals(sparseStr) : context.termBufferSparseFile();

        final String groupStr = channelUri.get(GROUP_PARAM_NAME);
        params.group = null != groupStr ? InferableBoolean.parse(groupStr) : context.receiverGroupConsideration();

        final int initialWindowLength = UdpChannel.parseBufferLength(channelUri, RECEIVER_WINDOW_LENGTH_PARAM_NAME);
        params.initialWindowLength = 0 != initialWindowLength ? initialWindowLength : context.initialWindowLength();

        return params;
    }

    static void validateInitialWindowForRcvBuf(
        final SubscriptionParams params,
        final UdpChannel udpChannel,
        final int channelSocketRcvbufLength,
        final MediaDriver.Context ctx)
    {
        if (0 != channelSocketRcvbufLength && params.initialWindowLength > channelSocketRcvbufLength)
        {
            throw new IllegalStateException(
                "Initial window greater than SO_RCVBUF for channel: rcv-wnd=" + params.initialWindowLength +
                " so-rcvbuf=" + channelSocketRcvbufLength + " uri=" + udpChannel.originalUriString());
        }
        else if (0 == channelSocketRcvbufLength && params.initialWindowLength > ctx.osDefaultSocketRcvbufLength())
        {
            throw new IllegalStateException(
                "Initial window greater than SO_RCVBUF for channel: rcv-wnd=" + params.initialWindowLength +
                " so-rcvbuf=" + ctx.osDefaultSocketRcvbufLength() + " (OS default)" +
                " uri=" + udpChannel.originalUriString());
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SubscriptionParams{" +
            "initialTermId=" + initialTermId +
            ", termId=" + termId +
            ", termOffset=" + termOffset +
            ", sessionId=" + sessionId +
            ", hasJoinPosition=" + hasJoinPosition +
            ", hasSessionId=" + hasSessionId +
            ", isReliable=" + isReliable +
            ", isRejoin=" + isRejoin +
            ", isSparse=" + isSparse +
            ", isTether=" + isTether +
            ", group=" + group +
            '}';
    }
}
