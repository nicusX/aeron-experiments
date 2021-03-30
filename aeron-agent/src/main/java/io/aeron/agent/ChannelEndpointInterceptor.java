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
package io.aeron.agent;

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import net.bytebuddy.asm.Advice;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventLogger.LOGGER;

class ChannelEndpointInterceptor
{
    static class SenderProxy
    {
        static class RegisterSendChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void registerSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(SEND_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
            }
        }

        static class CloseSendChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(SEND_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
            }
        }
    }

    static class ReceiverProxy
    {
        static class RegisterReceiveChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(RECEIVE_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
            }
        }

        static class CloseReceiveChannelEndpoint
        {
            @Advice.OnMethodEnter
            static void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
            {
                LOGGER.logString(RECEIVE_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
            }
        }
    }

    static class UdpChannelTransport
    {
        static class SendHook
        {
            @Advice.OnMethodEnter
            static void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
            {
                LOGGER.logFrameOut(buffer, address);
            }
        }

        static class ReceiveHook
        {
            @Advice.OnMethodEnter
            static void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
            {
                LOGGER.logFrameIn(buffer, 0, length, address);
            }
        }
    }
}
