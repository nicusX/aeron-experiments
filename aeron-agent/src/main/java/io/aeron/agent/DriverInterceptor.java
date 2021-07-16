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

import net.bytebuddy.asm.Advice;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventLogger.LOGGER;

class DriverInterceptor
{
    static class UntetheredSubscriptionStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(
            final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
        {
            LOGGER.logUntetheredSubscriptionStateChange(oldState, newState, subscriptionId, streamId, sessionId);
        }
    }

    static class NameResolution
    {
        static class NeighborAdded
        {
            @Advice.OnMethodEnter
            static void neighborAdded(final long nowMs, final InetSocketAddress address)
            {
                LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, address);
            }
        }

        static class NeighborRemoved
        {
            @Advice.OnMethodEnter
            static void neighborRemoved(final long nowMs, final InetSocketAddress address)
            {
                LOGGER.logAddress(NAME_RESOLUTION_NEIGHBOR_REMOVED, address);
            }
        }

        static class Resolve
        {
            @Advice.OnMethodEnter
            static void resolveHook(
                final String resolverName,
                final String name,
                final InetAddress address)
            {
                LOGGER.logResolve(DriverEventCode.NAME_RESOLUTION_RESOLVE, resolverName, name, address);
            }
        }
    }

    static class FlowControl
    {
        static class ReceiverAdded
        {
            @Advice.OnMethodEnter
            static void receiverAdded(
                final long receiverId,
                final int sessionId,
                final int streamId,
                final String channel,
                final int receiverCount)
            {
                LOGGER.logFlowControlReceiver(
                    FLOW_CONTROL_RECEIVER_ADDED, receiverId, sessionId, streamId, channel, receiverCount);
            }
        }

        static class ReceiverRemoved
        {
            @Advice.OnMethodEnter
            static void receiverRemoved(
                final long receiverId,
                final int sessionId,
                final int streamId,
                final String channel,
                final int receiverCount)
            {
                LOGGER.logFlowControlReceiver(
                    FLOW_CONTROL_RECEIVER_REMOVED, receiverId, sessionId, streamId, channel, receiverCount);
            }
        }
    }
}
