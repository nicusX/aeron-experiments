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

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Supplier of channel endpoints which extend {@link ReceiveChannelEndpoint} to add specialised behaviour for the
 * receiver.
 */
@FunctionalInterface
public interface ReceiveChannelEndpointSupplier
{
    /**
     * A new instance of a specialised {@link ReceiveChannelEndpoint}.
     *
     * @param udpChannel      on which the receiver is listening.
     * @param dispatcher      for dispatching packets to publication images.
     * @param statusIndicator for the channel.
     * @param context         for the configuration of the driver.
     * @return new instance of a specialised {@link ReceiveChannelEndpoint}.
     */
    ReceiveChannelEndpoint newInstance(
        UdpChannel udpChannel,
        DataPacketDispatcher dispatcher,
        AtomicCounter statusIndicator,
        MediaDriver.Context context);
}
