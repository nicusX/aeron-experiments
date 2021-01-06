package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SigInt;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pong component of Ping-Pong, using embedded, low-latency MediaDriver
 * <p>
 * Echoes back messages from {@link EmbeddedLowLatencyPing}.
 * @see EmbeddedLowLatencyPing
 *
 * @throws UnknownHostException if something is wrong with the canonical hostname (shouldn't)
 */
public class EmbeddedLowLatencyPong
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
    private static final boolean INFO_FLAG = SampleConfiguration.INFO_FLAG;
    private static final boolean EXCLUSIVE_PUBLICATIONS = SampleConfiguration.EXCLUSIVE_PUBLICATIONS;

    private static final IdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args) throws UnknownHostException
    {
        // Always use embedded media driver
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .termBufferSparseFile(false)
                .useWindowsHighResTimer(true)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(BusySpinIdleStrategy.INSTANCE)
                .receiverIdleStrategy(NoOpIdleStrategy.INSTANCE)
                .senderIdleStrategy(NoOpIdleStrategy.INSTANCE);
        final MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);

        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        if (INFO_FLAG)
        {
            ctx.availableImageHandler(SamplesUtil::printAvailableImage);
            ctx.unavailableImageHandler(SamplesUtil::printUnavailableImage);
        }

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream id " + PING_STREAM_ID);
        System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream id " + PONG_STREAM_ID);
        System.out.println("Using exclusive publications " + EXCLUSIVE_PUBLICATIONS);
        System.out.println("My Canonical Host name: " + Inet4Address.getLocalHost().getCanonicalHostName());

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (Aeron aeron = Aeron.connect(ctx);
             Subscription subscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID);
             Publication publication = EXCLUSIVE_PUBLICATIONS ?
                     aeron.addExclusivePublication(PONG_CHANNEL, PONG_STREAM_ID) :
                     aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID))
        {
            final BufferClaim bufferClaim = new BufferClaim();
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
                    pingHandler(bufferClaim, publication, buffer, offset, length, header);

            while (running.get())
            {
                idleStrategy.idle(subscription.poll(fragmentHandler, FRAME_COUNT_LIMIT));
            }

            System.out.println("Shutting down...");
        }

        CloseHelper.close(driver);
    }

    private static void pingHandler(
            final BufferClaim bufferClaim,
            final Publication pongPublication,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
    {
        PING_HANDLER_IDLE_STRATEGY.reset();
        while (pongPublication.tryClaim(length, bufferClaim) <= 0)
        {
            PING_HANDLER_IDLE_STRATEGY.idle();
        }

        bufferClaim
                .flags(header.flags())
                .putBytes(buffer, offset, length)
                .commit();
    }
}
