/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.cluster.tutorial;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;

import java.util.Map;
import java.util.HashMap;

import java.util.Objects;
import java.util.Arrays;

/**
 * Auction service implementing the business logic.
 */
// tag::new_service[]
public class BasicAuctionClusteredService implements ClusteredService
// end::new_service[]
{
    static final int CORRELATION_ID_OFFSET = 0;
    static final int ITEM_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int PRICE_OFFSET = ITEM_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int BID_MESSAGE_LENGTH = PRICE_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
    static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + BitUtil.SIZE_OF_BYTE;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
    private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();

    // tag::state[]
    private final Auction auction = new Auction();
    // end::state[]
    private Cluster cluster;
    private IdleStrategy idleStrategy;

    /**
     * {@inheritDoc}
     */
    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;                      // <1>
        this.idleStrategy = cluster.idleStrategy();  // <2>

        if (null != snapshotImage)                   // <3>
        {
            loadSnapshot(cluster, snapshotImage);
        }
    }
    // end::start[]

    /**
     * {@inheritDoc}
     */
    // tag::message[]
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);                   // <1>
        final long itemId = buffer.getLong(offset + ITEM_ID_OFFSET);
        final long price = buffer.getLong(offset + PRICE_OFFSET);

        // System.out.printf("[onSessionMessage] Received correlationId=%d for itemId=%d, price=%d%n", correlationId, itemId, price);
        if (price == -1)
        { // This is a read request
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
            egressMessageBuffer.putLong(ITEM_ID_OFFSET, itemId);
            egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice(itemId));
            egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, (byte)1); // always success for read

            idleStrategy.reset();
            while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0)
            {
                idleStrategy.idle();
            }
        } else {
            final boolean bidSucceeded = auction.attemptBid(price, itemId);                          // <2>

            if (null != session)                                                                         // <3>
            {
                egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);                       // <4>
                egressMessageBuffer.putLong(ITEM_ID_OFFSET, itemId);
                egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice(itemId));
                egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, bidSucceeded ? (byte)1 : (byte)0);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0)                 // <5>
                {
                    idleStrategy.idle();                                                                 // <6>
                }
            } 
        }
    }
    // end::message[]

    /**
     * {@inheritDoc}
     */
    // tag::takeSnapshot[]
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        System.out.println("onTakeSnapshot(snapshotPublication: " + snapshotPublication + ")");
        snapshotBuffer.putInt(0, auction.getSize()); // Always 10
        int offset = BitUtil.SIZE_OF_INT;
        for (int i = 0; i < auction.getSize(); i++) {
            snapshotBuffer.putLong(offset, auction.getBestPrice(i));
            offset += BitUtil.SIZE_OF_LONG;
        }

        idleStrategy.reset();
        while (snapshotPublication.offer(snapshotBuffer, 0, offset) < 0) {
            idleStrategy.idle();
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        System.out.println("onLoadSnapshot(snapshotImage: " + snapshotImage + ")");
        final MutableBoolean isAllDataLoaded = new MutableBoolean(false);
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final int count = buffer.getInt(offset);
            offset += BitUtil.SIZE_OF_INT;
            for (int i = 0; i < count; i++) {
                auction.loadInitialState((long) i, buffer.getLong(offset));
                offset += BitUtil.SIZE_OF_LONG;
            }
            isAllDataLoaded.set(true);
        };

        while (!snapshotImage.isEndOfStream()) {
            final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 1);

            if (isAllDataLoaded.value) {
                break;
            }

            idleStrategy.idle(fragmentsPolled);
        }

        assert snapshotImage.isEndOfStream();
        assert isAllDataLoaded.value;
    }
    // end::loadSnapshot[]

    /**
     * {@inheritDoc}
     */
    public void onRoleChange(final Cluster.Role newRole)
    {
        System.out.println("onRoleChange(newRole: " + newRole + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onTerminate(final Cluster cluster)
    {
        System.out.println("onTerminate(clutster: " + cluster + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("onSessionClose(timestamp: " + timestamp + ", session: " + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("onSessionClose(timestamp: " + timestamp + ", session: " + session + ", reason: " + closeReason + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        System.out.println("onTimerEvent(timestamp: " + timestamp + ", correlationId: " + correlationId + ")");
    }

    static class Auction
    {
        // private final Map<Long, Long> itemBestPrices = new HashMap<>();
        private final long[] bestPrices = new long[10];

        void loadInitialState(final long itemId, final long price)
        {
            if (itemId >= 0 && itemId < bestPrices.length) {
                bestPrices[(int) itemId] = price;
            }
        }

        long[] getBestPrices()
        {
            return bestPrices;
        }

        int getSize()
        {
            return bestPrices.length;
        }

        boolean attemptBid(final long price, final long itemId)
        {
            if (itemId < 0 || itemId >= bestPrices.length) {
                System.err.println("Invalid itemId: " + itemId);
                return false;
            }

            final int index = (int) itemId;
            final long currentPrice = bestPrices[index];

            if (price <= currentPrice) {
                return false;
            }

            bestPrices[index] = price;
            return true;
        }

        long getBestPrice(final long itemId)
        {
            if (itemId < 0 || itemId >= bestPrices.length) {
                System.err.println("Invalid itemId: " + itemId);
                return 0;
            }
            return bestPrices[(int) itemId];
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final Auction auction = (Auction) o;
            return Arrays.equals(bestPrices, auction.bestPrices);
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            return Arrays.hashCode(bestPrices);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Auction{" +
                "bestPrices=" + bestPrices +
                '}';
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final BasicAuctionClusteredService that = (BasicAuctionClusteredService)o;

        return auction.equals(that.auction);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(auction);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "BasicAuctionClusteredService{" +
            "auction=" + Arrays.toString(auction.getBestPrices()) +
            '}';
    }
}
