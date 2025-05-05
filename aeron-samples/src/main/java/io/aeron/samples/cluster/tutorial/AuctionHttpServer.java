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

import io.aeron.Aeron;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.EgressListener;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aeron.CommonContext;

import static spark.Spark.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.net.URLDecoder;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static spark.Spark.*;
import com.google.gson.Gson;

import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import spark.Response;
import java.util.concurrent.locks.LockSupport;
import io.aeron.Publication;

import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredServiceNode.calculatePort;

/**
 * Client for communicating with {@link BasicAuctionClusteredService}.
 */
// tag::client[]
public class AuctionHttpServer implements EgressListener
// end::client[]
{
    private AeronCluster aeronCluster;

    private final MutableDirectBuffer actionBidBuffer = new ExpandableArrayBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    
    private static final AtomicLong correlationId = new AtomicLong();
    private final Map<Long, CompletableFuture<Map<String, Object>>> pendingResponses = new ConcurrentHashMap<>();

    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int ITEM_ID_OFFSET = CORRELATION_ID_OFFSET + Long.BYTES;
    private static final int PRICE_OFFSET = ITEM_ID_OFFSET + Long.BYTES;
    private static final int BID_SUCCEEDED_OFFSET = PRICE_OFFSET + Long.BYTES;
    private static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + Byte.BYTES;

    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);

    /**
     * Construct a new cluster client for the auction.
     */
    public AuctionHttpServer()
    { 

    }

    /**
     * {@inheritDoc}
     */
    // tag::response[]
    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
        final long itemId = buffer.getLong(offset + ITEM_ID_OFFSET);
        final long currentWinningPrice = buffer.getLong(offset + PRICE_OFFSET);
        final boolean success = 0 != buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

        CompletableFuture<Map<String, Object>> future = pendingResponses.get(correlationId);
        if (future != null) {
            // System.out.printf("[MATCH] Completing future for correlationId=%d%n", correlationId);
            Map<String, Object> result = new HashMap<>();
            result.put("itemId", itemId);
            result.put("price", currentWinningPrice);
            result.put("success", success);
            future.complete(result);
            pendingResponses.remove(correlationId);
        } else {
            System.out.printf("[WARN] No future found for correlationId=%d! Possible race or double complete.%n", correlationId);
        }

        // System.out.println(
        //     "OnMessage: { Cluster Session Id: " + clusterSessionId + ", Correlation Id: " + correlationId +
        //     ", Item Id: " + itemId + ", Current Winning Price: " + currentWinningPrice +
        //     ", Succeed: " + success + " }");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionEvent(
        final long correlationId,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        System.out.println(
            "onSessionEvent: { Correlation Id: " + correlationId +
            ", Leadership Term Id: " + leadershipTermId + ", Leadership Member Id: " + leaderMemberId +
            ", Code: " + code + ", Detail: " + detail + " }");
    }

    /**
     * {@inheritDoc}
     */
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        System.out.println(
            "onNewLeader: { Cluster Session Id: " + clusterSessionId +
            ", Leadership Term Id: " + leadershipTermId + ", Leadership Member Id: " + leaderMemberId + "}");
    }
    // end::response[]

    /**
     * Ingress endpoints generated from a list of hostnames.
     *
     * @param hostnames for the cluster members.
     * @return a formatted string of ingress endpoints for connecting to a cluster.
     */
    public static String ingressEndpoints(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(
                calculatePort(i, BasicAuctionClusteredServiceNode.CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private String waitForFuture(
        CompletableFuture<Map<String, Object>> future, 
        long corrId, 
        Map<Long, CompletableFuture<Map<String, Object>>> responseMap, 
        Response res
    ) {
        long start = System.nanoTime();
        long maxWaitNanos = TimeUnit.SECONDS.toNanos(20);
        long sleepNanos = TimeUnit.MILLISECONDS.toNanos(5); // tiny sleeps between polls

        while (true) {
            if (future.isDone()) {
                try {
                    Map<String, Object> result = future.getNow(null);
                    responseMap.remove(corrId);
                    res.status(200);
                    // System.out.printf("[RESPONSE] correlationId=%d | result=%s%n", corrId, result);

                    Map<String, Object> wrappedResult = new HashMap<>();
                    wrappedResult.put("status", "OK");
                    wrappedResult.putAll(result);

                    return new Gson().toJson(wrappedResult);
                } catch (Exception e) {
                    responseMap.remove(corrId);
                    res.status(500);
                    // System.err.println("    Future failed for correlationId: " + corrId + ", error: " + e.getMessage());

                    return new Gson().toJson(Map.of("status", "ERROR", "message", e.getMessage()));
                }
            }
            if (System.nanoTime() - start > maxWaitNanos) {
                responseMap.remove(corrId);
                res.status(504);
                // System.err.printf("     [TIMEOUT] correlationId=%d, pendingResponses.size()=%d%n", corrId, responseMap.size());

                return new Gson().toJson(Map.of("status", "ERROR", "message", "Timeout waiting for cluster response"));
            }
            LockSupport.parkNanos(sleepNanos);
        }
    }

    // private synchronized void reconnectCluster()
    // {
    //     try
    //     {
    //         if (isReconnecting.get()) {
    //             return;
    //         }

    //         isReconnecting.set(true);

    //         System.out.println("[RECONNECT] Closing old AeronCluster connection...");
    //         if (aeronCluster != null)
    //         {
    //             aeronCluster.close();
    //         }
    //     }
    //     catch (Exception e)
    //     {
    //         System.err.println("[RECONNECT] Error closing old AeronCluster: " + e.getMessage());
    //     }

    //     try
    //     {
    //         System.out.println("[RECONNECT] Creating new AeronCluster connection...");
    //         this.aeronCluster = AeronCluster.connect(
    //             new AeronCluster.Context()
    //                 .egressListener(this)
    //                 .egressChannel("aeron:udp?endpoint=localhost:0")
    //                 .aeronDirectoryName(this.aeronCluster.context().aeronDirectoryName())
    //                 .ingressChannel("aeron:udp")
    //                 .ingressEndpoints(this.aeronCluster.context().ingressEndpoints())
    //         );
    //         System.out.println("[RECONNECT] New AeronCluster connection established!");
    //     }
    //     catch (Exception e)
    //     {
    //         System.err.println("[RECONNECT] Failed to reconnect to Aeron cluster: " + e.getMessage());
    //         e.printStackTrace();
    //     }
    //     finally
    //     {
    //         isReconnecting.set(false);
    //     }
    // }

    private boolean offerWithRetries(DirectBuffer buffer, int length, long corrId, Response res)
    {
        idleStrategy.reset();
        while (aeronCluster.offer(buffer, 0, length) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }
        return true;

        // int attempts = 0;
        // long result;

        // // System.out.printf("Sending request: correlationId=%d%n", corrId);
        // while (true)
        // {
        //     if (isReconnecting.get()) {
        //         // Wait for reconnect to complete
        //         for (int i = 0; i < 100; i++) {
        //             if (!isReconnecting.get()) break;
        //             LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5)); // wait 5ms
        //         }
        //     }
            
        //     result = aeronCluster.offer(buffer, 0, length);

        //     if (result > 0)
        //     {
        //         // System.out.printf("[OFFER SUCCESS] correlationId=%d, result=%d after %d attempts%n", corrId, result, attempts);
        //         return true;
        //     }

        //     if (result == Publication.NOT_CONNECTED)
        //     {
        //         System.err.printf("[OFFER FAIL] correlationId=%d: NOT_CONNECTED (-1)%n", corrId);
        //     }
        //     else if (result == Publication.CLOSED)
        //     {
        //         System.err.printf("[OFFER FAIL] correlationId=%d: CLOSED (-2). Reconnecting (not actually)...%n", corrId);
        //         // reconnectCluster();
        //         pendingResponses.remove(corrId);
        //         res.status(500);
        //         return false;
        //     }
        //     else if (result == Publication.MAX_POSITION_EXCEEDED)
        //     {
        //         System.err.printf("[OFFER FAIL] correlationId=%d: MAX_POSITION_EXCEEDED (-3)%n", corrId);
        //     }
        //     else if (result == 0)
        //     {
        //         // Backpressure: OK to retry
        //         if (attempts % 100 == 0) {
        //             System.err.printf("[OFFER FAIL] correlationId=%d: BACKPRESSURE (0) after %d attempts%n", corrId, attempts);
        //         }
        //     }
        //     else
        //     {
        //         System.err.printf("[OFFER FAIL] correlationId=%d: UNKNOWN ERROR (result=%d)%n", corrId, result);
        //     }

        //     if (++attempts > 10000)
        //     {
        //         // System.err.printf("[OFFER FAILED] correlationId=%d after 10000 attempts%n", corrId);
        //         pendingResponses.remove(corrId);
        //         res.status(500);
        //         return false;
        //     }
        //     LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
        // }
    }

    public void startHttpServer() {
        // ipAddress("0.0.0.0");
        port(8081);

        post("/bid", (req, res) -> {
            // System.out.printf("...post/bid itemId=%s, price=%s%n", req.queryParams("itemId"), req.queryParams("price"));

            if (aeronCluster == null) {
                System.err.println("Aeron not connected. Aborting HTTP server.");
                System.exit(1);
            }
            
            res.type("application/json");
            res.header("Content-Type", "application/json");

            final String itemIdStr = req.queryParams("itemId");
            final String priceStr = req.queryParams("price");

            if (itemIdStr == null || priceStr == null)
            {
                res.status(400);
                return "{\"error\": \"Missing 'itemId' or 'price' parameter\"}";
            }

            final long iid, price;
            try
            {
                iid = Long.parseLong(itemIdStr);
                price = Long.parseLong(priceStr);
            } catch (NumberFormatException ex)
            {
                res.status(400);
                return "{\"error\": \"Invalid 'itemId' or 'price' format\"}";
            }

            final long corrId = correlationId.incrementAndGet(); 
            final CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(CORRELATION_ID_OFFSET, corrId);
            buffer.putLong(ITEM_ID_OFFSET, iid);
            buffer.putLong(PRICE_OFFSET, price);
            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            offerWithRetries(aeronBuffer, buffer.capacity(), corrId, res);

            return waitForFuture(resultFuture, corrId, pendingResponses, res);           
        });

        get("/item", (req, res) -> {
            // System.out.printf("...get/item itemId=%s%n", req.queryParams("itemId"));

            res.type("application/json");
            res.header("Content-Type", "application/json");

            final String itemIdStr = req.queryParams("itemId");

            if (itemIdStr == null)
            {
                res.status(400);
                return "{\"error\": \"Missing 'itemId' parameter\"}";
            }

            final long itemId;
            try
            {
                itemId = Long.parseLong(itemIdStr);
            }
            catch (NumberFormatException ex)
            {
                res.status(400);
                return "{\"error\": \"Invalid 'itemId' format\"}";
            }

            final long corrId = correlationId.incrementAndGet();
            final CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(CORRELATION_ID_OFFSET, corrId);
            buffer.putLong(ITEM_ID_OFFSET, itemId);
            buffer.putLong(PRICE_OFFSET, -1); // Special -1 to indicate "read"

            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            offerWithRetries(aeronBuffer, buffer.capacity(), corrId, res);

            // Wait for the response (same mechanism as bid)
            return waitForFuture(resultFuture, corrId, pendingResponses, res);
        });

        get("/health", (req, res) -> 
        {
            res.type("application/json");
            res.header("Content-Type", "application/json");

            return aeronCluster != null ? "{\"status\": \"OK\"}" : "{\"status\": \"ERROR\"}";
        });
    }

    public void connect(String ingressEndpoints) {
        try {
            MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

            System.out.println("Ingress endpoints: " + ingressEndpoints);
            this.aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                    .egressListener(this)
                    // .egressChannel("aeron:udp?endpoint=localhost:0")
                    .egressChannel("aeron:udp?endpoint=10.42.0.1:0")
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingressEndpoints)
                    .isIngressExclusive(false)
            );

            // Launch keep-alive thread
            new Thread(() -> {
                final long keepAliveIntervalNanos = TimeUnit.SECONDS.toNanos(1); // send every 1 second
                long lastKeepAliveTime = System.nanoTime();

                while (!Thread.currentThread().isInterrupted()) {
                    int fragments = aeronCluster.pollEgress();

                    long now = System.nanoTime();
                    if (now - lastKeepAliveTime >= keepAliveIntervalNanos) {
                        aeronCluster.sendKeepAlive();
                        lastKeepAliveTime = now;
                    }

                    if (fragments == 0) {
                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(5));
                    } 
                }
            }, "Aeron-KeepAlive-Thread").start();

        } catch (Exception e) {
            System.err.println("Failed to connect to Aeron cluster:");
            e.printStackTrace();
        }
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        // System.out.println("Beginning AuctionHttpServer");
        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");
        final String ingressEndpoints = ingressEndpoints(Arrays.asList(hostnames));

        // System.out.println("Creating client");
        final AuctionHttpServer client = new AuctionHttpServer();

        client.connect(ingressEndpoints);
        client.startHttpServer();

        // Keep Aeron running
        System.out.println("HTTP server started");
    }
}
