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
import java.util.concurrent.atomic.AtomicInteger;

import io.aeron.CommonContext;

import static spark.Spark.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
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

import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredServiceNode.calculatePort;

/**
 * Client for communicating with {@link BasicAuctionClusteredService}.
 */
// tag::client[]
public class AuctionHttpServer implements EgressListener
// end::client[]
{
    private String ingressEndpoints;
    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final Lock aeronInitLock = new ReentrantLock();

    private final ThreadLocal<IdleStrategy> idleStrategy = ThreadLocal.withInitial(() -> new BackoffIdleStrategy());
    private final ThreadLocal<MediaDriver> mediaDriverStore = ThreadLocal.withInitial(() -> null);

    private final ThreadLocal<AeronCluster> aeronCluster = ThreadLocal.withInitial(() -> 
        {
            try {
                aeronInitLock.lock();
                MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));

                mediaDriverStore.set(mediaDriver);

                AeronCluster.Context aeronClusterContext = new AeronCluster.Context()
                            .egressListener(this)
                            // .egressChannel("aeron:udp?endpoint=127.0.0.1:0")
                            .egressChannel("aeron:udp?endpoint=10.42.0.1:0")
                            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                            .ingressChannel("aeron:udp")
                            .ingressEndpoints(this.ingressEndpoints);

                AeronCluster newCluster = AeronCluster.connect(aeronClusterContext);

                new Thread(() -> {
                    try {
                        final long keepAliveIntervalNanos = TimeUnit.SECONDS.toNanos(1); // send every 1 second
                        long lastKeepAliveTime = System.nanoTime();
                        long lastPollTime = System.nanoTime();

                        while (!Thread.currentThread().isInterrupted()) {
                            long now = System.nanoTime();
                            if (now - lastPollTime > TimeUnit.MILLISECONDS.toNanos(100)) {
                                System.out.println("[WARNING] " + Thread.currentThread().getName() + " pollEgress delay: " + ((now - lastPollTime) / 1_000_000) + " ms");
                            }

                            int fragments = newCluster.pollEgress();
                            lastPollTime = System.nanoTime();
                            if (now - lastKeepAliveTime >= keepAliveIntervalNanos) {
                                newCluster.sendKeepAlive();
                                lastKeepAliveTime = now;
                            }

                            // if (fragments == 0) {
                            //     LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(1));
                            // } 
                        }
                    } catch (Exception e) {
                        System.out.println("Keep Alive Thread Interupted:");
                        e.printStackTrace();
                    }
                }, "Aeron-KeepAlive-Thread-" + threadCount.getAndIncrement()).start();
                System.out.println("Aeron cluster created.");
                return newCluster;
            } catch (Exception e) {
                System.err.println("Failed to connect to Aeron cluster:");
                e.printStackTrace();
                return null;
            } finally {
                aeronInitLock.unlock();
            }
        }
    );
    
    private static final AtomicLong correlationId = new AtomicLong();
    private final Map<Long, CompletableFuture<Map<String, Object>>> pendingResponses = new ConcurrentHashMap<>();
    private final Set<Long> allCorrelationIds = ConcurrentHashMap.newKeySet();

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
            Map<String, Object> result = new HashMap<>();
            result.put("itemId", itemId);
            result.put("price", currentWinningPrice);
            result.put("success", success);
            future.complete(result);
            pendingResponses.remove(correlationId);
        } else {
            if (allCorrelationIds.contains(correlationId)) {
                System.out.printf("[WARN] Correlation ID %d was already completed. Possible double complete.%n", correlationId);
            } else {
                System.out.printf("[WARN] Correlation ID %d was never registered! Message possibly out of band or stale.%n", correlationId);
            }
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
            "onSessionEvent: { Correlation Id: " + correlationId + ", Cluster Session Id: " + clusterSessionId + 
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
            ", Leadership Term Id: " + leadershipTermId + ", Leadership Member Id: " + leaderMemberId + ", num pendingResponses: " + pendingResponses.size() + "}");

        // List<Long> keys = new ArrayList<>(pendingResponses.keySet());
        // for (Long corrId : keys) {
        //     CompletableFuture<Map<String, Object>> future = pendingResponses.get(corrId);
        //     if (future != null) {
        //         future.complete(Map.of(
        //             "status", "OK",
        //             "message", "NewLeader, cannot guarantee request was sent to cluster"
        //         ));
        //         System.err.printf("     [NEWLEADER- REQ DROPPED] correlationId=%d, pendingResponses.size()=%d%n", corrId, pendingResponses.size());
        //     }
        // }
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
        long maxWaitNanos = TimeUnit.SECONDS.toNanos(3);//15); // client timeout  
        long sleepNanos = TimeUnit.MILLISECONDS.toNanos(5);

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
                System.err.printf("     [TIMEOUT] correlationId=%d, pendingResponses.size()=%d%n", corrId, responseMap.size());

                return new Gson().toJson(Map.of("status", "ERROR", "message", "Timeout waiting for cluster response"));
            }
            LockSupport.parkNanos(sleepNanos);
        }
    }

    private long offerWithRetries(DirectBuffer buffer, int length, long corrId, Response res)
    {
        final long startTime = System.nanoTime();
        int retries = 0;
        long clusterResponse = 0;
        List<Long> seenResponses = new ArrayList<>();
        idleStrategy.get().reset();
        final long timeResetIdleStrategy = System.nanoTime();
        final long clusterSessionId = aeronCluster.get().clusterSessionId();
        final long timeClusterConnected = System.nanoTime();
        while ((clusterResponse = aeronCluster.get().offer(buffer, 0, length)) < 0)
        {
            // System.out.println("        RETRYING: " + clusterResponse);
            if (!seenResponses.contains(clusterResponse)) {
                seenResponses.add(clusterResponse);
            }
            if (++retries >= 10000) {
                System.out.println("CorrId: " + corrId + "; num retries: " + retries + "; seen responses: " + seenResponses);
                
                if (clusterResponse == -4L) // CLOSED
                {
                    throw new RuntimeException("Client CLOSED");
                }
                return clusterResponse;
            }
            idleStrategy.get().idle(aeronCluster.get().pollEgress());
        }
        if (retries > 0) {
           System.out.println("CorrId: " + corrId + "; num retries: " + retries + "; seen responses: " + seenResponses);
        }
        final long endTime = System.nanoTime();
        if (endTime - startTime > 1_000_000_000L) { // 1 sec
            System.out.println("[offerWithRetries] CorrId: " + corrId + " took " + ((endTime - startTime) / 1_000_000) + " ms total");
            System.out.println("    resetting Ideal Strategy took " + ((timeResetIdleStrategy - startTime) / 1_000_000) + " ms");
            System.out.println("    time to get cluster session id " + ((timeClusterConnected - timeResetIdleStrategy) / 1_000_000) + " ms; session ID: " + clusterSessionId);
            System.out.println("    time to offer took " + ((endTime - timeClusterConnected) / 1_000_000) + " ms");
        }
        return 0L;
    }

    public void startHttpServer() {
        port(8081);

        threadPool(
            10,
            10,
            300_000
        );

        post("/bid", (req, res) -> {
            if (aeronCluster.get() == null) {
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
            allCorrelationIds.add(corrId);
            pendingResponses.put(corrId, resultFuture);

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(CORRELATION_ID_OFFSET, corrId);
            buffer.putLong(ITEM_ID_OFFSET, iid);
            buffer.putLong(PRICE_OFFSET, price);
            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            final long clusterResponse = offerWithRetries(aeronBuffer, buffer.capacity(), corrId, res);

            if (clusterResponse < 0) {
                res.status(500);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Issue sending to aeron client: " + clusterResponse));
            }
            // final long startTime = System.nanoTime();
            final String result = waitForFuture(resultFuture, corrId, pendingResponses, res);

            return result;
        });

        get("/item", (req, res) -> {
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
            allCorrelationIds.add(corrId);
            pendingResponses.put(corrId, resultFuture);

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(CORRELATION_ID_OFFSET, corrId);
            buffer.putLong(ITEM_ID_OFFSET, itemId);
            buffer.putLong(PRICE_OFFSET, -1); // Special -1 to indicate "read"
            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            final long clusterResponse = offerWithRetries(aeronBuffer, buffer.capacity(), corrId, res);

            if (clusterResponse < 0) {
                res.status(500);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Issue sending to aeron client: " + clusterResponse));
            }

            return waitForFuture(resultFuture, corrId, pendingResponses, res);
        });

        get("/health", (req, res) -> 
        {
            res.type("application/json");
            res.header("Content-Type", "application/json");

            return aeronCluster.get() != null ? "{\"status\": \"OK\"}" : "{\"status\": \"ERROR\"}";
        });

        exception(Exception.class, (e, req, res) -> 
        {
            AeronCluster cluster = aeronCluster.get();
            MediaDriver mediaDriver = mediaDriverStore.get();
            if (cluster != null) {
                cluster.close();
            }
            if (mediaDriver != null) {
                mediaDriver.close();
            }
            aeronCluster.remove();
            mediaDriverStore.remove();
            idleStrategy.remove();
            System.out.println("    [Exception] Aeron cluster closed: " + e.getMessage());
        });
    }

    public void connect(String ingressEndpoints) {
        this.ingressEndpoints = ingressEndpoints;
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
