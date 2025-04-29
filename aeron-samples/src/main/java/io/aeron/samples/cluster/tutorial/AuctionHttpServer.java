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

import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredServiceNode.calculatePort;

/**
 * Client for communicating with {@link BasicAuctionClusteredService}.
 */
// tag::client[]
public class AuctionHttpServer implements EgressListener
// end::client[]
{
    private final Object aeronLock = new Object();
    private AeronCluster aeronCluster;

    private final MutableDirectBuffer actionBidBuffer = new ExpandableArrayBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    
    private static final AtomicLong correlationId = new AtomicLong();
    private final Map<Long, CompletableFuture<Boolean>> pendingResponses = new ConcurrentHashMap<>();

    private long winningCustomerId;
    private long winningPrice;

    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + Long.BYTES;
    private static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + Long.BYTES;
    private static final int BID_SUCCEEDED_OFFSET = PRICE_OFFSET + Long.BYTES;
    private static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + Byte.BYTES;

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
        final long winningCustomerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long currentWinningPrice = buffer.getLong(offset + PRICE_OFFSET);
        final boolean bidSucceed = 0 != buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

        this.winningCustomerId = winningCustomerId;
        this.winningPrice = currentWinningPrice;
        CompletableFuture<Boolean> future = pendingResponses.get(correlationId);
        if (future != null) {
            // System.out.printf("[MATCH] Completing future for correlationId=%d%n", correlationId);
            future.complete(bidSucceed);
            pendingResponses.remove(correlationId);
        } else {
            System.out.printf("[WARN] No future found for correlationId=%d! Possible race or double complete.%n", correlationId);
        }

        // System.out.println(
        //     "OnMessage: { Cluster Session Id: " + clusterSessionId + ", Correlation Id: " + correlationId +
        //     ", Winning Customer Id: " + winningCustomerId + ", Current Winning Price: " + currentWinningPrice +
        //     ", Bid Succeed: " + bidSucceed + " }");
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
        // System.out.println(
        //     "onSessionEvent: { Correlation Id: " + correlationId +
        //     ", Leadership Term Id: " + leadershipTermId + ", Leadership Member Id: " + leaderMemberId +
        //     ", Code: " + code + ", Detail: " + detail + " }");
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
        
        // System.out.println(
        //     "onNewLeader: { Cluster Session Id: " + clusterSessionId +
        //     ", Leadership Term Id: " + leadershipTermId + ", Leadership Member Id: " + leaderMemberId + "}");
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

    public long getWinningCustomerId() {
        return winningCustomerId;
    }

    public long getWinningPrice() {
        return winningPrice;
    }

    private <Boolean> String waitForFuture(
        CompletableFuture<Boolean> future, 
        long corrId, 
        Map<Long, CompletableFuture<Boolean>> responseMap, 
        Response res
    ) {
        long start = System.nanoTime();
        long maxWaitNanos = TimeUnit.SECONDS.toNanos(1); // 1 seconds wait until timeout detected. 
        long sleepNanos = TimeUnit.MILLISECONDS.toNanos(5); // tiny sleeps between polls

        while (true) {
            if (future.isDone()) {
                try {
                    Boolean result = future.getNow(null);
                    responseMap.remove(corrId);
                    res.status(200);
                    // System.out.printf("[RESPONSE] correlationId=%d | result=%s%n", corrId, result);
                    return new Gson().toJson(Map.of("status", "OK", "bidSucceed", result));
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
                // System.err.println("    Timeout waiting for correlationId: " + corrId);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Timeout waiting for cluster response"));
            }
            LockSupport.parkNanos(sleepNanos);
        }
    }

    public void startHttpServer() {
        port(8081);

        post("/bid", (req, res) -> {
            // System.out.printf("...post/bid customerId=%s, price=%s%n", req.queryParams("customerId"), req.queryParams("price"));

            final AeronCluster clusterRef;
            synchronized (aeronLock)
            {
                if (aeronCluster == null)
                {
                    System.err.println("Aeron not connected. Aborting HTTP server.");
                    System.exit(1);
                }
                clusterRef = aeronCluster;
            }
            
            res.type("application/json");
            res.header("Content-Type", "application/json");

            final String customerIdStr = req.queryParams("customerId");
            final String priceStr = req.queryParams("price");

            if (customerIdStr == null || priceStr == null)
            {
                res.status(400);
                return "{\"error\": \"Missing 'customerId' or 'price' parameter\"}";
            }

            final long cid, price;
            try
            {
                cid = Long.parseLong(customerIdStr);
                price = Long.parseLong(priceStr);
            } catch (NumberFormatException ex)
            {
                res.status(400);
                return "{\"error\": \"Invalid 'customerId' or 'price' format\"}";
            }

            final long corrId = correlationId.incrementAndGet(); 
            final CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            // Log if too many responses are waiting
            if (pendingResponses.size() > 100) {
                System.err.printf("⚠️⚠️⚠️ Too many unacknowledged bids! (%d pending)%n", pendingResponses.size());
            }

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(CORRELATION_ID_OFFSET, corrId);
            buffer.putLong(CUSTOMER_ID_OFFSET, cid);
            buffer.putLong(PRICE_OFFSET, price);
            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            int attempts = 0;
            // System.out.printf("Sending bid: cid=%d, price=%d, correlationId=%d%n", cid, price, corrId);

            while (true)
            {
                long result;
                // synchronized (aeronLock)
                // {
                result = clusterRef.offer(aeronBuffer, 0, buffer.capacity());
                // }
                if (result > 0)
                {
                    break;
                }
                if (++attempts > 1000)
                {
                    // System.err.println("Failed to send bid to Aeron, correlationID: " + corrId);
                    pendingResponses.remove(corrId);
                    res.status(500);
                    return "{\"error\": \"Failed to deliver bid to cluster\"}";
                }
                // Thread.yield();
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
            }

            return waitForFuture(resultFuture, corrId, pendingResponses, res);           
        });

        get("/status", (req, res) -> {
            res.type("application/json");
            res.header("Content-Type", "application/json");

            Map<String, Object> status = new HashMap<>();
            status.put("winningCustomerId", getWinningCustomerId());
            status.put("winningPrice", getWinningPrice());
            return new Gson().toJson(status);
        });

        get("/health", (req, res) -> 
        {
            res.type("application/json");
            res.header("Content-Type", "application/json");

            synchronized (aeronLock) {
                return aeronCluster != null ? "{\"status\": \"OK\"}" : "{\"status\": \"ERROR\"}";
            }
        });
    }

    public void connect(String aeronDir, String ingressEndpoints) {
        try {
            AeronCluster newCluster = AeronCluster.connect(
                new AeronCluster.Context()
                    .egressListener(this)
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingressEndpoints)
            );

            synchronized (aeronLock) {
                this.aeronCluster = newCluster;
            }

            // Launch keep-alive thread
            new Thread(() -> {
                final long keepAliveIntervalNanos = TimeUnit.SECONDS.toNanos(1); // send every 1 second
                long lastKeepAliveTime = System.nanoTime();

                while (!Thread.currentThread().isInterrupted()) {
                    int fragments;
                    synchronized (aeronLock) {
                        fragments = aeronCluster.pollEgress();
                    }

                    long now = System.nanoTime();
                    if (now - lastKeepAliveTime >= keepAliveIntervalNanos) {
                        synchronized (aeronLock) {
                            aeronCluster.sendKeepAlive();
                        }
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

       String hostname = "unknown";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println("Failed to resolve local hostname:");
            e.printStackTrace();
        }
        String nodeId = "1";
        if (hostname.matches(".*node(\\d+).*"))
        {
            nodeId = hostname.replaceAll(".*node(\\d+).*", "$1");
        }
        final String aeronDir = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";
        // System.out.println("Aeron directory: " + aeronDir);

        // Start HTTP server to control Aeron bidding
        client.connect(aeronDir, ingressEndpoints);
        client.startHttpServer();

        // Keep Aeron running
        System.out.println("HTTP server started");
    }
}
