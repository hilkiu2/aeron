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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import spark.Response;
import java.util.concurrent.locks.LockSupport;

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

import static io.aeron.samples.cluster.tutorial.BasicKVClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicKVClusteredServiceNode.calculatePort;

/**
 * Client for communicating with {@link BasicKVClusteredService}.
 */
// tag::client[]
public class KVHttpServer implements EgressListener
// end::client[]
{
    private AeronCluster aeronCluster;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    
    private final Map<Long, CompletableFuture<Long>> longResponses = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Boolean>> boolResponses = new ConcurrentHashMap<>();

    private static final AtomicLong correlationId = new AtomicLong();

    /**
     * Construct a new cluster client for the KV.
     */
    public KVHttpServer() { }

    public static String timestamp() {
        return "[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")) + "] ";
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
        final byte status = buffer.getByte(offset + CORRELATION_ID_OFFSET + Long.BYTES);

        System.out.print(timestamp() + "OnMessage: { Cluster Session Id: " + clusterSessionId + ", Correlation Id: " + correlationId + ", Status: " + status);

        if (length > Long.BYTES + 1) {
            final CompletableFuture<Long> future = longResponses.remove(correlationId);
            if (future == null) {
                System.out.println(" }");
                System.err.println(timestamp() + "No pending LONG future found for correlationId: " + correlationId);
                return;
            }

            final long value = buffer.getLong(offset + CORRELATION_ID_OFFSET + Long.BYTES + 1);
            System.out.println(", Value: " + value + " }");
            System.out.println(timestamp() + "    Future complete for correlationId: " + correlationId);

            future.complete(value);
        } else {
            final CompletableFuture<Boolean> future = boolResponses.remove(correlationId);
            if (future == null) {
                System.out.println(" }");
                System.err.println(timestamp() + "No pending BOOL future found for correlationId: " + correlationId);
                return;
            }

            System.out.println(" }");
            System.out.println(timestamp() + "    Future complete for correlationId: " + correlationId);
            future.complete(status == 1);
        }
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
        System.out.println(timestamp() + 
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
        System.out.println(timestamp() + 
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
                calculatePort(i, BasicKVClusteredServiceNode.CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private void pollAndWait(final MutableDirectBuffer buffer, final int messageLength) {
        idleStrategy.reset();
        while (true)
        {
            long result = aeronCluster.offer(buffer, 0, messageLength);

            if (result > 0)
            {
                System.out.println(timestamp() + " Offer succeeded: position=" + result);
                break;
            }
            else
            {
                System.err.println(timestamp() + " Offer failed: result=" + result);
                idleStrategy.idle(aeronCluster.pollEgress());
            }
        }
    }

    private void sendPut(final AeronCluster aeronCluster, final long key, final long value, final long corrId)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final int messageLength = 8 + 1 + 8 + 8; // corrId (8) + opcode (1) + key (8) + value (8)

        buffer.putLong(CORRELATION_ID_OFFSET, corrId);
        buffer.putByte(OPCODE_OFFSET, OPCODE_PUT);
        buffer.putLong(KEY_OFFSET, key);
        buffer.putLong(FIRST_LONG_OFFSET, value);

        pollAndWait(buffer, messageLength);
        
        System.out.println(timestamp() + "    Sent PUT: { Correlation Id: " + corrId + ", Key: " + key + ", Value: " + value + " }");
    }

    private void sendGet(final AeronCluster aeronCluster, final long key, final long corrId)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final int messageLength = 8 + 1 + 8; // corrId (8) + opcode (1) + key (8)

        buffer.putLong(CORRELATION_ID_OFFSET, corrId);
        buffer.putByte(OPCODE_OFFSET, OPCODE_GET);
        buffer.putLong(KEY_OFFSET, key);

        pollAndWait(buffer, messageLength);
        System.out.println(timestamp() + "    Sent GET: { Correlation Id: " + corrId + ", Key: " + key + " }");
    }

    private void sendCAS(
        final AeronCluster aeronCluster,
        final long key,
        final long expectedValue,
        final long newValue, 
        final long corrId)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final int messageLength = 8 + 1 + 8 + 8 + 8; // corrId (8) + opcode (1) + key (8) + expected (8) + new (8)

        buffer.putLong(CORRELATION_ID_OFFSET, corrId);
        buffer.putByte(OPCODE_OFFSET, OPCODE_CAS);
        buffer.putLong(KEY_OFFSET, key);
        buffer.putLong(FIRST_LONG_OFFSET, expectedValue);
        buffer.putLong(SECOND_LONG_OFFSET, newValue);

        pollAndWait(buffer, messageLength);
        System.out.println(timestamp() + "    Sent CAS: { Correlation Id: " + corrId + ", Key: " + key +
            ", Expected Value: " + expectedValue + ", New Value: " + newValue + " }");
    }

    private void sendDelete(final AeronCluster aeronCluster, final long key, final long corrId)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final int messageLength = 8 + 1 + 8; // corrId (8) + opcode (1) + key (8)

        buffer.putLong(CORRELATION_ID_OFFSET, corrId);
        buffer.putByte(OPCODE_OFFSET, OPCODE_DELETE);
        buffer.putLong(KEY_OFFSET, key);

        pollAndWait(buffer, messageLength);
        System.out.println(timestamp() + "    Sent DELETE: { Correlation Id: " + corrId + ", Key: " + key + " }");
    }

    private <T> String waitForFuture(
        CompletableFuture<T> future, 
        long corrId, 
        Map<Long, CompletableFuture<T>> responseMap, 
        Response res, 
        String resultFieldName // e.g., "value" or "success"
    ) {
        long start = System.nanoTime();
        long maxWaitNanos = TimeUnit.SECONDS.toNanos(5);
        long sleepNanos = TimeUnit.MILLISECONDS.toNanos(5); // tiny sleeps between polls

        while (true) {
            if (future.isDone()) {
                try {
                    T result = future.getNow(null);
                    responseMap.remove(corrId);
                    res.status(200);
                    System.out.println(timestamp() + "    Future complete for correlationId: " + corrId + ", result: " + result);
                    return new Gson().toJson(Map.of("status", "OK", resultFieldName, result));
                } catch (Exception e) {
                    responseMap.remove(corrId);
                    res.status(500);
                    System.err.println(timestamp() + "    Future failed for correlationId: " + corrId + ", error: " + e.getMessage());
                    return new Gson().toJson(Map.of("status", "ERROR", "message", e.getMessage()));
                }
            }
            if (System.nanoTime() - start > maxWaitNanos) {
                responseMap.remove(corrId);
                res.status(504);
                System.err.println(timestamp() + "    Timeout waiting for correlationId: " + corrId);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Timeout waiting for cluster response"));
            }
            LockSupport.parkNanos(sleepNanos);
        }
    }

    public void startHttpServer() {
        // threadPool(100, 8, 30000);
        port(8081);

        post("/kv/put", (req, res) -> {
            System.out.println(timestamp() + "Received PUT request");
            res.type("application/json");

            final String keyStr = req.queryParams("key");
            final String valueStr = req.queryParams("value");

            if (keyStr == null || valueStr == null) { 
                res.status(400);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Missing 'key' or 'value' parameter"));
            }

            final long key = Long.parseLong(keyStr);
            final long value = Long.parseLong(valueStr);
            final long corrId = correlationId.getAndIncrement();

            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            boolResponses.put(corrId, future);
            System.out.println(timestamp() + "    Pending BOOL future for correlationId: " + corrId);

            sendPut(aeronCluster, key, value, corrId);

            return waitForFuture(future, corrId, boolResponses, res, "success");
        });

        post("/kv/get", (req, res) -> {
            System.out.println(timestamp() + "Received GET request");
            res.type("application/json");

            final String keyStr = req.queryParams("key");

            if (keyStr == null) {
                res.status(400);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Missing 'key' parameter"));
            }
            
            final long key = Long.parseLong(keyStr);
            final long corrId = correlationId.getAndIncrement();

            final CompletableFuture<Long> future = new CompletableFuture<>();
            longResponses.put(corrId, future);
            System.out.println(timestamp() + "    Pending LONG future for correlationId: " + corrId);

            sendGet(aeronCluster, key, corrId);

            return waitForFuture(future, corrId, longResponses, res, "value");
        });

        post("/kv/delete", (req, res) -> {
            System.out.println(timestamp() + "Received DELETE request");
            res.type("application/json");

            final String keyStr = req.queryParams("key");
            if (keyStr == null) {
                res.status(400);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Missing 'key' parameter"));
            }

            final long key = Long.parseLong(keyStr);
            final long corrId = correlationId.getAndIncrement();

            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            boolResponses.put(corrId, future);
            System.out.println(timestamp() + "    Pending BOOL future for correlationId: " + corrId);

            sendDelete(aeronCluster, key, corrId);

            return waitForFuture(future, corrId, boolResponses, res, "success");
        });

        post("/kv/cas", (req, res) -> {
            System.out.println(timestamp() + "Received CAS request");
            res.type("application/json");

            final String keyStr = req.queryParams("key");
            final String expectedStr = req.queryParams("expected");
            final String newValueStr = req.queryParams("new");

            if (keyStr == null || expectedStr == null || newValueStr == null) {
                res.status(400);
                return new Gson().toJson(Map.of("status", "ERROR", "message", "Missing 'key', 'expected', or 'new' parameter"));
            }

            final long key = Long.parseLong(keyStr);
            final long expected = Long.parseLong(expectedStr);
            final long newValue = Long.parseLong(newValueStr);
            final long corrId = correlationId.getAndIncrement();

            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            boolResponses.put(corrId, future);
            System.out.println(timestamp() + "    Pending BOOL future for correlationId: " + corrId);

            sendCAS(aeronCluster, key, expected, newValue, corrId); 

            return waitForFuture(future, corrId, boolResponses, res, "success");

            // return CompletableFuture.supplyAsync(() -> {
                // try {
                //     boolean success = future.get(3, TimeUnit.SECONDS);
                //     return new Gson().toJson(Map.of("status", "OK", "success", success));
                // } catch (TimeoutException e) {
                //     try {
                //         Boolean success = future.get(5, TimeUnit.SECONDS);
                //         return new Gson().toJson(Map.of("status", "OK", "success", success));
                //     } catch (TimeoutException e2) {
                //         res.status(504);
                //         return "{\"error\": \"Timeout waiting for cluster response\"}";
                //     } catch (Exception e2) {
                //         res.status(500);
                //         return "{\"error\": \"Unexpected error: " + e.getMessage() + "\"}";
                //     }
                // } catch (Exception e) {
                //     res.status(500);
                //     return "{\"error\": \"Unexpected error: " + e.getMessage() + "\"}";
                // }
                
                // try {
                //     return future
                //                 .orTimeout(3, TimeUnit.SECONDS)
                //                 .handle((result, ex) -> {
                //                     boolResponses.remove(corrId);
                //                     if (ex instanceof TimeoutException) {
                //                         res.status(504);
                //                         return new Gson().toJson(Map.of("status", "ERROR", "message", "Cluster response timeout"));
                //                     } else if (ex != null) {
                //                         res.status(500);
                //                         return new Gson().toJson(Map.of("status", "ERROR", "message", ex.getMessage()));
                //                     } else {
                //                         return new Gson().toJson(Map.of("status", "OK", "success", result));
                //                     }
                //                 }).get(); 
                // } catch (Exception e) {
                //     res.status(500);
                //     return new Gson().toJson(Map.of("status", "ERROR", "message", "Unexpected error: " + e.getMessage()));
                // }
            // });
        });
    }

    public void connect(String aeronDir, String ingressEndpoints) {
        try {
            this.aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                    .egressListener(this)
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingressEndpoints)
            );

            // Launch fast poller thread for egress handling
            new Thread(() -> {
                try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                while (!Thread.currentThread().isInterrupted()) {
                    // Poll frequently to service completed futures ASAP
                    // aeronCluster.pollEgress();
                    // Send keep-alives less frequently to reduce noise
                    long now = System.nanoTime();
                    if (now % 10 == 0) aeronCluster.sendKeepAlive();
                    // A short sleep can reduce CPU usage if needed
                    // try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                }
            }, "Aeron-Keep-Alive-Thread").start();

        } catch (Exception e) {
            System.err.println(timestamp() + "Failed to connect to Aeron cluster:");
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
        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");
        final String ingressEndpoints = ingressEndpoints(Arrays.asList(hostnames));

        final KVHttpServer client = new KVHttpServer();

       String hostname = "unknown";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println(timestamp() + "Failed to resolve local hostname:");
            e.printStackTrace();
        }
        String nodeId = "1";
        if (hostname.matches(".*node(\\d+).*"))
        {
            nodeId = hostname.replaceAll(".*node(\\d+).*", "$1");
        }
        final String aeronDir = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";

        client.connect(aeronDir, ingressEndpoints);
        client.startHttpServer();

        System.out.println(timestamp() + "HTTP server started...");
    }
}
