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

    private final MutableDirectBuffer actionBidBuffer = new ExpandableArrayBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    
    private final Map<Long, CompletableFuture<Boolean>> pendingResponses = new ConcurrentHashMap<>();
    private static final AtomicLong correlationId = new AtomicLong();

    /**
     * Construct a new cluster client for the KV.
     */
    public KVHttpServer() { }

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

        System.out.print("OnMessage: { Cluster Session Id: " + clusterSessionId + ", Correlation Id: " + correlationId + ", Status(0: fail, 1: success): " + status);

        final CompletableFuture<?> future = pendingResponses.remove(correlationId);
        if (future == null) {
            System.err.println("No pending future found for correlationId: " + correlationId + " }");
            return;
        }

        if (length > Long.BYTES + 1) {
            int valLen = buffer.getInt(offset + CORRELATION_ID_OFFSET + Long.BYTES + 1);
            byte[] valBytes = new byte[valLen];
            buffer.getBytes(offset + CORRELATION_ID_OFFSET + Long.BYTES + 1 + Integer.BYTES, valBytes);
            String value = new String(valBytes, StandardCharsets.UTF_8);
            System.out.println(", Value: " + value + " }");

            CompletableFuture<String> stringFuture = (CompletableFuture<String>) future;
            stringFuture.complete(value);
        } else {
            System.out.println(" }");
            CompletableFuture<Boolean> boolFuture = (CompletableFuture<Boolean>) future;
            boolFuture.complete(status == 1);
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
                calculatePort(i, BasicKVClusteredServiceNode.CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private long sendPut(final AeronCluster aeronCluster, final String key, final String value)
    {
        final long corrId = correlationId.getAndIncrement();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        int messageLength = HEADER_LENGTH + keyBytes.length + valBytes.length;

        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, corrId);
        actionBidBuffer.putByte(OPCODE_OFFSET, OPCODE_PUT);
        actionBidBuffer.putInt(KEY_LENGTH_OFFSET, keyBytes.length);
        actionBidBuffer.putInt(VALUE_LENGTH_OFFSET, valBytes.length);
        actionBidBuffer.putBytes(HEADER_LENGTH, keyBytes);
        actionBidBuffer.putBytes(HEADER_LENGTH + keyBytes.length, valBytes);

        idleStrategy.reset();
        while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return corrId;
    }

    private long sendGet(final AeronCluster aeronCluster, final String key)
    {
        final long corrId = correlationId.getAndIncrement();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int messageLength = HEADER_LENGTH + keyBytes.length;

        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, corrId);
        actionBidBuffer.putByte(OPCODE_OFFSET, OPCODE_GET);
        actionBidBuffer.putInt(KEY_LENGTH_OFFSET, keyBytes.length);
        actionBidBuffer.putInt(VALUE_LENGTH_OFFSET, 0);
        actionBidBuffer.putBytes(HEADER_LENGTH, keyBytes);

        idleStrategy.reset();
        while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return corrId;
    }

    private long sendCAS(
        final AeronCluster aeronCluster,
        final String key,
        final String expectedValue,
        final String newValue)
    {
        final long corrId = correlationId.getAndIncrement();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = expectedValue.getBytes(StandardCharsets.UTF_8);
        byte[] newBytes = newValue.getBytes(StandardCharsets.UTF_8);

        int headerSize = Long.BYTES + Byte.BYTES + 3 * Integer.BYTES;
        int messageLength = headerSize + keyBytes.length + expectedBytes.length + newBytes.length;

        int offset = 0;
        actionBidBuffer.putLong(offset, corrId);
        offset += Long.BYTES;

        actionBidBuffer.putByte(offset, OPCODE_CAS);
        offset += Byte.BYTES;

        actionBidBuffer.putInt(offset, keyBytes.length);
        offset += Integer.BYTES;

        actionBidBuffer.putInt(offset, expectedBytes.length);
        offset += Integer.BYTES;

        actionBidBuffer.putInt(offset, newBytes.length);
        offset += Integer.BYTES;

        actionBidBuffer.putBytes(offset, keyBytes);
        offset += keyBytes.length;

        actionBidBuffer.putBytes(offset, expectedBytes);
        offset += expectedBytes.length;

        actionBidBuffer.putBytes(offset, newBytes);

        idleStrategy.reset();
        while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return corrId;
    }

    // private long sendDelete(final AeronCluster aeronCluster, final String key)
    // {
    //     final long corrId = correlationId.getAndIncrement();

    //     byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    //     int messageLength = HEADER_LENGTH + keyBytes.length;

    //     actionBidBuffer.putLong(CORRELATION_ID_OFFSET, corrId);
    //     actionBidBuffer.putByte(OPCODE_OFFSET, OPCODE_DELETE);
    //     actionBidBuffer.putInt(KEY_LENGTH_OFFSET, keyBytes.length);
    //     actionBidBuffer.putInt(VALUE_LENGTH_OFFSET, 0); // No value for delete
    //     actionBidBuffer.putBytes(HEADER_LENGTH, keyBytes);

    //     idleStrategy.reset();
    //     while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
    //     {
    //         idleStrategy.idle(aeronCluster.pollEgress());
    //     }

    //     return corrId;
    // }


    public void startHttpServer() {
        port(8081);

        post("/kv/put", (req, res) -> {
            res.type("application/json");

            final String key = req.queryParams("key");
            final String value = req.queryParams("value");

            if (key == null || value == null) {
                res.status(400);
                return "{\"error\": \"Missing 'key' or 'value' parameter\"}";
            }

            final long corrId = sendPut(aeronCluster, key, value);

            final CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            try {
                boolean result = resultFuture.get(2, TimeUnit.SECONDS);
                return new Gson().toJson(Map.of("status", "OK", "putSucceeded", result));
            } catch (TimeoutException e) {
                res.status(504);
                return "{\"error\": \"Timeout waiting for cluster response\"}";
            } catch (Exception e) {
                res.status(500);
                return "{\"error\": \"Unexpected error: " + e.getMessage() + "\"}";
            }
        });

        post("/kv/get", (req, res) -> {
            res.type("application/json");

            final String key = req.queryParams("key");
            if (key == null) {
                res.status(400);
                return "{\"error\": \"Missing 'key' parameter\"}";
            }

            final long corrId = sendGet(aeronCluster, key);

            final CompletableFuture<String> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            try {
                String value = resultFuture.get(2, TimeUnit.SECONDS);
                return new Gson().toJson(Map.of("status", "OK", "value", value));
            } catch (TimeoutException e) {
                res.status(504);
                return "{\"error\": \"Timeout waiting for cluster response\"}";
            } catch (Exception e) {
                res.status(500);
                return "{\"error\": \"Unexpected error: " + e.getMessage() + "\"}";
            }
        });

        post("/kv/cas", (req, res) -> {
            res.type("application/json");

            final String key = req.queryParams("key");
            final String expected = req.queryParams("expected");
            final String newValue = req.queryParams("new");

            if (key == null || expected == null || newValue == null) {
                res.status(400);
                return "{\"error\": \"Missing 'key', 'expected', or 'new' parameter\"}";
            }

            final long corrId = sendCAS(aeronCluster, key, expected, newValue);

            final CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
            pendingResponses.put(corrId, resultFuture);

            try {
                boolean success = resultFuture.get(2, TimeUnit.SECONDS);
                return new Gson().toJson(Map.of("status", "OK", "casSucceeded", success));
            } catch (TimeoutException e) {
                res.status(504);
                return "{\"error\": \"Timeout waiting for cluster response\"}";
            } catch (Exception e) {
                res.status(500);
                return "{\"error\": \"Unexpected error: " + e.getMessage() + "\"}";
            }
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

            // Launch keep-alive thread
            new Thread(() -> {
                try {
                    while (true) {
                        aeronCluster.pollEgress();
                        aeronCluster.sendKeepAlive();
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    System.err.println("Keep-alive thread interrupted.");
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
        System.out.println("Beginning KVHttpServer");
        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");
        final String ingressEndpoints = ingressEndpoints(Arrays.asList(hostnames));

        System.out.println("Creating client");
        final KVHttpServer client = new KVHttpServer();

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
        System.out.println("Aeron directory: " + aeronDir);

        // Start HTTP server to control Aeron bidding
        client.connect(aeronDir, ingressEndpoints);
        client.startHttpServer();

        // Keep Aeron running
        System.out.println("HTTP server started");
    }
}
