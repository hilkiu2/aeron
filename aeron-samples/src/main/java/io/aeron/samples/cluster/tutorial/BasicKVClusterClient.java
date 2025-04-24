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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.CommonContext;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.aeron.samples.cluster.tutorial.BasicKVClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicKVClusteredServiceNode.calculatePort;

import java.nio.charset.StandardCharsets;

/**
 * Client for communicating with {@link BasicKVClusteredService}.
 */
// tag::client[]
public class BasicKVClusterClient implements EgressListener
// end::client[]
{
    private final MutableDirectBuffer actionBidBuffer = new ExpandableArrayBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    private long nextCorrelationId = 0;

    /**
     * Construct a new cluster client for the KV.
     */
    public BasicKVClusterClient()
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
        final byte status = buffer.getByte(offset + CORRELATION_ID_OFFSET + Long.BYTES);

        System.out.print("Response (" + correlationId + "): status=" + status);

        if (length > Long.BYTES + 1) {
            int valLen = buffer.getInt(offset + CORRELATION_ID_OFFSET + Long.BYTES + 1);
            byte[] valBytes = new byte[valLen];
            buffer.getBytes(offset + CORRELATION_ID_OFFSET + Long.BYTES + 1 + Integer.BYTES, valBytes);
            String value = new String(valBytes, StandardCharsets.UTF_8);
            System.out.println(", value=" + value);
        } else {
            System.out.println();
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
        printOutput(
            "SessionEvent(" + correlationId + ", " + leadershipTermId + ", " +
            leaderMemberId + ", " + code + ", " + detail + ")");
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
        printOutput("NewLeader(" + clusterSessionId + ", " + leadershipTermId + ", " + leaderMemberId + ")");
    }
    // end::response[]

    // tag::publish[]
    private long sendPut(final AeronCluster aeronCluster, final String key, final String value)
    {
        final long correlationId = nextCorrelationId++;

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        int messageLength = HEADER_LENGTH + keyBytes.length + valBytes.length;

        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
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

        return correlationId;
    }

    private long sendGet(final AeronCluster aeronCluster, final String key)
    {
        final long correlationId = nextCorrelationId++;

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int messageLength = HEADER_LENGTH + keyBytes.length;

        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
        actionBidBuffer.putByte(OPCODE_OFFSET, OPCODE_GET);
        actionBidBuffer.putInt(KEY_LENGTH_OFFSET, keyBytes.length);
        actionBidBuffer.putInt(VALUE_LENGTH_OFFSET, 0);
        actionBidBuffer.putBytes(HEADER_LENGTH, keyBytes);

        idleStrategy.reset();
        while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return correlationId;
    }

    private long sendCAS(
        final AeronCluster aeronCluster,
        final String key,
        final String expectedValue,
        final String newValue)
    {
        final long correlationId = nextCorrelationId++;

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = expectedValue.getBytes(StandardCharsets.UTF_8);
        byte[] newBytes = newValue.getBytes(StandardCharsets.UTF_8);

        int headerSize = Long.BYTES + Byte.BYTES + 3 * Integer.BYTES;
        int messageLength = headerSize + keyBytes.length + expectedBytes.length + newBytes.length;

        int offset = 0;
        actionBidBuffer.putLong(offset, correlationId);
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

        return correlationId;
    }

    // private long sendDelete(final AeronCluster aeronCluster, final String key)
    // {
    //     final long correlationId = nextCorrelationId++;

    //     byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    //     int messageLength = HEADER_LENGTH + keyBytes.length;

    //     actionBidBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
    //     actionBidBuffer.putByte(OPCODE_OFFSET, OPCODE_DELETE);
    //     actionBidBuffer.putInt(KEY_LENGTH_OFFSET, keyBytes.length);
    //     actionBidBuffer.putInt(VALUE_LENGTH_OFFSET, 0); // No value for delete
    //     actionBidBuffer.putBytes(HEADER_LENGTH, keyBytes);

    //     idleStrategy.reset();
    //     while (aeronCluster.offer(actionBidBuffer, 0, messageLength) < 0)
    //     {
    //         idleStrategy.idle(aeronCluster.pollEgress());
    //     }

    //     return correlationId;
    // }
    // end::publish[]

    public void runTest(final AeronCluster cluster)
    {
        long cid1 = sendPut(cluster, "foo", "bar");
        long cid2 = sendGet(cluster, "foo");

        while (!Thread.currentThread().isInterrupted()) {
            idleStrategy.idle(cluster.pollEgress());
        }
    }


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

    private void printOutput(final String message)
    {
        System.out.println(message);
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        System.out.println("[CLIENT] Beginning Client Main");
        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");
        System.out.println("[CLIENT] hostnames: " + Arrays.toString(hostnames));

        final String ingressEndpoints = ingressEndpoints(Arrays.asList(hostnames));
        System.out.println("[CLIENT] ingressEndpoints: " + ingressEndpoints);

        final BasicKVClusterClient client = new BasicKVClusterClient();
        System.out.println("[CLIENT] client: " + client);

        // try {
        //     System.out.println("[CLIENT] InetAddress.getLocalHost(): " + InetAddress.getLocalHost());
        // } catch (UnknownHostException e) {
        //     System.err.println("Failed to resolve local host:");
        //     e.printStackTrace();
        // }

        // try {
        //     System.out.println("[CLIENT] InetAddress.getLocalHost().getHostName(): " + InetAddress.getLocalHost().getHostName());
        // } catch (UnknownHostException e) {
        //     System.err.println("Failed to resolve local hostname:");
        //     e.printStackTrace();
        // }
        

        // MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()                      // <1>
        //     .threadingMode(ThreadingMode.SHARED)
        //     .dirDeleteOnStart(true)
        //     .dirDeleteOnShutdown(true));
        // System.out.println("[CLIENT] mediaDriver: " + mediaDriver);
        // System.out.println("[CLIENT] mediaDriver.aeronDirectoryName(): " + mediaDriver.aeronDirectoryName());

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

        // tag::connect[]
        try (
            AeronCluster aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                .egressListener(client)                                                                         // <2>
                .egressChannel("aeron:udp?endpoint=localhost:0")                                                // <3>
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:udp")                                                                    // <4>
                .ingressEndpoints(ingressEndpoints)))                                                           // <5>
        {
        // end::connect[]
            client.runTest(aeronCluster);
        }
    }
}
