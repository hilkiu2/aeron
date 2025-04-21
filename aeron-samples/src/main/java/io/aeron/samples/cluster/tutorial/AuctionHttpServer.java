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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

import io.aeron.CommonContext;

import static spark.Spark.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.net.URLDecoder;
import java.net.InetAddress;
import java.net.UnknownHostException;

// /**
//  * HTTP server that connects to a running Aeron cluster and submits auction commands.
//  */
// public class AuctionHttpServer
// {
//     private static final int MAX_MESSAGE_LENGTH = 1024;
//     private static final AtomicReference<AeronCluster> clusterRef = new AtomicReference<>();

//     private static final AtomicLong correlationId = new AtomicLong();
//     private static final int CORRELATION_ID_OFFSET = 0;
//     private static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + Long.BYTES;
//     private static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + Long.BYTES;
//     private static final int BID_SUCCEEDED_OFFSET = PRICE_OFFSET + Long.BYTES;
//     private static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + Byte.BYTES;

//     private static volatile long latestCustomerId = -1;
//     private static volatile long latestPrice = -1;

//     public static void main(final String[] args)
//     {
//         port(8080);
//         final String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
//         System.out.println("Auction REST server starting on port 8080 (" + time + ") ...");

//         final String hostname = InetAddress.getLocalHost().getHostName(); // "node1.aeron-jepsen.cs598fts.emulab.net"
//         final String nodeId = "1"; // fallback
//         if (hostname.matches(".*node(\\d+).*")) {
//             nodeId = hostname.replaceAll(".*node(\\d+).*", "$1");
//         }
//         final String aeronDir = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";
//         System.out.println("Aeron directory: " + aeronDir);
//         System.out.println("Node ID: " + nodeId);

//         final EgressListener listener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
//         {
//             if (length >= EGRESS_MESSAGE_LENGTH)
//             {
//                 final long corrId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
//                 final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
//                 final long price = buffer.getLong(offset + PRICE_OFFSET);
//                 final byte succeeded = buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

//                 latestCustomerId = customerId;
//                 latestPrice = price;

//                 System.out.printf("Cluster Response (corr=%d): customerId=%d, price=%d, success=%s%n",
//                     corrId, customerId, price, succeeded == 1);
//             }
//             else
//             {
//                 System.out.println("Received egress with unexpected length: " + length);
//             }
//         };

//         // Connect to Aeron Cluster
//         final AeronCluster.Context clusterContext = new AeronCluster.Context()
//             .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
//             {
//                 // Optionally handle cluster replies here
//             })
//             .ingressChannel("aeron:udp?term-length=64k")
//             .egressChannel("aeron:udp?term-length=64k|endpoint=localhost:0")
//             .aeronDirectoryName(aeronDir)
//             .ingressEndpoints("0=localhost:9002,1=localhost:9102,2=localhost:9202");

//         try
//         {
//             final AeronCluster cluster = AeronCluster.connect(clusterContext);
//             clusterRef.set(cluster);
//             System.out.println("Connected to Aeron Cluster.");
//         }
//         catch (ClusterException e)
//         {
//             System.err.println("Failed to connect to Aeron Cluster:");
//             e.printStackTrace();
//             System.exit(1);
//         }
//     }
// }

/**
 * HTTP server that connects to a running Aeron cluster and submits auction commands.
 */
public class AuctionHttpServer
{
    private static final int MAX_MESSAGE_LENGTH = 1024;
    private static final AtomicReference<AeronCluster> clusterRef = new AtomicReference<>();

    private static final AtomicLong correlationId = new AtomicLong();
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + Long.BYTES;
    private static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + Long.BYTES;
    private static final int BID_SUCCEEDED_OFFSET = PRICE_OFFSET + Long.BYTES;
    private static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + Byte.BYTES;

    private static volatile long latestCustomerId = -1;
    private static volatile long latestPrice = 0;

    public static void main(final String[] args)
    {
        port(8080);
        final String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        System.out.println("Auction REST server starting on port 8080 (" + time + ") ...");

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
        System.out.println("Node ID: " + nodeId);

        final EgressListener listener = (clusterSessionId, timestamp, buffer, offset, length, header) ->
        {
            if (length >= EGRESS_MESSAGE_LENGTH)
            {
                final long corrId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
                final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
                final long price = buffer.getLong(offset + PRICE_OFFSET);
                final byte succeeded = buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

                latestCustomerId = customerId;
                latestPrice = price;

                System.out.printf("Cluster Response (corr=%d): customerId=%d, price=%d, success=%s%n",
                    corrId, customerId, price, succeeded == 1);
            }
            else
            {
                System.out.println("Received egress with unexpected length: " + length);
            }
        };

        try
        {
            final AeronCluster cluster = AeronCluster.connect(
                new AeronCluster.Context()
                .egressListener(listener)
                .ingressChannel("aeron:udp?term-length=64k")
                .egressChannel("aeron:udp?term-length=64k|endpoint=localhost:0")
                .aeronDirectoryName(aeronDir)
                .ingressEndpoints("0=localhost:9002,1=localhost:9102,2=localhost:9202")
            );
            clusterRef.set(cluster);
            System.out.println("Connected to Aeron Cluster.");
        }
        catch (ClusterException e)
        {
            System.err.println("Failed to connect to Aeron Cluster:");
            e.printStackTrace();
            System.exit(1);
        }

        post("/auction/bid", (req, res) ->
        {
            System.out.printf("...post/auction/bid customerId=%s, price=%s%n", req.queryParams("customerId"), req.queryParams("price"));

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

            final ByteBuffer buffer = ByteBuffer.allocate(PRICE_OFFSET + Long.BYTES);
            buffer.putLong(CORRELATION_ID_OFFSET, correlationId.incrementAndGet());
            buffer.putLong(CUSTOMER_ID_OFFSET, cid);
            buffer.putLong(PRICE_OFFSET, price);
            final DirectBuffer aeronBuffer = new UnsafeBuffer(buffer.array());

            int attempts = 0;
            System.out.printf("Sending bid: cid=%d, price=%d, correlationId=%d%n", cid, price, correlationId.get());
            while (clusterRef.get().offer(aeronBuffer, 0, buffer.capacity()) < 0)
            {
                if (++attempts > 1000)
                {
                    System.err.println("Failed to send bid to Aeron.");
                    res.status(500);
                    return "{\"error\": \"Failed to deliver bid to cluster\"}";
                }
                Thread.yield();
            }

            return "{\"status\": \"OK\"}";
        });

        get("/auction/state", (req, res) -> 
        {
            res.type("application/json");
            return String.format("{\"price\": %d, \"customerId\": %d}", latestPrice, latestCustomerId);
        });

        get("/health", (req, res) -> 
        {
            return clusterRef.get() != null && !clusterRef.get().isClosed() ? "{\"status\": \"OK\"}" : "{\"status\": \"ERROR\"}";
        });
    }
}
