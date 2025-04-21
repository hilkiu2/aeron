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
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import io.aeron.CommonContext;

import static spark.Spark.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.net.URLDecoder;

/**
 * HTTP server that connects to a running Aeron cluster and submits auction commands.
 */
public class AuctionHttpServer
{
    private static final int MAX_MESSAGE_LENGTH = 1024;
    private static final AtomicReference<AeronCluster> clusterRef = new AtomicReference<>();

    public static void main(final String[] args)
    {
        port(8080);
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        System.out.println("Auction REST server starting on port 8080 (" + time + ") ...");

        exception(Exception.class, (e, req, res) -> {
            System.err.println("Exception during request:");
            e.printStackTrace();
        });

        get("/health", (req, res) -> {
            System.out.println("...get/health body = " + req.body());

            AeronCluster cluster = clusterRef.get();
            if (cluster != null && !cluster.isClosed()) {
                return "{\"status\": \"connected\"}";
            } else {
                res.status(503);
                return "{\"status\": \"not connected\"}";
            }
        });


        // Connect to Aeron Cluster
        final AeronCluster.Context clusterContext = new AeronCluster.Context()
            .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                // Optionally handle cluster replies here
            })
            .ingressChannel("aeron:udp?term-length=64k")
            .egressChannel("aeron:udp?term-length=64k|endpoint=localhost:0")
            .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-0-driver")
            .ingressEndpoints("0=localhost:9002,1=localhost:9102,2=localhost:9202");

        try
        {
            final AeronCluster cluster = AeronCluster.connect(clusterContext);
            clusterRef.set(cluster);
            System.out.println("Connected to Aeron Cluster.");
        }
        catch (ClusterException e)
        {
            System.err.println("Failed to connect to Aeron Cluster:");
            e.printStackTrace();
            System.exit(1);
        }

        post("/auction", (req, res) ->
        {
            System.out.println("...post/auction body = " + req.body());

            String item = req.queryParams("item");
            if (item == null) {
                Map<String, String> formParams = parseFormBody(req.body());
                item = formParams.get("item");
            }
            
            if (item == null) {
                res.status(400);
                return "{\"error\": \"Missing 'item'\"}";
            }

            final String message = "CREATE:" + item;
            res.type("application/json");
            return sendMessage(clusterRef.get(), message);
        });

        post("/auction/bid", (req, res) ->
        {
            System.out.println("...post/auction/bid body = " + req.body());
            
            String id = req.queryParams("id");
            String amount = req.queryParams("amount");

            if (id == null || amount == null) {
                Map<String, String> formParams = parseFormBody(req.body());
                if (id == null) id = formParams.get("id");
                if (amount == null) amount = formParams.get("amount");
            }

            if (id == null || amount == null)
            {
                res.status(400);
                return "{\"error\": \"Missing 'id' or 'amount'\"}";
            }

            final String message = "BID:" + id + ":" + amount;
            res.type("application/json");
            return sendMessage(clusterRef.get(), message);
        });

        post("/auction/close", (req, res) ->
        {
            System.out.println("...post/auction/close body = " + req.body());

            String id = req.queryParams("id");
            if (id == null) {
                Map<String, String> formParams = parseFormBody(req.body());
                id = formParams.get("id");
            }

            if (id == null)
            {
                res.status(400);
                return "{\"error\": \"Missing 'id'\"}";
            }

            final String message = "CLOSE:" + id;
            res.type("application/json");
            return sendMessage(clusterRef.get(), message);
        });
    }

    private static Map<String, String> parseFormBody(String body) {
        Map<String, String> formParams = new HashMap<>();
        if (body != null && !body.isEmpty()) {
            for (String pair : body.split("&")) {
                String[] parts = pair.split("=", 2);
                if (parts.length == 2) {
                    formParams.put(java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                                java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
                }
            }
        }
        return formParams;
    }


    private static String sendMessage(final AeronCluster cluster, final String message)
    {
        System.out.println("...sending message " + message);
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_MESSAGE_LENGTH)
        {
            return "{\"error\": \"Message too large\"}";
        }

        final DirectBuffer buffer = new UnsafeBuffer(bytes);
        int attempts = 0;
        while (cluster.offer(buffer, 0, bytes.length) < 0)
        {
            attempts++;
            if (attempts > 1000) {
                System.err.println("Failed to send message to Aeron cluster: " + message);
                return "{\"error\": \"Failed to deliver to Aeron\"}";
            }
            Thread.yield(); // Retry until successful
        }

        return "{\"status\": \"OK\"}";
    }
}
