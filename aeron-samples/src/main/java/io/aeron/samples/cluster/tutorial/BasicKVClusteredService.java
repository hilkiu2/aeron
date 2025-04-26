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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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

import java.util.Objects;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import java.util.Arrays;

/**
 * KV service implementing the business logic.
 */
// tag::new_service[]
public class BasicKVClusteredService implements ClusteredService
// end::new_service[]
{
    // Shared Offsets
    static final int CORRELATION_ID_OFFSET = 0;
    static final int OPCODE_OFFSET = CORRELATION_ID_OFFSET + Long.BYTES; 
    static final int KEY_OFFSET = OPCODE_OFFSET + Byte.BYTES;

    static final int FIRST_LONG_OFFSET = KEY_OFFSET + Long.BYTES; // 8 + 1 + 4 = 13
    static final int SECOND_LONG_OFFSET = FIRST_LONG_OFFSET + Long.BYTES;

    static final byte OPCODE_GET = 0;
    static final byte OPCODE_PUT = 1;
    static final byte OPCODE_DELETE = 2;
    static final byte OPCODE_CAS = 3;

    // private final ThreadLocal<MutableDirectBuffer> threadLocalEgressBuffer = ThreadLocal.withInitial(() -> new ExpandableArrayBuffer(1024));
    private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
    private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();

    // tag::state[]
    private final Map<Long, Long> kvStore = new ConcurrentHashMap<>();
    // end::state[]
    private Cluster cluster;
    private IdleStrategy idleStrategy;

    public static String timestamp() {
        return "[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")) + "] ";
    }

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
        System.out.println(timestamp() + "[SERVER] onStart called — node is starting");
    }
    // end::start[]

    private void respond(ClientSession session, String response) {
        final byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        final MutableDirectBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bytes.length));
        responseBuffer.putBytes(0, bytes);

        session.offer(responseBuffer, 0, bytes.length);
    }

    /**
     * {@inheritDoc}
     * GET = 0
     * PUT = 1
     * DELETE = 2
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
        System.out.println(timestamp() + "[onSessionMessage] offset: " + offset + ", length: " + length);
        
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
        final byte opCode = buffer.getByte(offset + OPCODE_OFFSET);
        final long key = buffer.getLong(offset + KEY_OFFSET);
        System.out.printf(timestamp() + 
            "[THREAD: onSessionMessage] %s handling message (corrId=%d)%n",
            Thread.currentThread().getName(),
            correlationId
        );
        System.out.printf(timestamp() + "     [onSessionMessage] correlationId: %d opCode: %d%n", correlationId, opCode);

        boolean casSucceeded = false;
        long value = 0L;
        if (opCode == OPCODE_PUT) { // PUT  
            value = buffer.getLong(offset + FIRST_LONG_OFFSET);
            kvStore.put(key, value);

            System.out.printf(timestamp() + "     [PUT] %d -> %d%n", key, value);
        } else if (opCode == OPCODE_GET) { // GET
            value = kvStore.getOrDefault(key, -1L);
            System.out.printf(timestamp() + "     [GET] %d -> %d%n", key, value);
        } else if (opCode == OPCODE_DELETE) { // DELETE
            kvStore.remove(key);

            System.out.printf(timestamp() + "     [DELETE] key: %s (removed)%n", key);
        } else if (opCode == OPCODE_CAS) {
            final long expected = buffer.getLong(offset + FIRST_LONG_OFFSET);
            final long newValue = buffer.getLong(offset + SECOND_LONG_OFFSET);

            Long current = kvStore.get(key);
            casSucceeded = (current != null && current == expected);

            System.out.printf(timestamp() + "     [CAS] key: %d expected: %d actual: %d new: %d success: %s%n",
                key, expected, current, newValue, casSucceeded);

            if (casSucceeded) {
                kvStore.put(key, newValue);
                System.out.println(timestamp() + "        [CAS] KV store updated.");
            }
        }
        
        // final MutableDirectBuffer egressMessageBuffer = threadLocalEgressBuffer.get();
        if (null != session)                                                                         // <3>
        {
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);                       // <4>

            if (opCode == OPCODE_GET) { // GET
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, (byte)(value != -1L ? 1 : 0));
                egressMessageBuffer.putLong(CORRELATION_ID_OFFSET + Long.BYTES + 1, value);

                final int responseLength = Long.BYTES + 1 + Long.BYTES;

                System.out.printf(timestamp() + "[RESPONSE-GET] correlationId: %d length: %d value: %d%n", correlationId, responseLength, value);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }

            } else if (opCode == OPCODE_CAS) { // CAS
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, (byte)(casSucceeded ? 1 : 0));

                final int responseLength = Long.BYTES + 1;

                System.out.printf(timestamp() + "[RESPONSE-CAS] correlationId: %d length: %d success: %s%n", correlationId, responseLength, casSucceeded);


                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }
            } else { // PUT or DELETE – simple ack
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, (byte)1);

                final int responseLength = Long.BYTES + 1;

                System.out.printf(timestamp() + "[RESPONSE-ACK] correlationId: %d length: %d%n", correlationId, responseLength);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
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
        System.out.println(timestamp() + "[onTakeSnapshot]");
        for (Map.Entry<Long, Long> entry : kvStore.entrySet()) {
            System.out.println(timestamp() + "    [onTakeSnapshot] " + entry.getKey() + " = " + entry.getValue());
            final MutableDirectBuffer snapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(2 * Long.BYTES));

            snapshotBuffer.putLong(0, entry.getKey());
            snapshotBuffer.putLong(Long.BYTES, entry.getValue());

            idleStrategy.reset();
            while (snapshotPublication.offer(snapshotBuffer, 0, 2 * Long.BYTES) < 0)
            {
                idleStrategy.idle();
            }
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        System.out.println(timestamp() + "[loadSnapshot]");
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            final long key = buffer.getLong(offset);
            final long value = buffer.getLong(offset + Long.BYTES);

            kvStore.put(key, value);  // Restore into map
        };
        while (!snapshotImage.isEndOfStream())
        {
            final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 10);
            idleStrategy.idle(fragmentsPolled);
        }

        assert snapshotImage.isEndOfStream();   
    }
    // end::loadSnapshot[]

    /**
     * {@inheritDoc}
     */
    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onTerminate(final Cluster cluster)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println(timestamp() + "onSessionOpen(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println(timestamp() + "onSessionClose(" + session + ") with reason: " + closeReason);
    }

    /**
     * {@inheritDoc}
     */
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
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

        final BasicKVClusteredService that = (BasicKVClusteredService)o;

        return kvStore.equals(that.kvStore);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(kvStore);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "BasicKVClusteredService{" +
            "KV=" + kvStore +
            '}';
    }
}