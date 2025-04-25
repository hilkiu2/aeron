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
    static final int OPCODE_OFFSET         = CORRELATION_ID_OFFSET + Long.BYTES;
    static final int KEY_LENGTH_OFFSET     = OPCODE_OFFSET + Byte.BYTES;

    // PUT-specific 
    static final int VALUE_LENGTH_OFFSET   = KEY_LENGTH_OFFSET + Integer.BYTES; // 8 + 1 + 4 = 13
    static final int HEADER_LENGTH_PUT     = VALUE_LENGTH_OFFSET + Integer.BYTES; // 8 + 1 + 4 + 4 = 17

    // GET-specific 
    static final int HEADER_LENGTH_GET     = KEY_LENGTH_OFFSET + Integer.BYTES;   // 8 + 1 + 4 = 13

    // CAS-specific
    static final int EXPECTED_LENGTH_OFFSET = KEY_LENGTH_OFFSET + Integer.BYTES;
    static final int NEW_LENGTH_OFFSET      = EXPECTED_LENGTH_OFFSET + Integer.BYTES;
    static final int HEADER_LENGTH_CAS      = NEW_LENGTH_OFFSET + Integer.BYTES;  // 8 + 1 + 4 + 4 + 4 = 21

    static final byte OPCODE_GET = 0;
    static final byte OPCODE_PUT = 1;
    static final byte OPCODE_DELETE = 2;
    static final byte OPCODE_CAS = 3;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
    private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();

    // tag::state[]
    private final Map<String, String> kvStore = new ConcurrentHashMap<>();
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
        System.out.println("[SERVER] onStart called — node is starting");
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
        System.out.println("[onSessionMessage] offset: " + offset + ", length: " + length);
        
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
        final byte opCode = buffer.getByte(offset + OPCODE_OFFSET);
        final int keyLen = buffer.getInt(offset + KEY_LENGTH_OFFSET);
        System.out.printf("     [onSessionMessage] correlationId: %d opCode: %d%n", correlationId, opCode);

        boolean casSucceeded = false;
        String value = null;
        if (opCode == OPCODE_PUT) { // PUT  
            final int keyOffset = offset + HEADER_LENGTH_PUT;
            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(keyOffset, keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            final int valueLen = buffer.getInt(offset + VALUE_LENGTH_OFFSET);
            final int valueOffset = keyOffset + keyLen;
            byte[] valueBytes = new byte[valueLen];
            buffer.getBytes(valueOffset, valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
            
            kvStore.put(key, value);
            System.out.printf("     [PUT] %s -> %s%n", key, value);

        } else if (opCode == OPCODE_GET) { // GET
            final int keyOffset = offset + HEADER_LENGTH_GET;
            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(keyOffset, keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            value = kvStore.get(key);
            System.out.printf("     [GET] %s -> %s%n", key, value);
        } else if (opCode == OPCODE_DELETE) { // DELETE
            final int keyOffset = offset + HEADER_LENGTH_GET;
            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(keyOffset, keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            kvStore.remove(key);
            System.out.printf("     [DELETE] key: %s (removed)%n", key);
        } else if (opCode == OPCODE_CAS) {
            System.out.printf("     [CAS] offset: %d, HEADER_LENGTH_CAS: %d%n", offset, HEADER_LENGTH_CAS);

            final int keyOffset = offset + HEADER_LENGTH_CAS;
            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(keyOffset, keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);
            System.out.printf("     [CAS] key (decoded): '%s'%n", key);

            final int expectedLen = buffer.getInt(offset + EXPECTED_LENGTH_OFFSET);
            int readOffset = keyOffset + keyLen;
            final byte[] expectedBytes = new byte[expectedLen];
            buffer.getBytes(readOffset, expectedBytes);
            final String expectedValue = new String(expectedBytes, StandardCharsets.UTF_8);
            System.out.printf("     [CAS] expectedValue (decoded): '%s'%n", expectedValue);

            final int newLen = buffer.getInt(offset + NEW_LENGTH_OFFSET);
            readOffset += expectedLen;
            final byte[] newBytes = new byte[newLen];
            buffer.getBytes(readOffset, newBytes);
            final String newValue = new String(newBytes, StandardCharsets.UTF_8);
            System.out.printf("     [CAS] newValue (decoded): %s%n", newValue);

            String current = kvStore.get(key);
            casSucceeded = Objects.equals(current, expectedValue);
            System.out.printf("     [CAS] key: %s expected: '%s' actual: '%s' new: '%s' success: %s%n", key, expectedValue, current, newValue, casSucceeded);

            if (casSucceeded) {
                kvStore.put(key, newValue);
                System.out.println("        [CAS] KV store updated.");
            }
        }
        
        if (null != session)                                                                         // <3>
        {
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);                       // <4>

            if (opCode == OPCODE_GET) { // GET
                byte[] valBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, (byte)(value != null ? 1 : 0));
                egressMessageBuffer.putInt(CORRELATION_ID_OFFSET + Long.BYTES + 1, valBytes.length);
                egressMessageBuffer.putBytes(CORRELATION_ID_OFFSET + Long.BYTES + 1 + Integer.BYTES, valBytes);
                final int responseLength = Long.BYTES + 1 + Integer.BYTES + valBytes.length;

                System.out.printf("[RESPONSE-GET] correlationId: %d length: %d value: %s%n", correlationId, responseLength, value);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }

            } else if (opCode == OPCODE_CAS) { // CAS
                byte successByte = (byte) (casSucceeded ? 1 : 0);
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, successByte);
                final int responseLength = Long.BYTES + 1;

                System.out.printf("[RESPONSE-CAS] correlationId: %d length: %d success: %s%n",
                                correlationId, responseLength, casSucceeded);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }
            } else { // PUT or DELETE – simple ack
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + Long.BYTES, (byte)1);
                final int responseLength = Long.BYTES + 1;

                System.out.printf("[RESPONSE-ACK] correlationId: %d length: %d%n", correlationId, responseLength);

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
        System.out.println("[onTakeSnapshot]");
        for (Map.Entry<String, String> entry : kvStore.entrySet()) {
            System.out.println("    [onTakeSnapshot] " + entry.getKey() + " = " + entry.getValue());
            final byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

            final int recordLength = Integer.BYTES + keyBytes.length + Integer.BYTES + valueBytes.length;
            final MutableDirectBuffer snapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(recordLength));

            int offset = 0;
            snapshotBuffer.putInt(offset, keyBytes.length);
            offset += Integer.BYTES;
            snapshotBuffer.putBytes(offset, keyBytes);
            offset += keyBytes.length;

            snapshotBuffer.putInt(offset, valueBytes.length);
            offset += Integer.BYTES;
            snapshotBuffer.putBytes(offset, valueBytes);

            // publish to snapshot log
            idleStrategy.reset();
            while (snapshotPublication.offer(snapshotBuffer, 0, recordLength) < 0)
            {
                idleStrategy.idle();
            }
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        System.out.println("[loadSnapshot]");
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            int readOffset = offset;

            // Parse key length and key
            final int keyLen = buffer.getInt(readOffset);
            readOffset += Integer.BYTES;

            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(readOffset, keyBytes);
            readOffset += keyLen;

            // Parse value length and value
            final int valLen = buffer.getInt(readOffset);
            readOffset += Integer.BYTES;

            final byte[] valBytes = new byte[valLen];
            buffer.getBytes(readOffset, valBytes);

            final String key = new String(keyBytes, StandardCharsets.UTF_8);
            final String value = new String(valBytes, StandardCharsets.UTF_8);

            kvStore.put(key, value);  // Restore into map
        };
        System.out.println("[loadSnapshot] 1");
        while (!snapshotImage.isEndOfStream())
        {
            final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 10);
            idleStrategy.idle(fragmentsPolled);
        }

        assert snapshotImage.isEndOfStream();   
        System.out.println("[loadSnapshot] 2");
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
        System.out.println("onSessionOpen(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("onSessionClose(" + session + ")");
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