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

/**
 * KV service implementing the business logic.
 */
// tag::new_service[]
public class BasicKVClusteredService implements ClusteredService
// end::new_service[]
{
    static final int CORRELATION_ID_OFFSET = 0;
    static final int OPCODE_OFFSET          = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG; 
    static final int KEY_LENGTH_OFFSET      = OPCODE_OFFSET + BitUtil.SIZE_OF_BYTE;
    static final int VALUE_LENGTH_OFFSET    = KEY_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
    static final int HEADER_LENGTH          = VALUE_LENGTH_OFFSET + BitUtil.SIZE_OF_INT; 

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
        System.out.println("[onSessionMessage] offset=" + offset + ", length=" + length);
        
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);                   // <1>
        final byte opCode = buffer.getByte(offset + OPCODE_OFFSET);
        final int keyLen = buffer.getInt(offset + KEY_LENGTH_OFFSET);
        final int valueLen = buffer.getInt(offset + VALUE_LENGTH_OFFSET);

        System.out.println("    [onSessionMessage] 1");
        final int keyOffset = offset + HEADER_LENGTH;
        final byte[] keyBytes = new byte[keyLen];
        buffer.getBytes(keyOffset, keyBytes);
        final String key = new String(keyBytes, StandardCharsets.UTF_8);

        System.out.println("    [onSessionMessage] 2: key=" + key + ", opCode=" + opCode);
        String value = null;
        if (opCode == OPCODE_PUT) { // PUT
            byte[] valueBytes = new byte[valueLen];
            buffer.getBytes(keyOffset + keyLen, valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
            kvStore.put(key, value);
        } else if (opCode == OPCODE_GET) { // GET
            value = kvStore.get(key);
        } else if (opCode == OPCODE_DELETE) { // DELETE
            kvStore.remove(key);
        } 
        else if (opCode == OPCODE_CAS) {
            // Advance offset to read expected and new values
            int valueOffset = keyOffset + keyLen;

            final int expectedLen = buffer.getInt(valueOffset);
            valueOffset += BitUtil.SIZE_OF_INT;

            final byte[] expectedBytes = new byte[expectedLen];
            buffer.getBytes(valueOffset, expectedBytes);
            valueOffset += expectedLen;

            final int newLen = buffer.getInt(valueOffset);
            valueOffset += BitUtil.SIZE_OF_INT;

            final byte[] newBytes = new byte[newLen];
            buffer.getBytes(valueOffset, newBytes);

            final String expectedValue = new String(expectedBytes, StandardCharsets.UTF_8);
            final String newValue = new String(newBytes, StandardCharsets.UTF_8);

            // Perform CAS
            String current = kvStore.get(key);
            boolean success = Objects.equals(current, expectedValue);
            if (success) kvStore.put(key, newValue);

            // Send back response like: [correlationId][byte success=1/0]

        }
        
        System.out.println("    [onSessionMessage] 4");
        if (null != session)                                                                         // <3>
        {
            System.out.println("    [onSessionMessage] 5");
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);                       // <4>

            if (opCode == OPCODE_GET) { // GET
                System.out.println("    [onSessionMessage] 6");
                byte[] valBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG, (byte)(value != null ? 1 : 0));
                egressMessageBuffer.putInt(CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG + 1, valBytes.length);
                egressMessageBuffer.putBytes(CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG + 1 + BitUtil.SIZE_OF_INT, valBytes);
                final int responseLength = BitUtil.SIZE_OF_LONG + 1 + BitUtil.SIZE_OF_INT + valBytes.length;

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }

            } else { // PUT or DELETE – simple ack
                System.out.println("    [onSessionMessage] 7");
                egressMessageBuffer.putByte(CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG, (byte)1);
                final int responseLength = BitUtil.SIZE_OF_LONG + 1;

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, responseLength) < 0) {
                    idleStrategy.idle();
                }
            }
        }
        System.out.println("    [onSessionMessage] 8");
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

            final int recordLength = BitUtil.SIZE_OF_INT + keyBytes.length + BitUtil.SIZE_OF_INT + valueBytes.length;
            final MutableDirectBuffer snapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(recordLength));

            int offset = 0;
            snapshotBuffer.putInt(offset, keyBytes.length);
            offset += BitUtil.SIZE_OF_INT;
            snapshotBuffer.putBytes(offset, keyBytes);
            offset += keyBytes.length;

            snapshotBuffer.putInt(offset, valueBytes.length);
            offset += BitUtil.SIZE_OF_INT;
            snapshotBuffer.putBytes(offset, valueBytes);

            System.out.println("[onTakeSnapshot] 1");
            // publish to snapshot log
            idleStrategy.reset();
            while (snapshotPublication.offer(snapshotBuffer, 0, recordLength) < 0)
            {
                idleStrategy.idle();
            }
        }
        System.out.println("    [onTakeSnapshot] 2");
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
            readOffset += BitUtil.SIZE_OF_INT;

            final byte[] keyBytes = new byte[keyLen];
            buffer.getBytes(readOffset, keyBytes);
            readOffset += keyLen;

            // Parse value length and value
            final int valLen = buffer.getInt(readOffset);
            readOffset += BitUtil.SIZE_OF_INT;

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