/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.messages;

import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.query.stat.StatsType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;

import java.util.Map;

/**
 * Statistics for some object (index or table) in database.
 */
public class StatsObjectData implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 184;

    /** Statistics key. */
    private StatsKeyMessage key;

    /** Total row count in current object. */
    private long rowsCnt;

    /** Type of statistics. */
    private StatsType type;

    /** Partition id if statistics was collected by partition. */
    private int partId;

    /** Update counter if statistics was collected by partition. */
    private long updCnt;

    /** Columns key to statistic map. */
    @GridDirectMap(keyType = String.class, valueType = StatsColumnData.class)
    private Map<String, StatsColumnData> data;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param rowsCnt Total row count.
     * @param type Statistics type.
     * @param partId Partition id.
     * @param updCnt Partition update counter.
     * @param data Map of statistics column data.
     */
    public StatsObjectData(
            StatsKeyMessage key,
            long rowsCnt,
            StatsType type,
            int partId,
            long updCnt,
            Map<String, StatsColumnData> data
    ) {
        this.key = key;
        this.rowsCnt = rowsCnt;
        this.type = type;
        this.partId = partId;
        this.updCnt = updCnt;
        this.data = data;
    }

    /**
     * @return Statistics key.
     */
    public StatsKeyMessage key() {
        return key;
    }

    /**
     * @return Total rows count.
     */
    public long rowsCnt() {
        return rowsCnt;
    }

    /**
     * @return Statistics type.
     */
    public StatsType type() {
        return type;
    }

    /**
     * @return Partition id.
     */
    public int partId() {
        return partId;
    }

    /**
     * @return Partition update counter.
     */
    public long updCnt() {
        return updCnt;
    }

    /**
     * @return Statistics column data.
     */
    public Map<String, StatsColumnData> data() {
        return data;
    }

    /**
     * Default constructor.
     */
    public StatsObjectData() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap("data", data, MessageCollectionItemType.STRING, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("rowsCnt", rowsCnt))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("updCnt", updCnt))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                data = reader.readMap("data", MessageCollectionItemType.STRING, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                rowsCnt = reader.readLong("rowsCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = StatsType.fromOrdinal(typeOrd);

                reader.incrementState();

            case 5:
                updCnt = reader.readLong("updCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsObjectData.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}
