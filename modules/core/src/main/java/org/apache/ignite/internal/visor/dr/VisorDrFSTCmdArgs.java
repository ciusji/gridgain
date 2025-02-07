/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.util.IgniteUtils.readGridUuid;
import static org.apache.ignite.internal.util.IgniteUtils.readSet;
import static org.apache.ignite.internal.util.IgniteUtils.writeCollection;
import static org.apache.ignite.internal.util.IgniteUtils.writeGridUuid;

/**
 * DR full state transfer task args.
 */
public class VisorDrFSTCmdArgs extends IgniteDataTransferObject {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** Cache names. */
    private Set<String> caches = Collections.emptySet();

    /** Snapshot id. */
    private long snapshotId = -1;

    /** Data center id. */
    private Set<Byte> dcIds = Collections.emptySet();

    /** Action. */
    private int action;

    /** FST id. */
    @Nullable private IgniteUuid operationId;

    /** */
    private int senderGroup = VisorDrCacheTaskArgs.SENDER_GROUP_NAMED;

    /** */
    private String senderGrpName;

    /**
     * Default constructor.
     */
    public VisorDrFSTCmdArgs() {
        // No-op.
    }

    /** */
    public VisorDrFSTCmdArgs(int action, @Nullable IgniteUuid operationId) {
        this.action = action;
        this.operationId = operationId == null ? IgniteUuid.randomUuid() : operationId;
        senderGrpName = "";
    }

    /** */
    public VisorDrFSTCmdArgs(
        int action,
        Set<String> caches,
        long snapshotId,
        Set<Byte> dcIds,
        int senderGroup,
        String senderGrpName
    ) {
        this.action = action;
        this.caches = caches == null ? Collections.emptySet() : caches;
        this.snapshotId = snapshotId;
        this.dcIds = dcIds == null ? Collections.emptySet() : dcIds;
        this.senderGroup = senderGroup;
        this.senderGrpName = senderGrpName == null ? "" : senderGrpName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(action);
        writeCollection(out, caches);
        out.writeLong(snapshotId);
        writeCollection(out, dcIds);
        out.writeInt(senderGroup);
        out.writeUTF(senderGrpName);
        writeGridUuid(out, operationId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        action = in.readInt();
        caches = readSet(in);
        snapshotId = in.readLong();
        dcIds = readSet(in);
        senderGroup = in.readInt();
        senderGrpName = in.readUTF();
        operationId = readGridUuid(in);
    }

    /**
     * @return Snapshot id.
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     * @return Cache names.
     */
    public Set<String> caches() {
        return caches;
    }

    /**
     * @return Data center id.
     */
    public Set<Byte> dcIds() {
        return dcIds;
    }

    /**
     * @return Action.
     */
    public int action() {
        return action;
    }

    /**
     * @return FST id.
     */
    public @Nullable IgniteUuid operationId() {
        return operationId;
    }

    /**
     * @return Sender group.
     */
    public int senderGroup() {
        return senderGroup;
    }

    /**
     * @return Sender group name.
     */
    public String senderGroupName() {
        return senderGrpName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrFSTCmdArgs.class, this);
    }
}
