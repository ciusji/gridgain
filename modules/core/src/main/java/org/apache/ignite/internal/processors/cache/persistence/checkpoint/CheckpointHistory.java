/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.Checkpoint;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Checkpoint history. Holds chronological ordered map with {@link CheckpointEntry CheckpointEntries}.
 * Data is loaded from corresponding checkpoint directory.
 * This directory holds files for checkpoint start and end.
 */
public class CheckpointHistory {
    /** The message appears when no one checkpoint was not reserved for cache. */
    private static final String NOT_RESERVED_WAL_REASON = "Failed to perform reservation of historical WAL segment file";

    /** The message puts down to log when an exception happened during reading reserved WAL. */
    private static final String WAL_SEG_CORRUPTED_REASON = "Segment corrupted";

    /** Reason means no more history reserved for the cache. */
    private static final String NO_MORE_HISTORY_REASON = "Reserved checkpoint is the oldest in history";

    /** Node does not have owning partitions. */
    private static final String NO_PARTITIONS_OWNED_REASON = "Node didn't own any partitions for this group at the time of checkpoint";

    /** Reason means a checkpoint in history reserved can not be applied for cache. */
    private static final String CHECKPOINT_NOT_APPLICABLE_REASON = "Checkpoint was marked as inapplicable for historical rebalancing";

    /** That means all history reserved for cache. */
    private static final String FULL_HISTORY_REASON = "Full history were reserved";

    /** Logger. */
    private final IgniteLogger log;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Maps checkpoint's timestamp (from CP file name) to CP entry.
     * Using TS provides historical order of CP entries in map ( first is oldest )
     */
    private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

    /** The maximal number of checkpoints hold in memory. */
    private final int maxCpHistMemSize;

    /** Should WAL be truncated */
    private final boolean isWalTruncationEnabled;

    /** Maximum size of WAL archive (in bytes) */
    private final long maxWalArchiveSize;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public CheckpointHistory(GridKernalContext ctx) {
        cctx = ctx.cache().context();
        log = ctx.log(getClass());

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        maxWalArchiveSize = dsCfg.getMaxWalArchiveSize();

        isWalTruncationEnabled = maxWalArchiveSize != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

        maxCpHistMemSize = IgniteSystemProperties.getInteger(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, 100);
    }

    /**
     * @param checkpoints Checkpoints.
     */
    public void initialize(List<CheckpointEntry> checkpoints) {
        for (CheckpointEntry e : checkpoints)
            histMap.put(e.timestamp(), e);
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @return Initialized entry.
     * @throws IgniteCheckedException If failed to initialize entry.
     */
    private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
        CheckpointEntry entry = histMap.get(cpTs);

        if (entry == null)
            throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

        return entry;
    }

    /**
     * @return First checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry firstCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.firstEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return Last checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry lastCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.lastEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return First checkpoint WAL pointer if exists. Otherwise {@code null}.
     */
    public WALPointer firstCheckpointPointer() {
        CheckpointEntry entry = firstCheckpoint();

        return entry != null ? entry.checkpointMark() : null;
    }

    /**
     * @return Collection of checkpoint timestamps.
     */
    public Collection<Long> checkpoints(boolean descending) {
        if (descending)
            return histMap.descendingKeySet();

        return histMap.keySet();
    }

    /**
     *
     */
    public Collection<Long> checkpoints() {
        return checkpoints(false);
    }

    /**
     * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself
     * is not finished yet.
     *
     * @param entry Entry to add.
     */
    public void addCheckpoint(CheckpointEntry entry) {
        histMap.put(entry.timestamp(), entry);
    }

    /**
     * @return {@code true} if there is space for next checkpoint.
     */
    public boolean hasSpace() {
        return isWalTruncationEnabled || histMap.size() + 1 <= maxCpHistMemSize;
    }

    /**
     * Clears checkpoint history after WAL truncation.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> onWalTruncated(WALPointer ptr) {
        List<CheckpointEntry> removed = new ArrayList<>();

        FileWALPointer highBound = (FileWALPointer)ptr;

        for (CheckpointEntry cpEntry : histMap.values()) {
            FileWALPointer cpPnt = (FileWALPointer)cpEntry.checkpointMark();

            if (highBound.compareTo(cpPnt) <= 0)
                break;

            if (!removeCheckpoint(cpEntry))
                break;

            removed.add(cpEntry);
        }

        return removed;
    }

    /**
     * Removes checkpoints from history.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> removeCheckpoints(int countToRemove) {
        if (countToRemove == 0)
            return Collections.emptyList();

        List<CheckpointEntry> removed = new ArrayList<>();

        for (Iterator<Map.Entry<Long, CheckpointEntry>> iterator = histMap.entrySet().iterator();
                iterator.hasNext() && removed.size() < countToRemove; ) {
            Map.Entry<Long, CheckpointEntry> entry = iterator.next();

            CheckpointEntry checkpoint = entry.getValue();

            if (!removeCheckpoint(checkpoint))
                break;

            removed.add(checkpoint);
        }

        return removed;
    }

    /**
     * Remove checkpoint from history
     * @param checkpoint Checkpoint to be removed
     * @return Whether checkpoint was removed from history
     */
    private boolean removeCheckpoint(CheckpointEntry checkpoint) {
        if (cctx.wal().reserved(checkpoint.checkpointMark())) {
            U.warn(log, "Could not clear historyMap due to WAL reservation on cp: " + checkpoint +
                ", history map size is " + histMap.size());

            return false;
        }

        histMap.remove(checkpoint.timestamp());

        return true;
    }

    /**
     * Logs and clears checkpoint history after checkpoint finish.
     *
     * @return List of checkpoints removed from history.
     */
    public List<CheckpointEntry> onCheckpointFinished(Checkpoint chp) {
        chp.walSegsCoveredRange(calculateWalSegmentsCovered());

        int removeCount = isWalTruncationEnabled
            ? checkpointCountUntilDeleteByArchiveSize()
            : (histMap.size() - maxCpHistMemSize);

        if (removeCount <= 0)
            return Collections.emptyList();

        List<CheckpointEntry> deletedCheckpoints = removeCheckpoints(removeCount);

        if (isWalTruncationEnabled) {
            int deleted = cctx.wal().truncate(null, firstCheckpointPointer());

            chp.walFilesDeleted(deleted);
        }

        return deletedCheckpoints;
    }

    /**
     * Calculate count of checkpoints to delete by maximum allowed archive size.
     *
     * @return Checkpoint count to be deleted.
     */
     private int checkpointCountUntilDeleteByArchiveSize() {
        long absFileIdxToDel = cctx.wal().maxArchivedSegmentToDelete();

        if (absFileIdxToDel < 0)
            return 0;

        long fileUntilDel = absFileIdxToDel + 1;

        long checkpointFileIdx = absFileIdx(lastCheckpoint());

        int countToRemove = 0;

        for (CheckpointEntry cpEntry : histMap.values()) {
            long currFileIdx = absFileIdx(cpEntry);

            if (checkpointFileIdx <= currFileIdx || fileUntilDel <= currFileIdx)
                return countToRemove;

            countToRemove++;
        }

        return histMap.size() - 1;
    }

    /**
     * Retrieve absolute file index by checkpoint entry.
     *
     * @param pointer checkpoint entry for which need to calculate absolute file index.
     * @return absolute file index for given checkpoint entry.
     */
    private long absFileIdx(CheckpointEntry pointer) {
        return ((FileWALPointer)pointer.checkpointMark()).index();
    }

    /**
     * Calculates indexes of WAL segments covered by last checkpoint.
     *
     * @return list of indexes or empty list if there are no checkpoints.
     */
    private IgniteBiTuple<Long, Long> calculateWalSegmentsCovered() {
        IgniteBiTuple<Long, Long> tup = new IgniteBiTuple<>(-1L, -1L);

        Map.Entry<Long, CheckpointEntry> lastEntry = histMap.lastEntry();

        if (lastEntry == null)
            return tup;

        Map.Entry<Long, CheckpointEntry> previousEntry = histMap.lowerEntry(lastEntry.getKey());

        WALPointer lastWALPointer = lastEntry.getValue().checkpointMark();

        long lastIdx = 0;

        long prevIdx = 0;

        if (lastWALPointer instanceof FileWALPointer) {
            lastIdx = ((FileWALPointer)lastWALPointer).index();

            if (previousEntry != null)
                prevIdx = ((FileWALPointer)previousEntry.getValue().checkpointMark()).index();
        }

        tup.set1(prevIdx);
        tup.set2(lastIdx - 1);

        return tup;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public WALPointer searchPartitionCounter(int grpId, int part, long partCntrSince) {
        CheckpointEntry entry = searchCheckpointEntry(grpId, part, partCntrSince);

        if (entry == null)
            return null;

        return entry.checkpointMark();
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public CheckpointEntry searchCheckpointEntry(int grpId, int part, long partCntrSince) {
        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry entry = entry(cpTs);

                Long foundCntr = entry.partitionCounter(cctx, grpId, part);

                if (foundCntr != null && foundCntr <= partCntrSince)
                    return entry;
            }
            catch (IgniteCheckedException ignore) {
                break;
            }
        }

        return null;
    }

    /**
     * Finds and reserves earliest valid checkpoint for each of given groups and partitions.
     *
     * @param groupsAndPartitions Groups and partitions to find and reserve earliest valid checkpoint.
     *
     * @return Map (groupId, Map (partitionId, earliest valid checkpoint to history search)).
     */
    public Map<Integer, T2<String, Map<Integer, CheckpointEntry>>> searchAndReserveCheckpoints(
        final Map<Integer, Set<Integer>> groupsAndPartitions
    ) {
        if (F.isEmpty(groupsAndPartitions))
            return Collections.emptyMap();

        final Map<Integer, T2<String, Map<Integer, CheckpointEntry>>> res = new HashMap<>();

        CheckpointEntry prevReserved = null;

        // Iterate over all possible checkpoints starting from latest and moving to earliest.
        for (Long cpTs : checkpoints(true)) {
            CheckpointEntry chpEntry = null;

            try {
                chpEntry = entry(cpTs);

                boolean reserved = cctx.wal().reserve(chpEntry.checkpointMark());

                // If checkpoint WAL history can't be reserved, stop searching.
                if (!reserved) {
                    for (Integer grpId : groupsAndPartitions.keySet())
                        res.computeIfAbsent(grpId, key -> new T2<>())
                            .set1(NOT_RESERVED_WAL_REASON);

                    return res;
                }

                for (Integer grpId : new HashSet<>(groupsAndPartitions.keySet()))
                    if (!isCheckpointApplicableForGroup(grpId, chpEntry)) {
                        res.computeIfAbsent(grpId, key -> new T2<>())
                            .set1(CHECKPOINT_NOT_APPLICABLE_REASON);

                        groupsAndPartitions.remove(grpId);
                    }

                for (Map.Entry<Integer, CheckpointEntry.GroupState> state : chpEntry.groupState(cctx).entrySet()) {
                    int grpId = state.getKey();
                    CheckpointEntry.GroupState cpGrpState = state.getValue();

                    Set<Integer> applicablePartitions = groupsAndPartitions.get(grpId);

                    if (F.isEmpty(applicablePartitions))
                        continue;

                    Set<Integer> inapplicablePartitions = null;

                    for (Integer partId : applicablePartitions) {
                        int pIdx = cpGrpState.indexByPartition(partId);

                        if (pIdx >= 0)
                            res.computeIfAbsent(grpId, k -> new T2<>(null, new HashMap<>())).get2().put(partId, chpEntry);
                        else {
                            if (inapplicablePartitions == null)
                                inapplicablePartitions = new HashSet<>();

                            // Partition is no more applicable for history search, exclude partition from searching.
                            inapplicablePartitions.add(partId);
                        }
                    }

                    if (!F.isEmpty(inapplicablePartitions))
                        for (Integer partId : inapplicablePartitions)
                            applicablePartitions.remove(partId);
                }

                // Remove groups from search with empty set of applicable partitions.
                for (Map.Entry<Integer, Set<Integer>> e : new HashSet<>(groupsAndPartitions.entrySet()))
                    if (e.getValue().isEmpty()) {
                        res.compute(e.getKey(), (key, val) -> {
                            if (val == null)
                                return new T2<>(NO_PARTITIONS_OWNED_REASON, null);

                            val.set1(FULL_HISTORY_REASON);

                            return val;
                        });

                        groupsAndPartitions.remove(e.getKey());
                    }

                // All groups are no more applicable, release history and stop searching.
                if (groupsAndPartitions.isEmpty()) {
                    cctx.wal().release(chpEntry.checkpointMark());

                    break;
                }
                else {
                    // Release previous checkpoint marker.
                    if (prevReserved != null)
                        cctx.wal().release(prevReserved.checkpointMark());

                    prevReserved = chpEntry;
                }
            }
            catch (IgniteCheckedException ex) {
                U.warn(log, "Failed to process checkpoint: " + (chpEntry != null ? chpEntry : "none"), ex);

                for (Integer grpId : groupsAndPartitions.keySet())
                    res.computeIfAbsent(grpId, key -> new T2<>())
                        .set1(WAL_SEG_CORRUPTED_REASON);

                try {
                    cctx.wal().release(chpEntry.checkpointMark());
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to release checkpoint WAL pointer: " + chpEntry, e);
                }

                return res;
            }
        }

        for (Integer grpId : groupsAndPartitions.keySet())
            res.computeIfAbsent(grpId, key -> new T2<>())
                .set1(NO_MORE_HISTORY_REASON);

        return res;
    }

    /**
     * Checkpoint is not applicable when:
     * 1) WAL was disabled somewhere after given checkpoint.
     * 2) Checkpoint doesn't contain specified {@code grpId}.
     *
     * @param grpId Group ID.
     * @param cp Checkpoint.
     */
    public boolean isCheckpointApplicableForGroup(int grpId, CheckpointEntry cp) throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) cctx.database();

        if (dbMgr.isCheckpointInapplicableForWalRebalance(cp.timestamp(), grpId))
            return false;

        if (!cp.groupState(cctx).containsKey(grpId))
            return false;

        return true;
    }
}
