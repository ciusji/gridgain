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

package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Storage of actual information about current index of compressed segments.
 */
public class SegmentCompressStorage {
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /** If WAL compaction enabled. */
    private final boolean compactionEnabled;

    /** Last successfully compressed segment. */
    private volatile long lastCompressedIdx = -1L;

    /** Last enqueued to compress segment. */
    private long lastEnqueuedToCompressIdx = -1L;

    /** Segments to compress queue. */
    private final Queue<Long> segmentsToCompress = new ArrayDeque<>();

    /** List of currently compressing segments. */
    private final List<Long> compressingSegments = new ArrayList<>();

    /** Compressed segment with maximal index. */
    private long lastMaxCompressedIdx = -1L;

    /** Min uncompressed index to keep. */
    private volatile long minUncompressedIdxToKeep = -1L;

    /** Logger. */
    private final IgniteLogger log;

    /** Counter of segments that are being compressed now. */
    private final AtomicInteger inProgressCnt = new AtomicInteger();

    /**
     * @param compactionEnabled If WAL compaction enabled.
     * @param log Logger.
     */
    private SegmentCompressStorage(
        boolean compactionEnabled,
        IgniteLogger log
    ) {
        this.compactionEnabled = compactionEnabled;

        this.log = log;
    }

    /**
     * @param segmentArchivedStorage Storage of last archived segment.
     * @param compactionEnabled If WAL compaction enabled.
     * @param log Logger.
     */
    static SegmentCompressStorage buildCompressStorage(
        SegmentArchivedStorage segmentArchivedStorage,
        boolean compactionEnabled,
        IgniteLogger log
    ) {
        SegmentCompressStorage storage = new SegmentCompressStorage(compactionEnabled, log);

        segmentArchivedStorage.addObserver(storage::onSegmentArchived);

        return storage;
    }

    /**
     * Callback after segment compression finish.
     *
     * @param compressedIdx Index of compressed segment.
     */
    synchronized void onSegmentCompressed(long compressedIdx) {
        if (log.isInfoEnabled())
            log.info("Segment compressed notification [idx=" + compressedIdx + ']');

        if (compressedIdx > lastMaxCompressedIdx)
            lastMaxCompressedIdx = compressedIdx;

        compressingSegments.remove(compressedIdx);

        if (!compressingSegments.isEmpty())
            this.lastCompressedIdx = Math.min(lastMaxCompressedIdx, compressingSegments.get(0) - 1);
        else
            this.lastCompressedIdx = lastMaxCompressedIdx;

        if (compressedIdx > lastEnqueuedToCompressIdx)
            lastEnqueuedToCompressIdx = compressedIdx;
    }

    /**
     * @return Last compressed segment.
     */
    long lastCompressedIdx() {
        return lastCompressedIdx;
    }

    /**
     * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation. Waits if
     * there's no segment to archive right now.
     */
    synchronized long nextSegmentToCompressOrWait() throws IgniteInterruptedCheckedException {
        try {
            while (segmentsToCompress.peek() == null && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        checkInterrupted();

        Long idx = segmentsToCompress.poll();

        assert idx != null;

        compressingSegments.add(idx);

        return idx;
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Check for interrupt flag was set.
     */
    private void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change compressed idx");
    }

    /**
     * Callback for waking up compressor when new segment is archived.
     */
    private synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        while (lastEnqueuedToCompressIdx < lastAbsArchivedIdx && compactionEnabled) {
            if (log.isInfoEnabled())
                log.info("Enqueuing segment for compression [idx=" + (lastEnqueuedToCompressIdx + 1) + ']');

            segmentsToCompress.add(++lastEnqueuedToCompressIdx);
        }

        notifyAll();
    }

    /**
     * @param idx Minimum raw segment index that should be preserved from deletion.
     */
    void keepUncompressedIdxFrom(long idx) {
        minUncompressedIdxToKeep = idx;
    }

    /**
     * @return  Minimum raw segment index that should be preserved from deletion.
     */
    long keepUncompressedIdxFrom() {
        return minUncompressedIdxToKeep;
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        interrupted = false;
    }

    /**
     * Callback at start of segment compression.
     */
    public void onStartSegmentCompression() {
        inProgressCnt.incrementAndGet();
    }

    /**
     * Callback at finish of segment compression.
     */
    public void onFinishSegmentCompression() {
        inProgressCnt.decrementAndGet();
    }

    /**
     * Check if the segments is being compression now.
     *
     * @return {@code True} if in progress.
     */
    public boolean compressionInProgress() {
        return inProgressCnt.get() > 0;
    }
}
