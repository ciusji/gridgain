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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Context with information about current checkpoint.
 */
public class CheckpointContextImpl implements CheckpointListener.Context {
    /** Current checkpoint progress. */
    private final CheckpointProgressImpl curr;

    /** Partition map. */
    private final PartitionAllocationMap map;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private final IgniteThreadPoolExecutor asyncRunner;

    /** Heartbeat updater. */
    private final WorkProgressDispatcher heartbeatUpdater;

    /** Pending tasks from executor. */
    private GridCompoundFuture pendingTaskFuture;

    /**
     * @param curr Current checkpoint progress.
     * @param map Partition map.
     * @param asyncRunner Checkpoint runner thread pool.
     * @param heartbeat Heartbeat updater.
     */
    CheckpointContextImpl(
        CheckpointProgressImpl curr,
        PartitionAllocationMap map,
        @Nullable IgniteThreadPoolExecutor asyncRunner,
        WorkProgressDispatcher heartbeat
    ) {
        this.curr = curr;
        this.map = map;
        this.asyncRunner = asyncRunner;
        this.heartbeatUpdater = heartbeat;
        this.pendingTaskFuture = this.asyncRunner == null ? null : new GridCompoundFuture();
    }

    /** {@inheritDoc} */
    @Override public CheckpointProgress progress() {
        return curr;
    }

    /** {@inheritDoc} */
    @Override public boolean nextSnapshot() {
        return curr.nextSnapshot();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> finishedStateFut() {
        return curr.futureFor(FINISHED);
    }

    /** {@inheritDoc} */
    @Override public PartitionAllocationMap partitionStatMap() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public boolean needToSnapshot(String cacheOrGrpName) {
        return curr.snapshotOperation().cacheGroupIds().contains(CU.cacheId(cacheOrGrpName));
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return asyncRunner == null ? null : cmd -> {
            try {
                GridFutureAdapter<?> res = new GridFutureAdapter<>();

                res.listen(fut -> heartbeatUpdater.updateHeartbeat());

                asyncRunner.execute(U.wrapIgniteFuture(cmd, res));

                pendingTaskFuture.add(res);
            }
            catch (RejectedExecutionException e) {
                assert false : "A task should never be rejected by async runner";
            }
        };
    }

    /**
     * Await all async tasks from executor was finished.
     *
     * @throws IgniteCheckedException if fail.
     */
    public void awaitPendingTasksFinished() throws IgniteCheckedException {
        GridCompoundFuture pendingFut = this.pendingTaskFuture;

        this.pendingTaskFuture = new GridCompoundFuture();

        if (pendingFut != null) {
            pendingFut.markInitialized();

            pendingFut.get();
        }
    }
}
