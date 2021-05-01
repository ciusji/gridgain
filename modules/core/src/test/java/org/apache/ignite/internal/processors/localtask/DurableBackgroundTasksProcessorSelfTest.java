/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.localtask;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult.complete;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult.restart;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.COMPLETED;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.INIT;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.STARTED;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessor.metaStorageKey;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing the {@link DurableBackgroundTasksProcessor}.
 */
public class DurableBackgroundTasksProcessorSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /**
     * Checking the correctness of work (without saving to the MetaStorage) of the task in normal mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleTaskExecutionWithoutMetaStorage() throws Exception {
        checkSimpleTaskExecute(false);
    }

    /**
     * Checking the correctness of work (with saving to the MetaStorage) of the task in normal mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleTaskExecutionWithMetaStorage() throws Exception {
        checkSimpleTaskExecute(true);
    }

    /**
     * Checking the correctness of restarting the task without MetaStorage.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartTaskExecutionWithoutMetaStorage() throws Exception {
        checkRestartTaskExecute(false);
    }

    /**
     * Checking the correctness of restarting the task with MetaStorage.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartTaskExecutionWithMetaStorage() throws Exception {
        checkRestartTaskExecute(true);
    }

    /**
     * Checking the correctness of cancelling the task.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelTaskExecution() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        SimpleTask t = new SimpleTask("t");
        IgniteInternalFuture<Void> execAsyncFut = execAsync(n, t, false);

        t.onExecFut.get(getTestTimeout());
        checkStateAndMetaStorage(n, t, STARTED, false);
        assertFalse(execAsyncFut.isDone());

        n.cluster().state(INACTIVE);

        t.onExecFut.get(getTestTimeout());
        t.taskFut.onDone(complete(null));

        execAsyncFut.get(getTestTimeout());
    }

    /**
     * Check that the task will be restarted after restarting the node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartTaskAfterRestartNode() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        SimpleTask t = new SimpleTask("t");
        execAsync(n, t, true);

        t.onExecFut.get(getTestTimeout());
        checkStateAndMetaStorage(n, t, STARTED, true);
        t.taskFut.onDone(restart(null));

        stopAllGrids();

        n = startGrid(0);
        n.cluster().state(ACTIVE);

        t = ((SimpleTask)tasks(n).get(t.name()).task());

        t.onExecFut.get(getTestTimeout());
        checkStateAndMetaStorage(n, t, STARTED, true);
        t.taskFut.onDone(complete(null));
    }

    /**
     * Check that the task will be restarted correctly.
     *
     * @param save Save to MetaStorage.
     * @throws Exception If failed.
     */
    private void checkRestartTaskExecute(boolean save) throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        SimpleTask t = new SimpleTask("t");
        IgniteInternalFuture<Void> execAsyncFut = execAsync(n, t, save);

        t.onExecFut.get(getTestTimeout());
        checkStateAndMetaStorage(n, t, STARTED, save);
        assertFalse(execAsyncFut.isDone());

        if (save) {
            ObservingCheckpointListener checkpointLsnr = new ObservingCheckpointListener();
            dbMgr(n).addCheckpointListener(checkpointLsnr);

            dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

            t.taskFut.onDone(restart(null));
            checkStateAndMetaStorage(n, t, INIT, true);
            assertFalse(execAsyncFut.isDone());

            GridFutureAdapter<Void> onMarkCheckpointBeginFut = checkpointLsnr.onMarkCheckpointBeginAsync(ctx -> {
                checkStateAndMetaStorage(n, t, INIT, true);
                assertFalse(toRmv(n).containsKey(t.name()));
            });

            dbMgr(n).enableCheckpoints(true).get(getTestTimeout());
            onMarkCheckpointBeginFut.get(getTestTimeout());
        }
        else {
            t.taskFut.onDone(restart(null));
            checkStateAndMetaStorage(n, t, INIT, false);
            assertFalse(execAsyncFut.isDone());
        }

        t.reset();

        n.cluster().state(INACTIVE);
        n.cluster().state(ACTIVE);

        t.onExecFut.get(getTestTimeout());
        checkStateAndMetaStorage(n, t, STARTED, save);
        assertFalse(execAsyncFut.isDone());

        t.taskFut.onDone(complete(null));
        execAsyncFut.get(getTestTimeout());
    }

    /**
     * Checking that the task will be processed correctly in the normal mode.
     *
     * @param save Save to MetaStorage.
     * @throws Exception If failed.
     */
    private void checkSimpleTaskExecute(boolean save) throws Exception {
        IgniteEx n = startGrid(0);

        SimpleTask t = new SimpleTask("t");
        IgniteInternalFuture<Void> execAsyncFut = execAsync(n, t, save);

        checkStateAndMetaStorage(n, t, INIT, save);

        checkExecuteSameTask(n, t);
        checkStateAndMetaStorage(n, t, INIT, save);

        assertFalse(t.onExecFut.isDone());
        assertFalse(execAsyncFut.isDone());

        n.cluster().state(ACTIVE);

        t.onExecFut.get(getTestTimeout());

        checkStateAndMetaStorage(n, t, STARTED, save);
        assertFalse(execAsyncFut.isDone());

        if (save) {
            dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

            t.taskFut.onDone(complete(null));
            execAsyncFut.get(getTestTimeout());

            checkStateAndMetaStorage(n, t, COMPLETED, true);

            ObservingCheckpointListener checkpointLsnr = new ObservingCheckpointListener();

            GridFutureAdapter<Void> onMarkCheckpointBeginFut = checkpointLsnr.onMarkCheckpointBeginAsync(
                ctx -> {
                    checkStateAndMetaStorage(n, t, null, true);
                    assertTrue(toRmv(n).containsKey(t.name()));
                }
            );

            GridFutureAdapter<Void> afterCheckpointEndFut = checkpointLsnr.afterCheckpointEndAsync(
                ctx -> {
                    checkStateAndMetaStorage(n, t, null, false);
                    assertFalse(toRmv(n).containsKey(t.name()));
                }
            );

            dbMgr(n).addCheckpointListener(checkpointLsnr);
            dbMgr(n).enableCheckpoints(true).get(getTestTimeout());

            onMarkCheckpointBeginFut.get(getTestTimeout());
            afterCheckpointEndFut.get(getTestTimeout());
        }
        else {
            t.taskFut.onDone(complete(null));
            execAsyncFut.get(getTestTimeout());

            checkStateAndMetaStorage(n, t, null, false);
        }
    }

    /**
     * Checking that until the task is completed it is impossible to add a
     * task with the same {@link DurableBackgroundTask#name name}.
     *
     * @param n Node.
     * @param t Task.
     */
    private void checkExecuteSameTask(IgniteEx n, DurableBackgroundTask t) {
        assertThrows(log, () -> execAsync(n, t, false), IllegalArgumentException.class, null);
        assertThrows(log, () -> execAsync(n, t, true), IllegalArgumentException.class, null);
        assertThrows(log, () -> execAsync(n, new SimpleTask(t.name()), false), IllegalArgumentException.class, null);
        assertThrows(log, () -> execAsync(n, new SimpleTask(t.name()), true), IllegalArgumentException.class, null);
    }

    /**
     * Checking the internal {@link DurableBackgroundTaskState state} of the task and storage in the MetaStorage.
     *
     * @param n Node.
     * @param t Task.
     * @param expState Expected state of the task, {@code null} means that the task should not be.
     * @param expSaved Task is expected to be stored in MetaStorage.
     * @throws IgniteCheckedException If failed.
     */
    private void checkStateAndMetaStorage(
        IgniteEx n,
        DurableBackgroundTask t,
        @Nullable State expState,
        boolean expSaved
    ) throws IgniteCheckedException {
        DurableBackgroundTaskState taskState = tasks(n).get(t.name());

        if (expState == null)
            assertNull(taskState);
        else
            assertEquals(expState, taskState.state());

        DurableBackgroundTask ser = (DurableBackgroundTask)metaStorageOperation(n, ms -> ms.read(metaStorageKey(t)));

        if (expSaved)
            assertEquals(t.name(), ser.name());
        else
            assertNull(ser);
    }

    /**
     * Asynchronous execution of a durable background task.
     *
     * @param n Node.
     * @param t Task.
     * @param save Save task to MetaStorage.
     * @return Task future.
     */
    private IgniteInternalFuture<Void> execAsync(IgniteEx n, DurableBackgroundTask t, boolean save) {
        return durableBackgroundTasksProcessor(n).executeAsync(t, save);
    }

    /**
     * Getting {@code DurableBackgroundTasksProcessor#toRmv}.
     *
     * @param n Node.
     * @return Map of tasks that will be removed from the MetaStorage.
     */
    private Map<String, DurableBackgroundTask> toRmv(IgniteEx n) {
        return getFieldValue(durableBackgroundTasksProcessor(n), "toRmv");
    }

    /**
     * Getting {@code DurableBackgroundTasksProcessor#tasks}.
     *
     * @param n Node.
     * @return Task states map.
     */
    private Map<String, DurableBackgroundTaskState> tasks(IgniteEx n) {
        return getFieldValue(durableBackgroundTasksProcessor(n), "tasks");
    }

    /**
     * Getting durable background task processor.
     *
     * @param n Node.
     * @return Durable background task processor.
     */
    private DurableBackgroundTasksProcessor durableBackgroundTasksProcessor(IgniteEx n) {
        return n.context().durableBackgroundTasksProcessor();
    }

    /**
     * Performing an operation on a MetaStorage.
     *
     * @param n Node.
     * @param fun Function for working with MetaStorage, the argument can be {@code null}.
     * @return The function result.
     * @throws IgniteCheckedException If failed.
     */
    private <R> R metaStorageOperation(
        IgniteEx n,
        IgniteThrowableFunction<MetaStorage, R> fun
    ) throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = dbMgr(n);

        dbMgr.checkpointReadLock();

        try {
            return fun.apply(dbMgr.metaStorage());
        }
        finally {
            dbMgr.checkpointReadUnlock();
        }
    }
}
