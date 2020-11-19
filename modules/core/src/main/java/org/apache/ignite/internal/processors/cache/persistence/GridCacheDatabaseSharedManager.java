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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccTxRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.ReencryptionStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistoryResult;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointStatus;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.Checkpointer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.ReservationReason;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_RECOVERY_SEMAPHORE_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_LOCK_HOLD_COUNT;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;
import static org.apache.ignite.internal.util.IgniteUtils.checkpointBufferSize;

/**
 *
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class GridCacheDatabaseSharedManager extends IgniteCacheDatabaseSharedManager {
    /** */
    public static final String IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC = "IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC";

    /** Skip checkpoint on node stop flag. */
    public static final String IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP = "IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP";

    /** Log read lock holders. */
    public static final String IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS = "IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS";

    /** MemoryPolicyConfiguration name reserved for meta store. */
    public static final String METASTORE_DATA_REGION_NAME = "metastoreMemPlc";

    /**
     * Threshold to calculate limit for pages list on-heap caches.
     * <p>
     * Note: When a checkpoint is triggered, we need some amount of page memory to store pages list on-heap cache.
     * If a checkpoint is triggered by "too many dirty pages" reason and pages list cache is rather big, we can get
     * {@code IgniteOutOfMemoryException}. To prevent this, we can limit the total amount of cached page list buckets,
     * assuming that checkpoint will be triggered if no more then 3/4 of pages will be marked as dirty (there will be
     * at least 1/4 of clean pages) and each cached page list bucket can be stored to up to 2 pages (this value is not
     * static, but depends on PagesCache.MAX_SIZE, so if PagesCache.MAX_SIZE > PagesListNodeIO#getCapacity it can take
     * more than 2 pages). Also some amount of page memory needed to store page list metadata.
     */
    private static final double PAGE_LIST_CACHE_LIMIT_THRESHOLD = 0.1;

    /** */
    private final int walRebalanceThreshold = getInteger(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, 500);

    /** Prefer historical rebalance flag. */
    private final boolean preferWalRebalance = getBoolean(IGNITE_PREFER_WAL_REBALANCE);

    /** Value of property for throttling policy override. */
    private final String throttlingPlcOverride = IgniteSystemProperties.getString(
        IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED);

    /** */
    private static final String MBEAN_NAME = "DataStorageMetrics";

    /** */
    private static final String MBEAN_GROUP = "Persistent Store";

    /** WAL marker prefix for meta store. */
    private static final String WAL_KEY_PREFIX = "grp-wal-";

    /** Prefix for meta store records which means that WAL was disabled globally for some group. */
    private static final String WAL_GLOBAL_KEY_PREFIX = WAL_KEY_PREFIX + "disabled-";

    /** Prefix for meta store records which means that WAL was disabled locally for some group. */
    private static final String WAL_LOCAL_KEY_PREFIX = WAL_KEY_PREFIX + "local-disabled-";

    /** Prefix for meta store records which means that checkpoint entry for some group is not applicable for WAL rebalance. */
    private static final String CHECKPOINT_INAPPLICABLE_FOR_REBALANCE = "cp-wal-rebalance-inapplicable-";

    /** Throttle logging threshold. Warning will be raised if thread is being parked for this long. */
    private static final long THROTTLE_LOGGING_THRESHOLD = TimeUnit.SECONDS.toNanos(5);

    /** Throttle queue size threshold. Async applying will be throttled starting from this queue size. */
    private static final int THROTTLE_QUEUE_SIZE_THRESHOLD = 10_000;

    /** */
    private FilePageStoreManager storeMgr;

    /** */
    @Nullable CheckpointManager checkpointManager;

    /** Database configuration. */
    private final DataStorageConfiguration persistenceCfg;

    /**
     * The position of last seen WAL pointer. Used for resumming logging from this pointer.
     *
     * If binary memory recovery pefrormed on node start, the checkpoint END pointer will store
     * not the last WAL pointer and can't be used for resumming logging.
     */
    private volatile WALPointer walTail;

    /** Map from a cacheId to a future indicating that there is an in-progress index rebuild for the given cache. */
    private final ConcurrentMap<Integer, GridFutureAdapter<Void>> idxRebuildFuts = new ConcurrentHashMap<>();

    /**
     * Lock holder for compatible folders mode. Null if lock holder was created at start node. <br>
     * In this case lock is held on PDS resover manager and it is not required to manage locking here
     */
    @Nullable private FileLockHolder fileLockHolder;

    /** Lock wait time. */
    private final long lockWaitTime;

    /** This is the earliest WAL pointer that was reserved during exchange and would release after exchange completed. */
    private WALPointer reservedForExchange;

    /** This is the earliest WAL pointer that was reserved during preloading. */
    private volatile WALPointer reservedForPreloading;

    /** Snapshot manager. */
    private IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private DataStorageMetricsImpl persStoreMetrics;

    /**
     * MetaStorage instance. Value {@code null} means storage not initialized yet.
     * Guarded by {@link GridCacheDatabaseSharedManager#checkpointReadLock()}
     */
    private MetaStorage metaStorage;

    /** Temporary metastorage to migration of index partition. {@see IGNITE-8735}. */
    private MetaStorage.TmpStorage tmpMetaStorage;

    /** */
    private List<MetastorageLifecycleListener> metastorageLifecycleLsnrs;

    /** Initially disabled cache groups. */
    private Collection<Integer> initiallyGlobalWalDisabledGrps = new HashSet<>();

    /** Initially local wal disabled groups. */
    private Collection<Integer> initiallyLocWalDisabledGrps = new HashSet<>();

    /** Flag allows to log additional information about partitions during recovery phases. */
    private final boolean recoveryVerboseLogging =
        getBoolean(IgniteSystemProperties.IGNITE_RECOVERY_VERBOSE_LOGGING, false);

    /** Page list cache limits per data region. */
    private final Map<String, AtomicLong> pageListCacheLimits = new ConcurrentHashMap<>();

    /** Lock for releasing history for preloading. */
    private ReentrantLock releaseHistForPreloadingLock = new ReentrantLock();

    /** Data regions which should be checkpointed. */
    protected final Set<DataRegion> checkpointedDataRegions = new GridConcurrentHashSet<>();

    /**
     * @param ctx Kernal context.
     */
    public GridCacheDatabaseSharedManager(GridKernalContext ctx) {
        IgniteConfiguration cfg = ctx.config();

        persistenceCfg = cfg.getDataStorageConfiguration();

        assert persistenceCfg != null;

        lockWaitTime = persistenceCfg.getLockWaitTime();

        persStoreMetrics = new DataStorageMetricsImpl(
            ctx.metric(),
            persistenceCfg.isMetricsEnabled(),
            persistenceCfg.getMetricsRateTimeInterval(),
            persistenceCfg.getMetricsSubIntervalCount()
        );
    }

    /**
     * @return File store manager.
     */
    public FilePageStoreManager getFileStoreManager() {
        return storeMgr;
    }

    /** */
    private void notifyMetastorageReadyForRead() throws IgniteCheckedException {
        for (MetastorageLifecycleListener lsnr : metastorageLifecycleLsnrs)
            lsnr.onReadyForRead(metaStorage);
    }

    /** */
    private void notifyMetastorageReadyForReadWrite() throws IgniteCheckedException {
        for (MetastorageLifecycleListener lsnr : metastorageLifecycleLsnrs)
            lsnr.onReadyForReadWrite(metaStorage);
    }

    /**
     *
     */
    public Checkpointer getCheckpointer() {
        return checkpointManager.getCheckpointer();
    }

    /**
     * Returns true if historical rebalance is preferred,
     * false means using heuristic for determine rebalance type.
     *
     * @return Flag of preferred historical rebalance.
     */
    public boolean preferWalRebalance() {
        return preferWalRebalance;
    }

    /**
     * For test use only.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        IgniteInternalFuture<Void> fut = checkpointManager.enableCheckpoints(enable);

        wakeupForCheckpoint("enableCheckpoints()");

        return fut;
    }

    /** {@inheritDoc} */
    @Override protected void initDataRegions0(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        super.initDataRegions0(memCfg);

        addDataRegion(
            memCfg,
            createMetastoreDataRegionConfig(memCfg),
            false
        );

        persStoreMetrics.regionMetrics(memMetricsMap.values());
    }

    /**
     * Create metastorage data region configuration with enabled persistence by default.
     *
     * @param storageCfg Data storage configuration.
     * @return Data region configuration.
     */
    private DataRegionConfiguration createMetastoreDataRegionConfig(DataStorageConfiguration storageCfg) {
        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setName(METASTORE_DATA_REGION_NAME);
        cfg.setInitialSize(storageCfg.getSystemRegionInitialSize());
        cfg.setMaxSize(storageCfg.getSystemRegionMaxSize());
        cfg.setPersistenceEnabled(true);
        cfg.setLazyMemoryAllocation(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        snapshotMgr = cctx.snapshot();

        IgnitePageStoreManager store = cctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        storeMgr = (FilePageStoreManager)store;

        final GridKernalContext kernalCtx = cctx.kernalContext();

        assert !kernalCtx.clientNode();

        if (!kernalCtx.clientNode()) {
            kernalCtx.internalSubscriptionProcessor().registerDatabaseListener(new MetastorageRecoveryLifecycle());

            checkpointManager = new CheckpointManager(
                kernalCtx::log,
                cctx.igniteInstanceName(),
                "db-checkpoint-thread",
                cctx.wal(),
                kernalCtx.workersRegistry(),
                persistenceCfg,
                storeMgr,
                this::isCheckpointInapplicableForWalRebalance,
                this::checkpointedDataRegions,
                this::cacheGroupContexts,
                this::getPageMemoryForCacheGroup,
                resolveThrottlingPolicy(),
                snapshotMgr,
                persistentStoreMetricsImpl(),
                kernalCtx.longJvmPauseDetector(),
                kernalCtx.failure(),
                kernalCtx.cache());

            final FileLockHolder preLocked = kernalCtx.pdsFolderResolver()
                .resolveFolders()
                .getLockedFileLockHolder();

            acquireFileLock(preLocked);

            cleanupTempCheckpointDirectory();

            persStoreMetrics.wal(cctx.wal());
        }
    }

    /** */
    public Collection<DataRegion> checkpointedDataRegions() {
        return checkpointedDataRegions;
    }

    /** */
    private Collection<CacheGroupContext> cacheGroupContexts() {
        return cctx.cache().cacheGroups();
    }

    /**
     * Cleanup checkpoint directory from all temporary files.
     */
    @Override public void cleanupTempCheckpointDirectory() throws IgniteCheckedException {
        checkpointManager.cleanupTempCheckpointDirectory();
    }

    /** {@inheritDoc} */
    @Override public void cleanupRestoredCaches() {
        if (dataRegionMap.isEmpty())
            return;

        boolean hasMvccCache = false;

        for (CacheGroupDescriptor grpDesc : cctx.cache().cacheGroupDescriptors().values()) {
            hasMvccCache |= grpDesc.config().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT;

            String regionName = grpDesc.config().getDataRegionName();

            DataRegion region = regionName != null ? dataRegionMap.get(regionName) : dfltDataRegion;

            if (region == null)
                continue;

            if (log.isInfoEnabled())
                log.info("Page memory " + region.config().getName() + " for " + grpDesc + " has invalidated.");

            int partitions = grpDesc.config().getAffinity().partitions();

            if (region.pageMemory() instanceof PageMemoryEx) {
                PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

                for (int partId = 0; partId < partitions; partId++)
                    memEx.invalidate(grpDesc.groupId(), partId);

                memEx.invalidate(grpDesc.groupId(), PageIdAllocator.INDEX_PARTITION);
            }
        }

        if (!hasMvccCache && dataRegionMap.containsKey(TxLog.TX_LOG_CACHE_NAME)) {
            PageMemory memory = dataRegionMap.get(TxLog.TX_LOG_CACHE_NAME).pageMemory();

            if (memory instanceof PageMemoryEx)
                ((PageMemoryEx)memory).invalidate(TxLog.TX_LOG_CACHE_ID, PageIdAllocator.INDEX_PARTITION);
        }

        final boolean hasMvccCache0 = hasMvccCache;

        storeMgr.cleanupPageStoreIfMatch(
            new Predicate<Integer>() {
                @Override public boolean test(Integer grpId) {
                    return MetaStorage.METASTORAGE_CACHE_ID != grpId &&
                        (TxLog.TX_LOG_CACHE_ID != grpId || !hasMvccCache0);
                }
            },
            true);
    }

    /** {@inheritDoc} */
    @Override public void cleanupCheckpointDirectory() throws IgniteCheckedException {
        checkpointManager.cleanupCheckpointDirectory();
    }

    /**
     * @param preLocked Pre-locked file lock holder.
     */
    private void acquireFileLock(FileLockHolder preLocked) throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode())
            return;

        fileLockHolder = preLocked == null ?
            new FileLockHolder(storeMgr.workDir().getPath(), cctx.kernalContext(), log) : preLocked;

        if (!fileLockHolder.isLocked()) {
            if (log.isDebugEnabled())
                log.debug("Try to capture file lock [nodeId=" +
                    cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

            fileLockHolder.tryLock(lockWaitTime);
        }
    }

    /**
     *
     */
    private void releaseFileLock() {
        if (cctx.kernalContext().clientNode() || fileLockHolder == null)
            return;

        if (log.isDebugEnabled())
            log.debug("Release file lock [nodeId=" +
                cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

        fileLockHolder.close();
    }

    /** {@inheritDoc} */
    @Override public DataRegion addDataRegion(DataStorageConfiguration dataStorageCfg, DataRegionConfiguration dataRegionCfg,
        boolean trackable) throws IgniteCheckedException {
        DataRegion region = super.addDataRegion(dataStorageCfg, dataRegionCfg, trackable);

        checkpointedDataRegions.add(region);

        return region;
    }

    /** */
    private void readMetastore() throws IgniteCheckedException {
        try {
            CheckpointStatus status = readCheckpointStatus();

            checkpointReadLock();

            try {
                dataRegion(METASTORE_DATA_REGION_NAME).pageMemory().start();

                performBinaryMemoryRestore(status, onlyMetastorageGroup(), physicalRecords(), false);

                metaStorage = createMetastorage(true);

                applyLogicalUpdates(status, onlyMetastorageGroup(), onlyMetastorageAndEncryptionRecords(), false);

                fillWalDisabledGroups();

                checkpointManager.initializeStorage();

                notifyMetastorageReadyForRead();
            }
            finally {
                metaStorage = null;

                dataRegion(METASTORE_DATA_REGION_NAME).pageMemory().stop(false);

                cctx.pageStore().cleanupPageStoreIfMatch(new Predicate<Integer>() {
                    @Override public boolean test(Integer grpId) {
                        return MetaStorage.METASTORAGE_CACHE_ID == grpId;
                    }
                }, false);

                checkpointReadUnlock();
            }
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        snapshotMgr = cctx.snapshot();

        checkpointManager.init();

        super.onActivate(ctx);

        if (!cctx.kernalContext().clientNode())
            finishRecovery();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        onKernalStop0(false);

        super.onDeActivate(kctx);

        /* Must be here, because after deactivate we can invoke activate and file lock must be already configured */
        checkpointManager.unblockCheckpointLock();
    }

    /** {@inheritDoc} */
    @Override protected void registerMetricsMBeans(IgniteConfiguration cfg) {
        super.registerMetricsMBeans(cfg);

        registerMetricsMBean(
            cctx.kernalContext().config(),
            MBEAN_GROUP,
            MBEAN_NAME,
            persStoreMetrics,
            DataStorageMetricsMXBean.class
        );
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override protected IgniteOutClosure<Long> freeSpaceProvider(final DataRegionConfiguration dataRegCfg) {
        if (!dataRegCfg.isPersistenceEnabled())
            return super.freeSpaceProvider(dataRegCfg);

        final String dataRegName = dataRegCfg.getName();

        return new IgniteOutClosure<Long>() {
            @Override public Long apply() {
                long freeSpace = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    freeSpace += ((GridCacheOffheapManager)grpCtx.offheap()).freeSpace();
                }

                return freeSpace;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected DataRegionMetricsProvider dataRegionMetricsProvider(final DataRegionConfiguration dataRegCfg) {
        if (!dataRegCfg.isPersistenceEnabled())
            return super.dataRegionMetricsProvider(dataRegCfg);

        final String dataRegName = dataRegCfg.getName();

        return new DataRegionMetricsProvider() {
            @Override public long partiallyFilledPagesFreeSpace() {
                long freeSpace = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    freeSpace += ((GridCacheOffheapManager)grpCtx.offheap()).freeSpace();
                }

                return freeSpace;
            }

            @Override public long emptyDataPages() {
                long emptyDataPages = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    emptyDataPages += ((GridCacheOffheapManager)grpCtx.offheap()).emptyDataPages();
                }

                return emptyDataPages;
            }
        };
    }

    /**
     * Restores last valid WAL pointer and resumes logging from that pointer.
     * Re-creates metastorage if needed.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void finishRecovery() throws IgniteCheckedException {
        assert !cctx.kernalContext().clientNode();

        long time = System.currentTimeMillis();

        CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);

        try {
            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.beforeResumeWalLogging(this);

            // Try to resume logging since last finished checkpoint if possible.
            if (walTail == null) {
                CheckpointStatus status = readCheckpointStatus();

                walTail = CheckpointStatus.NULL_PTR.equals(status.endPtr) ? null : status.endPtr;
            }

            resumeWalLogging();

            walTail = null;

            // Recreate metastorage to refresh page memory state after deactivation.
            if (metaStorage == null)
                metaStorage = createMetastorage(false);

            notifyMetastorageReadyForReadWrite();

            U.log(log, "Finish recovery performed in " + (System.currentTimeMillis() - time) + " ms.");
        }
        catch (IgniteCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class))
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
        }
    }

    /**
     * @param readOnly Metastorage read-only mode.
     * @return Instance of Metastorage.
     * @throws IgniteCheckedException If failed to create metastorage.
     */
    private MetaStorage createMetastorage(boolean readOnly) throws IgniteCheckedException {
        cctx.pageStore().initializeForMetastorage();

        MetaStorage storage = new MetaStorage(
            cctx,
            dataRegion(METASTORE_DATA_REGION_NAME),
            (DataRegionMetricsImpl) memMetricsMap.get(METASTORE_DATA_REGION_NAME),
            readOnly
        );

        storage.init(this);

        return storage;
    }

    /**
     * @param cacheGroupsPredicate Cache groups to restore.
     * @param recordTypePredicate Filter records by type.
     * @return Last seen WAL pointer during binary memory recovery.
     * @throws IgniteCheckedException If failed.
     */
    private RestoreBinaryState restoreBinaryMemory(
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate
    ) throws IgniteCheckedException {
        long time = System.currentTimeMillis();

        try {
            if (log.isInfoEnabled())
                log.info("Starting binary memory restore for: " + cctx.cache().cacheGroupDescriptors().keySet());

            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.beforeBinaryMemoryRestore(this);

            CheckpointStatus status = readCheckpointStatus();

            // First, bring memory to the last consistent checkpoint state if needed.
            // This method should return a pointer to the last valid record in the WAL.
            RestoreBinaryState binaryState = performBinaryMemoryRestore(
                status,
                cacheGroupsPredicate,
                recordTypePredicate,
                true
            );

            WALPointer restored = binaryState.lastReadRecordPointer();

            if (restored.equals(CheckpointStatus.NULL_PTR))
                restored = null; // This record is first
            else
                restored = restored.next();

            if (restored == null && !status.endPtr.equals(CheckpointStatus.NULL_PTR)) {
                throw new StorageException("The memory cannot be restored. The critical part of WAL archive is missing " +
                    "[tailWalPtr=" + restored + ", endPtr=" + status.endPtr + ']');
            }
            else if (restored != null)
                U.log(log, "Binary memory state restored at node startup [restoredPtr=" + restored + ']');

            // Wal logging is now available.
            cctx.wal().resumeLogging(restored);

            // Log MemoryRecoveryRecord to make sure that old physical records are not replayed during
            // next physical recovery.
            checkpointManager.memoryRecoveryRecordPtr(cctx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis())));

            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.afterBinaryMemoryRestore(this, binaryState);

            if (log.isInfoEnabled())
                log.info("Binary recovery performed in " + (System.currentTimeMillis() - time) + " ms.");

            return binaryState;
        }
        catch (IgniteCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class))
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        checkpointManager.stop(cancel);

        super.onKernalStop0(cancel);

        unregisterMetricsMBean(
            cctx.gridConfig(),
            MBEAN_GROUP,
            MBEAN_NAME
        );

        metaStorage = null;
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        releaseFileLock();
    }

    /** */
    private long[] calculateFragmentSizes(int concLvl, long cacheSize, long chpBufSize) {
        if (concLvl < 2)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = cacheSize / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        long[] sizes = new long[concLvl + 1];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        sizes[concLvl] = chpBufSize;

        return sizes;
    }

    /** {@inheritDoc} */
    @Override protected PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        DataStorageConfiguration memCfg,
        DataRegionConfiguration plcCfg,
        DataRegionMetricsImpl memMetrics,
        final boolean trackable,
        PageReadWriteManager pmPageMgr
    ) {
        if (!plcCfg.isPersistenceEnabled())
            return super.createPageMemory(memProvider, memCfg, plcCfg, memMetrics, trackable, pmPageMgr);

        memMetrics.persistenceEnabled(true);

        long cacheSize = plcCfg.getMaxSize();

        // Checkpoint buffer size can not be greater than cache size, it does not make sense.
        long chpBufSize = checkpointBufferSize(plcCfg);

        if (chpBufSize > cacheSize) {
            U.quietAndInfo(log,
                "Configured checkpoint page buffer size is too big, setting to the max region size [size="
                    + U.readableSize(cacheSize, false) + ",  memPlc=" + plcCfg.getName() + ']');

            chpBufSize = cacheSize;
        }

        GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker;

        if (trackable) {
            changeTracker = new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(
                    Long page,
                    FullPageId fullId,
                    PageMemoryEx pageMem
                ) throws IgniteCheckedException {
                    snapshotMgr.onChangeTrackerPage(page, fullId, pageMem);
                }
            };
        }
        else
            changeTracker = null;

        PageMemoryImpl pageMem = new PageMemoryImpl(
            wrapMetricsMemoryProvider(memProvider, memMetrics),
            calculateFragmentSizes(
                memCfg.getConcurrencyLevel(),
                cacheSize,
                chpBufSize
            ),
            cctx,
            pmPageMgr,
            memCfg.getPageSize(),
            (fullId, pageBuf, tag) -> {
                memMetrics.onPageWritten();

                // We can write only page from disk into snapshot.
                snapshotMgr.beforePageWrite(fullId);

                // Write page to disk.
                pmPageMgr.write(fullId.groupId(), fullId.pageId(), pageBuf, tag, true);

                getCheckpointer().currentProgress().updateEvictedPages(1);
            },
            changeTracker,
            this,
            memMetrics,
            resolveThrottlingPolicy(),
            new IgniteOutClosure<CheckpointProgress>() {
                @Override public CheckpointProgress apply() {
                    return getCheckpointer().currentProgress();
                }
            }
        );

        memMetrics.pageMemory(pageMem);

        return pageMem;
    }

    /**
     * @param memoryProvider0 Memory provider.
     * @param memMetrics Memory metrics.
     * @return Wrapped memory provider.
     */
    @Override protected DirectMemoryProvider wrapMetricsMemoryProvider(
        final DirectMemoryProvider memoryProvider0,
        final DataRegionMetricsImpl memMetrics
    ) {
        return new DirectMemoryProvider() {
            private AtomicInteger checkPointBufferIdxCnt = new AtomicInteger();

            private final DirectMemoryProvider memProvider = memoryProvider0;

            @Override public void initialize(long[] chunkSizes) {
                memProvider.initialize(chunkSizes);

                checkPointBufferIdxCnt.set(chunkSizes.length);
            }

            @Override public void shutdown(boolean deallocate) {
                memProvider.shutdown(deallocate);
            }

            @Override public DirectMemoryRegion nextRegion() {
                DirectMemoryRegion nextMemoryRegion = memProvider.nextRegion();

                if (nextMemoryRegion == null)
                    return null;

                int idx = checkPointBufferIdxCnt.decrementAndGet();

                long chunkSize = nextMemoryRegion.size();

                // Checkpoint chunk last in the long[] chunkSizes.
                if (idx != 0)
                    memMetrics.updateOffHeapSize(chunkSize);
                else
                    memMetrics.updateCheckpointBufferSize(chunkSize);

                return nextMemoryRegion;
            }
        };
    }

    /**
     * Resolves throttling policy according to the settings.
     */
    @NotNull private PageMemoryImpl.ThrottlingPolicy resolveThrottlingPolicy() {
        PageMemoryImpl.ThrottlingPolicy plc = persistenceCfg.isWriteThrottlingEnabled()
            ? PageMemoryImpl.ThrottlingPolicy.SPEED_BASED
            : PageMemoryImpl.ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY;

        if (throttlingPlcOverride != null) {
            try {
                plc = PageMemoryImpl.ThrottlingPolicy.valueOf(throttlingPlcOverride.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                log.error("Incorrect value of IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED property. " +
                    "The default throttling policy will be used [plc=" + throttlingPlcOverride +
                    ", defaultPlc=" + plc + ']');
            }
        }
        return plc;
    }

    /** {@inheritDoc} */
    @Override protected void checkRegionEvictionProperties(DataRegionConfiguration regCfg, DataStorageConfiguration dbCfg)
        throws IgniteCheckedException {
        if (!regCfg.isPersistenceEnabled())
            super.checkRegionEvictionProperties(regCfg, dbCfg);
        else if (regCfg.getPageEvictionMode() != DataPageEvictionMode.DISABLED) {
            U.warn(log, "Page eviction mode will have no effect because the oldest pages are evicted automatically " +
                "if Ignite persistence is enabled: " + regCfg.getName());
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkPageSize(DataStorageConfiguration memCfg) {
        if (memCfg.getPageSize() == 0) {
            try {
                assert cctx.pageStore() instanceof FilePageStoreManager :
                    "Invalid page store manager was created: " + cctx.pageStore();

                Path anyIdxPartFile = IgniteUtils.searchFileRecursively(
                    ((FilePageStoreManager)cctx.pageStore()).workDir().toPath(), FilePageStoreManager.INDEX_FILE_NAME);

                if (anyIdxPartFile != null) {
                    memCfg.setPageSize(resolvePageSizeFromPartitionFile(anyIdxPartFile));

                    return;
                }
            }
            catch (IgniteCheckedException | IOException | IllegalArgumentException e) {
                U.quietAndWarn(log, "Attempt to resolve pageSize from store files failed: " + e.getMessage());

                U.quietAndWarn(log, "Default page size will be used: " + DataStorageConfiguration.DFLT_PAGE_SIZE + " bytes");
            }

            memCfg.setPageSize(DataStorageConfiguration.DFLT_PAGE_SIZE);
        }
    }

    /**
     * @param partFile Partition file.
     */
    private int resolvePageSizeFromPartitionFile(Path partFile) throws IOException, IgniteCheckedException {
        FileIOFactory ioFactory = persistenceCfg.getFileIOFactory();

        try (FileIO fileIO = ioFactory.create(partFile.toFile())) {
            int minimalHdr = FilePageStore.HEADER_SIZE;

            if (fileIO.size() < minimalHdr)
                throw new IgniteCheckedException("Partition file is too small: " + partFile);

            ByteBuffer hdr = ByteBuffer.allocate(minimalHdr).order(ByteOrder.nativeOrder());

            fileIO.readFully(hdr);

            hdr.rewind();

            hdr.getLong(); // Read signature.

            hdr.getInt(); // Read version.

            hdr.get(); // Read type.

            int pageSize = hdr.getInt();

            if (pageSize == 2048) {
                U.quietAndWarn(log, "You are currently using persistent store with 2K pages (DataStorageConfiguration#" +
                    "pageSize). If you use SSD disk, consider migrating to 4K pages for better IO performance.");
            }

            return pageSize;
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        // Try to restore partition states.
        if (fut.localJoinExchange() || fut.activateCluster()
            || (fut.exchangeActions() != null && !F.isEmpty(fut.exchangeActions().cacheGroupsToStart()))) {
            U.doInParallel(
                cctx.kernalContext().getSystemExecutorService(),
                cctx.cache().cacheGroups(),
                cacheGroup -> {
                    if (cacheGroup.isLocal())
                        return null;

                    cctx.database().checkpointReadLock();

                    try {
                        cacheGroup.offheap().restorePartitionStates(Collections.emptyMap());

                        if (cacheGroup.localStartVersion().equals(fut.initialVersion()))
                            cacheGroup.topology().afterStateRestored(fut.initialVersion());

                        fut.timeBag().finishLocalStage("Restore partition states " +
                            "[grp=" + cacheGroup.cacheOrGroupName() + "]");
                    }
                    finally {
                        cctx.database().checkpointReadUnlock();
                    }

                    return null;
                }
            );

            fut.timeBag().finishGlobalStage("Restore partition states");

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                List<PartitionsExchangeAware> exchangeAwareComponents = cctx.exchange().exchangeAwareComponents();

                for (PartitionsExchangeAware comp : exchangeAwareComponents)
                    comp.onPartitionStatesRestored(fut);
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        if (cctx.kernalContext().query().moduleEnabled()) {
            ExchangeActions acts = fut.exchangeActions();

            if (acts != null) {
                if (!F.isEmpty(acts.cacheStartRequests())) {
                    for (ExchangeActions.CacheActionData actionData : acts.cacheStartRequests())
                        prepareIndexRebuildFuture(CU.cacheId(actionData.request().cacheName()));
                }
                else if (acts.localJoinContext() != null && !F.isEmpty(acts.localJoinContext().caches())) {
                    for (T2<DynamicCacheDescriptor, NearCacheConfiguration> tup : acts.localJoinContext().caches())
                        prepareIndexRebuildFuture(tup.get1().cacheId());
                }
            }
        }
    }

    /**
     * Creates a new index rebuild future that should be completed later after exchange is done. The future
     * has to be created before exchange is initialized to guarantee that we will capture a correct future
     * after activation or restore completes.
     * If there was an old future for the given ID, it will be completed.
     *
     * @param cacheId Cache ID.
     */
    private void prepareIndexRebuildFuture(int cacheId) {
        GridFutureAdapter<Void> old = idxRebuildFuts.put(cacheId, new GridFutureAdapter<>());

        if (old != null)
            old.onDone();
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexesIfNeeded(GridDhtPartitionsExchangeFuture exchangeFut) {
        rebuildIndexes(cctx.cacheContexts(), (cacheCtx) -> cacheCtx.startTopologyVersion().equals(exchangeFut.initialVersion()));
    }

    /** {@inheritDoc} */
    @Override public void forceRebuildIndexes(Collection<GridCacheContext> contexts) {
        contexts.forEach(ctx -> prepareIndexRebuildFuture(ctx.cacheId()));

        rebuildIndexes(contexts, (cacheCtx) -> true);
    }

    /**
     * @param contexts Collection of cache contexts for which indexes should be rebuilt.
     * @param rebuildCond Condition that should be met for indexes to be rebuilt for specific cache.
     */
    private void rebuildIndexes(Collection<GridCacheContext> contexts, Predicate<GridCacheContext> rebuildCond) {
        GridQueryProcessor qryProc = cctx.kernalContext().query();

        if (!qryProc.moduleEnabled())
            return;

        GridCompoundFuture allCacheIdxsCompoundFut = null;

        for (GridCacheContext cacheCtx : contexts) {
            if (!rebuildCond.test(cacheCtx))
                continue;

            int cacheId = cacheCtx.cacheId();
            GridFutureAdapter<Void> usrFut = idxRebuildFuts.get(cacheId);

            IgniteInternalFuture<?> rebuildFut = qryProc.rebuildIndexesFromHash(cacheCtx);

            if (nonNull(rebuildFut)) {
                if (log.isInfoEnabled())
                    log.info("Started indexes rebuilding for cache [" + cacheInfo(cacheCtx) + ']');

                if (isNull(allCacheIdxsCompoundFut))
                    allCacheIdxsCompoundFut = new GridCompoundFuture<>();

                allCacheIdxsCompoundFut.add(rebuildFut);

                rebuildFut.listen(fut -> {
                    idxRebuildFuts.remove(cacheId, usrFut);

                    Throwable err = fut.error();

                    usrFut.onDone(err);

                    if (isNull(err)) {
                        if (log.isInfoEnabled())
                            log.info("Finished indexes rebuilding for cache [" + cacheInfo(cacheCtx) + ']');
                    }
                    else {
                        if (!(err instanceof NodeStoppingException))
                            log.error("Failed to rebuild indexes for cache [" + cacheInfo(cacheCtx) + ']', err);
                    }
                });
            }
            else if (nonNull(usrFut)) {
                idxRebuildFuts.remove(cacheId, usrFut);

                usrFut.onDone();
            }
        }

        if (nonNull(allCacheIdxsCompoundFut)) {
            allCacheIdxsCompoundFut.listen(fut -> {
                if (log.isInfoEnabled())
                    log.info("Indexes rebuilding completed for all caches.");
            });

            allCacheIdxsCompoundFut.markInitialized();
        }
    }

    /**
     * Return short information about cache.
     *
     * @param cacheCtx Cache context.
     * @return Short cache info.
     */
    private String cacheInfo(GridCacheContext cacheCtx) {
        assert nonNull(cacheCtx);

        return "name=" + cacheCtx.name() + ", grpName=" + cacheCtx.group().name();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture indexRebuildFuture(int cacheId) {
        return idxRebuildFuts.get(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupsStopped(
        Collection<IgniteBiTuple<CacheGroupContext, Boolean>> stoppedGrps
    ) {
        Map<PageMemoryEx, Collection<Integer>> destroyed = new HashMap<>();

        for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
            CacheGroupContext gctx = tup.get1();

            if (!gctx.persistenceEnabled())
                continue;

            snapshotMgr.onCacheGroupStop(gctx, tup.get2());

            PageMemoryEx pageMem = (PageMemoryEx)gctx.dataRegion().pageMemory();

            Collection<Integer> grpIds = destroyed.computeIfAbsent(pageMem, k -> new HashSet<>());

            grpIds.add(tup.get1().groupId());

            if (gctx.config().isEncryptionEnabled())
                cctx.kernalContext().encryption().onCacheGroupStop(gctx.groupId());

            pageMem.onCacheGroupDestroyed(tup.get1().groupId());

            if (tup.get2())
                cctx.kernalContext().encryption().onCacheGroupDestroyed(gctx.groupId());
        }

        Collection<IgniteInternalFuture<Void>> clearFuts = new ArrayList<>(destroyed.size());

        for (Map.Entry<PageMemoryEx, Collection<Integer>> entry : destroyed.entrySet()) {
            final Collection<Integer> grpIds = entry.getValue();

            clearFuts.add(entry.getKey().clearAsync((grpId, pageIdg) -> grpIds.contains(grpId), false));
        }

        for (IgniteInternalFuture<Void> clearFut : clearFuts) {
            try {
                clearFut.get();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to clear page memory", e);
            }
        }

        if (cctx.pageStore() != null) {
            for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
                CacheGroupContext grp = tup.get1();

                try {
                    cctx.pageStore().shutdownForCacheGroup(grp, tup.get2());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to gracefully clean page store resources for destroyed cache " +
                        "[cache=" + grp.cacheOrGroupName() + "]", e);
                }
            }
        }
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    @Override public void checkpointReadLock() {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();
    }

    /** {@inheritDoc} */
    @Override public boolean checkpointLockIsHeldByThread() {
        return checkpointManager.checkpointTimeoutLock().checkpointLockIsHeldByThread();
    }

    /**
     * Releases the checkpoint read lock.
     */
    @Override public void checkpointReadUnlock() {
        checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
    }

    /** {@inheritDoc} */
    @Override public synchronized Map<Integer, Map<Integer, Long>> reserveHistoryForExchange() {
        assert reservedForExchange == null : reservedForExchange;

        Map</*grpId*/Integer, Set</*partId*/Integer>> applicableGroupsAndPartitions = partitionsApplicableForWalRebalance();

        Map</*grpId*/Integer, T2</*reason*/ReservationReason, Map</*partId*/Integer, CheckpointEntry>>> earliestValidCheckpoints;

        checkpointReadLock();

        try {
            CheckpointHistoryResult checkpointHistoryResult = checkpointHistory().searchAndReserveCheckpoints(applicableGroupsAndPartitions);

            earliestValidCheckpoints = checkpointHistoryResult.earliestValidCheckpoints();

            if (checkpointHistoryResult.reservedCheckoint() != null)
                reservedForExchange = checkpointHistoryResult.reservedCheckoint().checkpointMark();
        }
        finally {
            checkpointReadUnlock();
        }

        Map</*grpId*/Integer, Map</*partId*/Integer, /*updCntr*/Long>> grpPartsWithCnts = new HashMap<>();

        for (Map.Entry<Integer, T2</*reason*/ReservationReason, Map</*partId*/Integer, CheckpointEntry>>> e : earliestValidCheckpoints.entrySet()) {
            int grpId = e.getKey();

            if (e.getValue().get2() == null)
                continue;

            for (Map.Entry<Integer, CheckpointEntry> e0 : e.getValue().get2().entrySet()) {
                CheckpointEntry cpEntry = e0.getValue();

                int partId = e0.getKey();

                assert cctx.wal().reserved(cpEntry.checkpointMark())
                    : "WAL segment for checkpoint " + cpEntry + " has not reserved";

                Long updCntr = cpEntry.partitionCounter(cctx.wal(), grpId, partId);

                if (updCntr != null)
                    grpPartsWithCnts.computeIfAbsent(grpId, k -> new HashMap<>()).put(partId, updCntr);
            }
        }

        if (log.isInfoEnabled() && !F.isEmpty(earliestValidCheckpoints))
            printReservationToLog(earliestValidCheckpoints);

        return grpPartsWithCnts;
    }

    /**
     * Prints detail information about caches which were not reserved
     * and reservation depth for the caches which have WAL history enough.
     *
     * @param earliestValidCheckpoints Map contains information about caches' reservation.
     */
    private void printReservationToLog(
        Map<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> earliestValidCheckpoints) {
        try {
            Map<ReservationReason, List<Integer>> notReservedCachesToPrint = new HashMap<>();
            Map<ReservationReason, List<T2<Integer, CheckpointEntry>>> reservedCachesToPrint = new HashMap<>();

            for (Map.Entry<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> entry : earliestValidCheckpoints.entrySet()) {
                if (entry.getValue().get2() == null) {
                    notReservedCachesToPrint.computeIfAbsent(entry.getValue().get1(), reason -> new ArrayList<>())
                        .add(entry.getKey());
                }
                else {
                    reservedCachesToPrint.computeIfAbsent(entry.getValue().get1(), reason -> new ArrayList<>())
                        .add(new T2(entry.getKey(), entry.getValue().get2().values().stream().min(
                            Comparator.comparingLong(CheckpointEntry::timestamp)).get()));
                }
            }

            if (!F.isEmpty(notReservedCachesToPrint)) {
                log.info("Cache groups were not reserved [" +
                    notReservedCachesToPrint.entrySet().stream()
                        .map(entry -> '[' +
                            entry.getValue().stream().map(grpId -> "[grpId=" + grpId +
                                ", grpName=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ']')
                                .collect(Collectors.joining(", ")) +
                            ", reason=" + entry.getKey() + ']')
                        .collect(Collectors.joining(", ")) + ']');
            }

            if (!F.isEmpty(reservedCachesToPrint)) {
                log.info("Cache groups with earliest reserved checkpoint and a reason why a previous checkpoint was inapplicable: [" +
                    reservedCachesToPrint.entrySet().stream()
                        .map(entry -> '[' +
                            entry.getValue().stream().map(grpCp -> "[grpId=" + grpCp.get1() +
                                ", grpName=" + cctx.cache().cacheGroup(grpCp.get1()).cacheOrGroupName() +
                                ", cp=(" + grpCp.get2().checkpointId() + ", " + U.format(grpCp.get2().timestamp()) + ")]")
                                .collect(Collectors.joining(", ")) +
                            ", reason=" + entry.getKey() + ']')
                        .collect(Collectors.joining(", ")) + ']');
            }
        }
        catch (Exception e) {
            log.error("An error happened during printing partitions that were reserved for potential historical rebalance.", e);
        }
    }

    /**
     * @return Map of group id -> Set of partitions which can be used as suppliers for WAL rebalance.
     */
    private Map<Integer, Set<Integer>> partitionsApplicableForWalRebalance() {
        Map<Integer, Set<Integer>> res = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || grp.entryCompressionStrategy() != null)
                continue;

            for (GridDhtLocalPartition locPart : grp.topology().currentLocalPartitions()) {
                if (locPart.state() == OWNING && (preferWalRebalance() || locPart.fullSize() > walRebalanceThreshold))
                    res.computeIfAbsent(grp.groupId(), k -> new HashSet<>()).add(locPart.id());
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized void releaseHistoryForExchange() {
        if (reservedForExchange == null)
            return;

        assert cctx.wal().reserved(reservedForExchange)
            : "Earliest checkpoint WAL pointer is not reserved for exchange: " + reservedForExchange;

        try {
            cctx.wal().release(reservedForExchange);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to release earliest checkpoint WAL pointer: " + reservedForExchange, e);
        }

        reservedForExchange = null;
    }

    /** {@inheritDoc} */
    @Override public boolean reserveHistoryForPreloading(Map<T2<Integer, Integer>, Long> reservationMap) {
        Map<GroupPartitionId, CheckpointEntry> entries = checkpointHistory().searchCheckpointEntry(reservationMap);

        if (F.isEmpty(entries))
            return false;

        WALPointer oldestWALPointerToReserve = null;

        for (GroupPartitionId key : entries.keySet()) {
            WALPointer ptr = entries.get(key).checkpointMark();

            if (ptr == null)
                return false;

            if (oldestWALPointerToReserve == null ||
                ((FileWALPointer)ptr).index() < ((FileWALPointer)oldestWALPointerToReserve).index())
                oldestWALPointerToReserve = ptr;
        }

        if (cctx.wal().reserve(oldestWALPointerToReserve)) {
            reservedForPreloading = oldestWALPointerToReserve;

            return true;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public void releaseHistoryForPreloading() {
        releaseHistForPreloadingLock.lock();

        try {
            if (reservedForPreloading != null) {
                cctx.wal().release(reservedForPreloading);

                reservedForPreloading = null;
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Could not release WAL reservation", ex);

            throw new IgniteException(ex);
        }
        finally {
            releaseHistForPreloadingLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public WALPointer latestWalPointerReservedForPreloading() {
        return reservedForPreloading;
    }

    /**
     *
     */
    @Nullable @Override public IgniteInternalFuture wakeupForCheckpoint(String reason) {
        CheckpointProgress progress = checkpointManager.forceCheckpoint(reason, null);

        if (progress != null)
            return progress.futureFor(LOCK_RELEASED);

        return null;
    }

    /** {@inheritDoc} */
    @Override public <R> void waitForCheckpoint(String reason, IgniteInClosure<? super IgniteInternalFuture<R>> lsnr)
        throws IgniteCheckedException {
        CheckpointProgress progress = checkpointManager.forceCheckpoint(reason, lsnr);

        if (progress == null)
            return;

        progress.futureFor(FINISHED).get();
    }

    /** {@inheritDoc} */
    @Override public CheckpointProgress forceCheckpoint(String reason) {
        return checkpointManager.forceCheckpoint(reason, null);
    }

    /** {@inheritDoc} */
    @Override public WALPointer lastCheckpointMarkWalPointer() {
        CheckpointEntry lastCheckpointEntry = checkpointHistory() == null ? null : checkpointHistory().lastCheckpoint();

        return lastCheckpointEntry == null ? null : lastCheckpointEntry.checkpointMark();
    }

    /**
     * @return Checkpoint directory.
     */
    public File checkpointDirectory() {
        return checkpointManager.checkpointDirectory();
    }

    /**
     * @param lsnr Listener.
     * @param dataRegion Data region for which listener is corresponded to.
     */
    public void addCheckpointListener(CheckpointListener lsnr, DataRegion dataRegion) {
        checkpointManager.addCheckpointListener(lsnr, dataRegion);
    }

    /**
     * @param lsnr Listener.
     */
    public void addCheckpointListener(CheckpointListener lsnr) {
        checkpointManager.addCheckpointListener(lsnr, null);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(CheckpointListener lsnr) {
        checkpointManager.removeCheckpointListener(lsnr);
    }

    /**
     * @return Read checkpoint status.
     * @throws IgniteCheckedException If failed to read checkpoint status page.
     */
    @SuppressWarnings("TooBroadScope")
    private CheckpointStatus readCheckpointStatus() throws IgniteCheckedException {
        return checkpointManager.readCheckpointStatus();
    }

    /** {@inheritDoc} */
    @Override public void startMemoryRestore(GridKernalContext kctx, TimeBag startTimer) throws IgniteCheckedException {
        if (kctx.clientNode())
            return;

        MaintenanceRegistry mntcRegistry = kctx.maintenanceRegistry();

        MaintenanceTask mntcTask = mntcRegistry.activeMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

        if (mntcTask != null) {
            log.warning("Maintenance task found, stop restoring memory");

            File workDir = ((FilePageStoreManager) cctx.pageStore()).workDir();

            mntcRegistry.registerWorkflowCallback(CORRUPTED_DATA_FILES_MNTC_TASK_NAME,
                new CorruptedPdsMaintenanceCallback(workDir,
                    Arrays.asList(mntcTask.parameters().split(File.separator)))
            );

            return;
        }

        checkpointReadLock();

        RestoreLogicalState logicalState;

        try {
            // Preform early regions startup before restoring state.
            initAndStartRegions(kctx.config().getDataStorageConfiguration());

            startTimer.finishGlobalStage("Init and start regions");

            // Restore binary memory for all not WAL disabled cache groups.
            restoreBinaryMemory(
                groupsWithEnabledWal(),
                physicalRecords()
            );

            if (recoveryVerboseLogging && log.isInfoEnabled()) {
                log.info("Partition states information after BINARY RECOVERY phase:");

                dumpPartitionsInfo(cctx, log);
            }

            startTimer.finishGlobalStage("Restore binary memory");

            CheckpointStatus status = readCheckpointStatus();

            logicalState = applyLogicalUpdates(
                status,
                groupsWithEnabledWal(),
                logicalRecords(),
                true
            );

            if (recoveryVerboseLogging && log.isInfoEnabled()) {
                log.info("Partition states information after LOGICAL RECOVERY phase:");

                dumpPartitionsInfo(cctx, log);
            }

            startTimer.finishGlobalStage("Restore logical state");
        }
        catch (IgniteCheckedException e) {
            releaseFileLock();

            throw e;
        }
        finally {
            checkpointReadUnlock();
        }

        walTail = tailPointer(logicalState);

        cctx.wal().onDeActivate(kctx);
    }

    /** */
    public void resumeWalLogging() throws IgniteCheckedException {
        cctx.wal().resumeLogging(walTail);
    }

    /**
     * @param grpId Cache group id.
     * @param partId Partition ID.
     * @return Page store.
     * @throws IgniteCheckedException If failed.
     */
    public PageStore getPageStore(int grpId, int partId) throws IgniteCheckedException {
        return storeMgr.getStore(grpId, partId);
    }

    /**
     * @param gctx Group context.
     * @param f Consumer.
     * @return Accumulated result for all page stores.
     */
    public long forGroupPageStores(CacheGroupContext gctx, ToLongFunction<PageStore> f) {
        int groupId = gctx.groupId();

        long res = 0;

        try {
            Collection<PageStore> stores = storeMgr.getStores(groupId);

            if (stores != null) {
                for (PageStore store : stores)
                    res += f.applyAsLong(store);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return res;
    }

    /**
     * Calculates tail pointer for WAL at the end of logical recovery.
     *
     * @param logicalState State after logical recovery.
     * @return Tail pointer.
     * @throws IgniteCheckedException If failed.
     */
    private WALPointer tailPointer(RestoreLogicalState logicalState) throws IgniteCheckedException {
        // Should flush all data in buffers before read last WAL pointer.
        // Iterator read records only from files.
        WALPointer lastFlushPtr = cctx.wal().flush(null, true);

        // We must return null for NULL_PTR record, because FileWriteAheadLogManager.resumeLogging
        // can't write header without that condition.
        WALPointer lastReadPtr = logicalState.lastReadRecordPointer();

        if (lastFlushPtr != null && lastReadPtr == null)
            return lastFlushPtr;

        if (lastFlushPtr == null && lastReadPtr != null)
            return lastReadPtr;

        if (lastFlushPtr != null && lastReadPtr != null) {
            FileWALPointer lastFlushPtr0 = (FileWALPointer)lastFlushPtr;
            FileWALPointer lastReadPtr0 = (FileWALPointer)lastReadPtr;

            return lastReadPtr0.compareTo(lastFlushPtr0) >= 0 ? lastReadPtr : lastFlushPtr0;
        }

        return null;
    }

    /**
     * Called when all partitions have been fully restored and pre-created on node start.
     *
     * Starts checkpointing process and initiates first checkpoint.
     *
     * @throws IgniteCheckedException If first checkpoint has failed.
     */
    @Override public void onStateRestored(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        checkpointManager.start();

        CheckpointProgress chp = checkpointManager.forceCheckpoint("node started", null);

        if (chp != null)
            chp.futureFor(LOCK_RELEASED).get();
    }

    /**
     * @param status Checkpoint status.
     * @param cacheGroupsPredicate Cache groups to restore.
     * @throws IgniteCheckedException If failed.
     * @throws StorageException In case I/O error occurred during operations with storage.
     */
    private RestoreBinaryState performBinaryMemoryRestore(
        CheckpointStatus status,
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate,
        boolean finalizeState
    ) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Checking memory state [lastValidPos=" + status.endPtr + ", lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        WALPointer recPtr = status.endPtr;

        boolean apply = status.needRestoreMemory();

        try {
            WALRecord startRec = !CheckpointStatus.NULL_PTR.equals(status.startPtr) || apply ? cctx.wal().read(status.startPtr) : null;

            if (apply) {
                if (finalizeState)
                    U.quietAndWarn(log, "Ignite node stopped in the middle of checkpoint. Will restore memory state and " +
                        "finish checkpoint on node start.");

                cctx.cache().cacheGroupDescriptors().forEach((grpId, desc) -> {
                    if (!cacheGroupsPredicate.apply(grpId))
                        return;

                    try {
                        DataRegion region = cctx.database().dataRegion(desc.config().getDataRegionName());

                        if (region == null || !cctx.isLazyMemoryAllocation(region))
                            return;

                        region.pageMemory().start();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                });

                cctx.pageStore().beginRecover();

                if (!(startRec instanceof CheckpointRecord))
                    throw new StorageException("Checkpoint marker doesn't point to checkpoint record " +
                        "[ptr=" + status.startPtr + ", rec=" + startRec + "]");

                WALPointer cpMark = ((CheckpointRecord)startRec).checkpointMark();

                if (cpMark != null) {
                    if (log.isInfoEnabled())
                        log.info("Restoring checkpoint after logical recovery, will start physical recovery from " +
                            "back pointer: " + cpMark);

                    recPtr = cpMark;
                }
            }
            else
                cctx.wal().notchLastCheckpointPtr(status.startPtr);
        }
        catch (NoSuchElementException e) {
            throw new StorageException("Failed to read checkpoint record from WAL, persistence consistency " +
                "cannot be guaranteed. Make sure configuration points to correct WAL folders and WAL folder is " +
                "properly mounted [ptr=" + status.startPtr + ", walPath=" + persistenceCfg.getWalPath() +
                ", walArchive=" + persistenceCfg.getWalArchivePath() + "]");
        }

        AtomicReference<Throwable> applyError = new AtomicReference<>();

        StripedExecutor exec = cctx.kernalContext().getStripedExecutorService();

        Semaphore semaphore = new Semaphore(semaphorePertmits(exec));

        long start = U.currentTimeMillis();

        long lastArchivedSegment = cctx.wal().lastArchivedSegment();

        WALIterator it = cctx.wal().replay(recPtr, recordTypePredicate);

        RestoreBinaryState restoreBinaryState = new RestoreBinaryState(status, it, lastArchivedSegment, cacheGroupsPredicate);

        AtomicLong applied = new AtomicLong();

        try {
            while (it.hasNextX()) {
                if (applyError.get() != null)
                    break;

                WALRecord rec = restoreBinaryState.next();

                if (rec == null)
                    break;

                switch (rec.type()) {
                    case PAGE_RECORD:
                        if (restoreBinaryState.needApplyBinaryUpdate()) {
                            PageSnapshot pageSnapshot = (PageSnapshot)rec;

                            // Here we do not require tag check because we may be applying memory changes after
                            // several repetitive restarts and the same pages may have changed several times.
                            int groupId = pageSnapshot.fullPageId().groupId();
                            int partId = partId(pageSnapshot.fullPageId().pageId());

                            if (skipRemovedIndexUpdates(groupId, partId))
                                break;

                            stripedApplyPage((pageMem) -> {
                                    try {
                                        applyPageSnapshot(pageMem, pageSnapshot);

                                        applied.incrementAndGet();
                                    }
                                    catch (Throwable t) {
                                        U.error(log, "Failed to apply page snapshot. rec=[" + pageSnapshot + ']');

                                        applyError.compareAndSet(
                                            null,
                                            (t instanceof IgniteCheckedException) ?
                                                (IgniteCheckedException)t :
                                                new IgniteCheckedException("Failed to apply page snapshot", t));
                                    }
                                }, groupId, partId, exec, semaphore
                            );
                        }

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                    {
                        int groupId = metaStateRecord.groupId();
                        int partId = metaStateRecord.partitionId();

                        stripedApplyPage((pageMem) -> {
                            GridDhtPartitionState state = fromOrdinal(metaStateRecord.state());

                            if (state == null || state == GridDhtPartitionState.EVICTED)
                                schedulePartitionDestroy(groupId, partId);
                            else {
                                try {
                                    cancelOrWaitPartitionDestroy(groupId, partId);
                                }
                                catch (Throwable t) {
                                    U.error(log, "Failed to cancel or wait partition destroy. rec=[" + metaStateRecord + ']');

                                    applyError.compareAndSet(
                                        null,
                                        (t instanceof IgniteCheckedException) ?
                                            (IgniteCheckedException)t :
                                            new IgniteCheckedException("Failed to cancel or wait partition destroy", t));
                                }
                            }
                        }, groupId, partId, exec, semaphore);
                    }

                    break;

                    case PARTITION_DESTROY:
                        PartitionDestroyRecord destroyRecord = (PartitionDestroyRecord)rec;

                    {
                        int groupId = destroyRecord.groupId();
                        int partId = destroyRecord.partitionId();

                        stripedApplyPage((pageMem) -> {
                            pageMem.invalidate(groupId, partId);

                            schedulePartitionDestroy(groupId, partId);
                        }, groupId, partId, exec, semaphore);

                    }
                    break;

                    default:
                        if (restoreBinaryState.needApplyBinaryUpdate() && rec instanceof PageDeltaRecord) {
                            PageDeltaRecord pageDelta = (PageDeltaRecord)rec;

                            int groupId = pageDelta.groupId();
                            int partId = partId(pageDelta.pageId());

                            if (skipRemovedIndexUpdates(groupId, partId))
                                break;

                            stripedApplyPage((pageMem) -> {
                                try {
                                    applyPageDelta(pageMem, pageDelta, true);

                                    applied.incrementAndGet();
                                }
                                catch (Throwable t) {
                                    U.error(log, "Failed to apply page delta. rec=[" + pageDelta + ']');

                                    applyError.compareAndSet(
                                        null,
                                        (t instanceof IgniteCheckedException) ?
                                            (IgniteCheckedException)t :
                                            new IgniteCheckedException("Failed to apply page delta", t));
                                }
                            }, groupId, partId, exec, semaphore);
                        }
                }
            }
        }
        finally {
            it.close();

            awaitApplyComplete(exec, applyError);
        }

        if (!finalizeState)
            return null;

        FileWALPointer lastReadPtr = restoreBinaryState.lastReadRecordPointer();

        if (status.needRestoreMemory()) {
            if (restoreBinaryState.needApplyBinaryUpdate())
                throw new StorageException("Failed to restore memory state (checkpoint marker is present " +
                    "on disk, but checkpoint record is missed in WAL) " +
                    "[cpStatus=" + status + ", lastRead=" + lastReadPtr + "]");

            if (log.isInfoEnabled())
                log.info("Finished applying memory changes [changesApplied=" + applied +
                    ", time=" + (U.currentTimeMillis() - start) + " ms]");

            finalizeCheckpointOnRecovery(status.cpStartTs, status.cpStartId, status.startPtr, exec);
        }

        return restoreBinaryState;
    }

    /**
     * Calculate the maximum number of concurrent tasks for apply through the striped executor.
     *
     * @param exec Striped executor.
     * @return Number of permits.
     */
    private int semaphorePertmits(StripedExecutor exec) {
        // 4 task per-stripe by default.
        int permits = exec.stripesCount() * 4;

        long maxMemory = Runtime.getRuntime().maxMemory();

        // Heuristic calculation part of heap size as a maximum number of concurrent tasks.
        int permits0 = (int)((maxMemory * 0.2) / (4096 * 2));

        // May be for small heap. Get a low number of permits.
        if (permits0 < permits)
            permits = permits0;

        // Property for override any calculation.
        return getInteger(IGNITE_RECOVERY_SEMAPHORE_PERMITS, permits);
    }

    /**
     * @param exec Striped executor.
     * @param applyError Check error reference.
     */
    private void awaitApplyComplete(
        StripedExecutor exec,
        AtomicReference<Throwable> applyError
    ) throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        Throwable error = applyError.get();

        if (error != null)
            throw error instanceof IgniteCheckedException
                ? (IgniteCheckedException)error : new IgniteCheckedException(error);
    }

    /**
     * @param consumer Runnable task.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @param exec Striped executor.
     */
    public void stripedApplyPage(
        Consumer<PageMemoryEx> consumer,
        int grpId,
        int partId,
        StripedExecutor exec,
        Semaphore semaphore
    ) throws IgniteCheckedException {
        assert consumer != null;
        assert exec != null;
        assert semaphore != null;

        PageMemoryEx pageMem = getPageMemoryForCacheGroup(grpId);

        if (pageMem == null)
            return;

        stripedApply(() -> consumer.accept(pageMem), grpId, partId, exec, semaphore);
    }

    /**
     * @param run Runnable task.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @param exec Striped executor.
     */
    public void stripedApply(
        Runnable run,
        int grpId,
        int partId,
        StripedExecutor exec,
        Semaphore semaphore
    ) {
        assert run != null;
        assert exec != null;
        assert semaphore != null;

        int stripes = exec.stripesCount();

        int stripe = U.stripeIdx(stripes, grpId, partId);

        assert stripe >= 0 && stripe <= stripes : "idx=" + stripe + ", stripes=" + stripes;

        try {
            semaphore.acquire();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        exec.execute(stripe, () -> {
            // WA for avoid assert check in PageMemory, that current thread hold chpLock.
            CHECKPOINT_LOCK_HOLD_COUNT.set(1);

            try {
                run.run();
            }
            finally {
                CHECKPOINT_LOCK_HOLD_COUNT.set(0);

                semaphore.release();
            }
        });
    }

    /**
     * @param pageMem Page memory.
     * @param pageSnapshotRecord Page snapshot record.
     * @throws IgniteCheckedException If failed.
     */
    public void applyPageSnapshot(PageMemoryEx pageMem, PageSnapshot pageSnapshotRecord) throws IgniteCheckedException {
        int grpId = pageSnapshotRecord.fullPageId().groupId();
        long pageId = pageSnapshotRecord.fullPageId().pageId();

        long page = pageMem.acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, true);

        try {
            long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

            try {
                PageUtils.putBytes(pageAddr, 0, pageSnapshotRecord.pageData());

                if (PageIO.getCompressionType(pageAddr) != CompressionProcessor.UNCOMPRESSED_PAGE) {
                    int realPageSize = pageMem.realPageSize(pageSnapshotRecord.groupId());

                    assert pageSnapshotRecord.pageDataSize() <= realPageSize : pageSnapshotRecord.pageDataSize();

                    cctx.kernalContext().compress().decompressPage(pageMem.pageBuffer(pageAddr), realPageSize);
                }
            }
            finally {
                pageMem.writeUnlock(grpId, pageId, page, null, true, true);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param pageDeltaRecord Page delta record.
     * @param restore Get page for restore.
     * @throws IgniteCheckedException If failed.
     */
    private void applyPageDelta(PageMemoryEx pageMem, PageDeltaRecord pageDeltaRecord, boolean restore) throws IgniteCheckedException {
        int grpId = pageDeltaRecord.groupId();
        long pageId = pageDeltaRecord.pageId();

        // Here we do not require tag check because we may be applying memory changes after
        // several repetitive restarts and the same pages may have changed several times.
        long page = pageMem.acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, restore);

        try {
            long pageAddr = pageMem.writeLock(grpId, pageId, page, restore);

            try {
                pageDeltaRecord.applyDelta(pageMem, pageAddr);
            }
            finally {
                pageMem.writeUnlock(grpId, pageId, page, null, true, restore);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     */
    private boolean skipRemovedIndexUpdates(int grpId, int partId) {
        return (partId == PageIdAllocator.INDEX_PARTITION) && !storeMgr.hasIndexStore(grpId);
    }

    /**
     * Obtains PageMemory reference from cache descriptor instead of cache context.
     *
     * @param grpId Cache group id.
     * @return PageMemoryEx instance.
     * @throws IgniteCheckedException if no DataRegion is configured for a name obtained from cache descriptor.
     */
    // TODO IGNITE-12722: Get rid of GridCacheDatabaseSharedManager#getPageMemoryForCacheGroup functionality.
    private PageMemoryEx getPageMemoryForCacheGroup(int grpId) throws IgniteCheckedException {
        if (grpId == MetaStorage.METASTORAGE_CACHE_ID)
            return (PageMemoryEx)dataRegion(METASTORE_DATA_REGION_NAME).pageMemory();

        if (grpId == TxLog.TX_LOG_CACHE_ID)
            return (PageMemoryEx)dataRegion(TxLog.TX_LOG_CACHE_NAME).pageMemory();

        // TODO IGNITE-5075: cache descriptor can be removed.
        GridCacheSharedContext sharedCtx = context();

        CacheGroupDescriptor desc = sharedCtx.cache().cacheGroupDescriptors().get(grpId);

        if (desc == null)
            return null;

        String memPlcName = desc.config().getDataRegionName();

        return (PageMemoryEx)sharedCtx.database().dataRegion(memPlcName).pageMemory();
    }

    /**
     * Apply update from some iterator and with specific filters.
     *
     * @param it WAL iterator.
     * @param stopPred WAL record stop predicate.
     * @param entryPred Entry filter.
     */
    public void applyUpdatesOnRecovery(
        @Nullable WALIterator it,
        IgniteBiPredicate<WALPointer, WALRecord> stopPred,
        IgniteBiPredicate<WALRecord, DataEntry> entryPred
    ) throws IgniteCheckedException {
        if (it == null)
            return;

        cctx.walState().runWithOutWAL(() -> {
            if (it != null)
                applyUpdates(it, stopPred, entryPred, false, null, false);
        });
    }

    /**
     * Applies data entries updates from given WAL iterator if entry satisfies to provided predicates.
     * @param it WAL iterator.
     * @param stopPred WAL record predicate for stopping iteration.
     * @param entryPred Data record and corresponding data entries predicate.
     * @param lockEntries If true, update will be performed under entry lock.
     * @param onWalPointerApplied Listener to be invoked after every WAL record applied.
     * @param asyncApply If true, updates applying will be delegated to the striped executor.
     */
    public void applyUpdates(
        WALIterator it,
        @Nullable IgniteBiPredicate<WALPointer, WALRecord> stopPred,
        IgniteBiPredicate<WALRecord, DataEntry> entryPred,
        boolean lockEntries,
        IgniteInClosure<WALPointer> onWalPointerApplied,
        boolean asyncApply
    ) {
        StripedExecutor exec = cctx.kernalContext().getStripedExecutorService();

        AtomicReference<IgniteCheckedException> applyError = new AtomicReference<>();

        int[] stripesThrottleAccumulator = new int[exec.stripesCount()];

        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            WALRecord rec = next.get2();

            if (stopPred != null && stopPred.apply(next.get1(), rec))
                break;

            switch (rec.type()) {
                case MVCC_DATA_RECORD:
                case DATA_RECORD:
                    if (entryPred.apply(rec, null)) {
                        checkpointReadLock();

                        try {
                            DataRecord dataRec = (DataRecord)rec;

                            for (DataEntry dataEntry : dataRec.writeEntries()) {
                                if (entryPred.apply(rec, dataEntry)) {
                                    int cacheId = dataEntry.cacheId();

                                    GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                                    if (cacheCtx != null) {
                                        if (asyncApply) {
                                            applyUpdateAsync(cacheCtx, dataEntry, lockEntries, exec,
                                                applyError, stripesThrottleAccumulator);
                                        }
                                        else
                                            applyUpdate(cacheCtx, dataEntry, lockEntries);
                                    }
                                    else if (log != null) {
                                        log.warning("Cache is not started. Updates cannot be applied " +
                                            "[cacheId=" + cacheId + ']');
                                    }
                                }
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                        finally {
                            checkpointReadUnlock();
                        }
                    }

                    break;

                case MVCC_TX_RECORD:
                    if (entryPred.apply(rec, null)) {
                        checkpointReadLock();

                        try {
                            MvccTxRecord txRecord = (MvccTxRecord)rec;

                            byte txState = convertToTxState(txRecord.state());

                            cctx.coordinators().updateState(txRecord.mvccVersion(), txState, true);
                        }
                        finally {
                            checkpointReadUnlock();
                        }
                    }

                    break;

                // Rollback records should never be filtered to avoid gaps in updates.
                case ROLLBACK_TX_RECORD:
                    RollbackRecord rbRec = (RollbackRecord)rec;

                    checkpointReadLock();

                    try {
                        CacheGroupContext ctx = cctx.cache().cacheGroup(rbRec.groupId());

                        if (ctx != null && !ctx.isLocal()) {
                            try {
                                // Ensure the partition store exists.
                                ctx.topology().forceCreatePartition(rbRec.partitionId());

                                ctx.offheap().onPartitionInitialCounterUpdated(rbRec.partitionId(), rbRec.start(),
                                    rbRec.range());
                            }
                            catch (IgniteCheckedException e) {
                                throw new IgniteException(e);
                            }
                        }
                    }
                    finally {
                        checkpointReadUnlock();
                    }

                    break;

                default:
                    // Skip other records.
            }

            if (onWalPointerApplied != null)
                onWalPointerApplied.apply(rec.position());
        }

        if (applyError.get() != null)
            throw new IgniteException(applyError.get()); // Fail-fast check.
        else {
            CountDownLatch stripesClearLatch = new CountDownLatch(exec.stripesCount());

            // We have to ensure that all asynchronous updates are done.
            // StripedExecutor guarantees ordering inside stripe - it would enough to await "finishing" tasks.
            for (int i = 0; i < exec.stripesCount(); i++)
                exec.execute(i, stripesClearLatch::countDown);

            try {
                stripesClearLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedException(e);
            }
        }

        if (applyError.get() != null)
            throw new IgniteException(applyError.get());
    }

    /**
     * Applies update in given striped executor.
     *
     * @param cacheCtx Cache context.
     * @param dataEntry Data entry.
     * @param lockEntries Lock entries.
     * @param exec Executor.
     * @param applyError Apply error reference.
     * @param stripesThrottleAccumulator Array with accumulated throttle powers for each stripe.
     */
    private void applyUpdateAsync(
        GridCacheContext cacheCtx,
        DataEntry dataEntry,
        boolean lockEntries,
        StripedExecutor exec,
        AtomicReference<IgniteCheckedException> applyError,
        int[] stripesThrottleAccumulator
    ) throws IgniteCheckedException {
        if (applyError.get() != null)
            throw applyError.get();

        int stripeIdx = dataEntry.partitionId() % exec.stripesCount();

        assert stripeIdx >= 0 : "Stripe index should be non-negative: " + stripeIdx;

        if (exec.queueSize(stripeIdx) > THROTTLE_QUEUE_SIZE_THRESHOLD) {
            int throttlePower = ++stripesThrottleAccumulator[stripeIdx];

            long throttleParkTimeNs = (long)(1000L * Math.pow(1.05, throttlePower));

            if (throttleParkTimeNs > THROTTLE_LOGGING_THRESHOLD) {
                U.warn(log, "Parking thread=" + Thread.currentThread().getName()
                    + " for timeout(ms)=" + (throttleParkTimeNs / 1_000_000));
            }

            LockSupport.parkNanos(throttleParkTimeNs);
        }
        else
            stripesThrottleAccumulator[stripeIdx] = 0;

        exec.execute(stripeIdx, () -> {
            try {
                if (applyError.get() != null)
                    return;

                checkpointReadLock();

                try {
                    applyUpdate(cacheCtx, dataEntry, lockEntries);
                }
                finally {
                    checkpointReadUnlock();
                }
            }
            catch (IgniteCheckedException e) {
                applyError.compareAndSet(null, e);
            }
        });
    }

    /**
     * @param status Last registered checkpoint status.
     * @throws IgniteCheckedException If failed to apply updates.
     * @throws StorageException If IO exception occurred while reading write-ahead log.
     */
    private RestoreLogicalState applyLogicalUpdates(
        CheckpointStatus status,
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate,
        boolean skipFieldLookup
    ) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Applying lost cache updates since last checkpoint record [lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        if (skipFieldLookup)
            cctx.kernalContext().query().skipFieldLookup(true);

        long start = U.currentTimeMillis();

        AtomicReference<Throwable> applyError = new AtomicReference<>();

        AtomicLong applied = new AtomicLong();

        long lastArchivedSegment = cctx.wal().lastArchivedSegment();

        StripedExecutor exec = cctx.kernalContext().getStripedExecutorService();

        Semaphore semaphore = new Semaphore(semaphorePertmits(exec));

        Map<GroupPartitionId, Integer> partitionRecoveryStates = new HashMap<>();

        WALIterator it = cctx.wal().replay(status.startPtr, recordTypePredicate);

        RestoreLogicalState restoreLogicalState =
            new RestoreLogicalState(status, it, lastArchivedSegment, cacheGroupsPredicate, partitionRecoveryStates);

        try {
            while (it.hasNextX()) {
                WALRecord rec = restoreLogicalState.next();

                if (rec == null)
                    break;

                switch (rec.type()) {
                    case CHECKPOINT_RECORD: // Calculate initial partition states
                        CheckpointRecord cpRec = (CheckpointRecord)rec;

                        for (Map.Entry<Integer, CacheState> entry : cpRec.cacheGroupStates().entrySet()) {
                            CacheState cacheState = entry.getValue();

                            for (int i = 0; i < cacheState.size(); i++) {
                                int partId = cacheState.partitionByIndex(i);
                                byte state = cacheState.stateByIndex(i);

                                // Ignore undefined state.
                                if (state != -1) {
                                    partitionRecoveryStates.put(new GroupPartitionId(entry.getKey(), partId),
                                        (int)state);
                                }
                            }
                        }

                        break;

                    case ROLLBACK_TX_RECORD:
                        RollbackRecord rbRec = (RollbackRecord)rec;

                        CacheGroupContext ctx = cctx.cache().cacheGroup(rbRec.groupId());

                        if (ctx != null && !ctx.isLocal()) {
                            ctx.topology().forceCreatePartition(rbRec.partitionId());

                            ctx.offheap().onPartitionInitialCounterUpdated(rbRec.partitionId(), rbRec.start(),
                                rbRec.range());
                        }

                        break;

                    case MVCC_DATA_RECORD:
                    case DATA_RECORD:
                    case ENCRYPTED_DATA_RECORD:
                    case ENCRYPTED_DATA_RECORD_V2:
                        DataRecord dataRec = (DataRecord)rec;

                        for (DataEntry dataEntry : dataRec.writeEntries()) {
                            int cacheId = dataEntry.cacheId();

                            DynamicCacheDescriptor cacheDesc = cctx.cache().cacheDescriptor(cacheId);

                            // Can empty in case recovery node on blt changed.
                            if (cacheDesc == null)
                                continue;

                            stripedApply(() -> {
                                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                                if (skipRemovedIndexUpdates(cacheCtx.groupId(), PageIdAllocator.INDEX_PARTITION))
                                    cctx.kernalContext().query().markAsRebuildNeeded(cacheCtx);

                                try {
                                    applyUpdate(cacheCtx, dataEntry, false);
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to apply data entry, dataEntry=" + dataEntry +
                                        ", ptr=" + dataRec.position());

                                    applyError.compareAndSet(null, e);
                                }

                                applied.incrementAndGet();
                            }, cacheDesc.groupId(), dataEntry.partitionId(), exec, semaphore);
                        }

                        break;

                    case MVCC_TX_RECORD:
                        MvccTxRecord txRecord = (MvccTxRecord)rec;

                        byte txState = convertToTxState(txRecord.state());

                        cctx.coordinators().updateState(txRecord.mvccVersion(), txState, true);

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                        GroupPartitionId groupPartitionId = new GroupPartitionId(
                            metaStateRecord.groupId(), metaStateRecord.partitionId()
                        );

                        restoreLogicalState.partitionRecoveryStates.put(groupPartitionId, (int)metaStateRecord.state());

                        break;

                    case METASTORE_DATA_RECORD:
                        MetastoreDataRecord metastoreDataRecord = (MetastoreDataRecord)rec;

                        metaStorage.applyUpdate(metastoreDataRecord.key(), metastoreDataRecord.value());

                        break;

                    case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_ALLOCATED_INDEX:
                        PageDeltaRecord pageDelta = (PageDeltaRecord)rec;

                        stripedApplyPage((pageMem) -> {
                            try {
                                applyPageDelta(pageMem, pageDelta, false);
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to apply page delta, " + pageDelta);

                                applyError.compareAndSet(null, e);
                            }

                        }, pageDelta.groupId(), partId(pageDelta.pageId()), exec, semaphore);

                        break;

                    case MASTER_KEY_CHANGE_RECORD_V2:
                        cctx.kernalContext().encryption().applyKeys((MasterKeyChangeRecordV2)rec);

                        break;

                    case REENCRYPTION_START_RECORD:
                        cctx.kernalContext().encryption().applyReencryptionStartRecord((ReencryptionStartRecord)rec);

                        break;

                    default:
                        // Skip other records.
                }
            }
        }
        finally {
            it.close();

            if (skipFieldLookup)
                cctx.kernalContext().query().skipFieldLookup(false);
        }

        awaitApplyComplete(exec, applyError);

        if (log.isInfoEnabled())
            log.info("Finished applying WAL changes [updatesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + " ms]");

        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
            lsnr.afterLogicalUpdatesApplied(this, restoreLogicalState);

        return restoreLogicalState;
    }

    /**
     * Convert {@link TransactionState} to Mvcc {@link TxState}.
     *
     * @param state TransactionState.
     * @return TxState.
     */
    private byte convertToTxState(TransactionState state) {
        switch (state) {
            case PREPARED:
                return TxState.PREPARED;

            case COMMITTED:
                return TxState.COMMITTED;

            case ROLLED_BACK:
                return TxState.ABORTED;

            default:
                throw new IllegalStateException("Unsupported TxState.");
        }
    }

    /**
     * Wal truncate callBack.
     *
     * @param highBound WALPointer.
     */
    public void onWalTruncated(WALPointer highBound) throws IgniteCheckedException {
        CheckpointManager cpMgr = checkpointManager;

        if (cpMgr != null)
            cpMgr.removeCheckpointsUntil(highBound);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        StripedExecutor exec
    ) throws IgniteCheckedException {
        assert checkpointManager != null : "Checkpoint is null";

        checkpointManager.finalizeCheckpointOnRecovery(cpTs, cpId, walPtr, exec);

        cctx.pageStore().finishRecover();
    }

    /**
     * Replace thread local with buffers. Thread local should provide direct buffer with one page in length.
     *
     * @param threadBuf new thread-local with buffers for the checkpoint threads.
     */
    public void setThreadBuf(final ThreadLocal<ByteBuffer> threadBuf) {
        assert checkpointManager != null : "Checkpointer is null";

        checkpointManager.threadBuf(threadBuf);
    }

    /**
     * @return Checkpoint history.
     */
    @Nullable public CheckpointHistory checkpointHistory() {
        if (checkpointManager == null)
            return null;

        return checkpointManager.checkpointHistory();
    }

    /**
     * Adds given partition to checkpointer destroy queue.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public void schedulePartitionDestroy(int grpId, int partId) {
        checkpointManager.schedulePartitionDestroy(cctx.cache().cacheGroup(grpId), grpId, partId);
    }

    /**
     * Cancels or wait for partition destroy.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @throws IgniteCheckedException If failed.
     */
    public void cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
        checkpointManager.cancelOrWaitPartitionDestroy(grpId, partId);
    }

    /**
     * Timeout for checkpoint read lock acquisition.
     *
     * @return Timeout for checkpoint read lock acquisition in milliseconds.
     */
    @Override public long checkpointReadLockTimeout() {
        return checkpointManager.checkpointTimeoutLock().checkpointReadLockTimeout();
    }

    /**
     * Sets timeout for checkpoint read lock acquisition.
     *
     * @param val New timeout in milliseconds, non-positive value denotes infinite timeout.
     */
    @Override public void checkpointReadLockTimeout(long val) {
        checkpointManager.checkpointTimeoutLock().checkpointReadLockTimeout(val);
    }

    /**
     * @return Holder for page list cache limit for given data region.
     */
    public AtomicLong pageListCacheLimitHolder(DataRegion dataRegion) {
        if (dataRegion.config().isPersistenceEnabled()) {
            return pageListCacheLimits.computeIfAbsent(dataRegion.config().getName(), name -> new AtomicLong(
                (long)(((PageMemoryEx)dataRegion.pageMemory()).totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD)));
        }

        return null;
    }

    /**
     *
     */
    public static class FileLockHolder implements AutoCloseable {
        /** Lock file name. */
        private static final String lockFileName = "lock";

        /** File. */
        private File file;

        /** Channel. */
        private RandomAccessFile lockFile;

        /** Lock. */
        private volatile FileLock lock;

        /** Kernal context to generate Id of locked node in file. */
        @NotNull private GridKernalContext ctx;

        /** Logger. */
        private IgniteLogger log;

        /**
         * @param path Path.
         */
        public FileLockHolder(String path, @NotNull GridKernalContext ctx, IgniteLogger log) {
            try {
                file = Paths.get(path, lockFileName).toFile();

                lockFile = new RandomAccessFile(file, "rw");

                this.ctx = ctx;
                this.log = log;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param lockWaitTimeMillis During which time thread will try capture file lock.
         * @throws IgniteCheckedException If failed to capture file lock.
         */
        public void tryLock(long lockWaitTimeMillis) throws IgniteCheckedException {
            assert lockFile != null;

            FileChannel ch = lockFile.getChannel();

            SB sb = new SB();

            //write node id
            sb.a("[").a(ctx.localNodeId().toString()).a("]");

            //write ip addresses
            final GridDiscoveryManager discovery = ctx.discovery();

            if (discovery != null) { //discovery may be not up and running
                final ClusterNode node = discovery.localNode();

                if (node != null)
                    sb.a(node.addresses());
            }

            //write ports
            final GridPortProcessor ports = ctx.ports();

            if (ports != null) {
                sb.a("[");
                Iterator<GridPortRecord> it = ports.records().iterator();

                while (it.hasNext()) {
                    GridPortRecord rec = it.next();

                    sb.a(rec.protocol()).a(":").a(rec.port());

                    if (it.hasNext())
                        sb.a(", ");
                }

                sb.a("]");
            }

            String failMsg;

            try {
                String content = null;

                // Try to get lock, if not available wait 1 sec and re-try.
                for (int i = 0; i < lockWaitTimeMillis; i += 1000) {
                    try {
                        lock = ch.tryLock(0, 1, false);

                        if (lock != null && lock.isValid()) {
                            writeContent(sb.toString());

                            return;
                        }
                    }
                    catch (OverlappingFileLockException ignore) {
                        if (content == null)
                            content = readContent();

                        log.warning("Failed to acquire file lock. Will try again in 1s " +
                            "[nodeId=" + ctx.localNodeId() + ", holder=" + content +
                            ", path=" + file.getAbsolutePath() + ']');
                    }

                    U.sleep(1000);
                }

                if (content == null)
                    content = readContent();

                failMsg = "Failed to acquire file lock [holder=" + content + ", time=" + (lockWaitTimeMillis / 1000) +
                    " sec, path=" + file.getAbsolutePath() + ']';
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }

            if (failMsg != null)
                throw new IgniteCheckedException(failMsg);
        }

        /**
         * Write node id (who captured lock) into lock file.
         *
         * @param content Node id.
         * @throws IOException if some fail while write node it.
         */
        private void writeContent(String content) throws IOException {
            FileChannel ch = lockFile.getChannel();

            byte[] bytes = content.getBytes();

            ByteBuffer buf = ByteBuffer.allocate(bytes.length);
            buf.put(bytes);

            buf.flip();

            ch.write(buf, 1);

            ch.force(false);
        }

        /**
         *
         */
        private String readContent() throws IOException {
            FileChannel ch = lockFile.getChannel();

            ByteBuffer buf = ByteBuffer.allocate((int)(ch.size() - 1));

            ch.read(buf, 1);

            String content = new String(buf.array());

            buf.clear();

            return content;
        }

        /** Locked or not. */
        public boolean isLocked() {
            return lock != null && lock.isValid();
        }

        /** Releases file lock */
        public void release() {
            U.releaseQuiet(lock);
        }

        /** Closes file channel */
        @Override public void close() {
            release();

            U.closeQuiet(lockFile);
        }

        /**
         * @return Absolute path to lock file.
         */
        private String lockPath() {
            return file.getAbsolutePath();
        }
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics persistentStoreMetrics() {
        return new DataStorageMetricsSnapshot(persStoreMetrics);
    }

    /**
     *
     */
    public DataStorageMetricsImpl persistentStoreMetricsImpl() {
        return persStoreMetrics;
    }

    /** {@inheritDoc} */
    @Override public MetaStorage metaStorage() {
        return metaStorage;
    }

    /**
     * @return Temporary metastorage to migration of index partition.
     */
    public MetaStorage.TmpStorage temporaryMetaStorage() {
        return tmpMetaStorage;
    }

    /**
     * @param tmpMetaStorage Temporary metastorage to migration of index partition.
     */
    public void temporaryMetaStorage(MetaStorage.TmpStorage tmpMetaStorage) {
        this.tmpMetaStorage = tmpMetaStorage;
    }

    /** {@inheritDoc} */
    @Override public void notifyMetaStorageSubscribersOnReadyForRead() throws IgniteCheckedException {
        metastorageLifecycleLsnrs = cctx.kernalContext().internalSubscriptionProcessor().getMetastorageSubscribers();

        readMetastore();
    }

    /** {@inheritDoc} */
    @Override public boolean walEnabled(int grpId, boolean local) {
        if (local)
            return !initiallyLocWalDisabledGrps.contains(grpId);
        else
            return !initiallyGlobalWalDisabledGrps.contains(grpId);
    }

    /** {@inheritDoc} */
    @Override public void walEnabled(int grpId, boolean enabled, boolean local) {
        String key = walGroupIdToKey(grpId, local);

        checkpointReadLock();

        try {
            if (enabled)
                metaStorage.remove(key);
            else {
                metaStorage.write(key, true);

                lastCheckpointInapplicableForWalRebalance(grpId);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to write cache group WAL state [grpId=" + grpId +
                ", enabled=" + enabled + ']', e);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     * Checks that checkpoint with timestamp {@code cpTs} is inapplicable as start point for WAL rebalance for given group {@code grpId}.
     *
     * @param cpTs Checkpoint timestamp.
     * @param grpId Group ID.
     * @return {@code true} if checkpoint {@code cpTs} is inapplicable as start point for WAL rebalance for {@code grpId}.
     * @throws IgniteCheckedException If failed to check.
     */
    public boolean isCheckpointInapplicableForWalRebalance(Long cpTs, int grpId) throws IgniteCheckedException {
        return metaStorage.read(checkpointInapplicableCpAndGroupIdToKey(cpTs, grpId)) != null;
    }

    /**
     * Set last checkpoint as inapplicable for WAL rebalance for given group {@code grpId}.
     *
     * @param grpId Group ID.
     */
    @Override public void lastCheckpointInapplicableForWalRebalance(int grpId) {
        checkpointReadLock();

        try {
            CheckpointEntry lastCp = checkpointHistory().lastCheckpointMarkingAsInapplicable(grpId);
            long lastCpTs = lastCp != null ? lastCp.timestamp() : 0;

            if (lastCpTs != 0)
                metaStorage.write(checkpointInapplicableCpAndGroupIdToKey(lastCpTs, grpId), true);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to mark last checkpoint as inapplicable for WAL rebalance for group: " + grpId, e);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     *
     */
    private void fillWalDisabledGroups() {
        assert metaStorage != null;

        try {
            metaStorage.iterate(WAL_KEY_PREFIX, (key, val) -> {
                T2<Integer, Boolean> t2 = walKeyToGroupIdAndLocalFlag(key);

                if (t2 != null) {
                    if (t2.get2())
                        initiallyLocWalDisabledGrps.add(t2.get1());
                    else
                        initiallyGlobalWalDisabledGrps.add(t2.get1());
                }
            }, false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read cache groups WAL state.", e);
        }
    }

    /**
     * Convert cache group ID to WAL state key.
     *
     * @param grpId Group ID.
     * @return Key.
     */
    private static String walGroupIdToKey(int grpId, boolean local) {
        if (local)
            return WAL_LOCAL_KEY_PREFIX + grpId;
        else
            return WAL_GLOBAL_KEY_PREFIX + grpId;
    }

    /**
     * Convert checkpoint timestamp and cache group ID to key for {@link #CHECKPOINT_INAPPLICABLE_FOR_REBALANCE} metastorage records.
     *
     * @param cpTs Checkpoint timestamp.
     * @param grpId Group ID.
     * @return Key.
     */
    private static String checkpointInapplicableCpAndGroupIdToKey(long cpTs, int grpId) {
        return CHECKPOINT_INAPPLICABLE_FOR_REBALANCE + cpTs + "-" + grpId;
    }

    /**
     * Convert WAL state key to cache group ID.
     *
     * @param key Key.
     * @return Group ID.
     */
    private static T2<Integer, Boolean> walKeyToGroupIdAndLocalFlag(String key) {
        if (key.startsWith(WAL_LOCAL_KEY_PREFIX))
            return new T2<>(Integer.parseInt(key.substring(WAL_LOCAL_KEY_PREFIX.length())), true);
        else if (key.startsWith(WAL_GLOBAL_KEY_PREFIX))
            return new T2<>(Integer.parseInt(key.substring(WAL_GLOBAL_KEY_PREFIX.length())), false);
        else
            return null;
    }

    /**
     * Method dumps partitions info see {@link #dumpPartitionsInfo(CacheGroupContext, IgniteLogger)}
     * for all persistent cache groups.
     *
     * @param cctx Shared context.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    private static void dumpPartitionsInfo(GridCacheSharedContext cctx,
        IgniteLogger log) throws IgniteCheckedException {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || !grp.persistenceEnabled())
                continue;

            dumpPartitionsInfo(grp, log);
        }
    }

    /**
     * Retrieves from page memory meta information about given {@code grp} group partitions
     * and dumps this information to log INFO level.
     *
     * @param grp Cache group.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    private static void dumpPartitionsInfo(CacheGroupContext grp, IgniteLogger log) throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

        IgnitePageStoreManager pageStore = grp.shared().pageStore();

        assert pageStore != null : "Persistent cache should have initialize page store manager.";

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            GridDhtLocalPartition part = grp.topology().localPartition(p);

            if (part != null) {
                if (log.isInfoEnabled())
                    log.info("Partition [grp=" + grp.cacheOrGroupName()
                        + ", id=" + p
                        + ", state=" + part.state()
                        + ", counter=" + part.dataStore().partUpdateCounter()
                        + ", size=" + part.fullSize() + "]");

                continue;
            }

            if (!pageStore.exists(grp.groupId(), p))
                continue;

            pageStore.ensure(grp.groupId(), p);

            if (pageStore.pages(grp.groupId(), p) <= 1) {
                if (log.isInfoEnabled())
                    log.info("Partition [grp=" + grp.cacheOrGroupName() + ", id=" + p + ", state=N/A (only file header) ]");

                continue;
            }

            long partMetaId = pageMem.partitionMetaPageId(grp.groupId(), p);
            long partMetaPage = pageMem.acquirePage(grp.groupId(), partMetaId);

            try {
                long pageAddr = pageMem.readLock(grp.groupId(), partMetaId, partMetaPage);

                try {
                    PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                    GridDhtPartitionState partState = fromOrdinal(io.getPartitionState(pageAddr));

                    String state = partState != null ? partState.toString() : "N/A";

                    long updateCntr = io.getUpdateCounter(pageAddr);
                    long size = io.getSize(pageAddr);

                    if (log.isInfoEnabled())
                        log.info("Partition [grp=" + grp.cacheOrGroupName()
                            + ", id=" + p
                            + ", state=" + state
                            + ", counter=" + updateCntr
                            + ", size=" + size + "]");
                }
                finally {
                    pageMem.readUnlock(grp.groupId(), partMetaId, partMetaPage);
                }
            }
            finally {
                pageMem.releasePage(grp.groupId(), partMetaId, partMetaPage);
            }
        }
    }

    /**
     * Recovery lifecycle for read-write metastorage.
     */
    private class MetastorageRecoveryLifecycle implements DatabaseLifecycleListener {
        /** {@inheritDoc} */
        @Override public void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
            cctx.pageStore().initializeForMetastorage();
        }

        /** {@inheritDoc} */
        @Override public void afterBinaryMemoryRestore(
            IgniteCacheDatabaseSharedManager mgr,
            RestoreBinaryState restoreState
        ) throws IgniteCheckedException {
            assert metaStorage == null;

            metaStorage = createMetastorage(false);
        }
    }

    /**
     * @return Cache group predicate that passes only Metastorage cache group id.
     */
    private IgnitePredicate<Integer> onlyMetastorageGroup() {
        return groupId -> MetaStorage.METASTORAGE_CACHE_ID == groupId;
    }

    /**
     * @return Cache group predicate that passes only cache groups with enabled WAL.
     */
    private IgnitePredicate<Integer> groupsWithEnabledWal() {
        return groupId -> !initiallyGlobalWalDisabledGrps.contains(groupId)
            && !initiallyLocWalDisabledGrps.contains(groupId);
    }

    /**
     * @return WAL records predicate that passes only Metastorage and encryption data records.
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> onlyMetastorageAndEncryptionRecords() {
        return (type, ptr) -> type == METASTORE_DATA_RECORD ||
            type == MASTER_KEY_CHANGE_RECORD || type == MASTER_KEY_CHANGE_RECORD_V2;
    }

    /**
     * @return WAL records predicate that passes only physical and mixed WAL records.
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> physicalRecords() {
        return (type, ptr) -> type.purpose() == WALRecord.RecordPurpose.PHYSICAL
            || type.purpose() == WALRecord.RecordPurpose.MIXED;
    }

    /**
     * @return WAL records predicate that passes only logical and mixed WAL records +
     * CP record (used for restoring initial partition states).
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> logicalRecords() {
        return (type, ptr) -> type.purpose() == WALRecord.RecordPurpose.LOGICAL
            || type.purpose() == WALRecord.RecordPurpose.MIXED || type == CHECKPOINT_RECORD;
    }

    /**
     * Abstract class to create restore context.
     */
    private abstract class RestoreStateContext {
        /** Last archived segment. */
        protected final long lastArchivedSegment;

        /** Checkpoint status. */
        protected final CheckpointStatus status;

        /** WAL iterator. */
        private final WALIterator iterator;

        /** Only {@link WalRecordCacheGroupAware} records satisfied this predicate will be applied. */
        private final IgnitePredicate<Integer> cacheGroupPredicate;

        /**
         * @param status Checkpoint status.
         * @param iterator WAL iterator.
         * @param lastArchivedSegment Last archived segment index.
         * @param cacheGroupPredicate Cache groups predicate.
         */
        protected RestoreStateContext(
            CheckpointStatus status,
            WALIterator iterator,
            long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupPredicate
        ) {
            this.status = status;
            this.iterator = iterator;
            this.lastArchivedSegment = lastArchivedSegment;
            this.cacheGroupPredicate = cacheGroupPredicate;
        }

        /**
         * Advance iterator to the next record.
         *
         * @return WALRecord entry.
         * @throws IgniteCheckedException If CRC check fail during binary recovery state or another exception occurring.
         */
        public WALRecord next() throws IgniteCheckedException {
            try {
                for (;;) {
                    if (!iterator.hasNextX())
                        return null;

                    IgniteBiTuple<WALPointer, WALRecord> tup = iterator.nextX();

                    if (tup == null)
                        return null;

                    WALRecord rec = tup.get2();

                    WALPointer ptr = tup.get1();

                    rec.position(ptr);

                    // Filter out records by group id.
                    if (rec instanceof WalRecordCacheGroupAware) {
                        WalRecordCacheGroupAware grpAwareRecord = (WalRecordCacheGroupAware) rec;

                        if (!cacheGroupPredicate.apply(grpAwareRecord.groupId()))
                            continue;
                    }

                    // Filter out data entries by group id.
                    if (rec instanceof DataRecord)
                        rec = filterEntriesByGroupId((DataRecord) rec);

                    return rec;
                }
            }
            catch (IgniteCheckedException e) {
                boolean throwsCRCError = throwsCRCError();

                if (X.hasCause(e, IgniteDataIntegrityViolationException.class)) {
                    if (throwsCRCError)
                        throw e;
                    else
                        return null;
                }

                log.error("There is an error during restore state [throwsCRCError=" + throwsCRCError + ']', e);

                throw e;
            }
        }

        /**
         * Filter outs data entries from given data record that not satisfy {@link #cacheGroupPredicate}.
         *
         * @param record Original data record.
         * @return Data record with filtered data entries.
         */
        private DataRecord filterEntriesByGroupId(DataRecord record) {
            List<DataEntry> filteredEntries = record.writeEntries().stream()
                .filter(entry -> {
                    int cacheId = entry.cacheId();

                    return cctx.cacheContext(cacheId) != null && cacheGroupPredicate.apply(cctx.cacheContext(cacheId).groupId());
                })
                .collect(Collectors.toList());

            return record.setWriteEntries(filteredEntries);
        }

        /**
         *
         * @return Last read WAL record pointer.
         */
        public FileWALPointer lastReadRecordPointer() {
            assert status.startPtr != null && status.startPtr instanceof FileWALPointer;

            return iterator.lastRead()
                .map(ptr -> (FileWALPointer)ptr)
                .orElseGet(() -> (FileWALPointer)status.startPtr);
        }

        /**
         *
         * @return Flag indicates need throws CRC exception or not.
         */
        public boolean throwsCRCError() {
            return lastReadRecordPointer().index() <= lastArchivedSegment;
        }
    }

    /**
     * Restore memory context. Tracks the safety of binary recovery.
     */
    public class RestoreBinaryState extends RestoreStateContext {

        /** The flag indicates need to apply the binary update or no needed. */
        private boolean needApplyBinaryUpdates;

        /**
         * @param status Checkpoint status.
         * @param iterator WAL iterator.
         * @param lastArchivedSegment Last archived segment index.
         * @param cacheGroupsPredicate Cache groups predicate.
         */
        public RestoreBinaryState(
            CheckpointStatus status,
            WALIterator iterator,
            long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupsPredicate
        ) {
            super(status, iterator, lastArchivedSegment, cacheGroupsPredicate);

            this.needApplyBinaryUpdates = status.needRestoreMemory();
        }

        /**
         * Advance iterator to the next record.
         *
         * @return WALRecord entry.
         * @throws IgniteCheckedException If CRC check fail during binary recovery state or another exception occurring.
         */
        @Override public WALRecord next() throws IgniteCheckedException {
            WALRecord rec = super.next();

            if (rec == null)
                return null;

            if (rec.type() == CHECKPOINT_RECORD) {
                CheckpointRecord cpRec = (CheckpointRecord)rec;

                // We roll memory up until we find a checkpoint start record registered in the status.
                if (F.eq(cpRec.checkpointId(), status.cpStartId)) {
                    if (log.isInfoEnabled())
                        log.info("Found last checkpoint marker [cpId=" + cpRec.checkpointId() +
                            ", pos=" + rec.position() + ']');

                    needApplyBinaryUpdates = false;
                }
                else if (!F.eq(cpRec.checkpointId(), status.cpEndId))
                    U.warn(log, "Found unexpected checkpoint marker, skipping [cpId=" + cpRec.checkpointId() +
                        ", expCpId=" + status.cpStartId + ", pos=" + rec.position() + ']');
            }

            return rec;
        }

        /**
         *
         * @return Flag indicates need apply binary record or not.
         */
        public boolean needApplyBinaryUpdate() {
            return needApplyBinaryUpdates;
        }

        /**
         *
         * @return Flag indicates need throws CRC exception or not.
         */
        @Override public boolean throwsCRCError() {
            log.info("Throws CRC error check [needApplyBinaryUpdates=" + needApplyBinaryUpdates +
                ", lastArchivedSegment=" + lastArchivedSegment + ", lastRead=" + lastReadRecordPointer() + ']');

            if (needApplyBinaryUpdates)
                return true;

            return super.throwsCRCError();
        }
    }

    /**
     * Restore logical state context. Tracks the safety of logical recovery.
     */
    public class RestoreLogicalState extends RestoreStateContext {
        /** States of partitions recovered during applying logical updates. */
        private final Map<GroupPartitionId, Integer> partitionRecoveryStates;

        /**
         * @param lastArchivedSegment Last archived segment index.
         * @param partitionRecoveryStates Initial partition recovery states.
         */
        public RestoreLogicalState(CheckpointStatus status, WALIterator iterator, long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupsPredicate, Map<GroupPartitionId, Integer> partitionRecoveryStates) {
            super(status, iterator, lastArchivedSegment, cacheGroupsPredicate);

            this.partitionRecoveryStates = partitionRecoveryStates;
        }

        /**
         * @return Map of restored partition states for cache groups.
         */
        public Map<GroupPartitionId, Integer> partitionRecoveryStates() {
            return Collections.unmodifiableMap(partitionRecoveryStates);
        }
    }
}
