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

package org.apache.ignite.internal.processors.database;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.mockito.Mockito;

import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test with reuse list.
 */
public class BPlusTreeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(
        int cacheId,
        PageMemory pageMem,
        long rootId,
        boolean initNew
    ) throws IgniteCheckedException {
        return new TestReuseList(createContext(), cacheId, "test", pageMem, rootId, initNew);
    }

    /**
     * Creates a mock {@link GridCacheSharedContext}.
     */
    private static GridCacheSharedContext<?, ?> createContext() {
        GridCacheSharedContext<?, ?> ctx = mock(GridCacheSharedContext.class, Mockito.RETURNS_MOCKS);

        when(ctx.diagnostic().pageLockTracker().createPageLockTracker(anyString()))
            .thenReturn(new TestPageLockListener());

        when(ctx.wal()).thenReturn(null);

        return ctx;
    }

    /** {@inheritDoc} */
    @Override protected void assertNoLocks() {
        super.assertNoLocks();

        assertTrue(TestReuseList.checkNoLocks());
    }

    /**
     *
     */
    private static class TestReuseList extends ReuseListImpl {
        /**
         * @param cacheId    Cache ID.
         * @param name       Name (for debug purpose).
         * @param pageMem    Page memory.
         * @param metaPageId Metadata page ID.
         * @param initNew    {@code True} if new metadata should be initialized.
         * @throws IgniteCheckedException If failed.
         */
        public TestReuseList(
            GridCacheSharedContext<?, ?> ctx,
            int cacheId,
            String name,
            PageMemory pageMem,
            long metaPageId,
            boolean initNew
        ) throws IgniteCheckedException {
            super(ctx, cacheId, name, pageMem, metaPageId, initNew, null, PageIdAllocator.FLAG_IDX);
        }

        /**
         *
         */
        static boolean checkNoLocks() {
            return TestPageLockListener.readLocks.get().isEmpty() && TestPageLockListener.writeLocks.get().isEmpty();
        }
    }

    /** */
    private static class TestPageLockListener implements PageLockListener {

        /** */
        private static ThreadLocal<Set<Long>> readLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /** */
        private static ThreadLocal<Set<Long>> writeLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            if (pageAddr == 0L)
                return; // Failed to lock.

            checkPageId(pageId, pageAddr);

            assertTrue(writeLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            assertEquals(effectivePageId(pageId), effectivePageId(PageIO.getPageId(pageAddr)));

            assertTrue(writeLocks.get().remove(pageId));
        }
    }
}
