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

package org.apache.ignite.internal.processors.cache.checker.processor;

import com.sun.tools.javac.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationFixPartitionSizesTest1 extends GridCommonAbstractTest {
    public static void main1(String[] args) {
        Map<String, String> map = new ConcurrentHashMap<>();

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");

        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();

        Map.Entry<String, String> next = iterator.next();

        System.out.println(next);

        map.put("1", "1qwer");
        map.put("4", "4");
        map.put("3", "3qwer");

        while(iterator.hasNext()) {
            next = iterator.next();
            System.out.println(next);
        }

    }

    @Test
    public void main(/*String[] args*/) throws Exception {
        int partCount = 100;
        int maxKey = partCount * partCount;

        Part part = new Part(partCount);

//        Random rnd = new Random();

        for (int i = 0; i < maxKey; i += 2) {
            if (!(i > 350 && i < 450) && !(i > 750 && i < 850))
                part.put(i);
            System.out.println("preload put " + i);
        }

        AtomicBoolean doLoad = new AtomicBoolean(true);

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            System.out.println("qvsdhntsd loadFut start");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (doLoad.get()) {
                int i = rnd.nextInt(maxKey);
                part.put(i);

                int sleep = 5;

//                doSleep(sleep);

                i = rnd.nextInt(maxKey);
                part.remove(i);

//                doSleep(sleep);

                System.out.println("async load remove " + i);
            }
        });

        IgniteInternalFuture loadFut1 = GridTestUtils.runAsync(() -> {
            System.out.println("qvsdhntsd loadFut start");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (doLoad.get()) {
                int i = rnd.nextInt(maxKey);
                part.put(i);

                int sleep = 5;

//                doSleep(sleep);

                i = rnd.nextInt(maxKey);
                part.remove(i);

//                doSleep(sleep);

                System.out.println("async load remove " + i);
            }
        });

        doSleep(100);

        IgniteInternalFuture reconFut = GridTestUtils.runAsync(() -> {
            doRecon(part);

            doLoad.set(false);
            }
        );

        reconFut.get();
        loadFut.get();
        loadFut1.get();

        System.out.println("part.realSize() " + part.realSize());
        System.out.println("part.size " + part.size);
        System.out.println("part.reconSize " + part.reconSize);

        assertTrue(part.size.get() == part.realSize());

    }
    static void doRecon(Part part) {
//        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            part.partLock.writeLock().lock();

            try {
                part.reconInProgress = true;
            }
            finally {
                part.partLock.writeLock().unlock();
            }

            for (Page page : part.pages) {
                System.out.println("start iter page ++++++++++++");

                page.pageLock.readLock().lock();

                try {
                    //set border key
                    if (!page.keys.isEmpty()) {
                        part.borderKey = page.keys.first();
                        System.out.println("set new part.borderKey " + part.borderKey);
                    }
                    else {
                        System.out.println("continue");
                        continue;
                    }

                    //iterate over tempMap
                    Iterator<Map.Entry<Integer, Integer>> tempMapIter = part.tempMap.entrySet().iterator();

                    System.out.println("tempMap after get iterator " + part.tempMap.size() + part.tempMap);

//                    System.out.println("tempMap " + part.tempMap);

                    while (tempMapIter.hasNext()) {
                        Map.Entry<Integer, Integer> entry = tempMapIter.next();

                        System.out.println("entry from iterator " + entry);

                        if (entry.getKey() < part.borderKey) {
                            part.reconSize.addAndGet(entry.getValue());

                            tempMapIter.remove();
                        }
                        else
                            tempMapIter.remove();
                    }

//                    System.out.println("in recon --------------- reconSize after iter tempMap " + part.reconSize);

                    //iterate over page

//                    System.out.println("key in recon ---------------");
                    for (Integer key : page.keys) {
                        System.out.println("in recon added key to tempMap: key " + key + " delta " + 1);
                        part.tempMap.put(key, +1);

//                        doSleep(10);
                    }

//                    System.out.println("key in recon --------------- reconSize " + part.reconSize);
                    System.out.println("stop iter page ++++++++++++");

                }
                finally {
                    page.pageLock.readLock().unlock();
                }
            }

            part.partLock.writeLock().lock();

            try {
                Iterator<Map.Entry<Integer, Integer>> tempMapIter = part.tempMap.entrySet().iterator();

                System.out.println("tempMap " + part.tempMap.size() + part.tempMap);

                while (tempMapIter.hasNext()) {
                    Map.Entry<Integer, Integer> entry = tempMapIter.next();

//                    if (entry.getKey() < part.borderKey) {
                        part.reconSize.addAndGet(entry.getValue());

//                        tempMapIter.remove();
//                    }
//                    else
//                        tempMapIter.remove();
                }

                part.reconInProgress = false;

//                part.size.set(part.reconSize.get());
            }
            finally {
                part.partLock.writeLock().unlock();
            }
//        });

//        return loadFut;
    }

    static class Part {
        volatile ReentrantReadWriteLock partLock = new ReentrantReadWriteLock();

        volatile CopyOnWriteArrayList<Page> pages;

        volatile int pagesCount;

        volatile AtomicInteger size = new AtomicInteger();

        // recon
        volatile boolean reconInProgress;

        volatile AtomicInteger reconSize = new AtomicInteger();

        volatile Integer borderKey;

        volatile Map<Integer, Integer> tempMap = new ConcurrentSkipListMap<>();


        Part(int count) {
            pagesCount = count;
            pages = new CopyOnWriteArrayList<>();

            for (int i = 0; i < count; i++) {
                pages.add(new Page());

            }
        }

        void put(int key) {
            partLock.readLock().lock();

            try {
                int pageNumber = key / pagesCount;
//                System.out.println("asdf key " + key + " pagesCount " + pagesCount + " pageNumber " + pageNumber);

                Page page = pages.get(pageNumber);
                
                page.pageLock.writeLock().lock();
                
                try {
                    if (!page.keys.contains(key)) {
                        page.keys.add(key);

                        System.out.println("put key " + key);

//                    if (reconInProgress) {
//                        if (key < borderKey)
//                            reconSize.incrementAndGet();
//                        else if (tempMap.get(key) != null && tempMap.get(key) != 1)
//                            tempMap.remove(key);
//                        else
//                            tempMap.put(key, +1);
//                    }

                        if (reconInProgress) {
                            if (borderKey != null && key < borderKey) {
                                reconSize.incrementAndGet();
                                System.out.println("in PUT after increment reconSize: key " + key + " reconSize " + reconSize.get());
                            }
                            else
                                tempMap.compute(key, (k, v) -> {
                                    if (v != null && !v.equals(1)) {
                                        System.out.println("in PUT remove key from tempMap: key " + key);
                                        return null;
                                    }
                                    else {
                                        System.out.println("in PUT added key to tempMap: key " + key + " delta " + 1);
                                        return 1;
                                    }
                                });
                        }

                        size.incrementAndGet();
                    }
                }
                finally {
                    page.pageLock.writeLock().unlock();
                }
            }
            finally {
                partLock.readLock().unlock();
            }
        }

        void remove(int key) {
            partLock.readLock().lock();

            try {
                int pageNumber = key / pagesCount;

                Page page = pages.get(pageNumber);

                page.pageLock.writeLock().lock();

                try {
                    if (page.keys.contains(key)) {
                        page.keys.remove(key);

                        System.out.println("remove key " + key);

//                    if (reconInProgress) {
//                        if (key < borderKey)
//                            reconSize.incrementAndGet();
//                        else if (tempMap.get(key) != null && tempMap.get(key) != -1)
//                            tempMap.remove(key);
//                        else
//                            tempMap.put(key, -1);
//                    }

                        if (reconInProgress) {
                            if (borderKey != null && key < borderKey) {
                                reconSize.decrementAndGet();
                                System.out.println("in REMOVE after deccrement reconSize: key " + key + " reconSize " + reconSize.get());
                            }
                            else
                                tempMap.compute(key, (k, v) -> {
                                    if (v != null && !v.equals(-1)) {
                                        System.out.println("in REMOVE remove key from tempMap: key " + key);
                                        return null;
                                    }
                                    else {
                                        System.out.println("in REMOVE added key to tempMap: key " + key + " delta " + -1);
                                        return -1;
                                    }
                                });
                        }

                        size.decrementAndGet();
                    }
                }
                finally {
                    page.pageLock.writeLock().unlock();
                }
            }
            finally {
                partLock.readLock().unlock();
            }
        }

        int realSize() {
            int realSize = 0;

            for (Page page : pages) {
                realSize += page.keys.size();
            }

            return realSize;
        }
    }

    static class Page {
        volatile ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();

        volatile ConcurrentSkipListSet<Integer> keys = new ConcurrentSkipListSet<>();
    }

//    В тикете было описано как по логам определять наличие расхождения LWM и HWM. Если до того как стрельнет "AssertionError: LWM after HWM" пройдет мало времени, то вероятно мы не успеем в полуручном режиме починить LWM и HWM. Возможно мы могли бы вместо того чтобы сразу ассертить попробовать автоматически починить эту проблему.

}