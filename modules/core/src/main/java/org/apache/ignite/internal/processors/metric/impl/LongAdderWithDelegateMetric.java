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

package org.apache.ignite.internal.processors.metric.impl;

import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.Nullable;

/**
 * Long metric implementation based on {@link LongAdder} with {@link #delegate}.
 */
public class LongAdderWithDelegateMetric extends LongAdderMetric {
    /** */
    public interface Delegate {
        /**
         * Called when the parent metric's {@code increment} method is called.
         */
        public void increment();

        /**
         * Called when the parent metric's {@code decrement} method is called.
         */
        public void decrement();

        /**
         * Called when the parent metric's {@code add} method is called.
         */
        public void add(long x);
    }

    /** Delegate. */
    private final Delegate delegate;

    /**
     * @param name Name.
     * @param delegate Delegate to which all updates from new metric will be delegated to.
     * @param descr Description.
     */
    public LongAdderWithDelegateMetric(String name, Delegate delegate, @Nullable String descr) {
        super(name, descr);

        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void increment() {
        super.increment();

        delegate.increment();
    }

    /** {@inheritDoc} */
    @Override public void decrement() {
        super.decrement();

        delegate.decrement();
    }

    /** {@inheritDoc} */
    @Override public void add(long x) {
        super.add(x);

        delegate.add(x);
    }
}
