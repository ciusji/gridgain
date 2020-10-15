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
package org.apache.ignite.internal.processors.query.stat;

import org.jetbrains.annotations.Nullable;

/**
 * Types of statistics width.
 */
public enum StatsType {
    /** Statistics by some particular partition. */
    PARTITION,

    /** Statistics by some data node. */
    LOCAL,

    /** Statistics by whole object (table or index). */
    GLOBAL;

    /** Enumerated values. */
    private static final StatsType[] VALUES = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static StatsType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALUES.length ? VALUES[ord] : null;
    }
}
