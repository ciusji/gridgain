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
package org.apache.ignite.internal.processors.query.stat.config;

import java.io.Serializable;
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Describe configuration of the statistic for a database object' column.
 */
public class StatisticsColumnConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Columns name. */
    private final String name;

    /** Collection version. */
    private final long ver;

    /** Tombstone flag: {@code true} statistic for this column is dropped, otherwise {@code false}. */
    private final boolean tombstone;

    /**
     * Constructor.
     *
     * @param name Column name.
     */
    public StatisticsColumnConfiguration(String name) {
        this(name, 1, false);
    }

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param ver Collection version.
     * @param tombstone if {@code true} - object represents a tombstone of configuration,
     *                  if {@code false} - live configuration.
     */
    private StatisticsColumnConfiguration(String name, long ver, boolean tombstone) {
        this.name = name;
        this.ver = ver;
        this.tombstone = tombstone;
    }

    /**
     * Constructor.
     *
     * @param cfg Base statistics column configuration
     * @param ver New collection version.
     * @param tombstone if {@code true} - object represents a tombstone of configuration,
     *                  if {@code false} - live configuration.
     */
    private StatisticsColumnConfiguration(StatisticsColumnConfiguration cfg, long ver, boolean tombstone) {
        this.name = cfg.name;
        this.ver = ver;
        this.tombstone = tombstone;
    }

    /**
     * Get column name.
     *
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * Get collection version.
     *
     * @return Collection version.
     */
    public long version() {
        return ver;
    }

    /**
     * Tombstone flag.
     *
     * @return {@code true} statistic for this column is dropped, otherwise {@code false}.
     */
    public boolean tombstone() {
        return tombstone;
    }

    /**
     * Merge configuration changes with existing configuration.
     *
     * @param oldCfg Previous configuration. May be {@code null} when new configuration is created.
     * @param newCfg New configuration.
     * @return merged configuration.
     */
    public static StatisticsColumnConfiguration merge(
        StatisticsColumnConfiguration oldCfg,
        StatisticsColumnConfiguration newCfg
    ) {
        if (oldCfg == null)
            return newCfg;

        if (oldCfg.collectionAwareEqual(newCfg))
            return new StatisticsColumnConfiguration(newCfg, oldCfg.ver, oldCfg.tombstone);

        return new StatisticsColumnConfiguration(newCfg, oldCfg.ver + 1, false);
    }

    /**
     * Compare only collection or gathering related fields of config.
     *
     * @param o StatisticsColumnConfiguration to compare with.
     * @return {@code true} if configurations are equal from the gathering point of view, {@code false} - otherwise.
     */
    public boolean collectionAwareEqual(StatisticsColumnConfiguration o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsColumnConfiguration that = (StatisticsColumnConfiguration)o;

        return ver == that.ver && tombstone == that.tombstone
            && Objects.equals(name, that.name);
    }

    /**
     * Create configuration for dropped statistic column.
     *
     * @return Tombstone column configuration.
     */
    public StatisticsColumnConfiguration createTombstone()
    {
        return new StatisticsColumnConfiguration(this, ver + 1, true);
    }

    /**
     * Create configuration for dropped statistic column.
     *
     * @return Columns configuration for refresh statistic.
     */
    public StatisticsColumnConfiguration refresh()
    {
        return new StatisticsColumnConfiguration(this, tombstone ? ver : ver + 1, tombstone);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsColumnConfiguration that = (StatisticsColumnConfiguration)o;

        return ver == that.ver && tombstone == that.tombstone
            && Objects.equals(name, that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, ver, tombstone);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsColumnConfiguration.class, this);
    }
}
