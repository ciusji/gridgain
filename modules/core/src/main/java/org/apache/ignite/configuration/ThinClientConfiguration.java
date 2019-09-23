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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Server-side thin-client configuration.
 *
 * This configuration related only to lightweight (thin) Ignite clients and not related to ODBC and JDBC clients.
 */
public class ThinClientConfiguration {
    /** Default limit of active transactions count per connection. */
    public static final int DFLT_MAX_ACTIVE_TX_PER_CONNECTION = 100;

    /** Active transactions count per connection limit. */
    private int maxActiveTxPerConn = DFLT_MAX_ACTIVE_TX_PER_CONNECTION;

    /**
     * Creates thin-client configuration with all default values.
     */
    public ThinClientConfiguration() {
        // No-op.
    }

    /**
     * Creates thin-client configuration by copying all properties from given configuration.
     *
     * @param cfg Configuration to copy.
     */
    public ThinClientConfiguration(ThinClientConfiguration cfg) {
        assert cfg != null;

        maxActiveTxPerConn = cfg.maxActiveTxPerConn;
    }

    /**
     * Gets active transactions count per connection limit.
     */
    public int getMaxActiveTxPerConnection() {
        return maxActiveTxPerConn;
    }

    /**
     * Sets active transactions count per connection limit.
     *
     * @return {@code this} for chaining.
     */
    public ThinClientConfiguration setMaxActiveTxPerConnection(int maxActiveTxPerConn) {
        this.maxActiveTxPerConn = maxActiveTxPerConn;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ThinClientConfiguration.class, this);
    }
}
