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

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

/**
 *
 */
public class GridBinaryCacheEntryMemorySizeSelfTest extends GridCacheEntryMemorySizeSelfTest {
    /** {@inheritDoc} */
    @Override protected Marshaller createMarshaller() throws IgniteCheckedException {
        BinaryMarshaller marsh = new BinaryMarshaller();

        IgniteConfiguration iCfg = new IgniteConfiguration();
        iCfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                // No-op.
            }
        });
        iCfg.setClientMode(false);

        GridTestKernalContext kernCtx = new GridTestKernalContext(log, iCfg);
        kernCtx.add(new GridDiscoveryManager(kernCtx));

        MarshallerContextTestImpl marshCtx = new MarshallerContextTestImpl(null);
        marshCtx.onMarshallerProcessorStarted(kernCtx, null);

        marsh.setContext(marshCtx);

        BinaryContext pCtx = new BinaryContext(BinaryNoopMetadataHandler.instance(), iCfg, new NullLogger());

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", pCtx, iCfg);

        return marsh;
    }
}
