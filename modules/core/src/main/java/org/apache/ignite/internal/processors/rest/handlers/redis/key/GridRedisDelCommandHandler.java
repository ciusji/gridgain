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

package org.apache.ignite.internal.processors.rest.handlers.redis.key;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.DEL;

/**
 * Redis DEL command handler.
 */
public class GridRedisDelCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        DEL
    );

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisDelCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 2)
            throw new GridRedisGenericException("Wrong number of arguments");

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.command(CACHE_REMOVE_ALL);
        restReq.cacheName(msg.cacheName());

        List<String> keys = msg.auxMKeys();

        Map<Object, Object> mget = U.newHashMap(keys.size());

        for (String key : keys)
            mget.put(key, null);

        restReq.values(mget);

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        // It has to respond with the number of removed entries...
        return (restRes.getResponse() == null ? GridRedisProtocolParser.toInteger("0")
            : GridRedisProtocolParser.toInteger(String.valueOf(params.size())));
    }
}
