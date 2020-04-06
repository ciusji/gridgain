﻿/*
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

namespace Apache.Ignite.Core.Cache.Eviction
{
    /// <summary>
    /// Eviction policy based on First In First Out (FIFO) algorithm with batch eviction support.
    /// <para />
    /// The eviction starts in the following cases: 
    /// The cache size becomes <see cref="EvictionPolicyBase.BatchSize"/>
    /// elements greater than the maximum size;
    /// The size of cache entries in bytes becomes greater than the maximum memory size;
    /// The size of cache entry calculates as sum of key size and value size.
    /// <para />
    /// Note: Batch eviction is enabled only if maximum memory limit isn't set.
    /// <para />
    /// This implementation is very efficient since it does not create any additional
    /// table-like data structures. The FIFO ordering information is
    /// maintained by attaching ordering metadata to cache entries.
    /// </summary>
    public class FifoEvictionPolicy : EvictionPolicyBase
    {
        // No-op.
    }
}