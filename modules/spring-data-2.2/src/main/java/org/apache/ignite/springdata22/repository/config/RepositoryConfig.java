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

package org.apache.ignite.springdata22.repository.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * The annotation can be used to pass Ignite specific parameters to a bound repository.
 *
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RepositoryConfig {
    /**
     * Cache name string.
     *
     * @return A name of a distributed Apache Ignite cache an annotated repository will be mapped to.
     */
    String cacheName() default "";

    /**
     * Name of the Spring Bean that must provide {@link Ignite} or {@link IgniteClient} instance for accessing the
     * Ignite cluster.
     */
    String igniteInstance() default "igniteInstance";

    /**
     * Name of the Spring Bean that must provide {@link IgniteConfiguration} or {@link ClientConfiguration} that is used
     * for instantination of Ignite node or Ignite thin client respectively for accessing the Ignite cluster.
     */
    String igniteCfg() default "igniteCfg";

    /**
     * Ignite spring cfg path string. Default "igniteSpringCfgPath".
     *
     * @return A path to Ignite's Spring XML configuration spring bean name
     */
    String igniteSpringCfgPath() default "igniteSpringCfgPath";

    /**
     * Auto create cache. Default false to enforce control over cache creation and to avoid cache creation by mistake
     * <p>
     * Tells to Ignite Repository factory wether cache should be auto created if not exists.
     *
     * @return the boolean
     */
    boolean autoCreateCache() default false;
}
