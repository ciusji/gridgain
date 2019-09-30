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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Configuration object for Ignite Binary Objects.
 * @see org.apache.ignite.IgniteBinary
 */
public class BinaryConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Default compact footer flag setting. */
    public static final boolean DFLT_COMPACT_FOOTER = true;

    /** Default protocol version. */
    public static final byte DFLT_PROTO_VER = GridBinaryMarshaller.CUR_PROTO_VER;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Name mapper. */
    private BinaryNameMapper nameMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Types. */
    private Collection<BinaryTypeConfiguration> typeCfgs;

    /** Compact footer flag. */
    private boolean compactFooter = DFLT_COMPACT_FOOTER;

    /** Protocol version. */
    private byte protoVer = GridBinaryMarshaller.CUR_PROTO_VER;

    /**
     * Sets class names of binary objects explicitly.
     *
     * @param clsNames Class names.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setClassNames(Collection<String> clsNames) {
        if (typeCfgs == null)
            typeCfgs = new ArrayList<>(clsNames.size());

        for (String clsName : clsNames)
            typeCfgs.add(new BinaryTypeConfiguration(clsName));

        return this;
    }

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setIdMapper(BinaryIdMapper idMapper) {
        this.idMapper = idMapper;

        return this;
    }

    /**
     * Gets name mapper.
     *
     * @return Name mapper.
     */
    public BinaryNameMapper getNameMapper() {
        return nameMapper;
    }

    /**
     * Sets name mapper.
     *
     * @param nameMapper Name mapper.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setNameMapper(BinaryNameMapper nameMapper) {
        this.nameMapper = nameMapper;

        return this;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public BinarySerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setSerializer(BinarySerializer serializer) {
        this.serializer = serializer;

        return this;
    }

    /**
     * Gets types configuration.
     *
     * @return Types configuration.
     */
    public Collection<BinaryTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * Sets type configurations.
     *
     * @param typeCfgs Type configurations.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setTypeConfigurations(Collection<BinaryTypeConfiguration> typeCfgs) {
        this.typeCfgs = typeCfgs;

        return this;
    }

    /**
     * Get whether to write footers in compact form. When enabled, Ignite will not write fields metadata
     * when serializing objects, because internally {@code BinaryMarshaller} already distribute metadata inside
     * cluster. This increases serialization performance.
     * <p>
     * <b>WARNING!</b> This mode should be disabled when already serialized data can be taken from some external
     * sources (e.g. cache store which stores data in binary form, data center replication, etc.). Otherwise binary
     * objects without any associated metadata could appear in the cluster and Ignite will not be able to deserialize
     * it.
     * <p>
     * Defaults to {@link #DFLT_COMPACT_FOOTER}.
     *
     * @return Whether to write footers in compact form.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /**
     * Set whether to write footers in compact form. See {@link #isCompactFooter()} for more info.
     *
     * @param compactFooter Whether to write footers in compact form.
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setCompactFooter(boolean compactFooter) {
        this.compactFooter = compactFooter;

        return this;
    }

    /**
     * Get version of the binary protocol that will be used for marshalling objects.
     * By default {@link #DFLT_PROTO_VER} is used.
     *
     * @return Current protocol version.
     */
    public byte getProtocolVersion() {
        return protoVer;
    }

    /**
     * Set binary protocol version.
     *
     * @param protoVer Protocol version. Should be in range 1..{@link #DFLT_PROTO_VER}
     * @return {@code this} for chaining.
     */
    public BinaryConfiguration setProtocolVersion(byte protoVer) {
        this.protoVer = protoVer;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryConfiguration.class, this);
    }
}
