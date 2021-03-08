/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.systemview.walker;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnConfigurationView;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link StatisticsColumnConfigurationView} attributes walker.
 * 
 * @see StatisticsColumnConfigurationView
 */
public class StatisticsColumnConfigurationViewWalker implements SystemViewRowAttributeWalker<StatisticsColumnConfigurationView> {
    /** Filter key for attribute "schema" */
    public static final String SCHEMA_FILTER = "schema";

    /** Filter key for attribute "type" */
    public static final String TYPE_FILTER = "type";

    /** Filter key for attribute "name" */
    public static final String NAME_FILTER = "name";

    /** Filter key for attribute "column" */
    public static final String COLUMN_FILTER = "column";

    /** List of filtrable attributes. */
    private static final List<String> FILTRABLE_ATTRS = Collections.unmodifiableList(F.asList(
        "schema", "type", "name", "column"
    ));

    /** {@inheritDoc} */
    @Override public List<String> filtrableAttributes() {
        return FILTRABLE_ATTRS;
    }

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "schema", String.class);
        v.accept(1, "type", String.class);
        v.accept(2, "name", String.class);
        v.accept(3, "column", String.class);
        v.accept(4, "version", long.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(StatisticsColumnConfigurationView row, AttributeWithValueVisitor v) {
        v.accept(0, "schema", String.class, row.schema());
        v.accept(1, "type", String.class, row.type());
        v.accept(2, "name", String.class, row.name());
        v.accept(3, "column", String.class, row.column());
        v.acceptLong(4, "version", row.version());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 5;
    }
}
