/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */
package io.crate.analyze;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.core.NumberOfReplicas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.*;


public class CreateTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateTableAnalysis> {

    private final ExpressionToObjectVisitor expressionVisitor = new ExpressionToObjectVisitor();

    protected final Map<String, SettingsApplier> supportedProperties =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put("number_of_replicas", new SettingsApplier() {
                        @Override
                        public void apply(ImmutableSettings.Builder settingsBuilder,
                                          Object[] parameters,
                                          List<Expression> expressions) {
                            Preconditions.checkArgument(expressions.size() == 1,
                                    "Invalid number of arguments passed to \"number_of_replicas\"");

                            Object numReplicas = expressionVisitor.process(expressions.get(0), parameters);

                            NumberOfReplicas numberOfReplicas = new NumberOfReplicas(numReplicas.toString());
                            settingsBuilder.put(numberOfReplicas.esSettingKey(), numberOfReplicas.esSettingValue());
                        }
                    })
            .build();

    protected Void visitNode(Node node, CreateTableAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Void visitCreateTable(CreateTable node, CreateTableAnalysis context) {
        TableIdent tableIdent = TableIdent.of(node.name());
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "A custom schema name must not be specified in the CREATE TABLE clause");
        context.table(tableIdent);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        context.indexSettingsBuilder().put("number_of_replicas", Constants.DEFAULT_NUM_REPLICAS);

        if (node.properties().isPresent()) {
            applySettingsFromProperties(node.properties().get(), context);
        }

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();
            context.indexSettingsBuilder().put("number_of_shards",
                    clusteredBy.numberOfShards().or(Constants.DEFAULT_NUM_SHARDS));
        }

        for (TableElement tableElement : node.tableElements()) {
            process(tableElement, context);
        }

        setCopyTo(context);

        return null;
    }

    protected void applySettingsFromProperties(GenericProperties genericProperties, CreateTableAnalysis context) {
        for (Map.Entry<String, List<Expression>> entry : genericProperties.properties().entrySet()) {
            SettingsApplier settingsApplier = supportedProperties.get(entry.getKey());
            if (settingsApplier == null) {
                throw new IllegalArgumentException(
                        String.format("The property \"%s\" is not valid in the CREATE TABLE context", entry.getKey()));
            }

            settingsApplier.apply(context.indexSettingsBuilder(), context.parameters(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void setCopyTo(CreateTableAnalysis context) {
        for (Map.Entry<String, Set<String>> entry : context.copyTo().entrySet()) {

            Map<String, Object> columnDef = context.mappingProperties();
            String[] key = entry.getKey().split("\\.");

            try {
                while (key.length > 1) {
                    columnDef = (Map<String, Object>)((Map<String, Object>)columnDef.get(key[0])).get("properties");
                    key = Arrays.copyOfRange(key, 1, key.length);
                }
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("fulltext index points to a column that doesn't exist");
            }

            columnDef = (Map<String, Object>)columnDef.get(key[0]);
            columnDef.put("copy_to", Lists.newArrayList(entry.getValue()));
        }
    }

    @Override
    public Void visitColumnDefinition(ColumnDefinition node, CreateTableAnalysis context) {
        Map<String, Object> columnDefinition = new HashMap<>();
        context.addColumnDefinition(node.ident(), columnDefinition);
        columnDefinition.put("store", false);

        for (ColumnConstraint columnConstraint : node.constraints()) {
            process(columnConstraint, context);
        }
        if (!columnDefinition.containsKey("index")) {
            // not set by the column constraints -> use default
            columnDefinition.put("index", "not_analyzed");
        }

        process(node.type(), context);
        context.propertiesStack().pop();

        return null;
    }

    @Override
    public Void visitIndexDefinition(IndexDefinition node, CreateTableAnalysis context) {
        Map<String, Object> columnDefinition = new HashMap<>();
        context.addIndexDefinition(node.ident(), columnDefinition);

        columnDefinition.put("store", false);
        columnDefinition.put("type", "string");
        setAnalyzer(columnDefinition, node.properties(), context);

        for (Expression expression : node.columns()) {
            String expressionName = expressionVisitor.process(expression, null).toString();
            context.addCopyTo(expressionName, node.ident());
        }

        return null;
    }

    @Override
    public Void visitColumnType(ColumnType node, CreateTableAnalysis context) {
        String typeName;
        if (node.name().equals("timestamp")) {
            typeName = "date";
        } else if (node.name().equals("int")) {
            typeName = "integer";
        } else {
            typeName = node.name();
        }

        Object indexName = context.currentColumnDefinition().get("index");
        assert indexName != null;
        if (indexName.equals("not_analyzed")) {
            context.currentColumnDefinition().put("doc_values", true);
        }
        context.currentColumnDefinition().put("type", typeName);

        return null;
    }

    @Override
    public Void visitObjectColumnType(ObjectColumnType node, CreateTableAnalysis context) {
        context.currentColumnDefinition().put("type", node.name());
        Map<String, Object> nestedProperties = new HashMap<>();
        context.currentColumnDefinition().put("properties", nestedProperties);

        switch (node.objectType().or("dynamic").toLowerCase(Locale.ENGLISH)) {
            case "dynamic":
                context.currentColumnDefinition().put("dynamic", "true");
                break;
            case "strict":
                context.currentColumnDefinition().put("dynamic", "strict");
                break;
            case "ignored":
                context.currentColumnDefinition().put("dynamic", "false");
                break;
        }

        context.propertiesStack().push(nestedProperties);
        for (ColumnDefinition columnDefinition : node.nestedColumns()) {
            process(columnDefinition, context);
        }
        context.propertiesStack().pop();

        return null;
    }

    @Override
    public Void visitCollectionColumnType(CollectionColumnType node, CreateTableAnalysis context) {
        if (node.type() == ColumnType.Type.SET) {
            throw new UnsupportedOperationException("the SET dataType is currently not supported");
        }
        context.currentMetaColumnDefinition().put("collection_type", "array");
        context.currentColumnDefinition().put("doc_values", false);

        if (node.innerType().type() != ColumnType.Type.PRIMITIVE) {
            throw new UnsupportedOperationException("Nesting ARRAY or SET types is currently not supported");
        }

        process(node.innerType(), context);
        return null;
    }

    @Override
    public Void visitIndexColumnConstraint(IndexColumnConstraint node, CreateTableAnalysis context) {
        if (node.indexMethod().equalsIgnoreCase("fulltext")) {
            setAnalyzer(context.currentColumnDefinition(), node.properties(), context);
        } else if (node.indexMethod().equalsIgnoreCase("plain")) {
            context.currentColumnDefinition().put("index", "not_analyzed");
        } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
            context.currentColumnDefinition().put("index", "no");
        } else {
            throw new IllegalArgumentException(
                    String.format("Invalid index method \"%s\"", node.indexMethod()));
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint node, CreateTableAnalysis context) {
        for (Expression expression : node.columns()) {
            context.addPrimaryKey(expressionVisitor.process(expression, null).toString());
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, CreateTableAnalysis context) {
        context.addPrimaryKey(context.currentColumnName());
        return null;
    }

    private void setAnalyzer(Map<String, Object> columnDefinition,
                             GenericProperties properties,
                             CreateTableAnalysis context) {
        columnDefinition.put("index", "analyzed");
        List<Expression> analyzerExpressions = properties.get("analyzer");
        if (analyzerExpressions == null) {
            columnDefinition.put("analyzer", "standard");
            return;
        }

        if (analyzerExpressions.size() != 1) {
            throw new IllegalArgumentException("Invalid argument(s) passed to the analyzer property");
        }

        String analyzerName = expressionVisitor.process(analyzerExpressions.get(0), context.parameters()).toString();
        if (context.analyzerService().hasCustomAnalyzer(analyzerName)) {
            Settings settings = context.analyzerService().resolveFullCustomAnalyzerSettings(analyzerName);
            context.indexSettingsBuilder().put(settings.getAsMap());
        }

        columnDefinition.put("analyzer", analyzerName);
    }
}