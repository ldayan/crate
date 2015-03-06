/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.*;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.projectors.*;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.CrateTestCluster;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    TransportExecutor executor;
    DocSchemaInfo docSchemaInfo;

    @Before
    public void transportSetUp() {
        CrateTestCluster cluster = cluster();
        executor = cluster.getInstance(TransportExecutor.class);
        docSchemaInfo = cluster.getInstance(DocSchemaInfo.class);
    }

    @After
    public void transportTearDown() {
        executor = null;
        docSchemaInfo = null;
    }

    private void setUpCharacters() {
        sqlExecutor.exec("create table characters (id int primary key, name string) " +
                "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureGreen();
        sqlExecutor.exec("insert into characters (id, name) values (?, ?)",
                new Object[][]{
                        new Object[] { 1, "Arthur"},
                        new Object[] { 2, "Ford"},
                }
        );
        sqlExecutor.refresh("characters");
    }

    private Job createCollectJob(Planner.Context plannerContext, List<Symbol> toCollect, boolean closeContext) {
        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");

        if (toCollect == null) {
            ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));
            Symbol docIdRef = new Reference(docIdRefInfo);
            toCollect = ImmutableList.of(docIdRef);
        }

        List<DataType> outputTypes = new ArrayList<>(toCollect.size());
        for (Symbol symbol : toCollect) {
            outputTypes.add(symbol.valueType());
        }

        CollectNode collectNode = new CollectNode("collect", tableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.toCollect(toCollect);
        collectNode.outputTypes(outputTypes);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.closeContext(closeContext);
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        Plan plan = new IterablePlan(collectNode);
        return executor.newJob(plan);
    }

    @Test
    public void testCollectDocId() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context();
        Job job = createCollectJob(plannerContext, null, true);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        int seenJobSearchContextId = -1;
        for (ListenableFuture<TaskResult> nodeResult : result) {
            TaskResult taskResult = nodeResult.get();
            assertThat(taskResult.rows().size(), is(1));
            Object docIdCol = taskResult.rows().iterator().next().get(0);
            assertNotNull(docIdCol);
            assertThat(docIdCol, instanceOf(Long.class));
            long docId = (long)docIdCol;
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            int doc = (int)docId;
            assertThat(doc, is(0));
            assertThat(jobSearchContextId, greaterThan(-1));
            if (seenJobSearchContextId == -1) {
                assertThat(jobSearchContextId, anyOf(is(0), is(1)));
                seenJobSearchContextId = jobSearchContextId;
            } else {
                assertThat(jobSearchContextId, is(seenJobSearchContextId == 0 ? 1 : 0));
            }
        }

    }

    @Test
    public void testFetchAction() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context();
        Job job = createCollectJob(plannerContext, null, false);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TransportFetchNodeAction transportFetchNodeAction = cluster().getInstance(TransportFetchNodeAction.class);

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo idRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("id")),
                RowGranularity.DOC,
                IntegerType.INSTANCE
                );
        ReferenceInfo nameRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("name")),
                RowGranularity.DOC,
                StringType.INSTANCE
        );
        List<Symbol> toFetchSymbols = ImmutableList.<Symbol>of(new Reference(idRefInfo), new Reference(nameRefInfo));

        // extract docIds by nodeId and jobSearchContextId
        Map<String, List<Long>> jobSearchContextDocIds = new HashMap<>();
        for (ListenableFuture<TaskResult> nodeResult : result) {
            TaskResult taskResult = nodeResult.get();
            long docId = (long)taskResult.rows().iterator().next().get(0);
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            String nodeId = plannerContext.nodeId(jobSearchContextId);
            List<Long> docIdsPerNode = jobSearchContextDocIds.get(nodeId);
            if (docIdsPerNode == null) {
                docIdsPerNode = new ArrayList<>();
                jobSearchContextDocIds.put(nodeId, docIdsPerNode);
            }
            docIdsPerNode.add(docId);
        }

        final CountDownLatch latch = new CountDownLatch(jobSearchContextDocIds.size());
        final List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, List<Long>> nodeEntry : jobSearchContextDocIds.entrySet()) {
            NodeFetchRequest nodeFetchRequest = new NodeFetchRequest();
            nodeFetchRequest.jobId(job.id());
            nodeFetchRequest.toFetchSymbols(toFetchSymbols);
            nodeFetchRequest.closeContext(true);
            nodeFetchRequest.jobSearchContextDocIds(nodeEntry.getValue());

            transportFetchNodeAction.execute(nodeEntry.getKey(), nodeFetchRequest, new ActionListener<NodeFetchResponse>() {
                @Override
                public void onResponse(NodeFetchResponse nodeFetchResponse) {
                    for (Row row : nodeFetchResponse.rows()) {
                        rows.add(row);
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    latch.countDown();
                    fail(e.getMessage());
                }
            });
        }
        latch.await();

        assertThat(rows.size(), is(2));
        Iterator<Row> it = rows.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            assertThat((Integer) row.get(0), anyOf(is(1), is(2)));
            assertThat((BytesRef) row.get(1), anyOf(is(new BytesRef("Arthur")), is(new BytesRef("Ford"))));
        }
    }


    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context();

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo idRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("id")),
                RowGranularity.DOC,
                IntegerType.INSTANCE
        );
        ReferenceInfo nameRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("name")),
                RowGranularity.DOC,
                StringType.INSTANCE
        );
        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));

        Symbol docIdSymbol = new InputColumn(0, DataTypes.STRING);

        List<Symbol> inputSymbols = ImmutableList.<Symbol>of(new Reference(docIdRefInfo), new Reference(idRefInfo));
        final List<Symbol> outputSymbols = ImmutableList.<Symbol>of(new Reference(idRefInfo), new Reference(nameRefInfo));

        Job job = createCollectJob(plannerContext, inputSymbols, false);
        List<ListenableFuture<TaskResult>> collectResult = executor.execute(job);

        FetchProjection projection = new FetchProjection(docIdSymbol, inputSymbols, outputSymbols, 100);

        Functions functions = cluster().getInstance(Functions.class);
        IndicesService indicesService = cluster().getInstance(IndicesService.class);
        IndexService indexService = indicesService.indexService("characters");
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                indexService.shardInjector(indexService.shardIds().iterator().next()).getProvider(ShardReferenceResolver.class).get(),
                functions,
                RowGranularity.SHARD);
        ProjectionToProjectorVisitor projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService(),
                ImmutableSettings.EMPTY,
                cluster().getInstance(TransportActionProvider.class),
                implementationSymbolVisitor
        );

        FetchProjector fetchProjector = (FetchProjector)projectorVisitor.process(
                projection,
                mock(RamAccountingContext.class),
                Optional.of(job.id()),
                Optional.of(plannerContext.jobSearchContextIdToNode()),
                Optional.of(tableInfo.getRouting(WhereClause.MATCH_ALL, null).nodes()));

        CollectingProjector resultProvider = new CollectingProjector();
        fetchProjector.downstream(resultProvider);


        fetchProjector.startProjection();

        for (ListenableFuture<TaskResult> nodeResult : collectResult) {
            TaskResult taskResult = nodeResult.get();
            for (Row row : taskResult.rows()) {
                fetchProjector.setNextRow(row);
            }
        }
        fetchProjector.upstreamFinished();

        final CountDownLatch latch = new CountDownLatch(1);
        Futures.addCallback(resultProvider.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                assertThat(result.size(), is(2));
                for (Row row : result) {
                    assertThat(row.size(), is(outputSymbols.size()));
                }
                // TODO: validate sorting when CollectNode supports sorting
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.getMessage());
            }
        });

        latch.await();
    }
}
