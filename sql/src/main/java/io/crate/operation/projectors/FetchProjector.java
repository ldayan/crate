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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import io.crate.core.collections.Row;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.fetch.PositionalRowMerger;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.ActionListener;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FetchProjector implements Projector, ProjectorUpstream {

    private PositionalRowMerger downstream;
    private final TransportFetchNodeAction transportFetchNodeAction;

    private final UUID jobId;
    private final CollectExpression<?> collectDocIdExpression;
    private final List<Symbol> toFetchSymbols;
    private final IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private final int bulkSize;
    private final AtomicLong inputCursor = new AtomicLong(0);
    private final Map<String, NodeBucket> nodeBuckets = new HashMap<>();
    private final RowDelegate collectRow = new RowDelegate();
    private final RowDelegate fetchRow = new RowDelegate();
    private final Row outputRow;
    private final AtomicInteger remainingFetchRequests = new AtomicInteger(0);
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);

    private final Map<String, Integer> nodeUpstreamIds;

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          Functions functions,
                          UUID jobId,
                          CollectExpression<?> collectDocIdExpression,
                          List<Symbol> inputSymbols,
                          List<Symbol> outputSymbols,
                          IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                          Set<String> affectedNodeIds,
                          int bulkSize) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.jobId = jobId;
        this.collectDocIdExpression = collectDocIdExpression;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.bulkSize = bulkSize;

        RowInputSymbolVisitor rowInputSymbolVisitor = new RowInputSymbolVisitor(functions);

        RowInputSymbolVisitor.Context collectRowContext = new RowInputSymbolVisitor.Context();
        collectRowContext.row(collectRow);
        collectRowContext.references(inputSymbols);

        RowInputSymbolVisitor.Context fetchRowContext = new RowInputSymbolVisitor.Context();
        fetchRowContext.row(fetchRow);
        toFetchSymbols = new ArrayList<>();
        toFetchSymbols.addAll(outputSymbols);
        toFetchSymbols.removeAll(inputSymbols);
        fetchRowContext.references(toFetchSymbols);

        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (inputSymbols.contains(symbol)) {
                inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
            } else {
                inputs.add(rowInputSymbolVisitor.process(symbol, fetchRowContext));
            }
        }

        outputRow = new InputRow(inputs);

        nodeUpstreamIds = new HashMap<>(jobSearchContextIdToNode.size());
        int upstreamId = 0;
        for (String nodeId : affectedNodeIds) {
            nodeUpstreamIds.put(nodeId, upstreamId++);
        }
    }

    @Override
    public void startProjection() {
    }

    @Override
    public boolean setNextRow(Row row) {
        collectDocIdExpression.setNextRow(row);

        long docId = (Long)collectDocIdExpression.value();
        int jobSearchContextId = (int)(docId >> 32);

        String nodeId = jobSearchContextIdToNode.get(jobSearchContextId);

        NodeBucket nodeBucket = nodeBuckets.get(nodeId);
        if (nodeBucket == null) {
            nodeBucket = new NodeBucket(nodeId);
            nodeBuckets.put(nodeId, nodeBucket);

        }
        nodeBucket.add(inputCursor.getAndIncrement(), docId, row);
        if (nodeBucket.size() >= bulkSize) {
            flushNodeBucket(nodeBucket);
            nodeBuckets.remove(nodeBucket.nodeId);
        }

        return true;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream =
                new PositionalRowMerger(downstream, nodeUpstreamIds.size(), new int[]{outputRow.size()});
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            // flush all remaining buckets
            Iterator<Map.Entry<String, NodeBucket>> it = nodeBuckets.entrySet().iterator();
            while (it.hasNext()) {
                flushNodeBucket(it.next().getValue());
                it.remove();
            }
            if (remainingFetchRequests.get() <= 0) {
                Iterator<Integer> nodeIt = nodeUpstreamIds.values().iterator();
                while (nodeIt.hasNext()) {
                    downstream.upstreamFinished();
                }
            }
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        downstream.upstreamFailed(throwable);
    }

    private void flushNodeBucket(final NodeBucket nodeBucket) {
        NodeFetchRequest request = new NodeFetchRequest();
        request.jobId(jobId);
        request.toFetchSymbols(toFetchSymbols);
        request.jobSearchContextDocIds(nodeBucket.docIds());

        remainingFetchRequests.incrementAndGet();

        transportFetchNodeAction.execute(nodeBucket.nodeId, request, new ActionListener<NodeFetchResponse>() {
            @Override
            public void onResponse(NodeFetchResponse response) {
                int idx = 0;
                for (Row row : response.rows()) {
                    collectRow.delegate(nodeBucket.inputRow(idx));
                    fetchRow.delegate(row);
                    downstream.setNextRow(nodeUpstreamIds.get(nodeBucket.nodeId), nodeBucket.cursor(idx), outputRow);
                    idx++;
                }
                if (remainingFetchRequests.decrementAndGet() <= nodeUpstreamIds.size()) {
                    downstream.upstreamFinished();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                downstream.upstreamFailed(e);
            }
        });

    }

    private static class NodeBucket {

        final String nodeId;
        final List<Row> inputRows = new ArrayList<>();
        final List<Long> cursors = new ArrayList<>();
        final List<Long> docIds = new ArrayList<>();

        public NodeBucket(String nodeId) {
            this.nodeId = nodeId;
        }

        public void add(Long cursor, Long docId, Row row) {
            cursors.add(cursor);
            docIds.add(docId);
            inputRows.add(row);
        }

        public int size() {
            return cursors.size();
        }

        public List<Long> docIds() {
            return docIds;
        }

        public Long cursor(int index) {
            return cursors.get(index);
        }

        public Row inputRow(int index) {
            return inputRows.get(index);
        }
    }

    private static class RowDelegate implements Row {
        Row delegate;

        public void delegate(Row row) {
            delegate = row;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Object get(int index) {
            return delegate.get(index);
        }
    }
}
