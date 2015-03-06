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

package io.crate.operation.fetch;

import com.google.common.primitives.Longs;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.Projector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class PositionalRowMerger implements Projector, ProjectorUpstream {

    private Projector downstream;
    private final AtomicInteger upstreamsRemaining;
    private final LinkedList<Row>[] upstreamBuffers;
    private final int[] orderingColumnIndices;
    private volatile long outputCursor = 0;
    private volatile long leastUpstreamBufferCursor = -1;

    private final ESLogger logger = Loggers.getLogger(getClass());

    @SuppressWarnings("unchecked")
    public PositionalRowMerger(Projector downstream,
                               int numUpstreams,
                               int[] orderingColumnIndices) {
        downstream(downstream);
        upstreamBuffers = new LinkedList[numUpstreams];
        upstreamsRemaining = new AtomicInteger(numUpstreams);
        this.orderingColumnIndices = orderingColumnIndices;
    }

    @Override
    public boolean setNextRow(Row row) {
        throw new AssertionError("use setNextRow(upstreamId, inputPosition, row) instead");
    }

    public synchronized boolean setNextRow(int upstreamId, long inputPosition, Row row) {
        row = new RowN(Buckets.materialize(row));
        logger.trace("next row of upstream {}, pos {}, row {}", upstreamId, inputPosition, row);
        if (inputPosition == outputCursor) {
            if (!emitRow(row)) {
                return false;
            }
        } else {
            LinkedList<Row> bufferedRows = upstreamBuffers[upstreamId];
            if (bufferedRows == null) {
                bufferedRows = new LinkedList<>();
                upstreamBuffers[upstreamId] = bufferedRows;
                if (leastUpstreamBufferCursor == -1) {
                    leastUpstreamBufferCursor = inputPosition;
                }
            }
            bufferedRows.add(new OrderedRowDelegate(row, Arrays.asList(inputPosition)));
        }
        emitRows();
        return true;
    }

    private boolean emitRows() {
        boolean success = true;

        int leastUpstreamBufferId = findLeastUpstreamBufferId();
        logger.trace("found leastUpstreamBufferId {}, leastUpstreamBufferCursor {}, outputCursor {}",
                leastUpstreamBufferId, leastUpstreamBufferCursor, outputCursor);

        while (leastUpstreamBufferCursor == outputCursor
                && leastUpstreamBufferId != -1) {
            LinkedList<Row> rows = upstreamBuffers[leastUpstreamBufferId];
            success = emitRow(rows.poll());
            leastUpstreamBufferId = findLeastUpstreamBufferId();
            logger.trace("[in-loop] found leastUpstreamBufferId {}, leastUpstreamBufferCursor {}, outputCursor {}",
                    leastUpstreamBufferId, leastUpstreamBufferCursor, outputCursor);
        }

        return success;
    }

    private int findLeastUpstreamBufferId() {
        int leastUpstreamBufferId = -1;
        for (int i = 0; i < upstreamBuffers.length; i++) {
            LinkedList<Row> rows = upstreamBuffers[i];
            if (rows == null) {
                continue;
            }
            try {
                Row row = rows.getFirst();
                Long orderingValue = (Long)row.get(orderingColumnIndices[0]);
                logger.trace("row in buffer {} with position {}", i, orderingValue);
                if (Longs.compare(orderingValue, leastUpstreamBufferCursor) <= 0
                        || Longs.compare(orderingValue, outputCursor) == 0) {
                    leastUpstreamBufferCursor = orderingValue;
                    leastUpstreamBufferId = i;
                }
            } catch (NoSuchElementException e) {
                // continue
            }
        }
        return leastUpstreamBufferId;
    }

    private boolean emitRow(Row row) {
        logger.trace("emitting row {}", row);
        outputCursor++;
        return downstream.setNextRow(row);
    }


    @Override
    public void startProjection() {

    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        upstreamsRemaining.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            downstream.upstreamFinished();
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            downstream.upstreamFailed(throwable);
        }
    }

    static class OrderedRowDelegate implements Row {
        private final List<?> orderBy;
        private final Row delegate;

        public OrderedRowDelegate(Row delegate, List<?> orderBy) {
            this.delegate = delegate;
            this.orderBy = orderBy;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Object get(int index) {
            if (index >= size() && index < size() + orderBy.size()) {
                return orderBy.get(index - size());
            } else if (index < size()) {
                return delegate.get(index);
            } else {
                return null;
            }
        }
    }
}
