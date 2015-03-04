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

import com.google.common.collect.Ordering;
import io.crate.core.collections.Row;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MergeProjector implements Projector  {

    private final Input<?>[] inputs;
    private final Ordering<Object[]> ordering;
    private final int numOutputs;
    private final CollectExpression<?>[] collectExpressions;
    private ArrayList<RowDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    private RowDownstreamHandle downstreamContext;

    private AtomicInteger remainingUpstreams = new AtomicInteger(0);

    public MergeProjector(Input<?>[] inputs,
                          CollectExpression[] collectExpressions,
                          int numOutputs,
                          int[] orderBy,
                          boolean[] reverseFlags,
                          Boolean[] nullsFirst) {
        this.collectExpressions = collectExpressions;
        this.inputs = inputs;
        this.numOutputs = numOutputs;
        List<Comparator<Object[]>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(new OrderingByPosition(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    @Override
    public void startProjection() {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }

        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        RowDownstreamHandle context = new MergeProjectorContext(this);
        downstreamHandles.add(context);
        remainingUpstreams.incrementAndGet();
        return context;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        downstreamContext = downstream.registerUpstream(this);
    }

    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstreamContext != null) {
            Throwable throwable = upstreamFailure.get();
            if (throwable == null) {
                downstreamContext.finish();
            } else {
                downstreamContext.fail(throwable);
            }
        }
    }

    public void upstreamFailed(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstreamContext != null) {
            downstreamContext.fail(throwable);
        }
    }

    public boolean hasNextRow(MergeProjectorContext context, Row row) {
        return downstreamContext.setNextRow(row);
    }

    public class MergeProjectorContext implements RowDownstreamHandle {

        private final MergeProjector projector;

        private ArrayList<Row> rows;

        public MergeProjectorContext(MergeProjector projector) {
            this.projector = projector;
        }

        @Override
        public synchronized boolean setNextRow(Row row) {
            //rows.add(row);
            return projector.hasNextRow(this, row);
        }

        @Override
        public void finish() {
            projector.upstreamFinished();
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable);
        }
    }
}
