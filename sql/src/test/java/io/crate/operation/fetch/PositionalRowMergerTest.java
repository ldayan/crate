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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.projectors.CollectingProjector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PositionalRowMergerTest {

    @Test
    public void testConcurrentSetNextRow() throws Exception {
        int numUpstreams = 3;

        CollectingProjector resultProvider = new CollectingProjector();
        final PositionalRowMerger rowMerger = new PositionalRowMerger(resultProvider, numUpstreams, new int[]{1});

        final List<List<Object[]>> rowsPerUpstream = new ArrayList<>(numUpstreams);
        rowsPerUpstream.add(ImmutableList.of(new Object[]{0L}, new Object[]{2L}, new Object[]{6L}));
        rowsPerUpstream.add(ImmutableList.of(new Object[]{1L}, new Object[]{4L}, new Object[]{7L}));
        rowsPerUpstream.add(ImmutableList.of(new Object[]{3L}, new Object[]{5L}, new Object[]{8L}, new Object[]{9L}));

        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);
        for (int i = 0; i < numUpstreams; i++) {
            final int upstreamId = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<Object[]> rows = rowsPerUpstream.get(upstreamId);
                    for (Object[] row : rows) {
                        try {
                            rowMerger.setNextRow(upstreamId, (long) row[0], new RowN(row));
                        } catch (Exception e) {
                            fail(e.getMessage());
                        }
                    }
                    rowMerger.upstreamFinished();
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        final CountDownLatch latch1 = new CountDownLatch(1);
        Futures.addCallback(resultProvider.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                assertThat(result.size(), is(10));
                Iterator<Row> it = result.iterator();
                for (int i = 0; i < 10; i++) {
                    assertThat((long) it.next().get(0), is((long) i));
                }
                latch1.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.getMessage());
            }
        });
        latch1.await();
    }

}
