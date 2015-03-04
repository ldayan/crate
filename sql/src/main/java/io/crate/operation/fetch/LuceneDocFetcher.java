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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.DocCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;

import java.io.IOException;
import java.util.List;

public class LuceneDocFetcher implements ProjectorUpstream {

    private Projector downstream;
    private RamAccountingContext ramAccountingContext;

    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final JobCollectContext jobCollectContext;
    private final CrateSearchContext searchContext;
    private final int jobSearchContextId;
    private final boolean closeContext;

    public LuceneDocFetcher(List<LuceneCollectorExpression<?>> collectorExpressions,
                            Projector downstreamProjector,
                            JobCollectContext jobCollectContext,
                            CrateSearchContext searchContext,
                            int jobSearchContextId,
                            boolean closeContext) {
        assert validateExpressions(collectorExpressions) : "ChildDocCollectorExpression only supported here";
        downstream(downstreamProjector);
        this.collectorExpressions = collectorExpressions;
        this.jobCollectContext = jobCollectContext;
        this.searchContext = searchContext;
        this.jobSearchContextId = jobSearchContextId;
        this.closeContext = closeContext;
    }

    @Override
    public void downstream(Projector downstream) {
        downstream.registerUpstream(this);
        this.downstream = downstream;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    private void fetch(int doc) throws Exception {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop fetching because breaker limit was reached
            throw new UnexpectedFetchTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }
        int i = 0;
        Object[] newRow = new Object[collectorExpressions.size()];
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
            newRow[i++] = e.value();
        }
        if (!downstream.setNextRow(newRow)) {
            // no more rows required, we can stop here
            throw new FetchAbortedException();
        }

    }

    public void doFetch(RamAccountingContext ramAccountingContext) throws Exception {
        this.ramAccountingContext = ramAccountingContext;

        jobCollectContext.acquireContext(searchContext);

        CollectorContext collectorContext = new CollectorContext()
                .searchContext(searchContext);
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }

        try {
            for (int index = 0; index < searchContext.docIdsToLoadSize(); index++) {
                int docId = searchContext.docIdsToLoad()[searchContext.docIdsToLoadFrom() + index];
                int readerIndex = ReaderUtil.subIndex(docId, searchContext.searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = searchContext.searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = docId - subReaderContext.docBase;
                setNextReader(subReaderContext);
                fetch(subDoc);
            }
            downstream.upstreamFinished();
        } catch (FetchAbortedException e) {
            // yeah, that's ok! :)
            downstream.upstreamFinished();
        } catch (Exception e) {
            downstream.upstreamFailed(e);
            throw e;
        } finally {
            jobCollectContext.releaseContext(searchContext);
            if (closeContext) {
                jobCollectContext.closeContext(jobSearchContextId);
            }
        }
    }

    private boolean validateExpressions(List<LuceneCollectorExpression<?>> collectorExpressions) {
        for (LuceneCollectorExpression expression : collectorExpressions) {
            if (!(expression instanceof DocCollectorExpression.ChildDocCollectorExpression)) {
                return false;
            }
        }
        return true;
    }
}
