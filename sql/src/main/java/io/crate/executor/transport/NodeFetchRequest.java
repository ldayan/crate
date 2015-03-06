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

package io.crate.executor.transport;

import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NodeFetchRequest extends TransportRequest {

    private UUID jobId;
    private List<Long> jobSearchContextDocIds;
    private List<Symbol> toFetchSymbols;
    private boolean closeContext = true;

    public NodeFetchRequest() {
    }

    public void jobId(UUID jobId) {
        this.jobId = jobId;
    }

    public UUID jobId() {
        return jobId;
    }

    public void jobSearchContextDocIds(List<Long> jobSearchContextDocIds) {
        this.jobSearchContextDocIds = jobSearchContextDocIds;
    }

    public List<Long> jobSearchContextDocIds() {
        return jobSearchContextDocIds;
    }

    public void toFetchSymbols(List<Symbol> toFetchSymbols) {
        this.toFetchSymbols = toFetchSymbols;
    }

    public List<Symbol> toFetchSymbols() {
        return toFetchSymbols;
    }

    public void closeContext(boolean closeContext) {
        this.closeContext = closeContext;
    }

    public boolean closeContext() {
        return closeContext;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        int listSize = in.readVInt();
        jobSearchContextDocIds = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            jobSearchContextDocIds.add(in.readVLong());
        }
        int symbolsSize = in.readVInt();
        toFetchSymbols = new ArrayList<>(symbolsSize);
        for (int i = 0; i < symbolsSize; i++) {
            toFetchSymbols.add(Symbol.fromStream(in));
        }
        closeContext = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(jobSearchContextDocIds.size());
        for (Long jobSearchContextDocId : jobSearchContextDocIds) {
            out.writeVLong(jobSearchContextDocId);
        }
        out.writeVInt(toFetchSymbols.size());
        for (Symbol symbol : toFetchSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeBoolean(closeContext);
    }

}
