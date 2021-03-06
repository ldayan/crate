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

package io.crate.operation.projectors.sorting;

import com.google.common.collect.Ordering;

import javax.annotation.Nullable;

public class OrderingByPosition extends Ordering<Object[]> {

    private final int position;
    private final Ordering<Comparable> ordering;

    public OrderingByPosition (int position, boolean reverse, Boolean nullFirst) {
        this.position = position;

        // note, that we are reverse for the queue so this conditional is by intent
        Ordering<Comparable> ordering;
        nullFirst = nullFirst != null ? !nullFirst : null; // swap because queue is reverse
        if (reverse) {
            ordering = Ordering.natural();
            if (nullFirst == null || !nullFirst) {
                ordering = ordering.nullsLast();
            } else {
                ordering = ordering.nullsFirst();
            }
        } else {
            ordering = Ordering.natural().reverse();
            if (nullFirst == null || nullFirst) {
                ordering = ordering.nullsFirst();
            } else {
                ordering = ordering.nullsLast();
            }
        }
        this.ordering = ordering;
    }

    @Override
    public int compare(@Nullable Object[] left, @Nullable Object[] right) {
        Comparable l = left != null ? (Comparable) left[position] : null;
        Comparable r = right != null ? (Comparable) right[position] : null;
        return ordering.compare(l, r);
    }
}
