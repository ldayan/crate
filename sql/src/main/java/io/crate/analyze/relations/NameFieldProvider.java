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

package io.crate.analyze.relations;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.Field;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

/**
 * Resolves QualifiedNames to Fields considering only one AnalyzedRelation
 *
 * The QualifiedNames must not contain a schema or a table.
 */
public class NameFieldProvider implements FieldProvider {

    private AnalyzedRelation relation;

    public NameFieldProvider(AnalyzedRelation relation) {
        this.relation = relation;
    }

    public Field resolveField(QualifiedName qualifiedName, boolean forWrite) {
        return resolveField(qualifiedName, null, forWrite);
    }

    public Field resolveField(QualifiedName qualifiedName, @Nullable List<String> path, boolean forWrite) {
        List<String> parts = qualifiedName.getParts();
        ColumnIdent columnIdent = new ColumnIdent(parts.get(parts.size() - 1), path);
        if(parts.size() != 1){
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Column reference \"%s\" has too many parts. " +
                    "A column must not have a schema or a table here.", qualifiedName));
        }

        Field field;
        if(forWrite){
            field = relation.getWritableField(columnIdent);
        } else {
            field = relation.getField(columnIdent);
        }
        if(field == null){
            throw new ColumnUnknownException(columnIdent.sqlFqn());
        }
        return field;
    }
}
