/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/** BankDebeziumRecordParser to support schemaless row data. */
public class BankDebeziumRecordParser extends DebeziumRecordParser {
    public BankDebeziumRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    protected Map<String, String> extractRowData(JsonNode record, RowType.Builder rowTypeBuilder) {
        if (!hasSchema) {
            return super.extractRowDataWithoutSchema(record, rowTypeBuilder);
        } else {
            return super.extractRowData(record, rowTypeBuilder);
        }
    }
}
