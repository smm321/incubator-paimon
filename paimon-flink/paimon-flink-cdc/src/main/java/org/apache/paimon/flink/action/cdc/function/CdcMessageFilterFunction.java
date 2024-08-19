package org.apache.paimon.flink.action.cdc.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import java.util.Map;

public class CdcMessageFilterFunction implements FilterFunction<RichCdcMultiplexRecord> {

    private final Map<String, String> filter;

    public CdcMessageFilterFunction(Map<String, String> map) {
        this.filter = map;
    }

    @Override
    public boolean filter(RichCdcMultiplexRecord richCdcMultiplexRecord) throws Exception {
        if (filter.size() > 0) {
            return richCdcMultiplexRecord.tableName().equalsIgnoreCase(filter.get("filter_table")) &&
                    richCdcMultiplexRecord.databaseName().equalsIgnoreCase(filter.get("filter_db"));
        } else return true;
    }
}
