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

package org.apache.paimon.flink.source;

/** Tests for file store sources with metrics. */
public class FileStoreSourceMetricsTest {
    //    private FileStoreTable table;
    //    private TestingSplitEnumeratorContextWithRegisteringGroup context;
    //    private MetricGroup scanMetricGroup;
    //
    //    @BeforeEach
    //    public void before(@TempDir java.nio.file.Path path) throws Exception {
    //        FileIO fileIO = LocalFileIO.create();
    //        Path tablePath = new Path(path.toString());
    //        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
    //        TableSchema tableSchema =
    //                schemaManager.createTable(
    //                        Schema.newBuilder()
    //                                .column("a", DataTypes.INT())
    //                                .column("b", DataTypes.BIGINT())
    //                                .build());
    //        table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    //        context = new TestingSplitEnumeratorContextWithRegisteringGroup(1);
    //        scanMetricGroup =
    //                context.metricGroup()
    //                        .addGroup("paimon")
    //                        .addGroup("table", table.name())
    //                        .addGroup("scan");
    //    }
    //
    //    @Test
    //    public void staticFileStoreSourceScanMetricsTest() throws Exception {
    //        writeOnce();
    //        StaticFileStoreSource staticFileStoreSource =
    //                new StaticFileStoreSource(
    //                        table.newReadBuilder(),
    //                        null,
    //                        1,
    //                        FlinkConnectorOptions.SplitAssignMode.FAIR);
    //        staticFileStoreSource.restoreEnumerator(context, null);
    //        assertThat(TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScannedManifests").getValue())
    //                .isEqualTo(1L);
    //        assertThat(
    //                        TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScanResultedTableFiles")
    //                                .getValue())
    //                .isEqualTo(1L);
    //    }
    //
    //    @Test
    //    public void continuousFileStoreSourceScanMetricsTest() throws Exception {
    //        writeOnce();
    //        ContinuousFileStoreSource continuousFileStoreSource =
    //                new ContinuousFileStoreSource(table.newReadBuilder(), table.options(), null);
    //        ContinuousFileSplitEnumerator enumerator =
    //                (ContinuousFileSplitEnumerator)
    //                        continuousFileStoreSource.restoreEnumerator(context, null);
    //        enumerator.scanNextSnapshot();
    //        assertThat(TestingMetricUtils.getHistogram(scanMetricGroup,
    // "scanDuration").getCount())
    //                .isEqualTo(1);
    //        assertThat(TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScannedManifests").getValue())
    //                .isEqualTo(1L);
    //        assertThat(
    //                        TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScanResultedTableFiles")
    //                                .getValue())
    //                .isEqualTo(1L);
    //
    //        writeAgain();
    //        enumerator.scanNextSnapshot();
    //        assertThat(TestingMetricUtils.getHistogram(scanMetricGroup,
    // "scanDuration").getCount())
    //                .isEqualTo(2);
    //        assertThat(TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScannedManifests").getValue())
    //                .isEqualTo(1L);
    //        assertThat(
    //                        TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScanResultedTableFiles")
    //                                .getValue())
    //                .isEqualTo(1L);
    //    }
    //
    //    @Test
    //    public void logHybridFileStoreSourceScanMetricsTest() throws Exception {
    //        writeOnce();
    //        FlinkSource logHybridFileStoreSource =
    //                LogHybridSourceFactory.buildHybridFirstSource(table, null, null);
    //        logHybridFileStoreSource.restoreEnumerator(context, null);
    //        assertThat(TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScannedManifests").getValue())
    //                .isEqualTo(1L);
    //        assertThat(
    //                        TestingMetricUtils.getGauge(scanMetricGroup,
    // "lastScanResultedTableFiles")
    //                                .getValue())
    //                .isEqualTo(1L);
    //    }
    //
    //    private void writeOnce() throws Exception {
    //        InnerTableWrite writer = table.newWrite("test");
    //        TableCommitImpl commit = table.newCommit("test");
    //        writer.write(GenericRow.of(1, 2L));
    //        writer.write(GenericRow.of(3, 4L));
    //        writer.write(GenericRow.of(5, 6L));
    //        writer.write(GenericRow.of(7, 8L));
    //        writer.write(GenericRow.of(9, 10L));
    //        commit.commit(writer.prepareCommit());
    //
    //        commit.close();
    //        writer.close();
    //    }
    //
    //    private void writeAgain() throws Exception {
    //        InnerTableWrite writer = table.newWrite("test");
    //        TableCommitImpl commit = table.newCommit("test");
    //        writer.write(GenericRow.of(10, 2L));
    //        writer.write(GenericRow.of(13, 24L));
    //        writer.write(GenericRow.of(15, 26L));
    //        writer.write(GenericRow.of(17, 28L));
    //        writer.write(GenericRow.of(19, 10L));
    //        commit.commit(writer.prepareCommit());
    //
    //        commit.close();
    //        writer.close();
    //    }

    //    private class TestingSplitEnumeratorContextWithRegisteringGroup
    //            extends TestingSplitEnumeratorContext<FileStoreSourceSplit> {
    //        private final SplitEnumeratorMetricGroup metricGroup;
    //
    //        public TestingSplitEnumeratorContextWithRegisteringGroup(int parallelism) {
    //            super(parallelism);
    //            final JobID jobId = new JobID();
    //            final JobVertexID jobVertexId = new JobVertexID();
    //            final OperatorID operatorId = new OperatorID();
    //            final MetricRegistry registry = TestingMetricRegistry.builder().build();
    //            JobManagerOperatorMetricGroup jmJobGroup =
    //                    JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost")
    //                            .addJob(jobId, "myJobName")
    //                            .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");
    //            InternalOperatorCoordinatorMetricGroup operatorCoordinatorMetricGroup =
    //                    new InternalOperatorCoordinatorMetricGroup(jmJobGroup);
    //            InternalSplitEnumeratorMetricGroup splitEnumeratorMetricGroup =
    //                    new InternalSplitEnumeratorMetricGroup(operatorCoordinatorMetricGroup);
    //            this.metricGroup = splitEnumeratorMetricGroup;
    //        }
    //
    //        @Override
    //        public SplitEnumeratorMetricGroup metricGroup() {
    //            return this.metricGroup;
    //        }
    //    }
}
