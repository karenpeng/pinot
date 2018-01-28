/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.rebalancer;

import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.utils.ReplicaGroupTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReplicaGroupRebalanceStrategyTest extends ControllerTest {
  private static final int MIN_NUM_REPLICAS = 3;
  private static final int NUM_BROKER_INSTANCES = 2;
  private static final int NUM_SERVER_INSTANCES = 6;
  private static final int INITIAL_NUM_SEGMENTS = 20;

  private static final String TABLE_NAME = "testReplicaRebalanceReplace";
  private final static String PARTITION_COLUMN = "memberId";
  private final static String OFFLINE_TENENT_NAME = "DefaultTenant_OFFLINE";
  private final static String NEW_SEGMENT_PREFIX = "new_segment_";

  private final TableConfig.Builder _offlineBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE);

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTableMinReplicas(MIN_NUM_REPLICAS);
    startController(config);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_BROKER_INSTANCES, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_SERVER_INSTANCES, true);

    _offlineBuilder.setTableName("testOfflineTable")
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5");

    setUpReplicaGroupTable();

    // Join 4 more servers as untagged
    String[] instanceNames = {"Server_localhost_a", "Server_localhost_b", "Server_localhost_c", "Server_localhost_d"};
    for (String instanceName : instanceNames) {
      ControllerRequestBuilderUtil.addFakeDataInstanceToAutoJoinHelixCluster(getHelixClusterName(),
          ZkStarter.DEFAULT_ZK_STR, instanceName, true);
      _helixAdmin.removeInstanceTag(getHelixClusterName(), instanceName, OFFLINE_TENENT_NAME);
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  @Test
  public void testReplicaGroupRebalancer() throws Exception {
    int targetNumInstancePerPartition = 3;
    int targetNumReplicaGroup = 2;

    // Test no server and configuration change
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Test replace
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_0", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Upload 10 more segments and validate the segment assignment
    addNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS + 10)) {
      Thread.sleep(100);
    }
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Test replace Again
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_0", OFFLINE_TENENT_NAME);
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Clean up new segments
    removeNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS)) {
      Thread.sleep(100);
    }

    // Test adding servers to each replica group
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_b", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_c", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_d", OFFLINE_TENENT_NAME);

    targetNumInstancePerPartition = 5;
    targetNumReplicaGroup = 2;
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Test removing servers to each replica group
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_d", OFFLINE_TENENT_NAME);
    targetNumInstancePerPartition = 4;
    targetNumReplicaGroup = 2;
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);

    // Upload 10 more segments and validate the segment assignment
    addNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS + 10)) {
      Thread.sleep(100);
    }
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Clean up new segments
    removeNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS)) {
      Thread.sleep(100);
    }

    // Test removing two more servers to each replica group with force run
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_b", OFFLINE_TENENT_NAME);
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_c", OFFLINE_TENENT_NAME);

    targetNumInstancePerPartition = 3;
    targetNumReplicaGroup = 2;
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, true);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Test adding a replica group
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_b", OFFLINE_TENENT_NAME);
    _helixAdmin.addInstanceTag(getHelixClusterName(), "Server_localhost_c", OFFLINE_TENENT_NAME);

    targetNumInstancePerPartition = 3;
    targetNumReplicaGroup = 3;
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Upload 10 more segments and validate the segment assignment
    addNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS + 10)) {
      Thread.sleep(100);
    }
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());

    // Clean up segments
    removeNewSegments();
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS)) {
      Thread.sleep(100);
    }

    // Test removing a replica group
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_a", OFFLINE_TENENT_NAME);
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_b", OFFLINE_TENENT_NAME);
    _helixAdmin.removeInstanceTag(getHelixClusterName(), "Server_localhost_c", OFFLINE_TENENT_NAME);

    targetNumInstancePerPartition = 3;
    targetNumReplicaGroup = 2;
    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE,
        targetNumInstancePerPartition, targetNumReplicaGroup, false, false);

    // Validate the segment assignment after rebalance
    Assert.assertTrue(validateTableLevelReplicaGroupRebalance());
  }

  private void addNewSegments() throws Exception {
    for (int i = 0; i < 10; i++) {
      ReplicaGroupTestUtils.uploadSingleSegmentWithPartitionNumber(TABLE_NAME, NEW_SEGMENT_PREFIX + i,
          PARTITION_COLUMN, _helixResourceManager);
    }
  }

  private void removeNewSegments() throws Exception {
    for (int i = 0; i < 10; i++) {
      _helixResourceManager.deleteSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
          NEW_SEGMENT_PREFIX + i);
    }
  }

  private boolean validateTableLevelReplicaGroupRebalance() {
    TableConfig tableConfig = _helixResourceManager.getTableConfig(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE);
    PartitionToReplicaGroupMappingZKMetadata replicaGroupMapping =
        ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(_propertyStore, TABLE_NAME);
    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(),
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME));
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    Map<Integer, Set<String>> segmentsPerPartition = new HashMap<>();
    segmentsPerPartition.put(0, segmentAssignment.keySet());
    return ReplicaGroupTestUtils.validateReplicaGroupSegmentAssignment(tableConfig, replicaGroupMapping,
        segmentAssignment, segmentsPerPartition);
  }

  private void setUpReplicaGroupTable() throws Exception {
    // Create the configuration for segment assignment strategy.
    int numInstancesPerPartition = 3;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(2)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();
    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Create the table and upload segments
    _helixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    // Upload segments
    ReplicaGroupTestUtils.uploadMultipleSegmentsWithPartitionNumber(TABLE_NAME, INITIAL_NUM_SEGMENTS, PARTITION_COLUMN,
        _helixResourceManager, 1);
    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME, INITIAL_NUM_SEGMENTS)) {
      Thread.sleep(100);
    }
  }

  private boolean allSegmentsPushedToIdealState(String tableName, int segmentNum) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    return idealState != null && idealState.getPartitionSet() != null
        && idealState.getPartitionSet().size() == segmentNum;
  }
}
