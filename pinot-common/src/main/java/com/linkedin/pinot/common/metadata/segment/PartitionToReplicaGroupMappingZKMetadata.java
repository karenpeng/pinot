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
package com.linkedin.pinot.common.metadata.segment;

import com.linkedin.pinot.common.metadata.ZKMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.helix.ZNRecord;

/**
 * Class for the partition mapping table. The table maps a tuple of (partition number, replica group number) to
 * a list of instances.
 *
 */
public class PartitionToReplicaGroupMappingZKMetadata implements ZKMetadata {
  private static final String SEPARATOR = "_";

  private Map<String, List<String>> _partitionToReplicaGroupMapping;
  private String _tableName;

  public PartitionToReplicaGroupMappingZKMetadata(ZNRecord znRecord) {
    _partitionToReplicaGroupMapping = znRecord.getListFields();
    _tableName = znRecord.getId();
  }

  public PartitionToReplicaGroupMappingZKMetadata() {
    _partitionToReplicaGroupMapping = new HashMap<>();
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  /**
   * Compute total number of partitions in the mapping.
   *
   * @return the number of partitions
   */
  public int getNumPartitions() {
    Set<String> partitions = new HashSet<>();
    for (String key : _partitionToReplicaGroupMapping.keySet()) {
      String partition = key.split(SEPARATOR)[0];
      partitions.add(partition);
    }
    return partitions.size();
  }

  /**
   * Compute total number of replica groups in the mapping.
   *
   * @return the number of replica groups
   */
  public int getNumReplicaGroups() {
    Set<String> partitions = new HashSet<>();
    for (String key : _partitionToReplicaGroupMapping.keySet()) {
      String partition = key.split(SEPARATOR)[1];
      partitions.add(partition);
    }
    return partitions.size();
  }

  /**
   * Add an instance to a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @param instanceName Name of an instance
   */
  public void addInstanceToReplicaGroup(int partition, int replicaGroup, String instanceName) {
    String key = createMappingKey(partition, replicaGroup);
    if (!_partitionToReplicaGroupMapping.containsKey(key)) {
      _partitionToReplicaGroupMapping.put(key, new ArrayList<String>());
    }
    _partitionToReplicaGroupMapping.get(key).add(instanceName);
  }

  /**
   * Get instances of a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @return List of instances belongs to the given partition and replica group
   */
  public List<String> getInstancesfromReplicaGroup(int partition, int replicaGroup) {
    String key = createMappingKey(partition, replicaGroup);
    if (!_partitionToReplicaGroupMapping.containsKey(key)) {
      throw new NoSuchElementException();
    }
    return _partitionToReplicaGroupMapping.get(key);
  }

  /**
   * Set instances of a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @param instances Instances that belongs to the gibn partition and replica group
   */
  public void setInstancesToReplicaGroup(int partition, int replicaGroup, List<String> instances) {
    String key = createMappingKey(partition, replicaGroup);
    _partitionToReplicaGroupMapping.put(key, instances);
  }


  /**
   * Get all instances.
   *
   * @return Set of all instances for this table
   */
  public List<String> getAllInstances() {
    Set<String> serverList = new HashSet<>();
    for(List<String> servers: _partitionToReplicaGroupMapping.values()) {
      serverList.addAll(servers);
    }
    return new ArrayList<>(serverList);
  }

  /**
   * Convert the partition mapping table to ZNRecord.
   *
   * @return ZNRecord of the partition to replica group mapping table
   */
  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableName);
    znRecord.setListFields(_partitionToReplicaGroupMapping);
    return znRecord;
  }

  /**
   * Helper method to create a key for the partition mapping table.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @return Key for the partition mapping table
   */
  private String createMappingKey(int partition, int replicaGroup) {
    return partition + SEPARATOR + replicaGroup;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionToReplicaGroupMappingZKMetadata that = (PartitionToReplicaGroupMappingZKMetadata) o;
    return _partitionToReplicaGroupMapping.equals(that._partitionToReplicaGroupMapping);
  }

  @Override
  public int hashCode() {
    return _partitionToReplicaGroupMapping != null ? _partitionToReplicaGroupMapping.hashCode() : 0;
  }
}
