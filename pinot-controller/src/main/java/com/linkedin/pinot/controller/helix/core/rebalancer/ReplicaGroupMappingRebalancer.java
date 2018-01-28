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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;


/**
 * Rebalancer for the replica group mapping stored in the property store
 *
 */
public class ReplicaGroupMappingRebalancer {
  private List<String> _addedServers;
  private List<String> _removedServers;
  private int _targetNumReplicaGroup;
  private int _targetNumInstancesPerPartition;
  private int _oldNumReplicaGroup;
  private int _oldNumInstancesPerParititon;

  private ReplicaGroupRebalanceType _rebalanceType;
  private PartitionToReplicaGroupMappingZKMetadata _oldReplicaGroupMapping;

  public ReplicaGroupMappingRebalancer(int targetNumInstancesPerPartition, int targetNumReplicaGroup,
      TableConfig tableConfig, PartitionToReplicaGroupMappingZKMetadata oldReplicaGroupMapping,
      List<String> addedServers, List<String> removedServers) {
    _oldNumReplicaGroup = tableConfig.getValidationConfig().getReplicationNumber();
    _oldNumInstancesPerParititon =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig().getNumInstancesPerPartition();
    _oldReplicaGroupMapping = oldReplicaGroupMapping;
    _targetNumInstancesPerPartition = targetNumInstancesPerPartition;
    _targetNumReplicaGroup = targetNumReplicaGroup;
    _addedServers = addedServers;
    _removedServers = removedServers;
    _rebalanceType = computeRebalanceType();

    Collections.sort(_addedServers);
    Collections.sort(_removedServers);
  }

  /**
   * Compute the new replica group mapping based on the new configurations
   *
   * @return New server mapping for replica group
   */
  public PartitionToReplicaGroupMappingZKMetadata computeNewReplicaGroupMapping() {
    PartitionToReplicaGroupMappingZKMetadata newReplicaGroupMapping = new PartitionToReplicaGroupMappingZKMetadata();
    newReplicaGroupMapping.setTableName(_oldReplicaGroupMapping.getTableName());

    // In case of replacement, create the removed to added server mapping
    Map<String, String> oldToNewServerMapping = new HashMap<>();
    if (_addedServers.size() == _removedServers.size()) {
      for (int i = 0; i < _addedServers.size(); i++) {
        oldToNewServerMapping.put(_removedServers.get(i), _addedServers.get(i));
      }
    }

    Set<Integer> replicaSizeCheck = new HashSet<>();
    for (int partitionId = 0; partitionId < _oldReplicaGroupMapping.getNumPartitions(); partitionId++) {
      for (int groupId = 0; groupId < _oldNumReplicaGroup; groupId++) {
        List<String> oldReplicaGroup = _oldReplicaGroupMapping.getInstancesfromReplicaGroup(partitionId, groupId);
        List<String> newReplicaGroup = new ArrayList<>();
        boolean removeGroup = false;
        switch(_rebalanceType) {
          case REPLACE:
            // Swap the removed server with the added one.
            for (String oldServer : oldReplicaGroup) {
              if (!oldToNewServerMapping.containsKey(oldServer)) {
                newReplicaGroup.add(oldServer);
              } else {
                newReplicaGroup.add(oldToNewServerMapping.get(oldServer));
              }
            }
            break;
          case ADD_SERVER:
            newReplicaGroup.addAll(oldReplicaGroup);
            // Assign new servers to the replica group
            for (int serverIndex = 0; serverIndex < _addedServers.size(); serverIndex++) {
              if (serverIndex % _targetNumReplicaGroup == groupId) {
                newReplicaGroup.add(_addedServers.get(serverIndex));
              }
            }
            break;
          case REMOVE_SERVER:
            // Only add the servers that are not in the removed list
            for (String oldServer: oldReplicaGroup) {
              if (!_removedServers.contains(oldServer)) {
                newReplicaGroup.add(oldServer);
              }
            }
            break;
          case ADD_REPLICA_GROUP:
            // Add all servers for original replica groups and add new replica groups later
            newReplicaGroup.addAll(oldReplicaGroup);
            break;
          case REMOVE_REPLICA_GROUP:
            // Add
            newReplicaGroup.addAll(oldReplicaGroup);
            if (_removedServers.containsAll(oldReplicaGroup)) {
              removeGroup = true;
            }
            break;
          default:
            throw new UnsupportedOperationException("Not supported operation");
        }
        replicaSizeCheck.add(newReplicaGroup.size());
        if (!removeGroup){
          newReplicaGroupMapping.setInstancesToReplicaGroup(partitionId, groupId, newReplicaGroup);
        }
      }

      // Adding new replica groups if needed
      int index = 0;
      for (int newGroupId = _oldNumReplicaGroup; newGroupId < _targetNumReplicaGroup; newGroupId++) {
        List<String> newReplicaGroup = new ArrayList<>();
        while (newReplicaGroup.size() < _targetNumInstancesPerPartition) {
          newReplicaGroup.add(_addedServers.get(index));
          index++;
        }
        newReplicaGroupMapping.setInstancesToReplicaGroup(partitionId, newGroupId, newReplicaGroup);
      }
    }

    // Validate the result replica group mapping
    if (replicaSizeCheck.size() > 1) {
      throw new RuntimeException(
          "Each replica group needs to have exactly the same number of servers");
    }

    return newReplicaGroupMapping;
  }

  /**
   * Given the list of added/removed servers and the old and new replica group configurations, compute the
   * type of update (e.g. replace server, add servers to each replica group, add replica groups..)
   * @return the update type
   */
  private ReplicaGroupRebalanceType computeRebalanceType() {
    boolean sameNumInstancesPerPartition = _oldNumInstancesPerParititon == _targetNumInstancesPerPartition;
    boolean sameNumReplicaGroup = _oldNumReplicaGroup == _targetNumReplicaGroup;
    boolean isAddedServersSizeZero = _addedServers.size() == 0;
    boolean isRemovedServersSizeZero = _removedServers.size() == 0;

    if (sameNumInstancesPerPartition && sameNumReplicaGroup && _addedServers.size() == _removedServers.size()) {
      return ReplicaGroupRebalanceType.REPLACE;
    } else if (sameNumInstancesPerPartition) {
      if (_oldNumReplicaGroup < _targetNumReplicaGroup && !isAddedServersSizeZero && isRemovedServersSizeZero
          && _addedServers.size() % _targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.ADD_REPLICA_GROUP;
      } else if (_oldNumReplicaGroup > _targetNumReplicaGroup && isAddedServersSizeZero && !isRemovedServersSizeZero
          && _removedServers.size() % _targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.REMOVE_REPLICA_GROUP;
      }
    } else if (sameNumReplicaGroup) {
      if (_oldNumInstancesPerParititon < _targetNumInstancesPerPartition && isRemovedServersSizeZero
          && !isAddedServersSizeZero && _addedServers.size() % _targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.ADD_SERVER;
      } else if (_oldNumInstancesPerParititon > _targetNumInstancesPerPartition && !isRemovedServersSizeZero
          && isAddedServersSizeZero && _removedServers.size() % _targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.REMOVE_SERVER;
      }
    }
    return ReplicaGroupRebalanceType.UNSUPPORTED;
  }

  public ReplicaGroupRebalanceType getRebalanceType() {
    return _rebalanceType;
  }
}
