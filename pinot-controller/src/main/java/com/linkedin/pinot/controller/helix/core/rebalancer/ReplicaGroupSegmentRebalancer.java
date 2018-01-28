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
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;


/**
 * Segment assignmnet rebalancer for tables using replica group segment assignment strategy.
 *
 */
public class ReplicaGroupSegmentRebalancer implements RebalanceStrategy {
  private static final String ONLINE = "ONLINE";
  private boolean _forceRun;
  private List<String> _addedServers;
  private List<String> _removedServers;
  private ReplicaGroupRebalanceType _rebalanceType;
  private PartitionToReplicaGroupMappingZKMetadata _replicaGroupMapping;
  private ReplicaGroupStrategyConfig _replicaGroupConfig;

  // TODO: Remove if use the separate interface
  private String _resourceName;
  private List<String> _partitions;
  private LinkedHashMap<String, Integer> _states;
  private int _maximumPerNode;

  public ReplicaGroupSegmentRebalancer(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, PartitionToReplicaGroupMappingZKMetadata replicaGroupMapping,
      TableConfig tableConfig, List<String> addedServers, List<String> removedServers,
      ReplicaGroupRebalanceType rebalanceType, boolean forceRun) {
    init(resourceName, partitions, states, Integer.MAX_VALUE);
    _forceRun = forceRun;
    _addedServers = addedServers;
    _removedServers = removedServers;
    _rebalanceType = rebalanceType;
    _replicaGroupMapping = replicaGroupMapping;
    _replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();

    Collections.sort(_addedServers);
    Collections.sort(_removedServers);
  }

  @Override
  public void init(String resourceName, List<String> partitions, LinkedHashMap<String, Integer> states,
      int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _states = states;
    _maximumPerNode = maximumPerNode;
  }

  @Override
  public ZNRecord computePartitionAssignment(List<String> allNodes, List<String> liveNodes,
      Map<String, Map<String, String>> currentMapping, ClusterDataCache clusterData) {
    Map<String, Map<String, String>> newMapping = computeNewSegmentAssignmentMapping(currentMapping);
    ZNRecord znRecord = new ZNRecord(_resourceName);
    znRecord.setMapFields(newMapping);
    return znRecord;
  }

  /**
   * Compute the new segment assignment after rebalancing.
   *
   * @param currentMapping Current segment mapping
   * @return New segment mapping after rebalance
   */
  private Map<String, Map<String, String>> computeNewSegmentAssignmentMapping(
      Map<String, Map<String, String>> currentMapping) {
    Map<String, LinkedList<String>> serverToSegments = buildServerToSegmentMapping(currentMapping);
    // Preprocess the server to segments mapping before rebalancing based on the rebalance type.
    switch (_rebalanceType) {
      case REPLACE:
        // Assign segments from the removed server to the new one.
        for (int i = 0; i < _addedServers.size(); i++) {
          String oldServer = _removedServers.get(i);
          String newServer = _addedServers.get(i);
          serverToSegments.put(newServer, serverToSegments.get(oldServer));
          serverToSegments.remove(oldServer);
        }
        break;
      case ADD_SERVER:
      case ADD_REPLICA_GROUP:
        // Add servers to the mapping
        for (String server : _addedServers) {
          serverToSegments.put(server, new LinkedList<String>());
        }
        break;
      case REMOVE_SERVER:
      case REMOVE_REPLICA_GROUP:
        // Remove servers from the mapping
        for (String server : _removedServers) {
          serverToSegments.remove(server);
        }
        break;
    }

    // Check if rebalance can cause the downtime
    Set<String> segmentsToCover = currentMapping.keySet();
    Set<String> coveredSegments = new HashSet<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      if (!_removedServers.contains(entry.getKey())) {
        coveredSegments.addAll(entry.getValue());
      }
    }

    // In case of possible downtime, throw exception unless 'force run' flag is set
    if (!segmentsToCover.equals(coveredSegments) && !_forceRun) {
      throw new RuntimeException("Segment is lost, this rebalance will cause downtime of the segment.");
    }

    boolean mirrorAssignment = _replicaGroupConfig.getMirrorAssignmentAcrossReplicaGroups();
    int numPartitions = _replicaGroupMapping.getNumPartitions();
    int numReplicaGroups = _replicaGroupMapping.getNumReplicaGroups();

    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      List<String> referenceReplicaGroup = new ArrayList<>();
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        List<String> serversInReplicaGroup = _replicaGroupMapping.getInstancesfromReplicaGroup(partitionId, replicaId);
        if (replicaId == 0) {
          // We need to keep the first replica group in case of mirroring.
          referenceReplicaGroup.addAll(serversInReplicaGroup);
        } else if (mirrorAssignment) {
          // Copy the segment assignment from the reference replica group
          for (int i = 0; i < serversInReplicaGroup.size(); i++) {
            serverToSegments.put(serversInReplicaGroup.get(i), serverToSegments.get(referenceReplicaGroup.get(i)));
          }
          continue;
        }

        // For the case where we remove servers, we need to add the segments that the removed server contained.
        if (_rebalanceType == ReplicaGroupRebalanceType.REMOVE_SERVER) {
          Set<String> currentCoveredSegments = new HashSet<>();
          for (String server : serversInReplicaGroup) {
            currentCoveredSegments.addAll(serverToSegments.get(server));
          }
          Set<String> segmentsToAdd = new HashSet<>(currentMapping.keySet());
          segmentsToAdd.removeAll(currentCoveredSegments);
          String server = serversInReplicaGroup.iterator().next();
          serverToSegments.get(server).addAll(segmentsToAdd);
        }

        // Uniformly distribute the segments among servers in a replica group
        rebalanceReplicaGroup(serversInReplicaGroup, serverToSegments);
      }
    }
    return buildSegmentToServerMapping(serverToSegments);
  }

  /**
   * Uniformly distribute segments across servers in a replica group. It adopts a simple algorithm to keep moving
   * the segment from the server with maximum size to the one with minimum size until all servers are balanced.
   *
   * @param serversInReplicaGroup A list of servers within the same replica group
   * @param serverToSegments A Mapping of servers to their segments
   */
  private void rebalanceReplicaGroup(List<String> serversInReplicaGroup,
      Map<String, LinkedList<String>> serverToSegments) {
    while (!isBalanced(serversInReplicaGroup, serverToSegments)) {
      String minKey = serversInReplicaGroup.get(0);
      String maxKey = minKey;
      int minSize = serverToSegments.get(minKey).size();
      int maxSize = minSize;

      for (int i = 1; i < serversInReplicaGroup.size(); i++) {
        String server = serversInReplicaGroup.get(i);
        int currentSegmentSize = serverToSegments.get(server).size();
        if (minSize > currentSegmentSize) {
          minSize = currentSegmentSize;
          minKey = server;
        }
        if (maxSize < currentSegmentSize) {
          maxSize = currentSegmentSize;
          maxKey = server;
        }
      }

      if (!maxKey.equals(minKey)) {
        String segmentToMove = serverToSegments.get(maxKey).poll();
        serverToSegments.get(minKey).push(segmentToMove);
      }
    }
  }

  /**
   * Helper method to check if segments in a replica group is balanced
   *
   * @param serversInReplicaGroup A list of servers for a replica group
   * @param serverToSegments A mapping of server to its segments list
   * @return True if the servers are balanced. False otherwise
   */
  private boolean isBalanced(List<String> serversInReplicaGroup, Map<String, LinkedList<String>> serverToSegments) {
    Set<Integer> numSegments = new HashSet<>();
    for (String server : serversInReplicaGroup) {
      numSegments.add(serverToSegments.get(server).size());
    }
    if (numSegments.size() == 2) {
      Iterator<Integer> iter = numSegments.iterator();
      int first = iter.next();
      int second = iter.next();
      if (Math.abs(first - second) < 2) {
        return true;
      }
    } else if (numSegments.size() < 2) {
      return true;
    }
    return false;
  }

  private Map<String, LinkedList<String>> buildServerToSegmentMapping(
      Map<String, Map<String, String>> segmentToServerMapping) {
    Map<String, LinkedList<String>> serverToSegments = new HashMap<>();
    for (String segment : segmentToServerMapping.keySet()) {
      for (String server : segmentToServerMapping.get(segment).keySet()) {
        if (!serverToSegments.containsKey(server)) {
          serverToSegments.put(server, new LinkedList<String>());
        }
        serverToSegments.get(server).add(segment);
      }
    }
    return serverToSegments;
  }

  private Map<String, Map<String, String>> buildSegmentToServerMapping(
      Map<String, LinkedList<String>> serverToSegments) {
    Map<String, Map<String, String>> segmentsToServerMapping = new HashMap<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      String server = entry.getKey();
      for (String segment : entry.getValue()) {
        if (!segmentsToServerMapping.containsKey(segment)) {
          segmentsToServerMapping.put(segment, new HashMap<String, String>());
        }
        segmentsToServerMapping.get(segment).put(server, ONLINE);
      }
    }
    return segmentsToServerMapping;
  }
}
