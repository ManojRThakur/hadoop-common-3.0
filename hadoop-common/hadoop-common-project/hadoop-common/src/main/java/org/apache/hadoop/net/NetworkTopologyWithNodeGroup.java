/**
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
package org.apache.hadoop.net;

import org.checkerframework.checker.tainting.qual.Tainted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The class extends NetworkTopology to represents a cluster of computer with
 *  a 4-layers hierarchical network topology.
 * In this network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers,
 * racks or physical host (with virtual switch).
 * 
 * @see NetworkTopology
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetworkTopologyWithNodeGroup extends @Tainted NetworkTopology {

  public final static @Tainted String DEFAULT_NODEGROUP = "/default-nodegroup";

  public @Tainted NetworkTopologyWithNodeGroup() {
    clusterMap = new @Tainted InnerNodeWithNodeGroup(InnerNode.ROOT);
  }

  @Override
  protected @Tainted Node getNodeForNetworkLocation(@Tainted NetworkTopologyWithNodeGroup this, @Tainted Node node) {
    // if node only with default rack info, here we need to add default
    // nodegroup info
    if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
      node.setNetworkLocation(node.getNetworkLocation()
          + DEFAULT_NODEGROUP);
    }
    @Tainted
    Node nodeGroup = getNode(node.getNetworkLocation());
    if (nodeGroup == null) {
      nodeGroup = new @Tainted InnerNodeWithNodeGroup(node.getNetworkLocation());
    }
    return getNode(nodeGroup.getNetworkLocation());
  }

  @Override
  public @Tainted String getRack(@Tainted NetworkTopologyWithNodeGroup this, @Tainted String loc) {
    netlock.readLock().lock();
    try {
      loc = InnerNode.normalize(loc);
      @Tainted
      Node locNode = getNode(loc);
      if (locNode instanceof @Tainted InnerNodeWithNodeGroup) {
        @Tainted
        InnerNodeWithNodeGroup node = (@Tainted InnerNodeWithNodeGroup) locNode;
        if (node.isRack()) {
          return loc;
        } else if (node.isNodeGroup()) {
          return node.getNetworkLocation();
        } else {
          // may be a data center
          return null;
        }
      } else {
        // not in cluster map, don't handle it
        return loc;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of a node group for a specific network
   * location
   * 
   * @param loc
   *            a path-like string representation of a network location
   * @return a node group string
   */
  public @Tainted String getNodeGroup(@Tainted NetworkTopologyWithNodeGroup this, @Tainted String loc) {
    netlock.readLock().lock();
    try {
      loc = InnerNode.normalize(loc);
      @Tainted
      Node locNode = getNode(loc);
      if (locNode instanceof @Tainted InnerNodeWithNodeGroup) {
        @Tainted
        InnerNodeWithNodeGroup node = (@Tainted InnerNodeWithNodeGroup) locNode;
        if (node.isNodeGroup()) {
          return loc;
        } else if (node.isRack()) {
          // not sure the node group for a rack
          return null;
        } else {
          // may be a leaf node
          return getNodeGroup(node.getNetworkLocation());
        }
      } else {
        // not in cluster map, don't handle it
        return loc;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  @Override
  public @Tainted boolean isOnSameRack( @Tainted NetworkTopologyWithNodeGroup this, @Tainted Node node1,  @Tainted Node node2) {
    if (node1 == null || node2 == null ||
        node1.getParent() == null || node2.getParent() == null) {
      return false;
    }
      
    netlock.readLock().lock();
    try {
      return isSameParents(node1.getParent(), node2.getParent());
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Check if two nodes are on the same node group (hypervisor) The
   * assumption here is: each nodes are leaf nodes.
   * 
   * @param node1
   *            one node (can be null)
   * @param node2
   *            another node (can be null)
   * @return true if node1 and node2 are on the same node group; false
   *         otherwise
   * @exception IllegalArgumentException
   *                when either node1 or node2 is null, or node1 or node2 do
   *                not belong to the cluster
   */
  @Override
  public @Tainted boolean isOnSameNodeGroup(@Tainted NetworkTopologyWithNodeGroup this, @Tainted Node node1, @Tainted Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
    netlock.readLock().lock();
    try {
      return isSameParents(node1, node2);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Check if network topology is aware of NodeGroup
   */
  @Override
  public @Tainted boolean isNodeGroupAware(@Tainted NetworkTopologyWithNodeGroup this) {
    return true;
  }

  /** Add a leaf node
   * Update node counter & rack counter if necessary
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave 
   *                                     or node to be added is not a leaf
   */
  @Override
  public void add(@Tainted NetworkTopologyWithNodeGroup this, @Tainted Node node) {
    if (node==null) return;
    if( node instanceof @Tainted InnerNode ) {
      throw new @Tainted IllegalArgumentException(
        "Not allow to add an inner node: "+NodeBase.getPath(node));
    }
    netlock.writeLock().lock();
    try {
      @Tainted
      Node rack = null;

      // if node only with default rack info, here we need to add default 
      // nodegroup info
      if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
        node.setNetworkLocation(node.getNetworkLocation() + 
            NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP);
      }
      @Tainted
      Node nodeGroup = getNode(node.getNetworkLocation());
      if (nodeGroup == null) {
        nodeGroup = new @Tainted InnerNodeWithNodeGroup(node.getNetworkLocation());
      }
      rack = getNode(nodeGroup.getNetworkLocation());

      // rack should be an innerNode and with parent.
      // note: rack's null parent case is: node's topology only has one layer, 
      //       so rack is recognized as "/" and no parent. 
      // This will be recognized as a node with fault topology.
      if (rack != null && 
          (!(rack instanceof @Tainted InnerNode) || rack.getParent() == null)) {
        throw new @Tainted IllegalArgumentException("Unexpected data node " 
            + node.toString() 
            + " at an illegal network location");
      }
      if (clusterMap.add(node)) {
        LOG.info("Adding a new node: " + NodeBase.getPath(node));
        if (rack == null) {
          // We only track rack number here
          numOfRacks++;
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n" + this.toString());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  /** Remove a node
   * Update node counter and rack counter if necessary
   * @param node node to be removed; can be null
   */
  @Override
  public void remove(@Tainted NetworkTopologyWithNodeGroup this, @Tainted Node node) {
    if (node==null) return;
    if( node instanceof @Tainted InnerNode ) {
      throw new @Tainted IllegalArgumentException(
          "Not allow to remove an inner node: "+NodeBase.getPath(node));
    }
    LOG.info("Removing a node: "+NodeBase.getPath(node));
    netlock.writeLock().lock();
    try {
      if (clusterMap.remove(node)) {
        @Tainted
        Node nodeGroup = getNode(node.getNetworkLocation());
        if (nodeGroup == null) {
          nodeGroup = new @Tainted InnerNode(node.getNetworkLocation());
        }
        @Tainted
        InnerNode rack = (@Tainted InnerNode)getNode(nodeGroup.getNetworkLocation());
        if (rack == null) {
          numOfRacks--;
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n" + this.toString());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  /** Sort nodes array by their distances to <i>reader</i>
   * It linearly scans the array, if a local node is found, swap it with
   * the first element of the array.
   * If a local node group node is found, swap it with the first element 
   * following the local node.
   * If a local rack node is found, swap it with the first element following
   * the local node group node.
   * If neither local node, node group node or local rack node is found, put a 
   * random replica location at position 0.
   * It leaves the rest nodes untouched.
   * @param reader the node that wishes to read a block from one of the nodes
   * @param nodes the list of nodes containing data for the reader
   */
  @Override
  public void pseudoSortByDistance( @Tainted NetworkTopologyWithNodeGroup this, @Tainted Node reader, @Tainted Node @Tainted [] nodes ) {

    if (reader != null && !this.contains(reader)) {
      // if reader is not a datanode (not in NetworkTopology tree), we will 
      // replace this reader with a sibling leaf node in tree.
      @Tainted
      Node nodeGroup = getNode(reader.getNetworkLocation());
      if (nodeGroup != null && nodeGroup instanceof @Tainted InnerNode) {
        @Tainted
        InnerNode parentNode = (@Tainted InnerNode) nodeGroup;
        // replace reader with the first children of its parent in tree
        reader = parentNode.getLeaf(0, null);
      } else {
        return;
      }
    }
    @Tainted
    int tempIndex = 0;
    @Tainted
    int localRackNode = -1;
    @Tainted
    int localNodeGroupNode = -1;
    if (reader != null) {  
      //scan the array to find the local node & local rack node
      for (@Tainted int i = 0; i < nodes.length; i++) {
        if (tempIndex == 0 && reader == nodes[i]) { //local node
          //swap the local node and the node at position 0
          if (i != 0) {
            swap(nodes, tempIndex, i);
          }
          tempIndex=1;

          if (localRackNode != -1 && (localNodeGroupNode !=-1)) {
            if (localRackNode == 0) {
              localRackNode = i;
            }
            if (localNodeGroupNode == 0) {
              localNodeGroupNode = i;
            }
            break;
          }
        } else if (localNodeGroupNode == -1 && isOnSameNodeGroup(reader, 
            nodes[i])) {
          //local node group
          localNodeGroupNode = i;
          // node local and rack local are already found
          if(tempIndex != 0 && localRackNode != -1) break;
        } else if (localRackNode == -1 && isOnSameRack(reader, nodes[i])) {
          localRackNode = i;
          if (tempIndex != 0 && localNodeGroupNode != -1) break;
        }
      }

      // swap the local nodegroup node and the node at position tempIndex
      if(localNodeGroupNode != -1 && localNodeGroupNode != tempIndex) {
        swap(nodes, tempIndex, localNodeGroupNode);
        if (localRackNode == tempIndex) {
          localRackNode = localNodeGroupNode;
        }
        tempIndex++;
      }

      // swap the local rack node and the node at position tempIndex
      if(localRackNode != -1 && localRackNode != tempIndex) {
        swap(nodes, tempIndex, localRackNode);
        tempIndex++;
      }
    }

    // put a random node at position 0 if there is not a local/local-nodegroup/
    // local-rack node
    if (tempIndex == 0 && localNodeGroupNode == -1 && localRackNode == -1
        && nodes.length != 0) {
      swap(nodes, 0, r.nextInt(nodes.length));
    }
  }

  /** InnerNodeWithNodeGroup represents a switch/router of a data center, rack
   * or physical host. Different from a leaf node, it has non-null children.
   */
  static class InnerNodeWithNodeGroup extends @Tainted InnerNode {
    public @Tainted InnerNodeWithNodeGroup(@Tainted String name, @Tainted String location, 
        @Tainted
        InnerNode parent, @Tainted int level) {
      super(name, location, parent, level);
    }

    public @Tainted InnerNodeWithNodeGroup(@Tainted String name, @Tainted String location) {
      super(name, location);
    }

    public @Tainted InnerNodeWithNodeGroup(@Tainted String path) {
      super(path);
    }

    @Override
    @Tainted
    boolean isRack(NetworkTopologyWithNodeGroup.@Tainted InnerNodeWithNodeGroup this) {
      // it is node group
      if (getChildren().isEmpty()) {
        return false;
      }

      @Tainted
      Node firstChild = children.get(0);

      if (firstChild instanceof @Tainted InnerNode) {
        @Tainted
        Node firstGrandChild = (((@Tainted InnerNode) firstChild).children).get(0);
        if (firstGrandChild instanceof @Tainted InnerNode) {
          // it is datacenter
          return false;
        } else {
          return true;
        }
      }
      return false;
    }

    /**
     * Judge if this node represents a node group
     * 
     * @return true if it has no child or its children are not InnerNodes
     */
    @Tainted
    boolean isNodeGroup(NetworkTopologyWithNodeGroup.@Tainted InnerNodeWithNodeGroup this) {
      if (children.isEmpty()) {
        return true;
      }
      @Tainted
      Node firstChild = children.get(0);
      if (firstChild instanceof @Tainted InnerNode) {
        // it is rack or datacenter
        return false;
      }
      return true;
    }
    
    @Override
    protected @Tainted boolean isLeafParent(NetworkTopologyWithNodeGroup.@Tainted InnerNodeWithNodeGroup this) {
      return isNodeGroup();
    }

    @Override
    protected @Tainted InnerNode createParentNode(NetworkTopologyWithNodeGroup.@Tainted InnerNodeWithNodeGroup this, @Tainted String parentName) {
      return new @Tainted InnerNodeWithNodeGroup(parentName, getPath(this), this,
          this.getLevel() + 1);
    }

    @Override
    protected @Tainted boolean areChildrenLeaves(NetworkTopologyWithNodeGroup.@Tainted InnerNodeWithNodeGroup this) {
      return isNodeGroup();
    }
  }
}
