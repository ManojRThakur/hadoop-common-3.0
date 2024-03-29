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
import org.checkerframework.checker.tainting.qual.Untainted;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.ReflectionUtils;

/** The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled 
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.  
 * 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetworkTopology {
  public final static @Tainted String DEFAULT_RACK = "/default-rack";
  public final static @Tainted int DEFAULT_HOST_LEVEL = 2;
  public static final @Tainted Log LOG = 
    LogFactory.getLog(NetworkTopology.class);
    
  public static class InvalidTopologyException extends @Tainted RuntimeException {
    private static final @Tainted long serialVersionUID = 1L;
    public @Tainted InvalidTopologyException(@Tainted String msg) {
      super(msg);
    }
  }
  
  /**
   * Get an instance of NetworkTopology based on the value of the configuration
   * parameter net.topology.impl.
   * 
   * @param conf the configuration to be used
   * @return an instance of NetworkTopology
   */
  public static @Tainted NetworkTopology getInstance(@Tainted Configuration conf){
    return ReflectionUtils.newInstance(
        conf.getClass(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY,
        NetworkTopology.class, NetworkTopology.class), conf);
  }

  /** InnerNode represents a switch/router of a data center or rack.
   * Different from a leaf node, it has non-null children.
   */
  static class InnerNode extends @Tainted NodeBase {
    protected @Tainted List<@Tainted Node> children=new @Tainted ArrayList<@Tainted Node>();
    private @Tainted int numOfLeaves;
        
    /** Construct an InnerNode from a path-like string */
    @Tainted
    InnerNode(@Tainted String path) {
      super(path);
    }
        
    /** Construct an InnerNode from its name and its network location */
    @Tainted
    InnerNode(@Tainted String name, @Tainted String location) {
      super(name, location);
    }
        
    /** Construct an InnerNode
     * from its name, its network location, its parent, and its level */
    @Tainted
    InnerNode(@Tainted String name, @Tainted String location, @Tainted InnerNode parent, @Tainted int level) {
      super(name, location, parent, level);
    }
        
    /** @return its children */
    @Tainted
    List<@Tainted Node> getChildren(NetworkTopology.@Tainted InnerNode this) {return children;}
        
    /** @return the number of children this node has */
    @Tainted
    int getNumOfChildren(NetworkTopology.@Tainted InnerNode this) {
      return children.size();
    }
        
    /** Judge if this node represents a rack 
     * @return true if it has no child or its children are not InnerNodes
     */ 
    @Tainted
    boolean isRack(NetworkTopology.@Tainted InnerNode this) {
      if (children.isEmpty()) {
        return true;
      }
            
      @Tainted
      Node firstChild = children.get(0);
      if (firstChild instanceof @Tainted InnerNode) {
        return false;
      }
            
      return true;
    }
        
    /** Judge if this node is an ancestor of node <i>n</i>
     * 
     * @param n a node
     * @return true if this node is an ancestor of <i>n</i>
     */
    @Tainted
    boolean isAncestor(NetworkTopology.@Tainted InnerNode this, @Tainted Node n) {
      return getPath(this).equals(NodeBase.PATH_SEPARATOR_STR) ||
        (n.getNetworkLocation()+NodeBase.PATH_SEPARATOR_STR).
        startsWith(getPath(this)+NodeBase.PATH_SEPARATOR_STR);
    }
        
    /** Judge if this node is the parent of node <i>n</i>
     * 
     * @param n a node
     * @return true if this node is the parent of <i>n</i>
     */
    @Tainted
    boolean isParent(NetworkTopology.@Tainted InnerNode this, @Tainted Node n) {
      return n.getNetworkLocation().equals(getPath(this));
    }
        
    /* Return a child name of this node who is an ancestor of node <i>n</i> */
    private @Tainted String getNextAncestorName(NetworkTopology.@Tainted InnerNode this, @Tainted Node n) {
      if (!isAncestor(n)) {
        throw new @Tainted IllegalArgumentException(
                                           this + "is not an ancestor of " + n);
      }
      @Tainted
      String name = n.getNetworkLocation().substring(getPath(this).length());
      if (name.charAt(0) == PATH_SEPARATOR) {
        name = name.substring(1);
      }
      @Tainted
      int index=name.indexOf(PATH_SEPARATOR);
      if (index !=-1)
        name = name.substring(0, index);
      return name;
    }
        
    /** Add node <i>n</i> to the subtree of this node 
     * @param n node to be added
     * @return true if the node is added; false otherwise
     */
    @Tainted
    boolean add(NetworkTopology.@Tainted InnerNode this, @Tainted Node n) {
      if (!isAncestor(n))
        throw new @Tainted IllegalArgumentException(n.getName()+", which is located at "
                +n.getNetworkLocation()+", is not a decendent of "
                +getPath(this));
      if (isParent(n)) {
        // this node is the parent of n; add n directly
        n.setParent(this);
        n.setLevel(this.level+1);
        for(@Tainted int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.set(i, n);
            return false;
          }
        }
        children.add(n);
        numOfLeaves++;
        return true;
      } else {
        // find the next ancestor node
        @Tainted
        String parentName = getNextAncestorName(n);
        @Tainted
        InnerNode parentNode = null;
        for(@Tainted int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(parentName)) {
            parentNode = (@Tainted InnerNode)children.get(i);
            break;
          }
        }
        if (parentNode == null) {
          // create a new InnerNode
          parentNode = createParentNode(parentName);
          children.add(parentNode);
        }
        // add n to the subtree of the next ancestor node
        if (parentNode.add(n)) {
          numOfLeaves++;
          return true;
        } else {
          return false;
        }
      }
    }

    /**
     * Creates a parent node to be added to the list of children.  
     * Creates a node using the InnerNode four argument constructor specifying 
     * the name, location, parent, and level of this node.
     * 
     * <p>To be overridden in subclasses for specific InnerNode implementations,
     * as alternative to overriding the full {@link #add(Node)} method.
     * 
     * @param parentName The name of the parent node
     * @return A new inner node
     * @see InnerNode#InnerNode(String, String, InnerNode, int)
     */
    protected @Tainted InnerNode createParentNode(NetworkTopology.@Tainted InnerNode this, @Tainted String parentName) {
      return new @Tainted InnerNode(parentName, getPath(this), this, this.getLevel()+1);
    }

    /** Remove node <i>n</i> from the subtree of this node
     * @param n node to be deleted 
     * @return true if the node is deleted; false otherwise
     */
    @Tainted
    boolean remove(NetworkTopology.@Tainted InnerNode this, @Tainted Node n) {
      @Tainted
      String parent = n.getNetworkLocation();
      @Tainted
      String currentPath = getPath(this);
      if (!isAncestor(n))
        throw new @Tainted IllegalArgumentException(n.getName()
                                           +", which is located at "
                                           +parent+", is not a descendent of "+currentPath);
      if (isParent(n)) {
        // this node is the parent of n; remove n directly
        for(@Tainted int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.remove(i);
            numOfLeaves--;
            n.setParent(null);
            return true;
          }
        }
        return false;
      } else {
        // find the next ancestor node: the parent node
        @Tainted
        String parentName = getNextAncestorName(n);
        @Tainted
        InnerNode parentNode = null;
        @Tainted
        int i;
        for(i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(parentName)) {
            parentNode = (@Tainted InnerNode)children.get(i);
            break;
          }
        }
        if (parentNode==null) {
          return false;
        }
        // remove n from the parent node
        @Tainted
        boolean isRemoved = parentNode.remove(n);
        // if the parent node has no children, remove the parent node too
        if (isRemoved) {
          if (parentNode.getNumOfChildren() == 0) {
            children.remove(i);
          }
          numOfLeaves--;
        }
        return isRemoved;
      }
    } // end of remove
        
    /** Given a node's string representation, return a reference to the node
     * @param loc string location of the form /rack/node
     * @return null if the node is not found or the childnode is there but
     * not an instance of {@link InnerNode}
     */
    private @Tainted Node getLoc(NetworkTopology.@Tainted InnerNode this, @Tainted String loc) {
      if (loc == null || loc.length() == 0) return this;
            
      @Tainted
      String @Tainted [] path = loc.split(PATH_SEPARATOR_STR, 2);
      @Tainted
      Node childnode = null;
      for(@Tainted int i=0; i<children.size(); i++) {
        if (children.get(i).getName().equals(path[0])) {
          childnode = children.get(i);
        }
      }
      if (childnode == null) return null; // non-existing node
      if (path.length == 1) return childnode;
      if (childnode instanceof @Tainted InnerNode) {
        return ((@Tainted InnerNode)childnode).getLoc(path[1]);
      } else {
        return null;
      }
    }
        
    /** get <i>leafIndex</i> leaf of this subtree 
     * if it is not in the <i>excludedNode</i>
     *
     * @param leafIndex an indexed leaf of the node
     * @param excludedNode an excluded node (can be null)
     * @return
     */
    @Tainted
    Node getLeaf(NetworkTopology.@Tainted InnerNode this, @Tainted int leafIndex, @Tainted Node excludedNode) {
      @Tainted
      int count=0;
      // check if the excluded node a leaf
      @Tainted
      boolean isLeaf =
        excludedNode == null || !(excludedNode instanceof @Tainted InnerNode);
      // calculate the total number of excluded leaf nodes
      @Tainted
      int numOfExcludedLeaves =
        isLeaf ? 1 : ((@Tainted InnerNode)excludedNode).getNumOfLeaves();
      if (isLeafParent()) { // children are leaves
        if (isLeaf) { // excluded node is a leaf node
          @Tainted
          int excludedIndex = children.indexOf(excludedNode);
          if (excludedIndex != -1 && leafIndex >= 0) {
            // excluded node is one of the children so adjust the leaf index
            leafIndex = leafIndex>=excludedIndex ? leafIndex+1 : leafIndex;
          }
        }
        // range check
        if (leafIndex<0 || leafIndex>=this.getNumOfChildren()) {
          return null;
        }
        return children.get(leafIndex);
      } else {
        for(@Tainted int i=0; i<children.size(); i++) {
          @Tainted
          InnerNode child = (@Tainted InnerNode)children.get(i);
          if (excludedNode == null || excludedNode != child) {
            // not the excludedNode
            @Tainted
            int numOfLeaves = child.getNumOfLeaves();
            if (excludedNode != null && child.isAncestor(excludedNode)) {
              numOfLeaves -= numOfExcludedLeaves;
            }
            if (count+numOfLeaves > leafIndex) {
              // the leaf is in the child subtree
              return child.getLeaf(leafIndex-count, excludedNode);
            } else {
              // go to the next child
              count = count+numOfLeaves;
            }
          } else { // it is the excluededNode
            // skip it and set the excludedNode to be null
            excludedNode = null;
          }
        }
        return null;
      }
    }
    
    protected @Tainted boolean isLeafParent(NetworkTopology.@Tainted InnerNode this) {
      return isRack();
    }

    /**
      * Determine if children a leaves, default implementation calls {@link #isRack()}
      * <p>To be overridden in subclasses for specific InnerNode implementations,
      * as alternative to overriding the full {@link #getLeaf(int, Node)} method.
      * 
      * @return true if children are leaves, false otherwise
      */
    protected @Tainted boolean areChildrenLeaves(NetworkTopology.@Tainted InnerNode this) {
      return isRack();
    }

    /**
     * Get number of leaves.
     */
    @Tainted
    int getNumOfLeaves(NetworkTopology.@Tainted InnerNode this) {
      return numOfLeaves;
    }
  } // end of InnerNode

  /**
   * the root cluster map
   */
  @Tainted
  InnerNode clusterMap;
  /** Depth of all leaf nodes */
  private @Tainted int depthOfAllLeaves = -1;
  /** rack counter */
  protected @Tainted int numOfRacks = 0;
  /** the lock used to manage access */
  protected @Tainted ReadWriteLock netlock = new @Tainted ReentrantReadWriteLock();

  public @Tainted NetworkTopology() {
    clusterMap = new @Tainted InnerNode(InnerNode.ROOT);
  }

  /** Add a leaf node
   * Update node counter & rack counter if necessary
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave 
                                         or node to be added is not a leaf
   */
  public void add(@Tainted NetworkTopology this, @Tainted Node node) {
    if (node==null) return;
    @Tainted
    String oldTopoStr = this.toString();
    if( node instanceof @Tainted InnerNode ) {
      throw new @Tainted IllegalArgumentException(
        "Not allow to add an inner node: "+NodeBase.getPath(node));
    }
    @Tainted
    int newDepth = NodeBase.locationToDepth(node.getNetworkLocation()) + 1;
    netlock.writeLock().lock();
    try {
      if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth)) {
        LOG.error("Error: can't add leaf node " + NodeBase.getPath(node) +
            " at depth " + newDepth + " to topology:\n" + oldTopoStr);
        throw new @Tainted InvalidTopologyException("Failed to add " + NodeBase.getPath(node) +
            ": You cannot have a rack and a non-rack node at the same " +
            "level of the network topology.");
      }
      @Tainted
      Node rack = getNodeForNetworkLocation(node);
      if (rack != null && !(rack instanceof @Tainted InnerNode)) {
        throw new @Tainted IllegalArgumentException("Unexpected data node " 
                                           + node.toString() 
                                           + " at an illegal network location");
      }
      if (clusterMap.add(node)) {
        LOG.info("Adding a new node: "+NodeBase.getPath(node));
        if (rack == null) {
          numOfRacks++;
        }
        if (!(node instanceof @Tainted InnerNode)) {
          if (depthOfAllLeaves == -1) {
            depthOfAllLeaves = node.getLevel();
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n" + this.toString());
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }
  
  /**
   * Return a reference to the node given its string representation.
   * Default implementation delegates to {@link #getNode(String)}.
   * 
   * <p>To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full {@link #add(Node)}
   *  method.
   * 
   * @param node The string representation of this node's network location is
   * used to retrieve a Node object. 
   * @return a reference to the node; null if the node is not in the tree
   * 
   * @see #add(Node)
   * @see #getNode(String)
   */
  protected @Tainted Node getNodeForNetworkLocation(@Tainted NetworkTopology this, @Tainted Node node) {
    return getNode(node.getNetworkLocation());
  }
  
  /**
   * Given a string representation of a rack, return its children
   * @param loc a path-like string representation of a rack
   * @return a newly allocated list with all the node's children
   */
  public @Tainted List<@Tainted Node> getDatanodesInRack(@Tainted NetworkTopology this, @Tainted String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      if (!NodeBase.ROOT.equals(loc)) {
        loc = loc.substring(1);
      }
      @Tainted
      InnerNode rack = (@Tainted InnerNode) clusterMap.getLoc(loc);
      if (rack == null) {
        return null;
      }
      return new @Tainted ArrayList<@Tainted Node>(rack.getChildren());
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** Remove a node
   * Update node counter and rack counter if necessary
   * @param node node to be removed; can be null
   */ 
  public void remove(@Tainted NetworkTopology this, @Tainted Node node) {
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
        InnerNode rack = (@Tainted InnerNode)getNode(node.getNetworkLocation());
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

  /** Check if the tree contains node <i>node</i>
   * 
   * @param node a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  public @Tainted boolean contains(@Tainted NetworkTopology this, @Tainted Node node) {
    if (node == null) return false;
    netlock.readLock().lock();
    try {
      @Tainted
      Node parent = node.getParent();
      for (@Tainted int level = node.getLevel(); parent != null && level > 0;
           parent = parent.getParent(), level--) {
        if (parent == clusterMap) {
          return true;
        }
      }
    } finally {
      netlock.readLock().unlock();
    }
    return false; 
  }
    
  /** Given a string representation of a node, return its reference
   * 
   * @param loc
   *          a path-like string representation of a node
   * @return a reference to the node; null if the node is not in the tree
   */
  public @Tainted Node getNode(@Tainted NetworkTopology this, @Tainted String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      if (!NodeBase.ROOT.equals(loc))
        loc = loc.substring(1);
      return clusterMap.getLoc(loc);
    } finally {
      netlock.readLock().unlock();
    }
  }
  
  /** Given a string representation of a rack for a specific network
   *  location
   * 
   * To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full 
   * {@link #getRack(String)} method.
   * @param loc
   *          a path-like string representation of a network location
   * @return a rack string
   */
  public @Tainted String getRack(@Tainted NetworkTopology this, @Tainted String loc) {
    return loc;
  }
  
  /** @return the total number of racks */
  public @Tainted int getNumOfRacks(@Tainted NetworkTopology this) {
    netlock.readLock().lock();
    try {
      return numOfRacks;
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** @return the total number of leaf nodes */
  public @Tainted int getNumOfLeaves(@Tainted NetworkTopology this) {
    netlock.readLock().lock();
    try {
      return clusterMap.getNumOfLeaves();
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** Return the distance between two nodes
   * It is assumed that the distance from one node to its parent is 1
   * The distance between two nodes is calculated by summing up their distances
   * to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance between node1 and node2 which is zero if they are the same
   *  or {@link Integer#MAX_VALUE} if node1 or node2 do not belong to the cluster
   */
  public @Tainted int getDistance(@Tainted NetworkTopology this, @Tainted Node node1, @Tainted Node node2) {
    if (node1 == node2) {
      return 0;
    }
    @Tainted
    Node n1=node1, n2=node2;
    @Tainted
    int dis = 0;
    netlock.readLock().lock();
    try {
      @Tainted
      int level1=node1.getLevel(), level2=node2.getLevel();
      while(n1!=null && level1>level2) {
        n1 = n1.getParent();
        level1--;
        dis++;
      }
      while(n2!=null && level2>level1) {
        n2 = n2.getParent();
        level2--;
        dis++;
      }
      while(n1!=null && n2!=null && n1.getParent()!=n2.getParent()) {
        n1=n1.getParent();
        n2=n2.getParent();
        dis+=2;
      }
    } finally {
      netlock.readLock().unlock();
    }
    if (n1==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node1));
      return Integer.MAX_VALUE;
    }
    if (n2==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node2));
      return Integer.MAX_VALUE;
    }
    return dis+2;
  }

  /** Check if two nodes are on the same rack
   * @param node1 one node (can be null)
   * @param node2 another node (can be null)
   * @return true if node1 and node2 are on the same rack; false otherwise
   * @exception IllegalArgumentException when either node1 or node2 is null, or
   * node1 or node2 do not belong to the cluster
   */
  public @Tainted boolean isOnSameRack( @Tainted NetworkTopology this, @Tainted Node node1,  @Tainted Node node2) {
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
  public @Tainted boolean isNodeGroupAware(@Tainted NetworkTopology this) {
    return false;
  }
  
  /** 
   * Return false directly as not aware of NodeGroup, to be override in sub-class
   */
  public @Tainted boolean isOnSameNodeGroup(@Tainted NetworkTopology this, @Tainted Node node1, @Tainted Node node2) {
    return false;
  }

  /**
   * Compare the parents of each node for equality
   * 
   * <p>To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full 
   * {@link #isOnSameRack(Node, Node)} method.
   * 
   * @param node1 the first node to compare
   * @param node2 the second node to compare
   * @return true if their parents are equal, false otherwise
   * 
   * @see #isOnSameRack(Node, Node)
   */
  protected @Tainted boolean isSameParents(@Tainted NetworkTopology this, @Tainted Node node1, @Tainted Node node2) {
    return node1.getParent()==node2.getParent();
  }

  final protected static @Tainted Random r = new @Tainted Random();
  /** randomly choose one node from <i>scope</i>
   * if scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
   * @param scope range of nodes from which a node will be chosen
   * @return the chosen node
   */
  public @Tainted Node chooseRandom(@Tainted NetworkTopology this, @Tainted String scope) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return chooseRandom(NodeBase.ROOT, scope.substring(1));
      } else {
        return chooseRandom(scope, null);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  private @Tainted Node chooseRandom(@Tainted NetworkTopology this, @Tainted String scope, @Tainted String excludedScope){
    if (excludedScope != null) {
      if (scope.startsWith(excludedScope)) {
        return null;
      }
      if (!excludedScope.startsWith(scope)) {
        excludedScope = null;
      }
    }
    @Tainted
    Node node = getNode(scope);
    if (!(node instanceof @Tainted InnerNode)) {
      return node;
    }
    @Tainted
    InnerNode innerNode = (@Tainted InnerNode)node;
    @Tainted
    int numOfDatanodes = innerNode.getNumOfLeaves();
    if (excludedScope == null) {
      node = null;
    } else {
      node = getNode(excludedScope);
      if (!(node instanceof @Tainted InnerNode)) {
        numOfDatanodes -= 1;
      } else {
        numOfDatanodes -= ((@Tainted InnerNode)node).getNumOfLeaves();
      }
    }
    @Tainted
    int leaveIndex = r.nextInt(numOfDatanodes);
    return innerNode.getLeaf(leaveIndex, node);
  }

  /** return leaves in <i>scope</i>
   * @param scope a path string
   * @return leaves nodes under specific scope
   */
  public @Tainted List<@Tainted Node> getLeaves(@Tainted NetworkTopology this, @Tainted String scope) {
    @Tainted
    Node node = getNode(scope);
    @Tainted
    List<@Tainted Node> leafNodes = new @Tainted ArrayList<@Tainted Node>();
    if (!(node instanceof @Tainted InnerNode)) {
      leafNodes.add(node);
    } else {
      @Tainted
      InnerNode innerNode = (@Tainted InnerNode) node;
      for (@Tainted int i=0;i<innerNode.getNumOfLeaves();i++) {
        leafNodes.add(innerNode.getLeaf(i, null));
      }
    }
    return leafNodes;
  }

  /** return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
   * if scope starts with ~, return the number of nodes that are not
   * in <i>scope</i> and <i>excludedNodes</i>; 
   * @param scope a path string that may start with ~
   * @param excludedNodes a list of nodes
   * @return number of available nodes
   */
  public @Tainted int countNumOfAvailableNodes(@Tainted NetworkTopology this, @Tainted String scope,
                                      @Tainted
                                      Collection<@Tainted Node> excludedNodes) {
    @Tainted
    boolean isExcluded=false;
    if (scope.startsWith("~")) {
      isExcluded=true;
      scope=scope.substring(1);
    }
    scope = NodeBase.normalize(scope);
    @Tainted
    int count=0; // the number of nodes in both scope & excludedNodes
    netlock.readLock().lock();
    try {
      for(@Tainted Node node:excludedNodes) {
        if ((NodeBase.getPath(node)+NodeBase.PATH_SEPARATOR_STR).
            startsWith(scope+NodeBase.PATH_SEPARATOR_STR)) {
          count++;
        }
      }
      @Tainted
      Node n=getNode(scope);
      @Tainted
      int scopeNodeCount=1;
      if (n instanceof @Tainted InnerNode) {
        scopeNodeCount=((@Tainted InnerNode)n).getNumOfLeaves();
      }
      if (isExcluded) {
        return clusterMap.getNumOfLeaves()-
          scopeNodeCount-excludedNodes.size()+count;
      } else {
        return scopeNodeCount-count;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** convert a network tree to a string */
  @Override
  public @Tainted String toString(@Tainted NetworkTopology this) {
    // print the number of racks
    @Tainted
    StringBuilder tree = new @Tainted StringBuilder();
    tree.append("Number of racks: ");
    tree.append(numOfRacks);
    tree.append("\n");
    // print the number of leaves
    @Tainted
    int numOfLeaves = getNumOfLeaves();
    tree.append("Expected number of leaves:");
    tree.append(numOfLeaves);
    tree.append("\n");
    // print nodes
    for(@Tainted int i=0; i<numOfLeaves; i++) {
      tree.append(NodeBase.getPath(clusterMap.getLeaf(i, null)));
      tree.append("\n");
    }
    return tree.toString();
  }
  
  /**
   * Divide networklocation string into two parts by last separator, and get 
   * the first part here.
   * 
   * @param networkLocation
   * @return
   */
  public static @Tainted String getFirstHalf(@Tainted String networkLocation) {
    @Tainted
    int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
    return networkLocation.substring(0, index);
  }

  /**
   * Divide networklocation string into two parts by last separator, and get 
   * the second part here.
   * 
   * @param networkLocation
   * @return
   */
  public static @Tainted String getLastHalf(@Tainted String networkLocation) {
    @Tainted
    int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
    return networkLocation.substring(index);
  }

  /** swap two array items */
  static protected void swap(@Tainted Node @Tainted [] nodes, @Tainted int i, @Tainted int j) {
    @Tainted
    Node tempNode;
    tempNode = nodes[j];
    nodes[j] = nodes[i];
    nodes[i] = tempNode;
  }
  
  /** Sort nodes array by their distances to <i>reader</i>
   * It linearly scans the array, if a local node is found, swap it with
   * the first element of the array.
   * If a local rack node is found, swap it with the first element following
   * the local node.
   * If neither local node or local rack node is found, put a random replica
   * location at position 0.
   * It leaves the rest nodes untouched.
   * @param reader the node that wishes to read a block from one of the nodes
   * @param nodes the list of nodes containing data for the reader
   */
  public void pseudoSortByDistance( @Tainted NetworkTopology this, @Tainted Node reader, @Tainted Node @Tainted [] nodes ) {
    @Tainted
    int tempIndex = 0;
    @Tainted
    int localRackNode = -1;
    if (reader != null ) {
      //scan the array to find the local node & local rack node
      for(@Tainted int i=0; i<nodes.length; i++) {
        if(tempIndex == 0 && reader == nodes[i]) { //local node
          //swap the local node and the node at position 0
          if( i != 0 ) {
            swap(nodes, tempIndex, i);
          }
          tempIndex=1;
          if(localRackNode != -1 ) {
            if(localRackNode == 0) {
              localRackNode = i;
            }
            break;
          }
        } else if(localRackNode == -1 && isOnSameRack(reader, nodes[i])) {
          //local rack
          localRackNode = i;
          if(tempIndex != 0 ) break;
        }
      }

      // swap the local rack node and the node at position tempIndex
      if(localRackNode != -1 && localRackNode != tempIndex ) {
        swap(nodes, tempIndex, localRackNode);
        tempIndex++;
      }
    }
    
    // put a random node at position 0 if it is not a local/local-rack node
    if(tempIndex == 0 && localRackNode == -1 && nodes.length != 0) {
      swap(nodes, 0, r.nextInt(nodes.length));
    }
  }
  
}
