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

package org.apache.hadoop.util;

import org.checkerframework.checker.tainting.qual.Tainted;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility to assist with generation of progress reports.  Applications build
 * a hierarchy of {@link Progress} instances, each modelling a phase of
 * execution.  The root is constructed with {@link #Progress()}.  Nodes for
 * sub-phases are created by calling {@link #addPhase()}.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class Progress {
  private static final @Tainted Log LOG = LogFactory.getLog(Progress.class);
  private @Tainted String status = "";
  private @Tainted float progress;
  private @Tainted int currentPhase;
  private @Tainted ArrayList<@Tainted Progress> phases = new @Tainted ArrayList<@Tainted Progress>();
  private @Tainted Progress parent;

  // Each phase can have different progress weightage. For example, in
  // Map Task, map phase accounts for 66.7% and sort phase for 33.3%.
  // User needs to give weightages as parameters to all phases(when adding
  // phases) in a Progress object, if he wants to give weightage to any of the
  // phases. So when nodes are added without specifying weightage, it means 
  // fixed weightage for all phases.
  private @Tainted boolean fixedWeightageForAllPhases = false;
  private @Tainted float progressPerPhase = 0.0f;
  private @Tainted ArrayList<@Tainted Float> progressWeightagesForPhases = new @Tainted ArrayList<@Tainted Float>();
  
  /** Creates a new root node. */
  public @Tainted Progress() {}

  /** Adds a named node to the tree. */
  public @Tainted Progress addPhase(@Tainted Progress this, @Tainted String status) {
    @Tainted
    Progress phase = addPhase();
    phase.setStatus(status);
    return phase;
  }

  /** Adds a node to the tree. Gives equal weightage to all phases */
  public synchronized @Tainted Progress addPhase(@Tainted Progress this) {
    @Tainted
    Progress phase = addNewPhase();
    // set equal weightage for all phases
    progressPerPhase = 1.0f / (@Tainted float)phases.size();
    fixedWeightageForAllPhases = true;
    return phase;
  }
  
  /** Adds a new phase. Caller needs to set progress weightage */
  private synchronized @Tainted Progress addNewPhase(@Tainted Progress this) {
    @Tainted
    Progress phase = new @Tainted Progress();
    phases.add(phase);
    phase.setParent(this);
    return phase;
  }

  /** Adds a named node with a specified progress weightage to the tree. */
  public @Tainted Progress addPhase(@Tainted Progress this, @Tainted String status, @Tainted float weightage) {
    @Tainted
    Progress phase = addPhase(weightage);
    phase.setStatus(status);

    return phase;
  }

  /** Adds a node with a specified progress weightage to the tree. */
  public synchronized @Tainted Progress addPhase(@Tainted Progress this, @Tainted float weightage) {
    @Tainted
    Progress phase = new @Tainted Progress();
    progressWeightagesForPhases.add(weightage);
    phases.add(phase);
    phase.setParent(this);

    // Ensure that the sum of weightages does not cross 1.0
    @Tainted
    float sum = 0;
    for (@Tainted int i = 0; i < phases.size(); i++) {
      sum += progressWeightagesForPhases.get(i);
    }
    if (sum > 1.0) {
      LOG.warn("Sum of weightages can not be more than 1.0; But sum = " + sum);
    }

    return phase;
  }

  /** Adds n nodes to the tree. Gives equal weightage to all phases */
  public synchronized void addPhases(@Tainted Progress this, @Tainted int n) {
    for (@Tainted int i = 0; i < n; i++) {
      addNewPhase();
    }
    // set equal weightage for all phases
    progressPerPhase = 1.0f / (@Tainted float)phases.size();
    fixedWeightageForAllPhases = true;
  }

  /**
   * returns progress weightage of the given phase
   * @param phaseNum the phase number of the phase(child node) for which we need
   *                 progress weightage
   * @return returns the progress weightage of the specified phase
   */
  @Tainted
  float getProgressWeightage(@Tainted Progress this, @Tainted int phaseNum) {
    if (fixedWeightageForAllPhases) {
      return progressPerPhase; // all phases are of equal weightage
    }
    return progressWeightagesForPhases.get(phaseNum);
  }

  synchronized @Tainted Progress getParent(@Tainted Progress this) { return parent; }
  synchronized void setParent(@Tainted Progress this, @Tainted Progress parent) { this.parent = parent; }
  
  /** Called during execution to move to the next phase at this level in the
   * tree. */
  public synchronized void startNextPhase(@Tainted Progress this) {
    currentPhase++;
  }

  /** Returns the current sub-node executing. */
  public synchronized @Tainted Progress phase(@Tainted Progress this) {
    return phases.get(currentPhase);
  }

  /** Completes this node, moving the parent node to its next child. */
  public void complete(@Tainted Progress this) {
    // we have to traverse up to our parent, so be careful about locking.
    @Tainted
    Progress myParent;
    synchronized(this) {
      progress = 1.0f;
      myParent = parent;
    }
    if (myParent != null) {
      // this will synchronize on the parent, so we make sure we release
      // our lock before getting the parent's, since we're traversing 
      // against the normal traversal direction used by get() or toString().
      // We don't need transactional semantics, so we're OK doing this. 
      myParent.startNextPhase();
    }
  }

  /** Called during execution on a leaf node to set its progress. */
  public synchronized void set(@Tainted Progress this, @Tainted float progress) {
    this.progress = progress;
  }

  /** Returns the overall progress of the root. */
  // this method probably does not need to be synchronized as getInternal() is
  // synchronized and the node's parent never changes. Still, it doesn't hurt. 
  public synchronized @Tainted float get(@Tainted Progress this) {
    @Tainted
    Progress node = this;
    while (node.getParent() != null) {                 // find the root
      node = parent;
    }
    return node.getInternal();
  }

  /**
   * Returns progress in this node. get() would give overall progress of the
   * root node(not just given current node).
   */
  public synchronized @Tainted float getProgress(@Tainted Progress this) {
    return getInternal();
  }
  
  /** Computes progress in this node. */
  private synchronized @Tainted float getInternal(@Tainted Progress this) {
    @Tainted
    int phaseCount = phases.size();
    if (phaseCount != 0) {
      @Tainted
      float subProgress = 0.0f;
      @Tainted
      float progressFromCurrentPhase = 0.0f;
      if (currentPhase < phaseCount) {
        subProgress = phase().getInternal();
        progressFromCurrentPhase =
          getProgressWeightage(currentPhase) * subProgress;
      }
      
      @Tainted
      float progressFromCompletedPhases = 0.0f;
      if (fixedWeightageForAllPhases) { // same progress weightage for each phase
        progressFromCompletedPhases = progressPerPhase * currentPhase;
      }
      else {
        for (@Tainted int i = 0; i < currentPhase; i++) {
          // progress weightages of phases could be different. Add them
          progressFromCompletedPhases += getProgressWeightage(i);
        }
      }
      return  progressFromCompletedPhases + progressFromCurrentPhase;
    } else {
      return progress;
    }
  }

  public synchronized void setStatus(@Tainted Progress this, @Tainted String status) {
    this.status = status;
  }

  @Override
  public @Tainted String toString(@Tainted Progress this) {
    @Tainted
    StringBuilder result = new @Tainted StringBuilder();
    toString(result);
    return result.toString();
  }

  private synchronized void toString(@Tainted Progress this, @Tainted StringBuilder buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase().toString(buffer);
    }
  }

}
