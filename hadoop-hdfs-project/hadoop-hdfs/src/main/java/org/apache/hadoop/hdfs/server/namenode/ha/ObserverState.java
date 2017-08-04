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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;

import java.io.IOException;

/**
 * Observer state of the namenode. In this state, the namenode service only
 * handle operations of type {@link org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory#READ}.
 * It also doesn't participate in either checkpointing or failover.
 */
@InterfaceAudience.Private
public class ObserverState extends StandbyState {
  @Override
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startObserverServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start observer services", e);
    }
  }

  @Override
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopObserverServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop observer services", e);
    }
  }

  @Override
  public void checkOperation(HAContext context, NameNode.OperationCategory op) throws StandbyException {
    if (op == NameNode.OperationCategory.UNCHECKED || op == NameNode.OperationCategory.READ) {
      return;
    }
    String faq = ". Visit https://s.apache.org/sbnn-error";
    String msg = "Operation category " + op + " is not supported in state "
        + context.getState() + faq;
    throw new StandbyException(msg);
  }

  @Override
  public String toString() {
    return "observer";
  }
}
