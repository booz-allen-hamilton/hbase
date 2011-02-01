
/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link HLog}.
 */
public class WALCoprocessorHost
    extends CoprocessorHost<WALCoprocessorHost.HLogEnvironment> {

  @Override
  public HLogEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, Priority priority) {
    // TODO Auto-generated method stub
    return new HLogEnvironment(implClass, instance, priority);
  }

  private static final Log LOG = LogFactory.getLog(WALCoprocessorHost.class);

  /**
   * Encapsulation of the environment of each coprocessor
   */
  static class HLogEnvironment extends CoprocessorHost.Environment
      implements WALCoprocessorEnvironment {

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public HLogEnvironment(Class<?> implClass, final Coprocessor impl,
        Coprocessor.Priority priority) {
      super(impl, priority);
    }
  }

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public WALCoprocessorHost(final HLog log, final Configuration conf) {

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, WAL_COPROCESSOR_CONF_KEY);
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void preWALWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (WALCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof WALCPObserver) {
          ((WALCPObserver)env.getInstance()).preWALWrite(env, info, logKey, logEdit);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void postWALWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (WALCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof WALCPObserver) {
          ((WALCPObserver)env.getInstance()).postWALWrite(env, info, logKey, logEdit);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }
}
