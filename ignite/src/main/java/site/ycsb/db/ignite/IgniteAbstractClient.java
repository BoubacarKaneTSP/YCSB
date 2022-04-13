/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */

package site.ycsb.db.ignite;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;

/**
 * Ignite abstract client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public abstract class IgniteAbstractClient extends DB {
  /** */
  protected static Logger log = LogManager.getLogger(IgniteAbstractClient.class);

  protected static final String DEFAULT_CACHE_NAME = "usertable";
  protected static final String HOSTS_PROPERTY = "hosts";
  protected static final String PORTS_PROPERTY = "ports";
  protected static final String PORTS_DEFAULTS = "47500..47509";

  /** Ignite cluster. */
  protected IgniteClient client = null;
  /** Ignite cache to store key-values. */
  protected ClientCache<String, BinaryObject> cache = null;
  /** Debug flag. */
  protected boolean debug = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Check if the cluster has already been initialized
    if (client != null) {
      throw new IllegalArgumentException("This should not happen.");
    }

    try {
      debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
      String host = getProperties().getProperty(HOSTS_PROPERTY);
      if (host == null) {
        throw new DBException(String.format(
            "Required property \"%s\" missing for Ignite Cluster",
            HOSTS_PROPERTY));
      }

      String ports = getProperties().getProperty(PORTS_PROPERTY, PORTS_DEFAULTS);
      if (ports == null) {
        throw new DBException(String.format(
            "Required property \"%s\" missing for Ignite Cluster",
            PORTS_PROPERTY));
      }
      Collection<String> addrs = new LinkedHashSet<>();
      addrs.add(host + ":" + ports);

      log.info("Start Ignite client.");
      ClientConfiguration clCfg = new ClientConfiguration();
      String hostPort = host + ":10800";
      clCfg.setAddresses(hostPort);
      client = Ignition.startClient(clCfg);
      cache = client.cache(DEFAULT_CACHE_NAME).withKeepBinary();

      if(cache == null) {
        throw new DBException(new IgniteCheckedException("Failed to find cache " + DEFAULT_CACHE_NAME));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    client = null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
