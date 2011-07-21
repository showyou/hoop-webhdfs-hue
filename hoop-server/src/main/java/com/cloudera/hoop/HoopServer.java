/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.hoop;

import com.cloudera.lib.server.ServerException;
import com.cloudera.lib.service.Hadoop;
import com.cloudera.lib.servlet.ServerWebApp;
import com.cloudera.lib.util.XConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Bootstrap class that manages the initialization and destruction of the
 * Hoop server, it is a <code>javax.servlet.ServletContextListener</code>
 * implementation that is wired in Hoop's WAR <code>WEB-INF/web.xml</code>.
 * <p/>
 * It provides acces to the server context via the singleton {@link #get}.
 * <p/>
 * All the configuration is loaded from configuration properties prefixed
 * with <code>hoop.</code>.
 */
public class HoopServer extends ServerWebApp {
  private static final Logger LOG = LoggerFactory.getLogger(HoopServer.class);

  /**
   * Prefix for all configuration properties.
   */
  public static final String NAME = "hoop";

  /**
   * Configuration property that defines Hoop admin group.
   */
  public static final String CONF_ADMIN_GROUP = "admin.group";

  /**
   * Configuration property that defines Hoop's base URL.
   */
  public static final String CONF_BASE_URL = "base.url";

  private static HoopServer HOOP_SERVER;

  private String adminGroup;
  private String baseUrl;

  /**
   * Default constructor.
   * @throws IOException thrown if the home/conf/log/temp directory paths
   * could not be resolved.
   */
  public HoopServer() throws IOException {
    super("hoop");
  }

  /**
   * Constructor used for testing purposes.
   */
  protected HoopServer(String homeDir, String configDir, String logDir, String tempDir,
                       XConfiguration config) {
    super(NAME, homeDir, configDir, logDir, tempDir, config);
  }

  /**
   * Constructor used for testing purposes.
   */
  public HoopServer(String homeDir, XConfiguration config) {
    super(NAME, homeDir, config);
  }

  /**
   * Initializes the Hoop server, loads configuration and required services.
   *
   * @throws ServerException thrown if Hoop server could not be initialized.
   */
  @Override
  public void init() throws ServerException {
    super.init();
    if (HOOP_SERVER != null) {
      throw new RuntimeException("Hoop server already initialized");
    }
    HOOP_SERVER = this;
    adminGroup = getConfig().get(getPrefixedName(CONF_ADMIN_GROUP), "admin");
    baseUrl = getConfig().get(getPrefixedName(CONF_BASE_URL), "http://localhost:14000");
    LOG.info("Connects to Namenode [{}]",
             HoopServer.get().get(Hadoop.class).getDefaultConfiguration().get("fs.default.name"));
  }

  /**
   * Shutdowns all running services.
   */
  @Override
  public void destroy() {
    HOOP_SERVER = null;
    super.destroy();
  }

  /**
   * Returns Hoop server singleton, configuration and services are accessible through it.
   *
   * @return the Hoop server singleton.
   */
  public static HoopServer get() {
    return HOOP_SERVER;
  }

  /**
   * Returns Hoop admin group.
   *
   * @return Hoop admin group.
   */
  public String getAdminGroup() {
    return adminGroup;
  }

  /**
   * Returns Hoop base URL.
   *
   * @return Hoop base URL.
   */
  public String getBaseUrl() {
    return baseUrl;
  }

}
