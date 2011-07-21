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
package com.cloudera.lib.server;

/**
 * Service interface for components to be managed by the {@link Server} class.
 */
public interface Service {

  /**
   * Initializes the service. This method is called once, when the
   * {@link Server} owning the service is being initialized.
   *
   * @param server the server initializing the service, give access to the
   * server context.
   * @throws ServiceException thrown if the service could not be initialized.
   */
  public void init(Server server) throws ServiceException;

  /**
   * Post initializes the service. This method is called by the
   * {@link Server} after all services of the server have been initialized.
   *
   * @throws ServiceException thrown if the service could not be
   * post-initialized.
   */
  public void postInit() throws ServiceException;

  /**
   * Destroy the services.  This method is called once, when the
   * {@link Server} owning the service is being destroyed.
   */
  public void destroy();

  /**
   * Returns the service dependencies of this service. The service will be
   * instantiated only if all the service dependencies are already initialized.
   *
   * @return the service dependencies.
   */
  public Class[] getServiceDependencies();

  /**
   * Returns the interface implemented by this service. This interface is used
   * the {@link Server} when the {@link Server#get(Class)} method is used to
   * retrieve a service.
   *
   * @return the interface that identifies the service.
   */
  public Class getInterface();

  /**
   * Notification callback when the server changes its status.
   *
   * @param oldStatus old server status.
   * @param newStatus new server status.
   * @throws ServiceException thrown if the service could not process the status change.
   */
  public void serverStatusChange(Server.Status oldStatus, Server.Status newStatus) throws ServiceException;

}
