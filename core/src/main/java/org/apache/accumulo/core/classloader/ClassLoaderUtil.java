/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.classloader;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassLoaderUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderUtil.class);
  private static ContextClassLoaderFactory FACTORY;

  private ClassLoaderUtil() {
    // cannot construct; static utilities only
  }

  /**
   * Initialize the ContextClassLoaderFactory
   */
  public static synchronized void initContextFactory(AccumuloConfiguration conf) {
    if (FACTORY == null) {
      LOG.debug("Creating {}", ContextClassLoaderFactory.class.getName());
      String factoryName = conf.get(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY);
      if (factoryName == null || factoryName.isEmpty()) {
        // load the default implementation
        LOG.info("Using default {}, which is subject to change in a future release",
            ContextClassLoaderFactory.class.getName());
        FACTORY = new URLContextClassLoaderFactory();
      } else {
        // load user's selected implementation and provide it with the service environment
        try {
          var factoryClass = Class.forName(factoryName).asSubclass(ContextClassLoaderFactory.class);
          LOG.info("Creating {}: {}", ContextClassLoaderFactory.class.getName(), factoryName);
          FACTORY = factoryClass.getDeclaredConstructor().newInstance();
          FACTORY.init(() -> new ConfigurationImpl(conf));
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException("Unable to load and initialize class: " + factoryName, e);
        }
      }
    } else {
      LOG.debug("{} already initialized with {}.", ContextClassLoaderFactory.class.getName(),
          FACTORY.getClass().getName());
    }
  }

  // for testing
  static ContextClassLoaderFactory getContextFactory() {
    return FACTORY;
  }

  // for testing
  public static synchronized void resetContextFactoryForTests() {
    FACTORY = null;
  }

  public static ClassLoader getClassLoader() throws ContextClassLoaderException {
    return getClassLoader(null);
  }

  public static ClassLoader getClassLoader(String context) throws ContextClassLoaderException {
    if (context != null && !context.isEmpty()) {
      return FACTORY.getClassLoader(context);
    } else {
      return ClassLoader.getSystemClassLoader();
    }
  }

  public static boolean isValidContext(String context) {
    if (context != null && !context.isEmpty()) {
      try {
        var loader = FACTORY.getClassLoader(context);
        if (loader == null) {
          LOG.debug("Context {} resulted in a null classloader from {}.", context,
              FACTORY.getClass().getName());
          return false;
        }
        return true;
      } catch (ContextClassLoaderException e) {
        LOG.debug("Context {} is not valid.", context, e);
        return false;
      }
    } else {
      return true;
    }
  }

  public static <U> Class<? extends U> loadClass(String context, String className,
      Class<U> extension) throws ClassNotFoundException {
    try {
      return getClassLoader(context).loadClass(className).asSubclass(extension);
    } catch (ContextClassLoaderException e) {
      throw new ClassNotFoundException("Error loading class from context: " + context, e);
    }
  }

  public static <U> Class<? extends U> loadClass(String className, Class<U> extension)
      throws ClassNotFoundException {
    return loadClass(null, className, extension);
  }

  /**
   * Retrieve the classloader context from a table's configuration.
   */
  public static String tableContext(AccumuloConfiguration conf) {
    return conf.get(Property.TABLE_CLASSLOADER_CONTEXT);
  }

}
