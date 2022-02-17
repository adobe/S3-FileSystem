/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package com.adobe.s3fs.metrics;

import com.adobe.s3fs.metrics.data.S3FsFilesCreatedMetricsSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;

public class S3FsFilesCreatedMetricsSourceTest {

  private static final String METRICS_DESCRIPTION = "Metrics about statuses of the files created";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=FilesCreated";

  private S3FsFilesCreatedMetricsSource metrics;

  @Before
  public void setUp() {
    metrics = S3FsFilesCreatedMetricsSource.getNewInstance();
    S3FsMetricsSystemPackagePrivateAccessor.register(
        METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, metrics);
  }

  @After
  public void clear() {
    S3FsMetricsSystemPackagePrivateAccessor.unregister(METRICS_JMX_CONTEXT);
    metrics = null;
  }

  private Set<ObjectName> getRegisteredMBeans() throws IOException {
    Set<ObjectName> mBeanNames = new HashSet<>();
    MBeanServerConnection beanConn = ManagementFactory.getPlatformMBeanServer();
    for (ObjectInstance instance : beanConn.queryMBeans(null, null)) {
      mBeanNames.add(instance.getObjectName());
    }
    return mBeanNames;
  }

  @Test
  public void testIsRegisteredToJmx() throws IOException, MalformedObjectNameException {
    metrics.incrCreatedFilesSuccessful();
    // Verify
    Set<ObjectName> registeredBeans = getRegisteredMBeans();
    ObjectName registeredName = new ObjectName("Hadoop:service=S3FS,name=" + METRICS_JMX_CONTEXT);
    Set<ObjectName> foundRegistered =
        registeredBeans.stream()
            .filter(objName -> objName.equals(registeredName))
            .collect(Collectors.toSet());
    await(5000L); // 5s
    Assert.assertEquals(1, foundRegistered.size());
  }

  private void verifyCounters(List<String> acceptedNames, long expected)
      throws MalformedObjectNameException, IntrospectionException, ReflectionException,
          InstanceNotFoundException, IOException {
    ObjectName registeredName = new ObjectName("Hadoop:service=S3FS,name=" + METRICS_JMX_CONTEXT);
    MBeanServerConnection beanConn = ManagementFactory.getPlatformMBeanServer();
    MBeanInfo info = beanConn.getMBeanInfo(registeredName);
    Assert.assertNotNull(info);
    MBeanAttributeInfo[] attrInfos = info.getAttributes();
    Map<String, MBeanAttributeInfo> name2AttrInfo = new LinkedHashMap<>();
    for (int idx = 0; idx < attrInfos.length; ++idx) {
      MBeanAttributeInfo attr = attrInfos[idx];
      if (!attr.isReadable()) {
        continue;
      }
      name2AttrInfo.put(attr.getName(), attr);
    }

    final AttributeList attributes;
    attributes =
        beanConn.getAttributes(registeredName, name2AttrInfo.keySet().toArray(new String[0]));

    for (Object attributeObj : attributes.asList()) {
      if (attributeObj instanceof Attribute) {
        Attribute attribute = (Attribute) (attributeObj);
        MBeanAttributeInfo attr = name2AttrInfo.get(attribute.getName());
        if (acceptedNames.contains(attr.getName())) {
          Assert.assertEquals(expected, (long) attribute.getValue());
        }
      }
    }
  }

  @Test
  public void testCounters()
      throws MalformedObjectNameException, IOException, InstanceNotFoundException,
          IntrospectionException, ReflectionException, InterruptedException {
    final int times = 3;
    for (int i = 0; i < times; i++) {
      metrics.incrCreatedFilesSuccessful();
    }
    List<String> acceptedNames = Collections.singletonList("CreatedFilesSuccessful");

    await(11000L); // 11s
    verifyCounters(acceptedNames, 3L);
  }

  private void await(long period) {
    long start = System.currentTimeMillis();
    while (true) {
      long diff = System.currentTimeMillis() - start;
      if (diff - period > 0L) {
        break;
      }
    }
  }
}
