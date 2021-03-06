/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestContexts {

  private static final String RESOURCES_PATH = "/spark";

  @Before
  public void setup() {
    Contexts.closeSparkSession(true);
  }

  @Test
  public void testSparkPassthroughGood() {
    Config config = ConfigUtils.configFromPath(
      this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-good.conf").getPath());
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
    assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));
    assertTrue(sparkConf.contains("spark.master"));
    assertEquals("local[1]", sparkConf.get("spark.master"));
  }
  
  @Test
  public void testApplicationNameProvided() {
    Properties props = new Properties();
    props.setProperty("application.name", "test");
    Config config = ConfigFactory.parseProperties(props);
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertEquals(sparkConf.get("spark.app.name"), "test");
  }
  
  @Test
  public void testApplicationNameNotProvided() {
    Config config = ConfigFactory.empty();
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertEquals(sparkConf.get("spark.app.name"), "");
  }
  
  @Test
  public void testDefaultBatchConfiguration() {
    Config config = ConfigFactory.empty();
    Contexts.initialize(config, Contexts.ExecutionMode.BATCH);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertTrue(!sparkConf.contains("spark.dynamicAllocation.enabled"));
    assertTrue(!sparkConf.contains("spark.sql.shuffle.partitions"));
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "hive");
  }
  
  @Test
  public void testDefaultStreamingConfiguration() {
    Config config = ConfigFactory.empty();
    Contexts.initialize(config, Contexts.ExecutionMode.STREAMING);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertTrue(sparkConf.contains("spark.dynamicAllocation.enabled"));
    assertTrue(sparkConf.contains("spark.sql.shuffle.partitions"));
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "hive");
  }
  
  @Test
  public void testDefaultUnitTestConfiguration() {
    Config config = ConfigFactory.empty();
    Contexts.initialize(config, Contexts.ExecutionMode.UNIT_TEST);
    SparkConf sparkConf = Contexts.getSparkSession().sparkContext().getConf();
    assertEquals(sparkConf.get("spark.sql.catalogImplementation"), "in-memory");
    assertEquals(sparkConf.get("spark.sql.shuffle.partitions"), "1");
  }

  @Test (expected = AnalysisException.class)
  public void testHiveDisabledConfiguration() throws Exception {
    Map<String, Object> sparamMap = new HashMap<>();
    sparamMap.put(Contexts.SPARK_SESSION_ENABLE_HIVE_SUPPORT, "false");
    sparamMap.put(Contexts.SPARK_CONF_PROPERTY_PREFIX + "spark.sql.warehouse.dir",
        "target/spark-warehouse");
    Contexts.initialize(ConfigFactory.parseMap(sparamMap), Contexts.ExecutionMode.BATCH);
    Contexts.getSparkSession().sql("CREATE TABLE testHiveDisabled(d int)");
    try {
      Contexts.getSparkSession().sql("SELECT count(*) from testHiveDisabled");
    } finally {
      Contexts.getSparkSession().sql("DROP TABLE testHiveDisabled");
    }
  }

  @Test
  public void testHiveEnabledConfiguration() throws Exception {
    Map<String, Object> sparamMap = new HashMap<>();
    sparamMap.put(Contexts.SPARK_CONF_PROPERTY_PREFIX + "spark.sql.warehouse.dir",
        "target/spark-warehouse");
    Contexts.initialize(ConfigFactory.parseMap(sparamMap), Contexts.ExecutionMode.BATCH);
    Contexts.getSparkSession().sql("CREATE TABLE testHiveEnabled(d int)");
    Contexts.getSparkSession().sql("SELECT count(*) from testHiveEnabled");
    Contexts.getSparkSession().sql("DROP TABLE testHiveEnabled");

    sparamMap.put(Contexts.SPARK_SESSION_ENABLE_HIVE_SUPPORT, "true");
    Contexts.initialize(ConfigFactory.parseMap(sparamMap), Contexts.ExecutionMode.BATCH);
    Contexts.getSparkSession().sql("CREATE TABLE testHiveEnabled(d int)");
    Contexts.getSparkSession().sql("SELECT count(*) from testHiveEnabled");
    Contexts.getSparkSession().sql("DROP TABLE testHiveEnabled");
  }
}