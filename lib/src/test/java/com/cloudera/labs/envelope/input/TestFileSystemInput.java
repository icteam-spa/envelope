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
package com.cloudera.labs.envelope.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.cloudera.labs.envelope.input.translate.DummyInputFormatTranslator;
import com.cloudera.labs.envelope.input.translate.KVPTranslator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 */
public class TestFileSystemInput {

  private static final String CSV_DATA = "/filesystem/sample-fs.csv";
  private static final String JSON_DATA = "/filesystem/sample-fs.json";
  private static final String TEXT_DATA = "/filesystem/sample-fs.txt";
  private static final String XML_DATA = "/filesystem/sample-fs.xml";
  private static final String AVRO_SCHEMA = "/filesystem/sample-fs.avsc";

  private Config config;

  @Test (expected = RuntimeException.class)
  public void missingFormat() throws Exception {
    config = ConfigFactory.parseString(FileSystemInput.FORMAT_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void invalidFormat() throws Exception {
    config = ConfigFactory.parseString(FileSystemInput.FORMAT_CONFIG + ": WILLGOBOOM").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingPath() throws Exception {
    config = ConfigFactory.parseString(FileSystemInput.PATH_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void multipleSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.AVRO_FILE_CONFIG, "foo");
    paramMap.put(FileSystemInput.FIELD_NAMES_CONFIG, "foo");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingFieldNames() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.FIELD_TYPES_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingFieldTypes() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.FIELD_NAMES_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void multipleAvroSchemas() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.AVRO_FILE_CONFIG, "foo");
    paramMap.put(FileSystemInput.AVRO_LITERAL_CONFIG, "foo");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingAvroLiteral() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.AVRO_LITERAL_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingAvroFile() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.AVRO_FILE_CONFIG, "");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test
  public void readCsvNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("Four", first.getString(3));
  }

  @Test
  public void readCsvWithOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
  }

  @Test
  public void readCsvWithFieldsSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    paramMap.put(FileSystemInput.FIELD_NAMES_CONFIG, Lists.newArrayList("A Long", "An Int", "A String",
        "Another String"));
    paramMap.put(FileSystemInput.FIELD_TYPES_CONFIG, Lists.newArrayList("long", "int", "string", "string"));
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
    assertEquals("Another String", first.schema().fields()[3].name());
    assertEquals(1L, first.get(0));
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readCsvWithAvroSchema() throws Exception {
    StringBuilder avroLiteral = new StringBuilder()
      .append("{ \"type\" : \"record\", \"name\" : \"example\", \"fields\" : [")
      .append("{ \"name\" : \"A_Long\", \"type\" : \"long\" },")
      .append("{ \"name\" : \"An_Int\", \"type\" : \"int\" },")
      .append("{ \"name\" : \"A_String\", \"type\" : \"string\" },")
      .append("{ \"name\" : \"Another_String\", \"type\" : \"string\" }")
      .append("] }");

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    paramMap.put(FileSystemInput.AVRO_LITERAL_CONFIG, avroLiteral.toString());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
    assertEquals("Another_String", first.schema().fields()[3].name());
    assertEquals(1L, first.get(0));
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readJsonNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "json");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(JSON_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("dog", first.getString(3));
    assertEquals("field1", first.schema().fields()[0].name());
    assertEquals(DataTypes.LongType, first.schema().fields()[0].dataType());
  }

  @Test
  public void readJsonWithSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "json");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(JSON_DATA).getPath());
    paramMap.put(FileSystemInput.FIELD_NAMES_CONFIG, Lists.newArrayList("field1", "field2", "field3", "field4"));
    paramMap.put(FileSystemInput.FIELD_TYPES_CONFIG, Lists.newArrayList("int", "string", "boolean", "string"));
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    dataFrame.printSchema();
    dataFrame.show();

    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("dog", first.getString(3));
    assertEquals("field1", first.schema().fields()[0].name());
    assertEquals(DataTypes.IntegerType, first.schema().fields()[0].dataType());
  }

  @Test (expected = RuntimeException.class)
  public void readInputFormatMissingInputFormat() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void readInputFormatMissingKey() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void readInputFormatMissingValue() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_KEY_CONFIG, LongWritable.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void readInputFormatMissingTranslator() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_KEY_CONFIG, LongWritable.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_VALUE_CONFIG, Text.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
  }

  @Test (expected = SparkException.class)
  public void readInputFormatMismatchTranslator() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, KeyValueTextInputFormat.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_KEY_CONFIG, Text.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_VALUE_CONFIG, Text.class.getCanonicalName());
    paramMap.put("translator.type", DummyInputFormatTranslator.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
    formatInput.read().show();
  }

  @Test
  public void readInputFormat() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "input-format");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.INPUT_FORMAT_TYPE_CONFIG, TextInputFormat.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_KEY_CONFIG, LongWritable.class.getCanonicalName());
    paramMap.put(FileSystemInput.INPUT_FORMAT_VALUE_CONFIG, Text.class.getCanonicalName());
    paramMap.put("translator.type", DummyInputFormatTranslator.class.getCanonicalName());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);

    Dataset<Row> results = formatInput.read();

    assertEquals("Invalid number of rows", 4, results.count());
    assertEquals("Invalid first row result", 0L, results.first().getLong(0));
    assertEquals("Invalid first row result", "One,Two,Three,Four", results.first().getString(1));
  }
  
  @Test
  public void readTextWithoutTranslator() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(FileSystemInput.FORMAT_CONFIG, FileSystemInput.TEXT_FORMAT);
    configMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(TEXT_DATA).getPath());
    config = ConfigFactory.parseMap(configMap);
    
    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
    
    Dataset<Row> results = formatInput.read();
    
    assertEquals(2, results.count());
    assertTrue(results.collectAsList().contains(RowFactory.create("a=1,b=hello,c=true")));
    assertTrue(results.collectAsList().contains(RowFactory.create("a=2,b=world,c=false")));
  }
  
  @Test
  public void readTextWithTranslator() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(FileSystemInput.FORMAT_CONFIG, FileSystemInput.TEXT_FORMAT);
    configMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(TEXT_DATA).getPath());
    configMap.put("translator.type", KVPTranslator.class.getName());
    configMap.put("translator.delimiter.kvp", ",");
    configMap.put("translator.delimiter.field", "=");
    configMap.put("translator.field.names", Lists.newArrayList("a", "b", "c"));
    configMap.put("translator.field.types", Lists.newArrayList("int", "string", "boolean"));
    config = ConfigFactory.parseMap(configMap);
    
    FileSystemInput formatInput = new FileSystemInput();
    formatInput.configure(config);
    
    Dataset<Row> results = formatInput.read();
    
    assertEquals(2, results.count());
    assertTrue(results.collectAsList().contains(RowFactory.create(1, "hello", true)));
    assertTrue(results.collectAsList().contains(RowFactory.create(2, "world", false)));
  }
  
  @Test
  public void readXmlNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "xml");
    paramMap.put(FileSystemInput.XML_ROW_TAG, "item");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(XML_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput xmlInput = new FileSystemInput();
    xmlInput.configure(config);

    Dataset<Row> dataFrame = xmlInput.read();

    StructType schema = dataFrame.schema();
    assertEquals("Schema", "StructField(double1,DoubleType,true)StructField(int1,LongType,true)StructField(long1,LongType,true)StructField(long2,LongType,true)StructField(string1,StringType,true)StructField(string2,StringType,true)StructField(subitems,StructType(StructField(subitem,ArrayType(StructType(StructField(double1,DoubleType,true), StructField(int1,LongType,true), StructField(long1,LongType,true), StructField(string1,StringType,true)),true),true)),true)StructField(timestamp1,StringType,true)",
    		schema.mkString());
    assertTrue("Field long1 index", schema.getFieldIndex("long1").isDefined());
    assertTrue("Field long2 index", schema.getFieldIndex("long2").isDefined());
    assertTrue("Field string1 index", schema.getFieldIndex("string1").isDefined());
    assertTrue("Field string2 index", schema.getFieldIndex("string2").isDefined());
    assertTrue("Field timestamp1 index", schema.getFieldIndex("timestamp1").isDefined());
    assertTrue("Field int1 index", schema.getFieldIndex("int1").isDefined());
    assertTrue("Field double1 index", schema.getFieldIndex("double1").isDefined());
    assertTrue("Field subitems index", schema.getFieldIndex("subitems").isDefined());
    
    assertEquals(2, dataFrame.count());
    
    Row first = dataFrame.first();
    assertEquals(9.99, first.getDouble(schema.fieldIndex("double1")), 0.01);
    assertEquals(2L, first.getLong(schema.fieldIndex("int1")));
    assertEquals(10001L, first.getLong(schema.fieldIndex("long1")));
    assertEquals(20001L, first.getLong(schema.fieldIndex("long2")));
    assertEquals("555-123456", first.getString(schema.fieldIndex("string1")));
    assertEquals("OPEN", first.getString(schema.fieldIndex("string2")));
    assertEquals("2018-02-02 02:02:02.000+0000", first.getString(schema.fieldIndex("timestamp1")));
    
    Row subitems = first.getStruct(schema.fieldIndex("subitems"));
    assertNotNull("Subitems row", subitems);
    assertEquals(1, subitems.length());
    List<Row> subitemsElems = subitems.getList(0);
    assertNotNull("Subitems elements row", subitemsElems);
    assertEquals(3, subitemsElems.size());
  }
  
  @Test
  public void readXmlWithFieldsName() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "xml");
    paramMap.put(FileSystemInput.XML_ROW_TAG, "item");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(XML_DATA).getPath());
    paramMap.put(FileSystemInput.FIELD_NAMES_CONFIG, Lists.newArrayList("long1", "long2", "string1", "timestamp1", "int1", "double1"));
    paramMap.put(FileSystemInput.FIELD_TYPES_CONFIG, Lists.newArrayList("long", "long", "string", "timestamp", "int", "double"));
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput xmlInput = new FileSystemInput();
    xmlInput.configure(config);

    Dataset<Row> dataFrame = xmlInput.read();
    StructType schema = dataFrame.schema();

    assertEquals(2, dataFrame.count());
    
    assertEquals(DataTypes.LongType, schema.fields()[schema.fieldIndex("long1")].dataType());
    assertEquals(DataTypes.StringType, schema.fields()[schema.fieldIndex("string1")].dataType());
    assertEquals(DataTypes.TimestampType, schema.fields()[schema.fieldIndex("timestamp1")].dataType());
    assertEquals(DataTypes.IntegerType, schema.fields()[schema.fieldIndex("int1")].dataType());
    assertEquals(DataTypes.DoubleType, schema.fields()[schema.fieldIndex("double1")].dataType());
  }
  
  @Test
  public void readXmlWithAvroSchema() throws Exception {
  	Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "xml");
    paramMap.put(FileSystemInput.XML_ROW_TAG, "item");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(XML_DATA).getPath());
    paramMap.put(FileSystemInput.AVRO_FILE_CONFIG, FileSystemInput.class.getResource(AVRO_SCHEMA).getPath());
    
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput xmlInput = new FileSystemInput();
    xmlInput.configure(config);

    Dataset<Row> dataFrame = xmlInput.read();
    
    
   // assertEquals("", dataFrame.schema().mkString());
    
    Dataset<Row> items = dataFrame.select("long1", "long2", "string1", "string2", "timestamp1", "int1", "double1", "subitems");
    
    assertEquals(2, items.count());
    
    StructType schema = items.schema();
    assertEquals(DataTypes.LongType, schema.fields()[schema.fieldIndex("long1")].dataType());
    assertEquals(DataTypes.StringType, schema.fields()[schema.fieldIndex("string1")].dataType());
    assertEquals(DataTypes.StringType, schema.fields()[schema.fieldIndex("timestamp1")].dataType());
    assertEquals(DataTypes.IntegerType, schema.fields()[schema.fieldIndex("int1")].dataType());
    assertEquals(DataTypes.DoubleType, schema.fields()[schema.fieldIndex("double1")].dataType());
    
    dataFrame.createOrReplaceTempView("items");
    Dataset<Row> subitems = dataFrame.sparkSession().sql("SELECT a.id, a.subitem.int1, a.subitem.long1 from (SELECT long1 as id, explode(subitems.subitem) as subitem from items) a");
    assertEquals(5, subitems.count());
    
    schema = subitems.schema();
    Row first = subitems.first();
    assertEquals(DataTypes.LongType, schema.fields()[schema.fieldIndex("id")].dataType());
    assertEquals(DataTypes.IntegerType, schema.fields()[schema.fieldIndex("int1")].dataType());
    assertEquals(DataTypes.LongType, schema.fields()[schema.fieldIndex("long1")].dataType());
  }
}