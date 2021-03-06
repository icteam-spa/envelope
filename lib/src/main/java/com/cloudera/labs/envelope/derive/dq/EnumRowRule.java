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
package com.cloudera.labs.envelope.derive.dq;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;

public class EnumRowRule implements RowRule, ProvidesAlias {

  private static final String FIELDS_CONFIG = "fields";
  private static final String FIELD_TYPE_CONFIG = "fieldtype";
  private static final String VALUES_CONFIG = "values";
  private static final String CASE_SENSITIVE_CONFIG = "case-sensitive";

  private static final boolean DEFAULT_CASE_SENSITIVITY = true;

  private String name;
  private Set validValues;
  private List<String> fields;
  private Class fieldType;
  private boolean caseSensitive = DEFAULT_CASE_SENSITIVITY;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    ConfigUtils.assertConfig(config, FIELDS_CONFIG);
    ConfigUtils.assertConfig(config, VALUES_CONFIG);
    if (config.hasPath(CASE_SENSITIVE_CONFIG)) {
      caseSensitive = config.getBoolean(CASE_SENSITIVE_CONFIG);
    }
    this.fieldType = getFieldType(config.getString(FIELD_TYPE_CONFIG));
    this.validValues = getValueSet(fieldType, config.getAnyRefList(VALUES_CONFIG));
    if (!caseSensitive && fieldType == String.class) {
      Set<String> replacementValues = new HashSet<>();
      for (Object o : validValues) {
        replacementValues.add(((String)o).toLowerCase());
      }
      validValues = replacementValues;
    }
    this.fields = config.getStringList(FIELDS_CONFIG);
  }

  @Override
  public boolean check(Row row) {
    boolean check = true;
    for (String field : fields) {
      if (fieldType == String.class && !caseSensitive) {
        check = check && validValues.contains(row.<String>getAs(field).toLowerCase());
      } else {
        check = check && validValues.contains(RowUtils.get(row, field));
      }
    }
    return check;
  }

  private static Class getFieldType(String fieldType) {
    Class clazz;
    switch (fieldType) {
      case "int":
        clazz = Integer.class;
        break;
      case "long":
        clazz = Long.class;
        break;
      case "boolean":
        clazz = Boolean.class;
        break;
      case "double":
      case "float":
        throw new RuntimeException("Cannot specify inexact floating point types in EnumRowRule");
      case "decimal":
        clazz = BigDecimal.class;
        break;
      default:
        clazz = String.class;
        break;
    }

    return clazz;
  }

  private static <T> Set<T> getValueSet(Class<T> clazz, List values) {
    Set<T> valueSet = new HashSet<>();
    for (Object o : values) {
      if (clazz == BigDecimal.class) {
        o = new BigDecimal((String)o);
      }
      if (!valueSet.add(clazz.cast(o))) {
        throw new RuntimeException("Could not cast object to type [" + clazz + "]");
      }
    }
    return valueSet;
  }

  @Override
  public String getAlias() {
    return "enum";
  }
}
