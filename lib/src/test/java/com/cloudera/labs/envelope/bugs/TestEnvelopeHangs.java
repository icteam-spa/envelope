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
package com.cloudera.labs.envelope.bugs;

import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

public class TestEnvelopeHangs {
    public static Server server;

    @BeforeClass
    public static void beforeClass() throws SQLException, ClassNotFoundException, InterruptedException {
        Class.forName("org.h2.Driver");
        server = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "9092").start();
        Connection connection = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:9092/mem:test;DB_CLOSE_DELAY=-1", "sa", "");
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("create table if not exists foo (a varchar(30), b varchar(30))");
        stmt.executeUpdate("insert into foo values ('f1','p1')");
        stmt.executeUpdate("insert into foo values ('f2','p1')");
        stmt.executeUpdate("insert into foo values ('f3','p1')");
    }

    @Test
    public void testInvalidDeriver() throws Throwable {
        Config config = ConfigUtils.configFromResource("/bugs/invalid.conf");

        Contexts.closeSparkSession(true);

        try {
            Runner.run(config);
        }
        // Data steps run off the main thread so we have to dig into the concurrency-related exception first
        catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @AfterClass
    public static void afterClass() {
        server.stop();
    }
}
