// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.hw;

import com.google.common.collect.Maps;
import net.andreinc.mockneat.abstraction.MockUnit;
import net.andreinc.mockneat.unit.text.CSVs;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static net.andreinc.mockneat.unit.objects.From.from;
import static net.andreinc.mockneat.unit.types.Ints.ints;

public class CSVGenTest {
    Logger LOG = LoggerFactory.getLogger(CSVGenTest.class);

    @Test
    public void simpleGenData() {
        String createSql = "CREATE TABLE example_db.table\n" +
                "(\n" +
                "k1 TINYINT,\n" +
                "k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n" +
                "v1 CHAR(10) REPLACE,\n" +
                "v2 INT SUM,\n" +
                "v3 DATETIME,\n" +
                "v4 VARCHAR(10)\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "COMMENT \"my first doris table\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 32\n" +
                "PROPERTIES (\"storage_type\"=\"column\");";
        CSVs csvs = new CSVGen().genFromCreateSQL(createSql, "doris");
        LOG.info(csvs.val());
    }

    @Test
    public void replaceColsInGenData() {
        String createSql = "CREATE TABLE example_db.table\n" +
                "(\n" +
                "k1 TINYINT,\n" +
                "k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n" +
                "v1 CHAR(10) REPLACE,\n" +
                "v2 INT SUM,\n" +
                "v3 DATETIME,\n" +
                "v4 VARCHAR(10)\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "COMMENT \"my first doris table\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 32\n" +
                "PROPERTIES (\"storage_type\"=\"column\");";

        Map<String, MockUnit> replaceCols = Maps.newHashMap();
        replaceCols.put("v2", ints().from(new int[] { 10086 }));
        CSVs csvs = new CSVGen().genFromCreateSQL(createSql, replaceCols, "doris");
        LOG.info(csvs.val());
        // csvs.separator("\t").write("test.txt", 1000);
    }

    @Test
    public void replaceSlowOpenMLDB() {
        String createSql = "create table sample_v1 (request_id int, is_click int, col1 string, col2 string, show_time int);";

        Map<String, MockUnit> replaceCols = Maps.newHashMap();
        replaceCols.put("request_id", ints().range(0, 100));
        replaceCols.put("col1", from(new String[] { "aa", "bb", "cc" }));
        replaceCols.put("show_time", ints().range(0, 120000));
        replaceCols.put("is_click", from(new Integer[] { 0, 1 }));
        CSVs csvs = new CSVGen().genFromCreateSQL(createSql, replaceCols, "mysql");
        LOG.info(csvs.val());
        csvs.separator(",").write("out.txt", 3000000);
    }
}