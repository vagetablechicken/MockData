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

import net.andreinc.mockneat.unit.text.CSVs;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVGenTest {
    Logger LOG = LoggerFactory.getLogger(CSVGenTest.class);

    @Test
    public void genFromCreateSQL() {
        String createSql = "CREATE TABLE example_db.table_hash\n" +
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
}