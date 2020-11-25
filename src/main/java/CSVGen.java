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

import static net.andreinc.mockneat.unit.text.CSVs.csvs;
import static net.andreinc.mockneat.unit.text.Formatter.fmt;
import static net.andreinc.mockneat.unit.text.Strings.strings;
import static net.andreinc.mockneat.unit.types.Ints.ints;
import static net.andreinc.mockneat.unit.types.Longs.longs;

import net.andreinc.mockneat.types.enums.StringType;
import net.andreinc.mockneat.unit.text.CSVs;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

class ArgsParser {
    @Option(names = "-sql", description = "create_sql")
    String createSql;

    @Option(names = "-t", description = "sql type(mysql, doris)")
    String sqlType = "doris";

    @Option(names = "-l", required = true, description = "how many lines we wanna gen")
    int lineNum;

    @Option(names = {"-f", "--file"}, description = "the output csv file")
    String file;

//    @Option(names = "-to", description = "the dest http url(unsupported)")
//    String url;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    boolean helpRequested = false;
}


public class CSVGen {
    Logger LOG = LoggerFactory.getLogger(CSVGen.class);

    public CSVs genFromCreateSQL(String createSql, String sqlType) {
        CSVs csvs = null;
        try {
            // TODO need to improve
            if (sqlType.equalsIgnoreCase("doris")) {
                createSql = createSql.substring(0, createSql.indexOf("ENGINE"));
                // TODO strim "" is better
                createSql = createSql.replaceAll("DEFAULT .*?,", ",");
                createSql = createSql.replace("REPLACE", "");
                createSql = createSql.replace("SUM", "");
            }
            LOG.info("standard sql:\n {}", createSql);

            Statement stmt = CCJSqlParserUtil.parse(createSql);
            if (stmt instanceof CreateTable) {
                List<ColumnDefinition> columnList = ((CreateTable) stmt).getColumnDefinitions();
                csvs = createCSVColumns(columnList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return csvs;
    }

    private CSVs createCSVColumns(List<ColumnDefinition> columnList) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        CSVs csvs = csvs();
        for (ColumnDefinition col : columnList) {
            String dataType = col.getColDataType().getDataType();
            LOG.info("col data type: {}", dataType);
            // dataType reflection
            Method findByColType = this.getClass().getMethod("mock" + StringUtils.capitalize(dataType),
                    CSVs.class, ColumnDefinition.class);
            csvs = (CSVs) findByColType.invoke(this, csvs, col);
        }
        LOG.info("sample: {}", csvs.val());
        return csvs;
    }

    // TINYINT
    // 1字节有符号整数，范围[-128, 127]
    public CSVs mockTINYINT(CSVs csvs, ColumnDefinition col) {
        // [0, bound)
        // If using range, the lowerBound should be >= 0.0 too.
        // TODO support negative integer
        return csvs.column(ints().bound(128));
    }

    // DECIMAL(M[,D])
    // 高精度定点数，M代表一共有多少个有效数字(precision)，D代表小数点后最多有多少数字(scale)
    // M的范围是[1,27], D的范围[1, 9], 另外，M必须要大于等于D的取值。默认的D取值为0
    public CSVs mockDECIMAL(CSVs csvs, ColumnDefinition col) {
        List<String> arg = col.getColDataType().getArgumentsStringList();
        Preconditions.checkArgument(arg.size() >= 1, "missing precision");
        int precision = Integer.parseInt(arg.get(0));

        int scale = arg.size() > 1 ? Integer.parseInt(arg.get(1)) : 0;
        Preconditions.checkArgument(precision >= scale, "precision should >= scale");
        // Can't support dynamic integral or fractional part size now, so we only gen
        // the number: integral size range is [1, precision-scale], fractional size is [scale]
        // It's OK to gen '1000.', which has no fractional part.
        return csvs.column(fmt("#{i}.#{f}")
                .param("i", strings()
                        .size(ints().rangeClosed(1, precision - scale)).type(StringType.NUMBERS))
                .param("f", strings()
                        .size(scale).type(StringType.NUMBERS))
        );
    }

    // CHAR(M)
    // 定长字符串，M代表的是定长字符串的长度。M的范围是1-255
    public CSVs mockCHAR(CSVs csvs, ColumnDefinition col) {
        List<String> arg = col.getColDataType().getArgumentsStringList();
        Preconditions.checkArgument(arg.size() == 1, "must have the string length");
        int m = Integer.parseInt(arg.get(0));
        return csvs.column(strings().size(m));
    }

    // INT
    // 4字节有符号整数，范围[-2147483648, 2147483647]
    public CSVs mockINT(CSVs csvs, ColumnDefinition col) {
        // ints().val() range is [Integer.MIN_VALUE, Integer.MAX_VALUE]
        return csvs.column(ints());
    }

    // BOOL, BOOLEAN
    // 与TINYINT一样，0代表false，1代表true
    public CSVs mockBOOL(CSVs csvs, ColumnDefinition col) {
        return csvs.column(ints().from(new int[]{0, 1}));
    }

    public CSVs mockBOOLEAN(CSVs csvs, ColumnDefinition col) {
        return mockBOOL(csvs, col);
    }

    // DATETIME
    // 日期时间类型，取值范围是['0000-01-01 00:00:00', '9999-12-31 23:59:59'].
    // 打印的形式是'YYYY-MM-DD HH:MM:SS'
    public CSVs mockDATETIME(CSVs csvs, ColumnDefinition col) {
        // Not too early
        // TODO need more flexible
        long past = Timestamp.valueOf("2020-01-01 00:00:00").getTime();
        long future = new Timestamp(System.currentTimeMillis()).getTime();
        return csvs.column(longs().range(past, future).mapToString(
                timestamp -> new Timestamp(timestamp).toLocalDateTime()
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        ));
    }

    // VARCHAR(M)
    // 变长字符串，M代表的是变长字符串的长度。M的范围是1-65533。
    // 注意：变长字符串是以UTF-8编码存储的，因此通常英文字符占1个字节，中文字符占3个字节。
    public CSVs mockVARCHAR(CSVs csvs, ColumnDefinition col) {
        List<String> arg = col.getColDataType().getArgumentsStringList();
        Preconditions.checkArgument(arg.size() == 1, "must have the max string length");
        int m = Integer.parseInt(arg.get(0));
        Preconditions.checkArgument(m >= 1 && m <= 65533);
        return csvs.column(strings().size(ints().rangeClosed(1, m)));
    }

    public static void main(String[] args) {
        ArgsParser argsParser = new ArgsParser();
        new CommandLine(argsParser).parseArgs(args);
        if (argsParser.helpRequested) {
            CommandLine.usage(new ArgsParser(), System.out);
            return;
        }

        CSVGen gen = new CSVGen();
        CSVs csvs = null;
        if (argsParser.createSql != null) {
            csvs = gen.genFromCreateSQL(argsParser.createSql, argsParser.sqlType);
        }
        if (argsParser.file == null || argsParser.file.length() == 0) {
            return;
        }

        File f = new File(argsParser.file);
        Preconditions.checkState(f.delete());
        // MacOS can't send the right sep in curl, so use default sep '\t'
        Objects.requireNonNull(csvs).separator("\t").write(argsParser.file, argsParser.lineNum);

        // TODO: can't get output now
//        if (argsParser.url != null && argsParser.url.length() > 0) {
//            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//            ProcessBuilder pb = new ProcessBuilder("curl", "--location-trusted", "-u ", "-T " + argsParser.file,
//                    "-H \"label:" + timestamp.toString() + "\"", // "-H \"timeout:36000\"",
//                    argsParser.url);
//            pb.redirectErrorStream(true);
//            Process p = pb.start();
//            System.setOut(new PrintStream(p.getOutputStream()));
//            p.waitFor();
//        }
    }
}
