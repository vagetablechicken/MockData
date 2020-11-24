import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.unit.text.CSVs;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

class ArgsParser {
    @Option(names = "-sql", description = "create_sql")
    String createSql;

    @Option(names = "-t", description = "sql type(mysql, doris)", defaultValue = "doris")
    String sqlType;

    @Option(names = "-l", required = true, description = "how many lines we wanna gen")
    int lineNum;

    @Option(names = {"-f", "--file"}, description = "the output csv file")
    String file;

    @Option(names = "-to", description = "the dest http url(unsupported)")
    String url;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    boolean helpRequested = false;
}


public class CSVGen {
    MockNeat mock = MockNeat.threadLocal();

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
            System.out.println("standard sql:\n" + createSql);

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

        CSVs csvs = mock.csvs();
        for (ColumnDefinition col : columnList) {
            String dataType = col.getColDataType().getDataType();
            System.out.println(col.getColDataType().getDataType());

            // 根据deviceTypeTitle查找对应方法名并调用方法
            Method findByColType = this.getClass().getMethod("mock" + StringUtils.capitalize(dataType),
                    CSVs.class, ColumnDefinition.class);
            csvs = (CSVs) findByColType.invoke(this, csvs, col);
            System.out.println(csvs.val());
        }
        return csvs;
    }

    // 1字节有符号整数，范围[-128, 127]
    public CSVs mockTINYINT(CSVs csvs, ColumnDefinition col) {
        return csvs.column(mock.ints().range(0, 127)); // TODO
    }

    // TODO
    public CSVs mockDECIMAL(CSVs csvs, ColumnDefinition col) {
        return csvs.column(mock.floats());
    }

    public CSVs mockCHAR(CSVs csvs, ColumnDefinition col) {
        System.out.println(col);
        System.out.println(col.getColumnSpecStrings()); // TODO where is (10)?
        return csvs.column(mock.strings().size(255));
    }

    public CSVs mockINT(CSVs csvs, ColumnDefinition col) {
        System.out.println(col.getColumnSpecStrings());
        return csvs.column(mock.ints().range(0, 65536));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
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
        boolean delete = f.delete();
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
