import junit.framework.TestCase;
import net.andreinc.mockneat.unit.text.CSVs;
import org.junit.Test;

public class CSVGenTest extends TestCase {
    @Test
    public void testGenFromCreateSQL() {
        String createSql = "CREATE TABLE example_db.table_hash\n" +
                "(\n" +
                "k1 TINYINT,\n" +
                "k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n" +
                "v1 CHAR(10) REPLACE,\n" +
                "v2 INT SUM\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "COMMENT \"my first doris table\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 32\n" +
                "PROPERTIES (\"storage_type\"=\"column\");";
        CSVs csvs = new CSVGen().genFromCreateSQL(createSql, "doris");
        System.out.println(csvs.val());
    }
}