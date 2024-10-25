package createTestFiles;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class PrintInputFile {
    public static void main(String[] args) {
        DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
        DateTime dt = new DateTime(df.parseDateTime("2019-12-25 00:00")); //Last: meter = 71221976; DateTime = 2020-01-08 23:00:00.0; Stand = 192.000;
        BigDecimal val = new BigDecimal("125000").setScale(3, RoundingMode.HALF_EVEN);
        String meterId = "71221975";
        System.out.println("Avlesningstid;Målerens serienummer;Forbrukstype;Volum 1;Enhet;Energi 1 Varme energi;Enhet");
        for (int i = 0; i < 72 ; i++) {
            System.out.println(df.print(dt) + ";" + meterId + ";Varme;145,25;m3;" + val + ";kWh");
            dt = dt.plusHours(1);
            val = val.add(BigDecimal.valueOf(10));
        }
    }
}
