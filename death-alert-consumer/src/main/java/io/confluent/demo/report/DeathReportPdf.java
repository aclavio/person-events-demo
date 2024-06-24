package io.confluent.demo.report;

import org.apache.avro.generic.GenericRecord;
import org.apache.fontbox.ttf.TrueTypeFont;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Calendar;

public class DeathReportPdf {

    public static void generateDeathReportPdf(String filename, GenericRecord record) throws IOException {
        try (PDDocument doc = new PDDocument()) {
            PDPage page = new PDPage();
            doc.addPage(page);
            PDFont font = new PDType1Font(Standard14Fonts.FontName.COURIER);
            float fontSize = 12f;
            float lineHeight = 1.5f * fontSize;
            try (PDPageContentStream contents = new PDPageContentStream(doc, page)) {
                contents.beginText();
                contents.setFont(font, 12);
                contents.newLineAtOffset(100, 700);
                contents.setLeading(lineHeight);

                contents.showText("Death Report");
                contents.newLine();

                contents.showText("Name: %s %s %s".formatted(
                        record.get("FIRST_NAME"),
                        record.get("MIDDLE_NAME"),
                        record.get("LAST_NAME")));
                contents.newLine();

                contents.showText("SSN: %s".formatted(record.get("SSN")));
                contents.newLine();

                contents.showText("DOB: %s".formatted(LocalDate.ofEpochDay(Long.parseLong(record.get("DATE_OF_BIRTH").toString()))));
                contents.newLine();

                contents.showText("DOD: %s".formatted(LocalDate.ofEpochDay(Long.parseLong(record.get("DATE_OF_DEATH").toString()))));
                contents.newLine();

                contents.showText("Death Certificate Number: %s".formatted(record.get("DEATH_CERTIFICATE_NUM")));
                contents.newLine();

                contents.showText("Death Source: %s".formatted(record.get("DEATH_SOURCE_CODE")));
                contents.newLine();

                contents.endText();
            }
            doc.save(filename);
        }
    }

}
