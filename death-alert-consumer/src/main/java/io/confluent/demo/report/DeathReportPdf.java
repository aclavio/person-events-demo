package io.confluent.demo.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.confluent.demo.util.DeathSourceReferenceUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.*;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DeathReportPdf {

    private final JedisPool jedisPool;
    private ObjectMapper mapper;
    private DeathSourceReferenceUtil dsUtil;

    public DeathReportPdf(final Properties config) {
        mapper = new JsonMapper();
        // Redis clients init
        jedisPool = new JedisPool(
                config.getProperty("redis.host"),
                Integer.parseInt(config.getProperty("redis.port")));
        dsUtil = new DeathSourceReferenceUtil(jedisPool, mapper);
    }

    public void generateDeathReportPdf(String filename, GenericRecord record) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("Death Report");
        lines.add("Name: %s %s %s".formatted(
                record.get("FIRST_NAME"),
                record.get("MIDDLE_NAME"),
                record.get("LAST_NAME")));
        lines.add("SSN: %s".formatted(record.get("SSN")));
        lines.add("DOB: %s".formatted(LocalDate.ofEpochDay(Long.parseLong(record.get("DATE_OF_BIRTH").toString()))));
        lines.add("DOD: %s".formatted(LocalDate.ofEpochDay(Long.parseLong(record.get("DATE_OF_DEATH").toString()))));
        lines.add("Death Certificate Number: %s".formatted(record.get("DEATH_CERTIFICATE_NUM")));


        String deathSourceCode = record.get("DEATH_SOURCE_CODE").toString();
        String deathSource = dsUtil.getDeathSourceReference(deathSourceCode).get("death_source").asText();
        lines.add("Death Source: %s".formatted(deathSource));

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

                for (String line : lines) {
                    contents.showText(line);
                    contents.newLine();
                }

                contents.endText();
            }
            doc.save(filename);
        }
    }

}
