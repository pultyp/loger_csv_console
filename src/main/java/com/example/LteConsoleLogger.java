package com.example;

import com.opencsv.CSVWriter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class LteConsoleLogger {
    private static final Map<String, CSVWriter> writers = new HashMap<>();
    private static final String T_UE_DELIMITER = "----DL----------------------- ----UL----------------------------------------------------";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java LteConsoleLogger <logfile1> <logfile2> ...");
            System.exit(1);
        }

        for (String logFilePath : args) {
            LogProcessor processor = new LogProcessor();
            TailerListenerAdapter listener = new TailerListenerAdapter() {
                @Override
                public void handle(String line) {
                    processor.processLine(line);
                }

                @Override
                public void fileNotFound() {
                    System.err.println("File not found: " + logFilePath);
                }

                @Override
                public void handle(Exception ex) {
                    System.err.println("Error in Tailer: " + ex.getMessage());
                    processor.flushPendingData(); // Запись данных при ошибке
                }
            };
            Tailer tailer = new Tailer(new File(logFilePath), listener, 1000, true);
            Thread thread = new Thread(tailer);
            thread.start();

            // Добавляем обработку завершения
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                processor.flushPendingData();
                closeWriters();
            }));
        }
    }

    private static void closeWriters() {
        for (CSVWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                System.err.println("Error closing CSV writer: " + e.getMessage());
            }
        }
        writers.clear();
    }

    private static class LogProcessor {
        private List<String> headers = null;
        private List<List<String>> tableBuffer = new ArrayList<>();
        private boolean inTable = false;
        private boolean inDataBlock = false;
        private String dataType = null;

        public void processLine(String line) {
            line = line.trim();
            if (line.isEmpty()) {
                return;
            }

            // Игнорируем лишние строки
            if (line.startsWith("PRACH:") || line.startsWith("Press [return] to stop the trace") || 
                line.startsWith("(enb) t")) {
                if (inDataBlock && !tableBuffer.isEmpty()) {
                    writeToCsv(); // Записываем данные перед выходом из блока
                    inDataBlock = false;
                }
                return;
            }

            // Обработка разделителя
            if (line.equals(T_UE_DELIMITER)) {
                handleDelimiter("t_ue");
                return;
            }

            if (inTable && dataType != null) {
                if (isHeaderLine(line)) {
                    headers = parseHeaders(line);
                    initializeCsvWriter();
                    inDataBlock = true;
                    return;
                }

                if (inDataBlock && isDataLine(line) && headers != null) {
                    List<String> row = parseDataRow(line, headers.size());
                    if (row != null && row.size() == headers.size()) {
                        tableBuffer.add(row);
                    }
                }
            }
        }

        private void handleDelimiter(String type) {
            if (inDataBlock && !tableBuffer.isEmpty()) {
                writeToCsv();
            }
            if (!type.equals(dataType)) {
                headers = null;
                tableBuffer.clear();
                inDataBlock = false;
            }
            dataType = type;
            inTable = true;
        }

        private boolean isHeaderLine(String line) {
            return line.trim().startsWith("UE_ID");
        }

        private boolean isDataLine(String line) {
            return line.trim().matches("^\\s*\\d+\\s+\\d+\\s+[0-9a-fA-F]+\\s+.*");
        }

        private List<String> parseHeaders(String headerLine) {
            String[] columns = headerLine.trim().split("\\s+");
            return new ArrayList<>(List.of(columns));
        }

        private List<String> parseDataRow(String line, int expectedColumns) {
            String[] columns = line.trim().split("\\s+");
            List<String> row = new ArrayList<>(List.of(columns));
            while (row.size() < expectedColumns) {
                row.add("-");
            }
            return row.size() == expectedColumns ? row : null;
        }

        private void writeToCsv() {
            if (dataType == null || tableBuffer.isEmpty() || headers == null) {
                return;
            }
            CSVWriter writer = writers.get(dataType);
            if (writer == null) {
                return;
            }
            for (List<String> row : tableBuffer) {
                writer.writeNext(row.toArray(new String[0]));
            }
            try {
                writer.flush();
            } catch (IOException e) {
                System.err.println("Error flushing CSV writer: " + e.getMessage());
            }
            tableBuffer.clear();
        }

        private void initializeCsvWriter() {
            if (dataType == null || headers == null || writers.containsKey(dataType)) {
                return;
            }
            String csvFilePath = dataType + ".csv";
            File csvFile = new File(csvFilePath);
            try {
                CSVWriter writer = new CSVWriter(new FileWriter(csvFile, true));
                if (csvFile.length() == 0) {
                    writer.writeNext(headers.toArray(new String[0]));
                    writer.flush();
                }
                writers.put(dataType, writer);
            } catch (IOException e) {
                System.err.println("Error initializing CSV writer: " + e.getMessage());
            }
        }

        public void flushPendingData() {
            if (!tableBuffer.isEmpty()) {
                writeToCsv();
            }
        }
    }
}
