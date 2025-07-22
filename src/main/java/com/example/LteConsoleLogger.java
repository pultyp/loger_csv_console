package com.example;

import com.opencsv.CSVWriter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LteConsoleLogger {
    private static final String TABLE_DELIMITER = "----DL----------------------- ----UL----------------------------------------------------";
    private static final Map<String, CSVWriter> writers = new HashMap<>();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java LteConsoleLogger <logfile1> <logfile2> ...");
            System.exit(1);
        }

        // Запускаем Tailer для каждого лог-файла
        for (String logFilePath : args) {
            LogProcessor processor = new LogProcessor();
            TailerListenerAdapter listener = new TailerListenerAdapter() {
                @Override
                public void handle(String line) {
                    processor.processLine(line);
                }
            };
            Tailer tailer = new Tailer(new File(logFilePath), listener, 1000, true); // true — читать с начала файла
            new Thread(tailer).start();
        }

        // Закрытие писателей при завершении программы
        Runtime.getRuntime().addShutdownHook(new Thread(LteConsoleLogger::closeWriters));
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

        public void processLine(String line) {
            line = line.trim();
            if (line.isEmpty()) {
                return;
            }

            // Начало или конец таблицы
            if (line.equals(TABLE_DELIMITER)) {
                if (inTable && !tableBuffer.isEmpty() && headers != null) {
                    writeToCsv(headers, tableBuffer);
                    tableBuffer.clear();
                }
                inTable = true;
                return;
            }

            if (inTable) {
                // Обработка заголовков
                if (isHeaderLine(line)) {
                    headers = parseHeaders(line);
                    initializeCsvWriter(headers);
                    return;
                }

                // Обработка строк данных
                if (isDataLine(line) && headers != null) {
                    List<String> row = parseDataRow(line, headers.size());
                    if (row != null && row.size() == headers.size()) {
                        tableBuffer.add(row);
                    }
                }
            }
        }

        private boolean isHeaderLine(String line) {
            return line.trim().startsWith("UE_ID");
        }

        private boolean isDataLine(String line) {
            return line.trim().matches("^\\s*\\d+\\s+.*");
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

        private void writeToCsv(List<String> headers, List<List<String>> dataRows) {
            String headersKey = getHeadersKey(headers);
            CSVWriter writer = writers.get(headersKey);
            if (writer == null) {
                return;
            }
            for (List<String> row : dataRows) {
                writer.writeNext(row.toArray(new String[0]));
            }
            try {
                writer.flush();
            } catch (IOException e) {
                System.err.println("Error flushing CSV writer: " + e.getMessage());
            }
        }

        private void initializeCsvWriter(List<String> headers) {
            String headersKey = getHeadersKey(headers);
            if (!writers.containsKey(headersKey)) {
                String csvFilePath = getCsvFileName(headers);
                File csvFile = new File(csvFilePath);
                try {
                    CSVWriter writer = new CSVWriter(new FileWriter(csvFile, true));
                    if (csvFile.length() == 0) {
                        writer.writeNext(headers.toArray(new String[0]));
                        writer.flush();
                    }
                    writers.put(headersKey, writer);
                } catch (IOException e) {
                    System.err.println("Error initializing CSV writer: " + e.getMessage());
                }
            }
        }

        private String getHeadersKey(List<String> headers) {
            return headers.stream().collect(Collectors.joining(","));
        }

        private String getCsvFileName(List<String> headers) {
            String headersKey = getHeadersKey(headers);
            return "lte_metrics_" + headersKey.hashCode() + ".csv";
        }
    }
}
