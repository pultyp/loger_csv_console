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
    private static final Map<String, CSVWriter> writers = new HashMap<>();

    // Определяем разделители для разных типов данных
    private static final String T_UE_DELIMITER = "----DL----------------------- ----UL----------------------------------------------------";
    private static final String T_G_DELIMITER = "--#UE---------       --RRC------------ --DL------------- --UL------------- -RT--";
    private static final String T_CPU_DELIMITER = "-Proc- --RX---------- --TX----------  --TX/RX diff (ms)--- --Err----";
    private static final String T_SPL_DELIMITER = "--P0/TX 1-- dBFS --P0/RX 1--";

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
            Tailer tailer = new Tailer(new File(logFilePath), listener, 1000, true);
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
        private String currentDelimiter = null;
        private String dataType = null;

        public void processLine(String line) {
            line = line.trim();
            if (line.isEmpty()) {
                return;
            }

            // Игнорируем строки PRACH
            if (line.startsWith("PRACH:")) {
                return;
            }

            // Определяем тип данных по разделителю
            if (line.equals(T_UE_DELIMITER)) {
                setDataType("t_ue", T_UE_DELIMITER);
                return;
            } else if (line.equals(T_G_DELIMITER)) {
                setDataType("t_g", T_G_DELIMITER);
                return;
            } else if (line.equals(T_CPU_DELIMITER)) {
                setDataType("t_cpu", T_CPU_DELIMITER);
                return;
            } else if (line.equals(T_SPL_DELIMITER)) {
                setDataType("t_spl", T_SPL_DELIMITER);
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

        private void setDataType(String type, String delimiter) {
            if (inTable && !tableBuffer.isEmpty() && headers != null) {
                writeToCsv(headers, tableBuffer);
                tableBuffer.clear();
            }
            inTable = true;
            currentDelimiter = delimiter;
            dataType = type;
            headers = null; // Сбрасываем заголовки для нового блока
        }

        private boolean isHeaderLine(String line) {
            // Проверяем начало строки на наличие ключевых слов заголовков
            return line.trim().startsWith("UE_ID") || // t ue
                   line.trim().startsWith("conn") || // t g
                   line.trim().startsWith("CPU") ||  // t cpu
                   line.trim().startsWith("RMS");    // t spl
        }

        private boolean isDataLine(String line) {
            // Проверяем, является ли строка данными (начинается с числа или процента для t cpu)
            return line.trim().matches("^\\s*\\d+\\s+.*") || // t ue, t g
                   line.trim().matches("^\\s*\\d+\\.\\d+%\\s+.*") || // t cpu
                   line.trim().matches("^\\s*-?\\d+\\.\\d+\\s+.*"); // t spl
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
            // Используем dataType для создания осмысленного имени файла
            return String.format("%s_metrics_%d.csv", dataType != null ? dataType : "unknown", headersKey.hashCode());
        }
    }
}
