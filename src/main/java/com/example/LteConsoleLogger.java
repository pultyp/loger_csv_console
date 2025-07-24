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

public class LteConsoleLogger {
    private static final Map<String, CSVWriter> writers = new HashMap<>();
    private static final Map<String, List<String>> headersMap = new HashMap<>();

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
        private String dataType = null;
        private List<String> headers = null;

        public void processLine(String line) {
            line = line.trim();
            if (line.isEmpty()) {
                return;
            }

            // Игнорируем строки PRACH и другие нерелевантные строки
            if (line.startsWith("PRACH:") || line.startsWith("Press [return] to stop the trace") || line.startsWith("(enb) t")) {
                return;
            }

            // Проверяем разделители для определения типа данных
            if (line.equals(T_UE_DELIMITER)) {
                setDataType("t_ue");
                return;
            } else if (line.equals(T_G_DELIMITER)) {
                setDataType("t_g");
                return;
            } else if (line.equals(T_CPU_DELIMITER)) {
                setDataType("t_cpu");
                return;
            } else if (line.equals(T_SPL_DELIMITER)) {
                setDataType("t_spl");
                return;
            }

            if (dataType != null) {
                // Обработка заголовков
                if (isHeaderLine(line)) {
                    headers = parseHeaders(line);
                    headersMap.put(dataType, headers);
                    initializeCsvWriter(headers);
                    return;
                }

                // Обработка строк данных
                if (isDataLine(line) && headers != null) {
                    List<String> row = parseDataRow(line, headers.size());
                    if (row != null && row.size() == headers.size()) {
                        writeToCsv(row);
                    }
                }
            }
        }

        private void setDataType(String type) {
            dataType = type;
            headers = headersMap.get(type);
        }

        private boolean isHeaderLine(String line) {
            return line.trim().startsWith("UE_ID") || // t_ue
                   line.trim().startsWith("conn") || // t_g
                   line.trim().startsWith("CPU") ||  // t_cpu
                   line.trim().startsWith("RMS");    // t_spl
        }

        private boolean isDataLine(String line) {
            return line.trim().matches("^\\s*\\d+\\s+.*") || // t_ue, t_g
                   line.trim().matches("^\\s*\\d+\\.\\d+%\\s+.*") || // t_cpu
                   line.trim().matches("^\\s*-?\\d+\\.\\d+\\s+.*"); // t_spl
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

        private void writeToCsv(List<String> row) {
            if (dataType == null) {
                return;
            }
            CSVWriter writer = writers.get(dataType);
            if (writer == null) {
                return;
            }
            writer.writeNext(row.toArray(new String[0]));
            try {
                writer.flush();
            } catch (IOException e) {
                System.err.println("Error flushing CSV writer: " + e.getMessage());
            }
        }

        private void initializeCsvWriter(List<String> headers) {
            if (dataType == null || writers.containsKey(dataType)) {
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
    }
}
