package com.example;

import com.opencsv.CSVWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LteConsoleLogger {
    private static final String OUTPUT_CSV_PATH = "lte_metrics.csv";
    private static final String TABLE_DELIMITER = "----DL----------------------- ----UL----------------------------------------------------";

    public static void logFromConsole() {
        try (CSVWriter writer = new CSVWriter(new FileWriter(OUTPUT_CSV_PATH, true))) {
            List<String> headers = null;
            List<List<String>> tableBuffer = new ArrayList<>();
            boolean inTable = false;

            // Чтение из консоли
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("PRACH") || line.startsWith("(enb)") ||
                        line.contains("Unknown command") || line.startsWith("[ bs-isp4-01:root")) {
                    continue;
                }

                if (line.equals(TABLE_DELIMITER)) {
                    if (inTable && !tableBuffer.isEmpty() && headers != null) {
                        writeToCsv(writer, headers, tableBuffer);
                        tableBuffer.clear();
                    }
                    inTable = true;
                    continue;
                }

                if (inTable) {
                    if (isHeaderLine(line)) {
                        headers = parseHeaders(line);
                        File csvFile = new File(OUTPUT_CSV_PATH);
                        if (csvFile.length() == 0) {
                            writer.writeNext(headers.toArray(new String[0]));
                            writer.flush();
                        }
                        continue;
                    }

                    if (isDataLine(line) && headers != null) {
                        List<String> row = parseDataRow(line, headers.size());
                        if (row != null && row.size() == headers.size()) {
                            tableBuffer.add(row);
                        }
                    }
                }
            }

            // Записываем оставшиеся данные в буфере при завершении ввода
            if (inTable && !tableBuffer.isEmpty() && headers != null) {
                writeToCsv(writer, headers, tableBuffer);
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static boolean isHeaderLine(String line) {
        return line.trim().startsWith("UE_ID");
    }

    private static boolean isDataLine(String line) {
        // Проверяем, начинается ли строка с числа (ue_id)
        return line.trim().matches("^\\s*\\d+\\s+.*");
    }

    private static List<String> parseHeaders(String headerLine) {
        List<String> headers = new ArrayList<>();
        String[] columns = headerLine.trim().split("\\s+");
        for (String column : columns) {
            headers.add(column);
        }
        return headers;
    }

    private static List<String> parseDataRow(String line, int expectedColumns) {
        String[] columns = line.trim().split("\\s+");
        List<String> row = new ArrayList<>();

        if (columns.length > 1 && "[stopped]".equals(columns[1])) {
            // Обработка строк с [stopped]
            row.add(columns[0]); // ue_id
            row.add("[stopped]");
            for (int i = 2; i < expectedColumns; i++) {
                row.add("-");
            }
            return row;
        }

        // Обычная строка данных
        for (String column : columns) {
            row.add(column);
        }

        // Проверяем, соответствует ли количество столбцов заголовкам
        if (row.size() == expectedColumns) {
            return row;
        }

        // Если количество столбцов не совпадает, заполняем недостающие значения "-"
        while (row.size() < expectedColumns) {
            row.add("-");
        }
        return row.size() == expectedColumns ? row : null;
    }

    private static void writeToCsv(CSVWriter writer, List<String> headers, List<List<String>> dataRows) throws IOException {
        for (List<String> row : dataRows) {
            writer.writeNext(row.toArray(new String[0]));
        }
        writer.flush();
        System.out.println("Data written to " + OUTPUT_CSV_PATH + ", rows: " + dataRows.size());
    }

    public static void main(String[] args) {
        logFromConsole();
    }
}