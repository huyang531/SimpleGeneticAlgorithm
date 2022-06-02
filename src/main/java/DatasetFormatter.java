import java.io.*;
import java.util.Scanner;

public class DatasetFormatter {
    public static void main(String[] args) throws IOException {
        FileInputStream fileInputStream = null;
        FileWriter fileWriter = null;
        Scanner scanner = null;
        try {
            fileInputStream = new FileInputStream("/Users/huyang/Desktop/Courses/大数据原理与技术/final_project/SimpleGeneticAlgorithm/test_input/extreme.txt");
            fileWriter = new FileWriter("/Users/huyang/Desktop/Courses/大数据原理与技术/final_project/SimpleGeneticAlgorithm/test_input/extreme_new.txt");

            scanner = new Scanner(fileInputStream);
            scanner.nextInt();
            while (scanner.hasNextLine()) {
                if (scanner.hasNextLong()) {
                    scanner.nextLong();
                    if (scanner.hasNextLong()) {
                        long profit = scanner.nextLong();
                        long weight = scanner.nextLong();
                        fileWriter.write(String.valueOf(weight));
                        fileWriter.write(' ');
                        fileWriter.write(String.valueOf(profit));
                        fileWriter.write('\n');
                    } else {
                        scanner.nextLine();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            assert scanner != null;
            scanner.close();
            fileInputStream.close();
            fileWriter.close();
        }



    }
}
