import java.io.*;
import java.util.Scanner;

public class DatasetFormatter {
    public static void main(String[] args) throws IOException {
        FileInputStream fileInputStream = new FileInputStream("/Users/huyang/Desktop/Courses/大数据原理与技术/final_project/SimpleGeneticAlgorithm/test_input/extreme.txt");
        FileWriter fileWriter = new FileWriter("/Users/huyang/Desktop/Courses/大数据原理与技术/final_project/SimpleGeneticAlgorithm/test_input/extreme.txt");

        Scanner scanner = new Scanner(fileInputStream);
        scanner.nextInt();
        while (scanner.hasNextLine()) {
            if (scanner.hasNextLong()) {
                scanner.nextLong();
                if (scanner.hasNextLong()) {
                    fileWriter.write(String.valueOf(scanner.nextLong()));
                    fileWriter.write(' ');
                    fileWriter.write(String.valueOf(scanner.nextLong()));
                    fileWriter.write('\n');
                } else {
                    scanner.nextLine();
                }
            }
        }

        scanner.close();
        fileInputStream.close();
        fileWriter.close();
    }
}
