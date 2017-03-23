import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MapReduce
 * Performs map or reduce depending on the command from cmd
 */
public class MapReduce {
    public static void main(String[] args) throws IOException, InterruptedException {
        String inputFilePath;
        String outputFilePath;
        List<String> striptParams;
        String command;
        
        if(args.length < 4) {
            throw new IllegalArgumentException();
        }
        else {
            command = args[0];
            String[] params = Arrays.copyOfRange(args, 1, args.length - 3);
            striptParams = Arrays.asList(params);
            inputFilePath = args[args.length - 2];
            outputFilePath = args[args.length - 1];
        }

        File input = new File(inputFilePath);
        File output = new File(outputFilePath);

        Process process;
        ProcessBuilder builder =  new ProcessBuilder(striptParams);
        String line;

        try (BufferedReader inputReader = new BufferedReader(new FileReader(input));
             BufferedWriter outputWriter = new BufferedWriter(new FileWriter(output))) {
            switch (command) {
                case "reduce":
                    List<String> strings = new ArrayList<>();

                    while ((line = inputReader.readLine()) != null) {
                        strings.add(line);
                    }
                    strings.sort(String::compareTo);

                    String cur = strings.get(0);
                    int idx = 0;

                    for (int i = 1; i < strings.size(); i++) {
                        if (!cur.equals(strings.get(i))) {
                            process = builder.start();

                            try (OutputStream stdin = process.getOutputStream();
                                 InputStream stdout = process.getInputStream();

                                 BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
                                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin))) {

                                for (; idx < i; idx++) {
                                    writer.write(strings.get(idx) + '\n');
                                }

                                writer.flush();
                                writer.close();
                                process.waitFor();

                                while ((line = reader.readLine()) != null) {
                                    outputWriter.append(line + '\n');
                                }
                                cur = strings.get(idx);
                            }
                        }
                    }
                    process = builder.start();

                    try (OutputStream stdin = process.getOutputStream();
                         InputStream stdout = process.getInputStream();

                         BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
                         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin))) {

                        for (; idx < strings.size(); idx++) {
                            writer.write(strings.get(idx) + '\n');
                        }

                        writer.flush();
                        writer.close();
                        process.waitFor();

                        while ((line = reader.readLine()) != null) {
                            outputWriter.append(line + '\n');
                        }
                        outputWriter.flush();
                    }
                    break;

                case "map":
                    while ((line = inputReader.readLine()) != null) {
                        process = builder.start();

                        try (OutputStream stdin = process.getOutputStream();
                             InputStream stdout = process.getInputStream();

                             BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
                             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin))) {

                            writer.write(line);
                            writer.flush();
                            writer.close();

                            process.waitFor();

                            while ((line = reader.readLine()) != null) {
                                outputWriter.append(line + '\n');
                            }
                        }
                        outputWriter.flush();
                    }
                    break;

                default:
                    throw new IllegalArgumentException("No such command. map/reduce required");
            }
        }
    }
}
