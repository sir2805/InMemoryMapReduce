import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
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
//        if (args.length == 0) {
//            inputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\output.txt";
//            outputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\result.txt";
//            striptParams = new LinkedList<>();
//            command = "reduce";
//            striptParams.add("C:\\Users\\PC\\Documents\\Visual Studio 2015\\Projects\\reduce_script\\Debug\\reduce_script.exe");
//            striptParams.add("run");
////            String resFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\result.txt";
////            File result = new File(resFilePath);
//        } else
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

        switch (command) {
            case "reduce":
                List<String> strings = new LinkedList<>();
                BufferedReader br = null;

                OutputStream stdin = null;
                InputStream stdout = null;

                BufferedReader reader = null;
                BufferedWriter writer = null;
                BufferedWriter outputWriter;

                try {
                    br = new BufferedReader(new FileReader(input));
                    String line;
                    while ((line = br.readLine()) != null) {
                        strings.add(line);
                    }
                    strings.sort(String::compareTo);

                    String cur = strings.get(0);
                    int idx = 0;

                    outputWriter = new BufferedWriter(new FileWriter(output));

                    for (int i = 1; i < strings.size(); i++) {
                        if (!cur.equals(strings.get(i))) {
                            process = builder.start();

                            stdin = process.getOutputStream();
                            stdout = process.getInputStream();

                            reader = new BufferedReader(new InputStreamReader(stdout));
                            writer = new BufferedWriter(new OutputStreamWriter(stdin));

                            for (; idx < i; idx++) {
                                writer.write(strings.get(idx) + '\n');
                            }
                            writer.flush();
                            writer.close();
                            process.waitFor();

                            while ( (line = reader.readLine()) != null) {
                                outputWriter.append(line + '\n');
                            }
                            cur = strings.get(idx);
                        }
                    }
                    process = builder.start();

                    stdin = process.getOutputStream();
                    stdout = process.getInputStream();

                    reader = new BufferedReader(new InputStreamReader(stdout));
                    writer = new BufferedWriter(new OutputStreamWriter(stdin));

                    for (; idx < strings.size(); idx++) {
                        writer.write(strings.get(idx) + '\n');
                    }
                    writer.flush();
                    writer.close();
                    process.waitFor();

                    while ( (line = reader.readLine()) != null) {
                        outputWriter.append(line + '\n');
                    }
                    outputWriter.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    writer.close();
                    reader.close();
                    stdin.close();
                    stdout.close();
                    br.close();
                }
                break;
            case "map":
                builder.redirectInput(input);
                builder.redirectErrorStream( true );
                builder.redirectOutput(output);

                process = builder.start();
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw e;
                }
                break;

            default:
                throw new IllegalArgumentException("No such command. map/reduce required");
        }
    }
}
