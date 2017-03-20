import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * MapReduce
 * Performs map or reduce depending on the command from cmd
 */
public class MapReduce {
    public static void main(String[] args) throws IOException{
        String inputFilePath;
        String outputFilePath;
        List<String> striptParams;
        String command;
//        if (args.length == 0) {
//            inputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\input.txt";
//            outputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\output.txt";
//            striptParams.add("F:\\Downloads\\Fragments\\InMemoryMapReduce\\map_script.exe");
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
        switch (command) {
            case "reduce":
                List<String> strings = new LinkedList<>();
                BufferedReader br = null;
                BufferedWriter fr = null;
                try {
                    br = new BufferedReader(new FileReader(input));
                    String line;
                    while ((line = br.readLine()) != null) {
                        strings.add(line);
                    }
                    strings.sort(String::compareTo);
                    br.close();
                    input.delete();
                    fr = new BufferedWriter(new FileWriter(input));
                    for (String st : strings) {
                        fr.append(st + '\n');
                    }
                    fr.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    br.close();
                    fr.close();
                }
                break;
            case "map":
                break;
            default:
                throw new IllegalArgumentException("No such command. map/reduce required");
        }

        try {
            ProcessBuilder builder = new ProcessBuilder(striptParams);

            builder.redirectInput(input);
            builder.redirectErrorStream( true );
            builder.redirectOutput(output);

            process = builder.start();
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
