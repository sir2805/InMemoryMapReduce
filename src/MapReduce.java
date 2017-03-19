import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by PC on 18.03.2017.
 */
public class MapReduce {
    public static void main(String[] args) {
        String inputFilePath = null;
        String outputFilePath = null;
        String striptPath = null;
        String command = "reduce";
        if (args.length == 0) {
            inputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\output.txt";
            outputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\result.txt";
            striptPath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\reduce_script.exe";
//            String resFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\result.txt";
//            File result = new File(resFilePath);
        } else if(args.length != 4) {
            System.err.println("Invalid params");
            return;
        }
        else {
            command = args[0];
            striptPath = args[1];
            inputFilePath = args[2];
            outputFilePath = args[3];
        }
        File input = new File(inputFilePath);
        File output = new File(outputFilePath);
        Process process = null;
        if(command.equals("reduce")) {
            List<String> strings = new LinkedList<>();
            try {
                BufferedReader br = new BufferedReader(new FileReader(input));
                String line;
                while((line = br.readLine()) != null) {
                    strings.add(line);
                }
                strings.sort(String::compareTo);
                input.delete();
                BufferedWriter fr = new BufferedWriter(new FileWriter(input));
                for(String st : strings) {
                    fr.append(st + '\n');
                }
                fr.flush();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            ProcessBuilder builder = new ProcessBuilder(striptPath);

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
