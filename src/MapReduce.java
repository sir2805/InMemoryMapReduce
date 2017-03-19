import java.io.*;

/**
 * Created by PC on 18.03.2017.
 */
public class MapReduce {
    public static void main(String[] args) {
        String inputFilePath = null;
        String outputFilePath = null;
        String striptPath = null;
        String command = "map";
        if (args.length == 0) {
            inputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\input.txt";
            outputFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\output.txt";
            striptPath = "C:\\Users\\PC\\Documents\\Visual Studio 2015\\Projects\\map_script\\Debug\\map_script.exe";
//            String resFilePath = "F:\\Downloads\\Fragments\\InMemoryMapReduce\\result.txt";
//            File result = new File(resFilePath);
        }
        else {
            try {
                command = args[0];
                striptPath = args[1];
                inputFilePath = args[2];
                outputFilePath = args[3];
            } catch (IndexOutOfBoundsException ex) {
                System.err.println("Invalid params");
            }
        }
        File input = new File(inputFilePath);
        File output = new File(outputFilePath);
        Process process = null;
//        if(command.equals("reduce")) {
//            File sortedInput = new File(input.getParentFile().getAbsolutePath());
//            try {
//                BufferedReader br = new BufferedReader(new FileReader(input));
//
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//        }
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
