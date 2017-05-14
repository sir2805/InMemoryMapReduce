import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by PC on 13.05.2017.
 */
class Master extends Thread {
    private static int queryNo = 0;

    private Socket socket;

    public Master(Socket s) throws IOException {
        super("Master");
        socket = s;
    }

    public void run() {
        try (
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream()));
        ) {
//                if (str.equals("END"))
//                    break;

            String address;
            address = socket.getLocalAddress().getHostAddress();

            String queryLine = in.readLine();

            String[] query = queryLine.split("[ \t]");
            try {
                queryNo++;
                String inputFilePath;
                String outputFilePath;
                List<String> scriptParams;
                String command;

                command = query[0];
                scriptParams = new LinkedList<>();
                int scriptParamsSize = Integer.valueOf(query[1]);
                for (int i = 0; i < scriptParamsSize; i++) {
                    scriptParams.add(query[2]);
                }
                inputFilePath = query[3];
                outputFilePath = query[4];
                List<String> mapReduceParams = new LinkedList<>();
                List<String> files = new LinkedList<>();

                try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
                    String line = br.readLine();
                    while (line != null) {
                        files.add(line);
                        line = br.readLine();
                    }
                }

                for (String file : files) {
                    mapReduceParams.clear();
                    mapReduceParams.add(command);
                    mapReduceParams.addAll(scriptParams);
                    mapReduceParams.add(file);
                    mapReduceParams.add(outputFilePath);
                    MapReduceNode.main(mapReduceParams.toArray(new String[0]));
                    out.append("Query #").append(String.valueOf(queryNo)).append(" finished successfully");
                    out.flush();
                }

            } catch (IOException | InterruptedException e) {
                System.err.println("Query #" + String.valueOf(queryNo) + " finished with an exception:");
                e.printStackTrace();
            }
        }
        catch (IOException e) {
            System.err.println("IO Exception");
            e.printStackTrace();
        }
    }
}

public class MapReduceMaster {
    public static void main(String[] args) {
        int port = 666;

        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }

        try (ServerSocket ss = new ServerSocket(port)) {
            while (true) {
                new Master(ss.accept()).start();
            }
        } catch (IOException e) {
            System.err.println("Unable to start server");
            System.exit(-1);
        }
    }
}