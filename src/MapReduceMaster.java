import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class FileLocator {
    private static final String INPUTFILEPATHS = "inputFilePaths.txt";
    private static Map<String, LinkedList<String>> filePaths;
    private FileLocator() {}

    static void setFilePaths() {
        filePaths = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(INPUTFILEPATHS))) {
            String line = br.readLine();
            while (line != null) {
                String[]file_node = line.split("[/]");
                LinkedList<String>chunks = new LinkedList<>();
                for (int i = 1; i < file_node.length; i++) {
                    chunks.add(file_node[i]);
//                    String[] file_chunks = file_node[1].split("[#]");
                }
                filePaths.put(file_node[0], chunks);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    static LinkedList<String> getHostPortNo(String filename) {
        if (filePaths == null) {
            setFilePaths();
        }
        return filePaths.get(filename);
    }
}

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


//            address = socket.getLocalAddress().getHostAddress();

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

                LinkedList<String>hostPortNoChunks = FileLocator.getHostPortNo(inputFilePath);

                for (String chunk : hostPortNoChunks) {
                    String[] hostPortNo = chunk.split("[:]");
                    String address = hostPortNo[0];
                    int portNo = Integer.parseInt(hostPortNo[1]);
                    String fileName = hostPortNo[2];

                    try (FileWriter pathsWriter = new FileWriter(outputFilePath, true)) {


                        try (
                                Socket echoSocket = new Socket(address, portNo);
                                BufferedWriter nout =
                                        new BufferedWriter(new OutputStreamWriter(echoSocket.getOutputStream()));
                                BufferedReader nin =
                                        new BufferedReader(
                                                new InputStreamReader(echoSocket.getInputStream()));
                        ) {
                            nout.append(command);
                            nout.append('\t');
                            for (String scriptParam : scriptParams) {
                                nout.append(scriptParam);
                                nout.append('\t');
                            }
                            nout.append(fileName);
                            nout.append('\t');
                            nout.append(outputFilePath);
                            nout.append('\n');
                            nout.flush();

                            String path = nin.readLine();
                            System.out.println(path + " created");
                            pathsWriter.write(path + '\n');
                            out.append('\n');
//                            out.flush();

                        } catch (UnknownHostException e) {
                            System.err.println("Don't know about host " + address);
                            System.exit(1);
                        } catch (IOException e) {
                            System.err.println("Couldn't get I/O for the connection to " +
                                    address);
                            System.exit(1);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Query #" + String.valueOf(queryNo) + " finished with an exception:");
                e.printStackTrace();
            } catch (RuntimeException e) {
                out.append("No such file on any machine, try put it again");
                out.flush();
                System.err.println("No such file on any machine, try put it again");
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
        int port = 667;

        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }

        FileLocator.setFilePaths();

//        List<String>inputPaths = new LinkedList<>();
//
//        try(BufferedReader br = new BufferedReader(new FileReader("inputFilePaths.txt"))) {
//            String line = br.readLine();
//            while (line != null) {
//                inputPaths.add(line);
//                line = br.readLine();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

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