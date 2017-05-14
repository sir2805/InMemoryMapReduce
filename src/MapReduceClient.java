import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

//class ClientThread extends Thread {
//    private Socket socket;
//    private BufferedReader in;
//    private PrintWriter out;
//    private String query;
//
//    public ClientThread(InetAddress addr, int port, String query) {
//        try {
//            socket = new Socket(addr, port);
//            this.query = query;
//        }
//        catch (IOException e) {
//            System.err.println("Socket failed");
//            // Если создание сокета провалилось,
//            // ничего ненужно чистить.
//        }
//        try {
//            in = new BufferedReader(new InputStreamReader(socket
//                    .getInputStream()));
//            // Включаем автоматическое выталкивание:
//            out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
//                    socket.getOutputStream())), true);
//            start();
//        }
//        catch (IOException e) {
//            // Сокет должен быть закрыт при любой
//            // ошибке, кроме ошибки конструктора сокета:
//            try {
//                socket.close();
//            }
//            catch (IOException e2) {
//                System.err.println("Socket not closed");
//            }
//        }
//        // В противном случае сокет будет закрыт
//        // в методе run() нити.
//    }
//
//    public void run() {
//        try {
////                out.println("Client " + id + ": " + i);
//            out.append(query);
//            out.flush(); // заставляем поток закончить передачу данных.
//
//            String str = in.readLine();
//            System.out.println(str);
//        } catch (IOException e) {
//            System.err.println("IO Exception");
//        }
//
//        finally {
//            // Всегда закрывает:
//            try {
//                socket.close();
//                in.close();
//                out.close();
//            }
//            catch (IOException e) {
//                System.err.println("Socket not closed");
//            }
////            threadcount--; // Завершаем эту нить
//        }
//            out.println("END");
//    }
//}


public class MapReduceClient {
    public static void main(String[] arg) {

        try (BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in))) {
            System.out.println("Input MapReduce params separated by space");
            String userInput;
            while ((userInput = keyboard.readLine()) != null) {
                String[]args = userInput.split("[ \t]");
                String inputFilePath = null;
                String outputFilePath = null;
                List<String> scriptParams = null;
                String command = null;
                String[] host_portNo;
                String address;
                int serverPort = 0;
                InetAddress ipAddress = null;

                try {
                    command = args[0];
                    String[] params = Arrays.copyOfRange(args, 1, args.length - 3);
                    scriptParams = Arrays.asList(params);
                    inputFilePath = args[args.length - 3];
                    outputFilePath = args[args.length - 2];
                    host_portNo = args[args.length - 1].split("[:]");

                    address = host_portNo[0];
                    serverPort = Integer.valueOf(host_portNo[1]);
                    ipAddress = InetAddress.getByName(address);
                } catch (Exception e) {
                    System.err.println("Invalid params");
                    e.printStackTrace();
                }
                StringBuilder query = new StringBuilder();
                query.append(command); // отсылаем введенную строку текста серверу.
                query.append('\t');

                query.append(String.valueOf(scriptParams.size()));
                query.append('\t');

                for (String param : scriptParams) {
                    query.append(param);
                    query.append('\t');
                }
                query.append(inputFilePath);
                query.append('\t');
                query.append(outputFilePath);

                try (Socket socket = new Socket(ipAddress, serverPort);
                        BufferedReader in = new BufferedReader
                                (new InputStreamReader(socket.getInputStream()));
                        PrintWriter out = new PrintWriter(new BufferedWriter
                                (new OutputStreamWriter(socket.getOutputStream())), true)
                ) {
                    out.println(query);

                    String str = in.readLine();
                    System.out.println(str);
                    System.out.println("Input MapReduce params separated by space");
                }
//                new ClientThread(ipAddress, serverPort, query.toString());
//                if (args.length < 5) {
//                    throw new IllegalArgumentException();
//                } else {
//                    try {
//                        command = args[0];
//                        String[] params = Arrays.copyOfRange(args, 1, args.length - 3);
//                        scriptParams = Arrays.asList(params);
//                        inputFilePath = args[args.length - 3];
//                        outputFilePath = args[args.length - 2];
//                        host_portNo = args[args.length - 1].split("[:]");
//
//                        address = host_portNo[0];
//                        serverPort = Integer.valueOf(host_portNo[1]);
//                        ipAddress = InetAddress.getByName(address);
//                    } catch (Exception e) {
//                        System.err.println("Invalid params");
//                        continue;
//                    }
//                }
//
//                try (Socket socket = new Socket(ipAddress, serverPort);
//                     InputStream sin = socket.getInputStream();
//                     OutputStream sout = socket.getOutputStream();
//
//                     DataInputStream in = new DataInputStream(sin);
//                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(sout), ExternalSort.getCAPACITY()))
//                {
//                    writer.append(command); // отсылаем введенную строку текста серверу.
//                    writer.append('\t');
//
//                    writer.append(String.valueOf(scriptParams.size()));
//                    writer.append('\t');
//
//                    for (String param : scriptParams) {
//                        writer.append(param);
//                        writer.append('\t');
//                    }
//                    writer.append(inputFilePath);
//                    writer.append('\t');
//                    writer.append(outputFilePath);
//                    writer.append('\n');
//
//                    writer.flush(); // заставляем поток закончить передачу данных.
//
//                    System.out.println(in.readUTF()); // ждем пока сервер отошлет строку текста.
//
//                }  catch (Exception e) {
//                    System.err.println("Socket problems");
//                    continue;
//                }
            }
        } catch (Exception e) {
            System.err.println("Unable to get std::in");
            e.printStackTrace();
        }
    }
}