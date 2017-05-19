import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.util.*;

/**
 * MapReduce
 * Performs map or reduce depending on the command from cmd
 */

class Node implements Comparable<Node> {
    private String key;
    private String value;

    public Node(String str){
        StringTokenizer tokenizer = new StringTokenizer(str, "\t");
        key = tokenizer.nextToken();
        value = tokenizer.nextToken();
        while (tokenizer.hasMoreTokens()) {
            value += ('\t' + tokenizer.nextToken());
        }
    }

    @Override
    public int compareTo(Node o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return  key + '\t' + value;
    }
}

class ExternalSort {
    private static final int CAPACITY = 100000;

    public static int getCAPACITY() {
        return CAPACITY;
    }

    private List<String> filePaths = new ArrayList<>();

    public ExternalSort(String inputFilePath) throws IOException {
        try (RandomAccessFile inputFile = new RandomAccessFile(inputFilePath, "r")) {
            filePaths.add("sorter0.tmp");
            File tmp = new File("sorter0.tmp");
            tmp.deleteOnExit();
            int i = 1;
            while (inputFile.getFilePointer() != inputFile.length()) {
                String chunk = read(inputFile, CAPACITY);
                createFile(i, chunk);
                i++;
            }
        }
    }

    public String sort() throws IOException {

        while(filePaths.size() > 2) {
            merge(filePaths.get(1), filePaths.get(2), filePaths.get(0));
            String tmp = filePaths.get(1);
            filePaths.set(1, filePaths.get(0));
            filePaths.set(0, tmp);

            int i = 3;
            int newSize = 1;
            for (; i < filePaths.size(); i += 2){
                if (i + 1 < filePaths.size()){
                    merge(filePaths.get(i), filePaths.get(i + 1), filePaths.get((i + 1)/2));
                }else{
                    tmp = filePaths.get((i + 1)/2);
                    filePaths.set((i + 1)/2, filePaths.get(i));
                    filePaths.set(i, tmp);
                }
                newSize = (i + 1)/2;
            }
            for (int j = filePaths.size() - 1; j > newSize; j--){
                filePaths.remove(j);
            }
        }
        return filePaths.get(1);
    }

    private void merge(String firstFilePath, String secondFilePath, String resFilePath) throws IOException {
        try (RandomAccessFile first = new RandomAccessFile(firstFilePath, "r");
             RandomAccessFile second = new RandomAccessFile(secondFilePath, "r");
             BufferedWriter res = new BufferedWriter(new FileWriter(resFilePath))) {
            Queue<Node> contentFirst = new PriorityQueue<>(Node::compareTo);
            Queue<Node> contentSecond = new PriorityQueue<>(Node::compareTo);
            String firstChunk = read(first, CAPACITY / 2);
            String secondChunk = read(second, CAPACITY / 2);
            split(firstChunk, contentFirst);
            split(secondChunk, contentSecond);
            while (true) {
                while (contentFirst.size() != 0 && contentSecond.size() != 0) {
                    if (contentFirst.peek().compareTo(contentSecond.peek()) < 0) {
                        res.write(contentFirst.poll().toString() + '\n');
                    } else {
                        res.write(contentSecond.poll().toString() + '\n');
                    }
                }
                if (contentFirst.size() == 0) {
                    firstChunk = read(first, CAPACITY / 2);
                    split(firstChunk, contentFirst);
                    if (contentFirst.size() == 0) {
                        while (true) {
                            secondChunk = read(second, CAPACITY / 2);
                            split(secondChunk, contentSecond);
                            if (contentSecond.size() == 0) {
                                break;
                            }
//                            split(secondChunk, contentSecond);
                            while (contentSecond.size() != 0) {
                                res.write(contentSecond.poll().toString() + '\n');
                            }
                        }
                        break;
                    }
                    //split(firstChunk, contentFirst);
                }
                else if (contentSecond.size() == 0) {
                    secondChunk = read(second, CAPACITY / 2);
                    split(secondChunk, contentSecond);
                    if (secondChunk.charAt(0) == 0) {
                        while (true) {
                            firstChunk = read(first, CAPACITY / 2);
                            split(firstChunk, contentFirst);
                            if (contentFirst.size() == 0) {
//                            if (firstChunk.charAt(0) == 0) {
                                break;
                            }
//                            split(firstChunk, contentFirst);
                            while (contentFirst.size() != 0) {
                                res.write(contentFirst.poll().toString() + '\n');
                            }
                        }
                        break;
                    }
                }
//                res.flush();
            }
        }
    }

    private void split(String chunk, Queue<Node> content) {
        StringTokenizer st = new StringTokenizer(chunk, "\n\r\u0000");
        while (st.hasMoreTokens()) {
            content.add(new Node(st.nextToken()));
        }
    }

    private void createFile(int idx, String chunk) throws IOException{
        filePaths.add("sorter" + idx + ".tmp");
        File sorter = new File(filePaths.get(idx));
        sorter.deleteOnExit();
        Queue<Node> content = new PriorityQueue<>();
        split(chunk, content);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sorter))) {
            while (content.size() != 0) {
                bw.write(content.poll().toString() + "\n");
            }
//            bw.flush();
        }
    }

    private String read(RandomAccessFile inputFile, int size) throws IOException {
        byte[] content = new byte[size];
        inputFile.read(content);
        String endOfLine = inputFile.readLine();
        if (endOfLine != null)
            return new String(content) + endOfLine;
        return new String(content);
    }
}

class NodeThread extends Thread {
    private static int outputFileNum = 0;
    private Socket socket = null;

    public NodeThread(Socket socket) {
        super("NodeThread");
        this.socket = socket;
    }

    public void run() {

        try (
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream()));
        ) {
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                String inputFilePath;
                String outputFilePath = String.valueOf(outputFileNum++);
                List<String> striptParams;
                String command;
                String[]args = inputLine.split("[ \t]");
                command = args[0];
                String[] params = Arrays.copyOfRange(args, 1, args.length - 2);
                striptParams = Arrays.asList(params);
                inputFilePath = args[args.length - 2];
                outputFilePath += args[args.length - 1];

                Process process;
                ProcessBuilder builder =  new ProcessBuilder(striptParams);
                String line;

                try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFilePath, true))) {
                    switch (command) {
                        case "reduce":
                            ExternalSort sorted = new ExternalSort(inputFilePath);
                            try (BufferedReader inputReader = new BufferedReader(new FileReader(sorted.sort()))) {
                                List<Node> sameKey = new ArrayList<>();
                                sameKey.add(new Node(inputReader.readLine()));
                                while ((line = inputReader.readLine()) != null) {
                                    Node curNode = new Node(line);
                                    if (curNode.compareTo(sameKey.get(0))== 0) {
                                        sameKey.add(curNode);
                                    } else {
                                        process = builder.start();

                                        try (OutputStream stdin = process.getOutputStream();
                                             InputStream stdout = process.getInputStream();

                                             BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
                                             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {

                                            for (Node st : sameKey) {
                                                writer.write(st.toString() + '\n');
                                            }

//                                    writer.flush();
                                            writer.close();
                                            process.waitFor();
                                            String res;

                                            while ((res = reader.readLine()) != null) {
                                                outputWriter.append(res).append(String.valueOf('\n'));
                                            }
                                            sameKey.clear();
                                            sameKey.add(curNode);
                                        }
                                    }
                                }
                                process = builder.start();

                                try (OutputStream stdin = process.getOutputStream();
                                     InputStream stdout = process.getInputStream();

                                     BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
                                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {

                                    for (Node st : sameKey) {
                                        writer.write(st.toString() + '\n');
                                    }
                                    writer.close();
                                    process.waitFor();

                                    while ((line = reader.readLine()) != null) {
                                        outputWriter.append(line).append(String.valueOf('\n'));
                                    }
                                }
                            }
                            break;

                        case "map":
                            try (BufferedReader inputReader = new BufferedReader(new FileReader(inputFilePath), ExternalSort.getCAPACITY())) {
                                while ((line = inputReader.readLine()) != null) {
                                    process = builder.start();

                                    try (OutputStream stdin = process.getOutputStream();
                                         InputStream stdout = process.getInputStream();

                                         BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
                                         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {

                                        writer.write(line);
                                        writer.close();

                                        process.waitFor();

                                        while ((line = reader.readLine()) != null) {
                                            outputWriter.append(line).append(String.valueOf('\n'));
                                        }
                                    }
                                }
                            }
                            break;

                        default:
                            throw new IllegalArgumentException("No such command. map/reduce required");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                out.append(outputFilePath);
                out.append(socket.getLocalSocketAddress().toString());
                out.append('\n');
                out.flush();
            }
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

public class MapReduceNode {

    public static void main(String[] args) throws IOException, InterruptedException {
//        String inputFilePath;
//        String outputFilePath;
//        List<String> striptParams;
//        String command;
        int portNumber = 777;

        if (args.length == 1) {
            portNumber = Integer.parseInt(args[0]);
        }

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            while (true) {
                new NodeThread(serverSocket.accept()).start();
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + portNumber);
            System.exit(-1);
        }
//        else if (args.length >= 4) {
//            command = args[0];
//            String[] params = Arrays.copyOfRange(args, 1, args.length - 2);
//            striptParams = Arrays.asList(params);
//            inputFilePath = args[args.length - 2];
//            outputFilePath = args[args.length - 1];
//        }

//        Process process;
//        ProcessBuilder builder =  new ProcessBuilder(striptParams);
//        String line;
//
//        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFilePath, true))) {
//            switch (command) {
//                case "reduce":
//                    ExternalSort sorted = new ExternalSort(inputFilePath);
//                    try (BufferedReader inputReader = new BufferedReader(new FileReader(sorted.sort()))) {
//                        List<Node> sameKey = new ArrayList<>();
//                        sameKey.add(new Node(inputReader.readLine()));
//                        while ((line = inputReader.readLine()) != null) {
//                            Node curNode = new Node(line);
//                            if (curNode.compareTo(sameKey.get(0))== 0) {
//                                sameKey.add(curNode);
//                            } else {
//                                process = builder.start();
//
//                                try (OutputStream stdin = process.getOutputStream();
//                                     InputStream stdout = process.getInputStream();
//
//                                     BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
//                                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {
//
//                                    for (Node st : sameKey) {
//                                        writer.write(st.toString() + '\n');
//                                    }
//
////                                    writer.flush();
//                                    writer.close();
//                                    process.waitFor();
//                                    String res;
//
//                                    while ((res = reader.readLine()) != null) {
//                                        outputWriter.append(res).append(String.valueOf('\n'));
//                                    }
//                                    sameKey.clear();
//                                    sameKey.add(curNode);
//                                }
//                            }
//                        }
//                        process = builder.start();
//
//                        try (OutputStream stdin = process.getOutputStream();
//                             InputStream stdout = process.getInputStream();
//
//                             BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
//                             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {
//
//                            for (Node st : sameKey) {
//                                writer.write(st.toString() + '\n');
//                            }
//                            writer.close();
//                            process.waitFor();
//
//                            while ((line = reader.readLine()) != null) {
//                                outputWriter.append(line).append(String.valueOf('\n'));
//                            }
//                        }
//                    }
//                    break;
//
//                case "map":
////                    try (RandomAccessFile inputFile = new RandomAccessFile(inputFilePath, "r")) {
////                        String buf;
////                        while (inputFile.getFilePointer() != inputFile.length()) {
////                            byte[] content = new byte[ExternalSort.getCAPACITY()];
////                            inputFile.read(content);
////                            String endOfLine = inputFile.readLine();
////                            if (endOfLine != null)
////                                buf = new String(content) + endOfLine + '\n';
////                            else {
////                                buf = new String(content) + '\n';
////                            }
////
////                            process = builder.start();
////
////                            try (OutputStream stdin = process.getOutputStream();
////                                 InputStream stdout = process.getInputStream();
////
////                                 BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), 2 * ExternalSort.getCAPACITY());
////                                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), 2  *ExternalSort.getCAPACITY())) {
////                                writer.write(buf);
//////                                //writer.flush();
////                                writer.close();
////
////                                process.waitFor();
////
////                                while ((line = reader.readLine()) != null) {
////                                    outputWriter.append(line).append('\n');
////                                }
////                            }
//////                            outputWriter.flush();
////                        }
////                    }
//                        try (BufferedReader inputReader = new BufferedReader(new FileReader(inputFilePath), ExternalSort.getCAPACITY())) {
//                            while ((line = inputReader.readLine()) != null) {
//                                process = builder.start();
//
//                                try (OutputStream stdin = process.getOutputStream();
//                                     InputStream stdout = process.getInputStream();
//
//                                     BufferedReader reader = new BufferedReader(new InputStreamReader(stdout), ExternalSort.getCAPACITY());
//                                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin), ExternalSort.getCAPACITY())) {
//
//                                    writer.write(line);
//                                    writer.close();
//
//                                    process.waitFor();
//
//                                    while ((line = reader.readLine()) != null) {
//                                        outputWriter.append(line).append(String.valueOf('\n'));
//                                    }
//                                }
//                            }
//                        }
//                    break;
//
//                default:
//                    throw new IllegalArgumentException("No such command. map/reduce required");
//            }
//        }
        System.out.println("Success");
    }
}
