import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final String RESPONSE_MESSAGE = "+PONG\r\n";

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;

        String pingCommand = "PING";

        try (ServerSocket serverSocket = new ServerSocket();
             ExecutorService executorService = Executors.newFixedThreadPool(10)) {

            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            while (!executorService.isShutdown()) {
                Socket clientSocket = serverSocket.accept();

                executorService.submit(() -> {
                    try {
                        handleClient(clientSocket);
                    } catch (IOException e) {
                        System.out.println("IOException in client handler: " + e.getMessage());
                    } finally {
                        try {
                            if (!clientSocket.isClosed()) {
                                clientSocket.close();
                            }
                        } catch (IOException e) {
                            System.out.println("IOException while closing client socket: " + e.getMessage());
                        }
                    }
                });
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) throws IOException {
        System.out.println("Client handled: " + clientSocket.getRemoteSocketAddress());

        try (BufferedReader bufferedReader = getBufferedReader(clientSocket);
             BufferedWriter bufferedWriter = getBufferedWriter(clientSocket)) {

            while (true) {
                try {
                    String readLine = bufferedReader.readLine();
                    if (readLine == null || readLine.isBlank()) {
                        continue;
                    }

                    System.out.println("Line received: " + readLine);

                    if ("PING".equalsIgnoreCase(readLine)) {
                        bufferedWriter.write(RESPONSE_MESSAGE);
                        bufferedWriter.flush();
                    }
                } catch (IOException e) {
                    System.out.println("IOException while handling client: " + e.getMessage());
                }
            }
        }
    }

    private static BufferedReader getBufferedReader(Socket clientSocket) throws IOException {
        return new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    private static BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
    }
}
