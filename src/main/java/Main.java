import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class Main {

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;

        String pingCommand = "PING";
        String responseMessage = "+PONG\r\n";

        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            try (Socket clientSocket = serverSocket.accept();
                 BufferedReader bufferedReader = getBufferedReader(clientSocket);
                 BufferedWriter bufferedWriter = getBufferedWriter(clientSocket)) {

                while (true) {
                    try {
                        String readLine = bufferedReader.readLine();
                        if (readLine.isBlank()) {
                            continue;
                        }

                        if (readLine.equalsIgnoreCase("PING")) {
                            bufferedWriter.write(responseMessage);
                            bufferedWriter.flush();
                        }
                    } catch (IOException e) {
                        System.out.println("IOException while handling client: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static BufferedReader getBufferedReader(Socket clientSocket) throws IOException {
        return new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    private static BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
    }
}
