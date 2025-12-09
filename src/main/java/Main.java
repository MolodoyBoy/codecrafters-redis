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
        String responseMessage = "+PONG\\r\\n";

        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            Socket clientSocket = serverSocket.accept();

            try (InputStream inputStream = clientSocket.getInputStream();
                 InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                 BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

                String readLine = bufferedReader.readLine();

                if (Objects.equals(readLine, pingCommand)) {
                    writeResponse(clientSocket, responseMessage);
                }
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void writeResponse(Socket clientSocket, String responseMessage) throws IOException {
        try (OutputStream outputStream = clientSocket.getOutputStream();
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter)) {

            bufferedWriter.write(responseMessage);
            bufferedWriter.flush();
        }
    }
}
