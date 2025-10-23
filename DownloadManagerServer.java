import java.io.*;
import java.net.*;
import java.nio.channels.*;

public class DownloadManagerServer {
    static final int PORT = 9000;
    static final int THREADS = 4;
    static final File SHARED_DIR = new File("SharedFiles");
    static String mode;

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Server started on port " + PORT);
        if (!SHARED_DIR.exists()) {
            System.out.println("Creating shared directory at " + SHARED_DIR.getAbsolutePath());
            SHARED_DIR.mkdir();
        }

        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                new Thread(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                         PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {

                        handleClient(clientSocket, reader, writer);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            clientSocket.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

            } catch (Exception e) {
                e.printStackTrace();
                serverSocket.close();
            }
        }
    }

    private static void handleClient(Socket clientSocket, BufferedReader reader, PrintWriter writer) throws Exception {
        String fileName = reader.readLine();
        long startByte = Long.parseLong(reader.readLine());
        long endByte = Long.parseLong(reader.readLine());
        System.out.println(Thread.currentThread().getName() + "Client requested file: " + fileName + " from byte " + startByte + " to " + endByte);

        File requestedFile = new File(SHARED_DIR, fileName);
        if (!requestedFile.exists()) {
            writer.println(Thread.currentThread().getName() + "ERROR: File not found");
            return;
        }

        writer.println("OK");
        writer.println(requestedFile.length());
        String mode = reader.readLine();

        // ส่งไฟล์
        ServerHandler handler = new ServerHandler(clientSocket, SHARED_DIR, reader, writer, requestedFile, mode,
                startByte, endByte);
        handler.run();
    }
}

class ServerHandler implements Runnable {
    final Socket clientSocket;
    final File sharedDir;
    final File UserRequestedFile;
    String mode;
    BufferedReader reader;
    PrintWriter writer;
    long start;
    long end;

    ServerHandler(Socket socket, File dir, BufferedReader reader, PrintWriter writer, File UserRequestedFile,
            String mode, long start, long end) {
        this.clientSocket = socket;
        this.sharedDir = dir;
        this.reader = reader;
        this.writer = writer;
        this.UserRequestedFile = UserRequestedFile;
        this.mode = mode;
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        try {
            System.out.println(Thread.currentThread().getName() +
                    " handling client: " + (String) clientSocket.getInetAddress().toString().substring(1));
            System.out.println(Thread.currentThread().getName() +
                    " processing file: " + UserRequestedFile.getName());

            if (mode.equalsIgnoreCase("ZeroCopy")) {
                ZeroCopySent.sendFile(clientSocket, UserRequestedFile, start, end);
            } else if (mode.equalsIgnoreCase("Buffered")) {
                BufferedSent.sendFile(clientSocket, UserRequestedFile, start, end);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class ZeroCopySent {
    static void sendFile(Socket socket, File file, long start, long end) {
        System.out.println(Thread.currentThread().getName() + " Starting Zero-Copy download for file: " + file.getName() + " in range: " + start + " - " + end);
        try {

            Thread.sleep(100);
            
            try (FileInputStream fis = new FileInputStream(file);
                 FileChannel fileChannel = fis.getChannel();
                 WritableByteChannel socketChannel = Channels.newChannel(socket.getOutputStream())) {

                long bytesToTransfer = end - start + 1;
                long position = start;
                long totalTransferred = 0;

                System.out.println("Transferring " + bytesToTransfer + " bytes from position " + position);

                while (bytesToTransfer > 0) {
                    long transferred = fileChannel.transferTo(position, bytesToTransfer, socketChannel);
                    if (transferred == 0) {
                        Thread.sleep(10);
                        continue;
                    }
                    position += transferred;
                    bytesToTransfer -= transferred;
                    totalTransferred += transferred;
                }

                System.out.println(Thread.currentThread().getName() + 
                                 " completed Zero-Copy transfer: " + totalTransferred + " bytes for file: " + file.getName());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class BufferedSent {
    static void sendFile(Socket socket, File file, long start, long end) {
        final int BUFFER_SIZE = 64 * 1024;
        long bytesToSend = end - start + 1;
        long totalSent = 0;
        System.out.println(Thread.currentThread().getName() +" (Buffered) Sending file: " + file.getName() +" range " + start + "-" + end);

        try (BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE);
             RandomAccessFile raf = new RandomAccessFile(file, "r")) {

            raf.seek(start);
            byte[] buffer = new byte[BUFFER_SIZE];
            int readLen;

            while (bytesToSend > 0 && (readLen = raf.read(buffer, 0, (int) Math.min(BUFFER_SIZE, bytesToSend))) != -1) {
                bos.write(buffer, 0, readLen);
                bytesToSend -= readLen;
                totalSent += readLen;
            }
            bos.flush();
            System.out.println(Thread.currentThread().getName() +
                    " Buffered send completed: " + totalSent + " bytes for file: " + file.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}