import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.Scanner;

public class DownloadManagerClient {
    static final int PORT = 9000;
    static final int THREADS = 4;
    final String Serverip;
    static final File DOWNLOAD_DIR = new File("Downloaded");
    static String mode;
    static long start;
    static long end;
    static long fileSize;

    DownloadManagerClient(String serverip) {
        this.Serverip = serverip;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java DownloadManagerClient.java <Server_Ip_Address>");
            return;
        }
        if (!DOWNLOAD_DIR.exists()) {
            System.out.println("Creating download directory at " + DOWNLOAD_DIR.getAbsolutePath());
            DOWNLOAD_DIR.mkdir();
        }
        Scanner sc = new Scanner(System.in);
        URL url = URI.create(args[0]).toURL();
        DownloadManagerClient client = new DownloadManagerClient(url.getHost());

        // ใช้ socket แยกสำหรับ file request
        try (Socket requestSocket = new Socket(client.Serverip, PORT);
                BufferedWriter requestWriter = new BufferedWriter(
                        new OutputStreamWriter(requestSocket.getOutputStream()));
                BufferedReader requestReader = new BufferedReader(
                        new InputStreamReader(requestSocket.getInputStream()))) {

            System.out.println("Connected to server: " + client.Serverip + " on port " + PORT);
            requestWriter.write("fileList\n");
            requestWriter.flush();
            String response;
            System.out.println("Available files on server:");
            while (!(response = requestReader.readLine()).equals("END_OF_LIST")) {
                System.out.println("- " + response);
            }
            System.out.print("Please enter file name to download: ");
            String fileName = sc.nextLine().trim();
            if (fileName.isEmpty()) {
                System.out.println("Invalid file URL.");
                return;
            }

            System.out.println("Requesting file: " + fileName);
            requestWriter.write(fileName + "\n0\n" + Long.MAX_VALUE + "\n");
            requestWriter.flush();
            response = requestReader.readLine();
            System.out.println("Server response: " + response);
            if (!response.equals("OK")) {
                System.out.println(response);
                return;
            } else {
                fileSize = Long.parseLong(requestReader.readLine());
                System.out.println("File size: " + fileSize + " bytes");
                System.out.println("Download Mode <ZeroCopy/Buffered>: ");
                mode = sc.nextLine().trim();
            }
            requestWriter.write(mode + "\n");
            requestWriter.flush();
            if (mode.equalsIgnoreCase("ZeroCopy")) {
                System.out.println("Downloading using ZeroCopy mode.");
            } else if (mode.equalsIgnoreCase("Buffered")) {
                System.out.println("Downloading using Buffered mode.");
            }

            // สร้าง array เก็บ threads
            Thread[] downloadThreads = new Thread[THREADS];

            for (int i = 0; i < THREADS; i++) {
                final int threadIndex = i;
                final long thisStart = i * (fileSize / THREADS);
                final long thisEnd;
                if (i == THREADS - 1) {
                    thisEnd = fileSize - 1;
                } else {
                    thisEnd = ((i + 1) * (fileSize / THREADS)) - 1;
                }

                downloadThreads[i] = new Thread(() -> {
                    try {
                        Socket threadSocket = new Socket(client.Serverip, PORT);
                        BufferedWriter threadWriter = new BufferedWriter(
                                new OutputStreamWriter(threadSocket.getOutputStream()));
                        BufferedReader threadReader = new BufferedReader(
                                new InputStreamReader(threadSocket.getInputStream()));
                        ClientHandler handler = new ClientHandler(threadSocket, DOWNLOAD_DIR, mode, threadReader,
                                threadWriter, thisStart, thisEnd, fileName, threadIndex);
                        handler.run();

                        threadSocket.close();
                        System.out.println("Thread " + threadIndex + " completed");

                    } catch (Exception e) {
                        System.out.println("Error in thread " + threadIndex + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                });
                downloadThreads[i].start();
            }

            long startTime = System.currentTimeMillis();
            System.out.println("Downloading... Please wait.");
            for (int i = 0; i < THREADS; i++) {
                try {
                    downloadThreads[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("All downloads completed!");
            if (mode.equalsIgnoreCase("ZeroCopy")) {
                System.out.println("Download time in zerocopy: " +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            } else {
                System.out.println("Download time in buffered: " +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }

            if (mode.equalsIgnoreCase("ZeroCopy")) {
                MergeFilesZeroCopy.mergeFiles(DOWNLOAD_DIR, fileName, THREADS);
            } else if (mode.equalsIgnoreCase("Buffered")) {
                MergeFilesBuffer.mergeFiles(DOWNLOAD_DIR, fileName, THREADS);
            }
            System.out.println("Download finished. Press Enter to exit...");
            sc.nextLine();
            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class ClientHandler implements Runnable {
    Socket socket;
    final File DownloadDir;
    String mode;
    BufferedReader reader;
    BufferedWriter writer;
    long start;
    long end;
    String UserRequestedFile;
    int threadId;

    ClientHandler(Socket socket, File dir, String mode, BufferedReader reader, BufferedWriter writer, long start,
            long end, String UserRequestedFile, int threadIndex) throws Exception {

        this.socket = socket;
        this.DownloadDir = dir;
        this.reader = reader;
        this.writer = writer;
        this.mode = mode;
        this.start = start;
        this.end = end;
        this.UserRequestedFile = UserRequestedFile;
        this.threadId = threadIndex;
    }

    @Override
    public void run() {
        try {
            writer.write("Download\n");
            writer.flush();
            writer.write(UserRequestedFile + "\n" + start + "\n" + end + "\n" + mode + "\n");
            writer.flush();

            String response = reader.readLine();
            if (!response.equals("OK")) {
                System.out.println("Error: " + response);
                return;
            }

            reader.readLine();

            if (mode.equalsIgnoreCase("ZeroCopy")) {
                String chunkFileName = "chunk_" + threadId + "_" + UserRequestedFile;
                File outputFile = new File(DownloadDir, chunkFileName);

                ZeroCopyReceveive.receiveFile(Channels.newChannel(socket.getInputStream()), outputFile,
                        end - start + 1);
            } else if (mode.equalsIgnoreCase("Buffered")) {
                String chunkFileName = "chunk_" + threadId + "_" + UserRequestedFile;
                File outputFile = new File(DownloadDir, chunkFileName);

                BufferedReceveive.receiveFile(socket.getInputStream(), outputFile, end - start + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MergeFilesZeroCopy {
    public static void mergeFiles(File downloadDir, String originalFileName, int numberOfChunks) {
        long startTime = System.currentTimeMillis();
        File outputFile = new File(downloadDir, "ZeroCopy_" + originalFileName);

        try (FileChannel outChannel = new FileOutputStream(outputFile, false).getChannel()) {
            for (int i = 0; i < numberOfChunks; i++) {
                String chunkFileName = "chunk_" + i + "_" + originalFileName;
                File chunkFile = new File(downloadDir, chunkFileName);

                if (!chunkFile.exists()) {
                    System.out.println("Warning: Chunk file not found: " + chunkFileName);
                    continue;
                }

                try (FileChannel inChannel = new FileInputStream(chunkFile).getChannel()) {
                    long size = inChannel.size();
                    long transferred = 0;

                    // ใช้ transferTo แบบ Zero-Copy
                    while (transferred < size) {
                        transferred += inChannel.transferTo(transferred, size - transferred, outChannel);
                    }

                    System.out.println("Merged chunk " + i + ": " + chunkFileName);
                } catch (IOException e) {
                    System.out.println("Error reading chunk " + i + ": " + e.getMessage());
                    continue;
                }

                if (!chunkFile.delete()) {
                    System.out.println("Warning: Could not delete chunk file: " + chunkFileName);
                }
            }

            System.out.println("Files merged successfully into: " + outputFile.getAbsolutePath());
            System.out.println("Merge time (ZeroCopy): " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        } catch (IOException e) {
            System.out.println("Error creating output file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

class MergeFilesBuffer {
    public static void mergeFiles(File downloadDir, String originalFileName, int numberOfChunks) {
        long startTime = System.currentTimeMillis();
        File outputFile = new File(downloadDir, "Buffered_" + originalFileName);
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            for (int i = 0; i < numberOfChunks; i++) {
                String chunkFileName = "chunk_" + i + "_" + originalFileName;
                File chunkFile = new File(downloadDir, chunkFileName);

                if (!chunkFile.exists()) {
                    System.out.println("Warning: Chunk file not found: " + chunkFileName);
                    continue;
                }

                try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(chunkFile))) {
                    byte[] buffer = new byte[64 * 1024]; // 64KB buffer
                    int bytesRead;
                    while ((bytesRead = bis.read(buffer)) != -1) {
                        bos.write(buffer, 0, bytesRead);
                    }
                    System.out.println("Merged chunk " + i + ": " + chunkFileName);
                } catch (IOException e) {
                    System.out.println("Error reading chunk " + i + ": " + e.getMessage());
                    continue;
                }

                if (!chunkFile.delete()) {
                    System.out.println("Warning: Could not delete chunk file: " + chunkFileName);
                }
            }
            System.out.println("Files merged successfully into: " + outputFile.getAbsolutePath());
            System.out.println("Merge time (Buffered): " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        } catch (IOException e) {
            System.out.println("Error creating output file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

class ZeroCopyReceveive {
    public static void receiveFile(ReadableByteChannel socketChannel, File outputFile, long expectedSize) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(outputFile, "rw");
                FileChannel fileChannel = randomAccessFile.getChannel()) {

            long totalTransferred = 0;
            long startTime = System.currentTimeMillis();

            System.out.println("Starting to receive " + expectedSize + " bytes...");

            while (totalTransferred < expectedSize) {
                long transferred = fileChannel.transferFrom(socketChannel, totalTransferred,
                        expectedSize - totalTransferred);

                if (transferred == 0) {
                    if (System.currentTimeMillis() - startTime > 30000) { // 30 วินาที
                        System.out.println("Timeout waiting for data");
                        break;
                    }
                    Thread.sleep(10);
                    continue;
                }

                totalTransferred += transferred;
                System.out.println("Received: " + totalTransferred + "/" + expectedSize + " bytes");
            }

            System.out.println("Expected: " + expectedSize + " bytes");
            System.out.println("Received: " + totalTransferred + " bytes");

            if (totalTransferred == expectedSize) {
                System.out.println(Thread.currentThread().getName() + " completed download: " + outputFile.getName());
            } else {
                System.out.println("Size mismatch in " + outputFile.getName() + " (difference: "
                        + (totalTransferred - expectedSize) + " bytes)");
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class BufferedReceveive {
    public static void receiveFile(InputStream inputStream, File outputFile, long expectedSize) {
        final int BUFFER_SIZE = 64 * 1024; // 64KB
        long totalReceived = 0;

        try (BufferedInputStream bis = new BufferedInputStream(inputStream, BUFFER_SIZE);
                BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile), BUFFER_SIZE)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int readLen;
            while (totalReceived < expectedSize &&
                    (readLen = bis.read(buffer, 0, (int) Math.min(BUFFER_SIZE, expectedSize - totalReceived))) != -1) {
                bos.write(buffer, 0, readLen);
                totalReceived += readLen;
            }
            bos.flush();

            System.out.println("Buffered receive completed: " + totalReceived + "/" + expectedSize + " bytes for "
                    + outputFile.getName());

            if (totalReceived != expectedSize) {
                System.err.println("Warning: Received size mismatch for " + outputFile.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}