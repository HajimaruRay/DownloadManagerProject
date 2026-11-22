# Download Manager Project

A Java socket-based download manager that compares two file-transfer implementations:

- **Buffered transfer** - uses 64 KB buffered input/output streams.
- **Zero-copy transfer** - uses Java NIO file channels and socket channels.

The client splits a file into four byte ranges, downloads the ranges in four parallel
connections, and merges the completed chunks into a single file.

## Project files

- `DownloadManagerServer.java` - starts the TCP server on port `9000`, lists available
  files, and serves requested byte ranges.
- `DownloadManagerClient.java` - connects to the server, selects a file and transfer
  mode, downloads in parallel, and merges the downloaded chunks.
- `SharedFiles/` - place files here to make them available for download.
- `Downloaded/` - receives chunk files and the merged download.

## Requirements

- Java 11 or newer
- A terminal on macOS, Linux, or Windows

Check the Java installation:

```bash
java -version
javac -version
```

## Build

From the project directory, compile both programs:

```bash
javac DownloadManagerServer.java DownloadManagerClient.java
```

This creates the `.class` files used by the Java runtime.

## Run locally

### 1. Start the server

Create `SharedFiles` if it does not already exist, and copy one or more files into it:

```bash
mkdir -p SharedFiles
cp /path/to/example-file SharedFiles/
```

In one terminal, start the server:

```bash
java DownloadManagerServer
```

The server prints `Server started on port 9000` and waits for client connections.

### 2. Start the client

Open a second terminal in the project directory and run:

```bash
java DownloadManagerClient http://127.0.0.1
```

Replace `127.0.0.1` with the server machine's IP address when connecting across
machines. The server must be reachable on TCP port `9000`, and the port should be
allowed through any firewall.

## Using the client

1. The client lists the files in the server's `SharedFiles` directory.
2. Enter the exact file name shown by the server.
3. Enter `Buffered` or `ZeroCopy` as the download mode.
4. Wait for all four download threads to finish.

The final file is written to `Downloaded/` using one of these names:

- `Buffered_<original-file-name>` for buffered downloads
- `ZeroCopy_<original-file-name>` for zero-copy downloads

The temporary `chunk_*` files are removed after the merge completes.

## Run over a network

On the server machine:

```bash
java DownloadManagerServer
```

Find the server machine's IP address, then run this on the client machine:

```bash
java DownloadManagerClient http://<SERVER_IP>
```

For example:

```bash
java DownloadManagerClient http://192.168.1.50
```

Both the server and client require the project directory as their working directory
because the program resolves `SharedFiles/` and `Downloaded/` using relative paths.

## Clean generated files

Remove Java build artifacts and downloaded data:

```bash
rm -f *.class
rm -rf Downloaded
```

