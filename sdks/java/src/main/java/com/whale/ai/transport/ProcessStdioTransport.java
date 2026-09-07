package com.whale.ai.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Transport communicating with whale-daemon subprocess via line-buffered stdin/stdout.
 */
public class ProcessStdioTransport implements Transport {
    private static final Logger logger = LoggerFactory.getLogger(ProcessStdioTransport.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Process process;
    private final BufferedWriter writer;
    private final BufferedReader reader;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object writeLock = new Object();
    private Consumer<JsonNode> messageHandler;
    private Thread stdoutThread;
    private Thread stderrThread;

    public ProcessStdioTransport(String customDaemonPath, List<String> extraArgs, String logLevel) throws IOException {
        Path binaryPath = findDaemonBinary(customDaemonPath);
        List<String> cmd = new ArrayList<>();
        cmd.add(binaryPath.toAbsolutePath().toString());
        cmd.add("--listen");
        cmd.add("stdio");
        cmd.add("--log-level");
        cmd.add(logLevel != null ? logLevel : "info");
        if (extraArgs != null) {
            cmd.addAll(extraArgs);
        }

        logger.info("Spawning whale-daemon process: {}", String.join(" ", cmd));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        this.process = pb.start();

        this.writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        this.reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));

        startReaderThreads();
    }

    public static Path findDaemonBinary(String customPath) throws IOException {
        if (customPath != null && !customPath.trim().isEmpty()) {
            Path p = Paths.get(customPath).toAbsolutePath();
            if (Files.isRegularFile(p) && Files.isExecutable(p)) {
                return p;
            }
            throw new IOException("Specified daemon binary not found or not executable: " + p);
        }

        String envPath = System.getenv("WHALE_DAEMON_PATH");
        if (envPath != null && !envPath.trim().isEmpty()) {
            Path p = Paths.get(envPath).toAbsolutePath();
            if (Files.isRegularFile(p) && Files.isExecutable(p)) {
                return p;
            }
            logger.warn("WHALE_DAEMON_PATH set to {}, but binary is not found or executable", envPath);
        }

        // Search upward from user directory / current directory
        Path current = Paths.get("").toAbsolutePath();
        Path candidate = searchUpwardForDaemon(current);
        if (candidate != null) {
            return candidate;
        }

        // Search from user directory
        String userDir = System.getProperty("user.dir");
        if (userDir != null) {
            candidate = searchUpwardForDaemon(Paths.get(userDir).toAbsolutePath());
            if (candidate != null) {
                return candidate;
            }
        }

        // Search from class location (e.g. sdks/java/...)
        try {
            java.net.URI uri = ProcessStdioTransport.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path codeSourcePath = Paths.get(uri).toAbsolutePath();
            candidate = searchUpwardForDaemon(codeSourcePath);
            if (candidate != null) {
                return candidate;
            }
        } catch (Exception ignored) {
        }

        // Check system PATH
        String systemPath = System.getenv("PATH");
        if (systemPath != null) {
            for (String part : systemPath.split(File.pathSeparator)) {
                Path p = Paths.get(part, "whale-daemon");
                if (Files.isRegularFile(p) && Files.isExecutable(p)) {
                    return p;
                }
            }
        }

        throw new IOException("Could not locate 'whale-daemon' binary. Please build it via `cargo build -p whale-daemon` or specify WHALE_DAEMON_PATH.");
    }

    private static Path searchUpwardForDaemon(Path start) {
        Path curr = start;
        String[] variants = {"debug", "release"};
        String[] binNames = {"whale-daemon", "whale-daemon.exe"};

        for (int i = 0; i < 7 && curr != null; i++) {
            for (String variant : variants) {
                for (String binName : binNames) {
                    Path candidate = curr.resolve("target").resolve(variant).resolve(binName);
                    if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
                        return candidate;
                    }
                }
            }
            curr = curr.getParent();
        }
        return null;
    }

    private void startReaderThreads() {
        this.stdoutThread = new Thread(() -> {
            try {
                String line;
                while (running.get() && (line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    logger.debug("Received line: {}", trimmed);
                    try {
                        JsonNode node = MAPPER.readTree(trimmed);
                        Consumer<JsonNode> handler = this.messageHandler;
                        if (handler != null) {
                            handler.accept(node);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to parse JSON line: {}", trimmed, e);
                    }
                }
            } catch (IOException e) {
                if (running.get()) {
                    logger.debug("Daemon stdout EOF or IO error: {}", e.getMessage());
                }
            } finally {
                running.set(false);
            }
        }, "whale-daemon-stdout-reader");
        this.stdoutThread.setDaemon(true);
        this.stdoutThread.start();

        this.stderrThread = new Thread(() -> {
            try (BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while (running.get() && (line = errReader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        logger.info("[daemon-stderr] {}", trimmed);
                    }
                }
            } catch (IOException ignored) {
            }
        }, "whale-daemon-stderr-reader");
        this.stderrThread.setDaemon(true);
        this.stderrThread.start();
    }

    @Override
    public void send(JsonNode message) throws IOException {
        if (!isAlive()) {
            throw new IOException("whale-daemon process is not running or transport is closed");
        }
        String jsonString = MAPPER.writeValueAsString(message);
        synchronized (writeLock) {
            writer.write(jsonString);
            writer.newLine();
            writer.flush();
        }
        logger.debug("Sent line: {}", jsonString);
    }

    @Override
    public void setMessageHandler(Consumer<JsonNode> handler) {
        this.messageHandler = handler;
    }

    @Override
    public boolean isAlive() {
        return running.get() && process != null && process.isAlive();
    }

    @Override
    public void close() {
        running.set(false);
        try {
            writer.close();
        } catch (Exception ignored) {
        }
        try {
            reader.close();
        } catch (Exception ignored) {
        }
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }
}
