package com.example.bitcaskserver;

import com.example.bitcaskstore.BitCaskStore;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;


public class BitCaskHttpServer {

    private static final Logger LOG = Logger.getLogger(BitCaskHttpServer.class.getName());

    private final BitCaskStore store;
    private final HttpServer   server;

    public BitCaskHttpServer(BitCaskStore store, int port) throws IOException {
        this.store  = store;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/get",    this::handleGet);
        server.createContext("/keys",   this::handleKeys);
        server.createContext("/all",    this::handleAll);
        server.createContext("/put",    this::handlePut);
        server.createContext("/health", this::handleHealth);

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(16));
    }

    public void start() {
        server.start();
        LOG.info("[BitCaskHttpServer] Listening on port " + server.getAddress().getPort());
    }

    public void stop() {
        server.stop(1);
    }

    /** GET /get?key=<key> */
    private void handleGet(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { send(ex, 405, "Method Not Allowed"); return; }

        String key = queryParam(ex.getRequestURI(), "key");
        if (key == null || key.isEmpty()) { send(ex, 400, "Missing ?key="); return; }

        try {
            String value = store.get(key);
            if (value == null) {
                send(ex, 404, "Key not found: " + key);
            } else {
                send(ex, 200, value);
            }
        } catch (IOException e) {
            send(ex, 500, "Internal error: " + e.getMessage());
        }
    }

    /** GET /keys */
    private void handleKeys(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { send(ex, 405, "Method Not Allowed"); return; }
        String body = String.join("\n", store.keys());
        send(ex, 200, body);
    }

    /** GET /all  → CSV */
    private void handleAll(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { send(ex, 405, "Method Not Allowed"); return; }
        try {
            StringBuilder sb = new StringBuilder("key,value\n");
            Map<String, String> all = store.getAll();
            for (Map.Entry<String, String> e : all.entrySet()) {
                // Escape double quotes inside values
                String safeVal = e.getValue().replace("\"", "\"\"");
                sb.append(e.getKey()).append(",\"").append(safeVal).append("\"\n");
            }
            send(ex, 200, sb.toString());
        } catch (IOException e) {
            send(ex, 500, "Internal error: " + e.getMessage());
        }
    }

    /** POST /put?key=<key>  body=<value> */
    private void handlePut(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { send(ex, 405, "Method Not Allowed"); return; }

        String key = queryParam(ex.getRequestURI(), "key");
        if (key == null || key.isEmpty()) { send(ex, 400, "Missing ?key="); return; }

        String value = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        try {
            store.put(key, value);
            send(ex, 200, "OK");
        } catch (IOException e) {
            send(ex, 500, "Write failed: " + e.getMessage());
        }
    }

    /** GET /health */
    private void handleHealth(HttpExchange ex) throws IOException {
        send(ex, 200, "alive");
    }


    private static void send(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) return null;
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) {
                return java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
