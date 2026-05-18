package com.example.bitcaskserver;

import java.io.IOException;
import java.util.logging.Logger;

import com.example.bitcaskstore.BitCaskStore;


public class BitCaskServer {

    private static final Logger LOG = Logger.getLogger(BitCaskServer.class.getName());

    public static void main(String[] args) throws IOException {
        String storageDir = args.length > 0 ? args[0] : "./bitcask-data";
        int    port       = args.length > 1 ? Integer.parseInt(args[1]) : 7070;

        LOG.info("[BitCaskServer] Storage directory : " + storageDir);
        LOG.info("[BitCaskServer] HTTP port         : " + port);

        BitCaskStore      store  = new BitCaskStore(storageDir);
        BitCaskHttpServer server = new BitCaskHttpServer(store, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("[BitCaskServer] Shutting down...");
            server.stop();
            try { store.close(); } catch (IOException ignored) {}
        }));

        server.start();
        LOG.info("[BitCaskServer] Ready.");
    }
}