package com.amazon.opendistro.performanceanalyzer.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;

public class CopyTestResource implements AutoCloseable {
    private String path;

    public String getPath() {
        return path;
    }

    public CopyTestResource(String srcPath, String desPath) {
        if (srcPath.equals(desPath)) {
            throw new RuntimeException("srcPath and desPath cannot be same");
        }

        System.out.println("copying from " + Paths.get(srcPath).toAbsolutePath().toString() +
                " to " + Paths.get(desPath).toAbsolutePath().toString());

        path = desPath;
        try {
            Files.walk(Paths.get(srcPath)).forEach((Path a) -> {
                Path b = Paths.get(desPath, a.toString().substring(srcPath.length()));
                try {
                    Files.copy(a, b,  REPLACE_EXISTING, COPY_ATTRIBUTES);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            delete(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void delete(File file) throws IOException {

        for (File childFile : file.listFiles()) {

            if (childFile.isDirectory()) {
                delete(childFile);
            } else {
                if (!childFile.delete()) {
                    throw new IOException();
                }
            }
        }

        if (!file.delete()) {
            throw new IOException();
        }
    }
}

