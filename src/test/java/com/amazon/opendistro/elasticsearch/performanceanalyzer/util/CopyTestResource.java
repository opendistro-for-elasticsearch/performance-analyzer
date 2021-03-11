/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

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

