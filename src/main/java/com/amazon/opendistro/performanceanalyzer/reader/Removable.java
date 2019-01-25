package com.amazon.opendistro.performanceanalyzer.reader;

/**
 * Interface that should be implemented by snapshot holders that need to be trimmed.
 */
public interface Removable {
    void remove() throws Exception;
}
