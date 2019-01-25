package com.amazon.opendistro.performanceanalyzer.metricsdb;

public class Metric<T> {
    private String name;
    private T sum;
    private T avg;
    private T min;
    private T max;

    public Metric(String name, T value) {
        this.name = name;
        this.sum = value;
        this.avg = value;
        this.min = value;
        this.max = value;
    }

    public Metric(String name, T sum, T avg, T min, T max) {
        this.name = name;
        this.sum = sum;
        this.avg = avg;
        this.min = min;
        this.max = max;
    }

    public String getName() {
        return this.name;
    }

    public T getSum() {
        return this.sum;
    }

    public T getAvg() {
        return this.avg;
    }

    public T getMin() {
        return this.min;
    }

    public T getMax() {
        return this.max;
    }

    public Class<?> getValueType() {
        return this.sum.getClass();
    }

    //Unit test helper methods
    public static Metric<Double> cpu(Double val) {
        return new Metric<Double>("cpu", val);
    }

    //Unit test helper methods
    public static Metric<Double> rss(Double val) {
        return new Metric<Double>("rss", val);
    }
}
