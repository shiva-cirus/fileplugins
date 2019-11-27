package io.cdap.plugin.batchsink.common;

public class FieldInfo {
    private String name;
    private boolean anonymize;
    private String format;

    public FieldInfo(String name, boolean anonymize, String format) {
        this.name = name;
        this.anonymize = anonymize;
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public boolean isAnonymize() {
        return anonymize;
    }

    public String getFormat() {
        return format;
    }
}
