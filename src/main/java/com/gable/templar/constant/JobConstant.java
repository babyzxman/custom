package com.gable.templar.constant;

public class JobConstant {

    public enum JOB_TYPE {
        INGEST_API("ingestApi"),
        INGEST_DB("ingestDb"),
        TRANSFORM("transform");

        private final String value;

        JOB_TYPE(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final String initialTitle = "initial";

    public static final String tableNmApiIngestion = "tbl_api_ingestion";

    public static final String tableNmDbIngestion = "tbl_db_ingestion";
}
