package com.gable.templar.constant;

public class JobConstant {

    public enum JOB_TYPE {
        INGEST_API("ingestApi"),
        INGEST_DB("ingestDb"),
        TRANSFORM("transform"),
        FILE("file"),
        KAFKA("kafka"),
        OUTBOUND("outbound");

        private final String value;

        JOB_TYPE(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public enum CATCHUP_TYPE{
        PERIOD("period"),
        SEQUENCE("sequence");

        private final String value;

        CATCHUP_TYPE(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final String initialTitle = "Initial";

    public static final String manualTitle = "Manual_Parameters";

    public static final String tableNmApiIngestion = "tbl_api_ingestion";

    public static final String tableNmOutBound = "tbl_job_outbound";

    public static final String tableNmDbIngestion = "tbl_db_ingestion";

    public static final String tableNmFileIngestion = "tbl_file_ingestion";

    public static final String tableNmKafkaIngestion = "tbl_kafka_ingestion";

    public static final String tableNmTrans = "tbl_job_trans";
}
