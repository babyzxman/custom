package com.gable.templar.constant;

import java.util.HashMap;
import java.util.Map;

public class JobConstant {

    public enum SCHEMA_LIST {
        VAR_RWZBTCH("rwzbtch","VAR_RWZBTCH"),
        VAR_RWZCDR("rwzcdr","VAR_RWZCDR"),
        VAR_RWZMR("rwzmr","VAR_RWZMR"),
        VAR_RWZRLTM("rwzrltm","VAR_RWZRLTM"),
        VAR_RFZACS("rfzacs","VAR_RFZACS"),
        VAR_RFZRPT("rfzrpt","VAR_RFZRPT"),
        VAR_RFZEDM("rfzedm","VAR_RFZEDM"),
        VAR_RFZCDR("rfzcdr","VAR_RFZCDR"),
        VAR_RFZLKP("rfzlkp","VAR_RFZLKP"),
        VAR_RFZNRT("rfznrt","VAR_RFZNRT"),
        VAR_RFZNTWK("rfzntwk","VAR_RFZNTWK"),
        VAR_TMPZ("tmpz","VAR_TMPZ"),
        VAR_RWZBTDT("rwzbtdt","VAR_RWZBTDT"),
        VAR_RFZDTA("rfzdta","VAR_RFZDTA"),
        VAR_RFZDTB("rfzdtb","VAR_RFZDTB"),
        VAR_TMPOUTZ("tmpoutz","VAR_TMPOUTZ"),
        VAR_FWCONFZ("fwconfz",
                "VAR_FWCONFZ");

        private String schemaName;

        private String schemaVariable;

        private String schemaNameUat;

        private String schemaNameTrueDev;

        private String schemaNameTrueSandbox;

        public static final Map<String,String> schemaMap = new HashMap<>();

        public static final Map<String,String> schemaUatMap = new HashMap<>();

        public static final Map<String,String> schemaTrueDevMap = new HashMap<>();

        public static final Map<String,String> schemaTrueSandboxMap = new HashMap<>();

        SCHEMA_LIST(String schemaName, String schemaVariable) {
            this.schemaName = schemaName;
            this.schemaVariable = schemaVariable;
            this.schemaNameUat = schemaName + "_uat";
            this.schemaNameTrueDev = schemaName + "_true_dev";
            this.schemaNameTrueSandbox = schemaName;
        }

        static {
            for(SCHEMA_LIST schemaList: SCHEMA_LIST.values()) {
                schemaMap.put(schemaList.getSchemaName(),schemaList.getSchemaVariable());
                schemaUatMap.put(schemaList.getSchemaNameUat(), schemaList.getSchemaVariable());
                schemaTrueDevMap.put(schemaList.getSchemaNameTrueDev(),schemaList.getSchemaVariable());
                schemaTrueSandboxMap.put(schemaList.getSchemaNameTrueSandbox(), schemaList.getSchemaVariable());
            }
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getSchemaVariable() {
            return schemaVariable;
        }

        public void setSchemaVariable(String schemaVariable) {
            this.schemaVariable = schemaVariable;
        }

        public String getSchemaNameUat() {
            return schemaNameUat;
        }

        public void setSchemaNameUat(String schemaNameUat) {
            this.schemaNameUat = schemaNameUat;
        }

        public String getSchemaNameTrueDev() {
            return schemaNameTrueDev;
        }

        public String getSchemaNameTrueSandbox() {
            return schemaNameTrueSandbox;
        }
    }

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

    public enum LOAD_TYPE {
        FULL_LOAD("full_load"),
        UPSERT("upsert");

        private final String value;

        LOAD_TYPE(String value) {
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

    public static final String importParameter = "import_parameter";

    public static final String manualTitle = "Manual_Parameters";

    public static final String tableNmApiIngestion = "tbl_api_ingestion";

    public static final String tableNmOutBound = "tbl_job_outbound";

    public static final String tableNmDbIngestion = "tbl_db_ingestion";

    public static final String tableNmFileIngestion = "tbl_file_ingestion";

    public static final String tableNmKafkaIngestion = "tbl_kafka_ingestion";

    public static final String tableNmTrans = "tbl_job_trans";
}
