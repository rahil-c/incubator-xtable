/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.hudi.catalog.HoodieCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enhanced Unified catalog implementation with robust namespace handling
 * for both Hudi and Iceberg tables through AWS Glue Data Catalog on AWS EMR.
 */
public class XTableSparkCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    private static final Logger LOG = LoggerFactory.getLogger(XTableSparkCatalog.class);

    private static final String ICEBERG_TABLE_TYPE = "ICEBERG";
    private static final String HUDI_TABLE_TYPE = "HUDI";
    private static final String TABLE_TYPE_PROP = "table_type";
    private static final String SPARK_SQL_SOURCES_PROVIDER = "provider";
    private static final String METADATA_LOCATION = "metadata_location";

    private String catalogName;
    private SparkCatalog icebergCatalog;
    private HoodieCatalog hudiCatalog;
    private GlueClient glueClient;
    private CaseInsensitiveStringMap options;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.options = options;

        LOG.warn("Initializing EnhancedUnifiedGlueCatalog with name: {}", name);

        // Initialize Glue client
        //TODO this should not be hardcoded region
        this.glueClient = GlueClient.builder()
                .region(software.amazon.awssdk.regions.Region.of(
                        options.getOrDefault("aws.region", "us-west-2")))
                .build();

        SparkSession spark = SparkSession.active();
        spark.conf().set("hoodie.schema.on.read.enable", "true");

        // Initialize sub-catalogs
        initializeIcebergCatalog(name, options);
        initializeHudiCatalog(name, options);
        
        // Get reference to existing spark_catalog for Hudi delegation
//        try {
//            SparkSession spark = SparkSession.active();
//            this.sparkCatalog = spark.sessionState().catalogManager().catalog("spark_catalog");
//            LOG.warn("Successfully obtained reference to spark_catalog: {} (class: {})",
//                    sparkCatalog.name(), sparkCatalog.getClass().getName());
//
//            // Set global SparkSession configuration for Hudi schema evolution
//            //spark.conf().set("hoodie.schema.on.read.enable", "true");
//            LOG.warn("Set global SparkSession config for Hudi schema evolution");
//        } catch (Exception e) {
//            LOG.error("Failed to get reference to spark_catalog", e);
//            throw new RuntimeException("Failed to get reference to spark_catalog", e);
//        }
    }

    private void initializeIcebergCatalog(String name, CaseInsensitiveStringMap options) {
        LOG.warn("Initializing Iceberg sub-catalog");

        icebergCatalog = new SparkCatalog();

        Map<String, String> icebergOptions = new HashMap<>(options);
        icebergOptions.put("catalog-impl", GlueCatalog.class.getName());

        if (options.containsKey("warehouse")) {
            icebergOptions.put("warehouse", options.get("warehouse"));
        }

        icebergCatalog.initialize(name + "_iceberg", new CaseInsensitiveStringMap(icebergOptions));
    }

    private void initializeHudiCatalog(String name, CaseInsensitiveStringMap options) {
        LOG.warn("Initializing Hudi sub-catalog");

        hudiCatalog = new HoodieCatalog();

        // Set up delegation to spark_catalog for metastore operations
        try {
            SparkSession spark = SparkSession.active();
            CatalogPlugin sparkCatalog = spark.sessionState().catalogManager().catalog("spark_catalog");
            hudiCatalog.setDelegateCatalog(sparkCatalog);
            LOG.warn("Successfully set spark_catalog as delegate for HoodieCatalog");
        } catch (Exception e) {
            LOG.error("Failed to set delegate catalog for HoodieCatalog. This may cause table operations to fail.", e);
            throw new RuntimeException("Failed to initialize HoodieCatalog with spark_catalog delegate", e);
        }

        Map<String, String> hudiOptions = new HashMap<>(options);
        
        // Add Hudi-specific configuration to ensure proper table recognition

        // Add provider hints to force Hudi table recognition
        hudiOptions.put("provider", "hudi");
        hudiOptions.put("spark.sql.sources.provider", "hudi");

        // Add debugging to see what options are being passed
        LOG.warn("HudiCatalog options: {}", hudiOptions);

        hudiCatalog.initialize("spark_catalog", new CaseInsensitiveStringMap(hudiOptions));
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Identifier[] listTables(String[] namespace) {
        Set<Identifier> allTables = new HashSet<>();

        // Get tables from catalogs that support this namespace
        if (icebergCatalog != null) {
            try {
                allTables.addAll(Arrays.asList(icebergCatalog.listTables(namespace)));
            } catch (Exception e) {
                LOG.warn("Failed to list Iceberg tables in namespace: {}", namespace, e);
            }
        }

        if (hudiCatalog != null) {
            try {
                allTables.addAll(Arrays.asList(hudiCatalog.listTables(namespace)));
            } catch (Exception e) {
                LOG.warn("Failed to list Hudi tables in namespace: {}", namespace, e);
            }
        }

        LOG.warn("Found {} tables in namespace: {}", allTables.size(), namespace);
        return allTables.toArray(new Identifier[0]);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        LOG.warn("Loading table: {}", ident);

        String tableFormat = getTableFormat(ident);

        try {
            switch (tableFormat.toUpperCase()) {
                case ICEBERG_TABLE_TYPE:
                    LOG.warn("Loading as Iceberg table: {}", ident);
                    return icebergCatalog.loadTable(ident);

                case HUDI_TABLE_TYPE:
                    LOG.warn("Loading as Hudi table via internal HudiCatalog: {}", ident);
                    Table hudiTable = hudiCatalog.loadTable(ident);
                    LOG.warn("HudiCatalog returned table class: {}, name: {}",
                            hudiTable.getClass().getName(), hudiTable.name());
                    return hudiTable;

                default:
                    throw new NoSuchTableException(ident);
            }
        } catch (Exception e) {
            LOG.error("Failed to load table: {}", ident, e);
            throw new NoSuchTableException(ident);
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {

        LOG.warn("Creating table: {} with properties: {}", ident, properties);

        // Determine table format from properties
        String provider = properties.getOrDefault(SPARK_SQL_SOURCES_PROVIDER, "").toLowerCase();
        String tableType = properties.getOrDefault(TABLE_TYPE_PROP, "").toUpperCase();

        Table table;
        String format;

        if (provider.contains("hudi") || HUDI_TABLE_TYPE.equals(tableType)) {
            LOG.warn("Creating Hudi table via spark_catalog delegation: {}", ident);
            table = hudiCatalog.createTable(ident, schema, partitions, properties);
            format = HUDI_TABLE_TYPE;
        } else if (provider.contains("iceberg") || ICEBERG_TABLE_TYPE.equals(tableType)) {
            LOG.warn("Creating Iceberg table: {}", ident);
            table = icebergCatalog.createTable(ident, schema, partitions, properties);
            format = ICEBERG_TABLE_TYPE;
        } else {
            // Default to Iceberg for tables without explicit format specification
            // This provides a consistent default while both formats can coexist in the same namespace
            LOG.warn("No specific format specified, defaulting to Iceberg for table: {}", ident);
            table = icebergCatalog.createTable(ident, schema, partitions, properties);
            format = ICEBERG_TABLE_TYPE;
        }

        return table;
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        String tableFormat = getTableFormat(ident);

        switch (tableFormat.toUpperCase()) {
            case ICEBERG_TABLE_TYPE:
                return icebergCatalog.alterTable(ident, changes);

            case HUDI_TABLE_TYPE:
                return hudiCatalog.alterTable(ident, changes);

            default:
                throw new NoSuchTableException(ident);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        LOG.warn("Dropping table: {}", ident);

        try {
            String tableFormat = getTableFormat(ident);
            boolean dropped = false;

            switch (tableFormat.toUpperCase()) {
                case ICEBERG_TABLE_TYPE:
                    dropped = icebergCatalog.dropTable(ident);
                    break;

                case HUDI_TABLE_TYPE:
                    dropped = hudiCatalog.dropTable(ident);
                    break;
            }

            return dropped;
        } catch (Exception e) {
            LOG.error("Failed to drop table: {}", ident, e);
            return false;
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {

        String tableFormat = getTableFormat(oldIdent);

        switch (tableFormat.toUpperCase()) {
            case ICEBERG_TABLE_TYPE:
                icebergCatalog.renameTable(oldIdent, newIdent);
                break;

            case HUDI_TABLE_TYPE:
                hudiCatalog.renameTable(oldIdent, newIdent);
                break;

            default:
                throw new NoSuchTableException(oldIdent);
        }
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) 
            throws NamespaceAlreadyExistsException {
        LOG.warn("Creating namespace: {} with metadata: {}", Arrays.toString(namespace), metadata);
        
        // Since both Hudi (via Hive-Glue connector) and Iceberg (via direct Glue) use the same 
        // underlying Glue database, only create the namespace once using Iceberg catalog.
        // This will be automatically visible to Hudi via the Hive-Glue connector.
        icebergCatalog.createNamespace(namespace, metadata);
        
        LOG.warn("Namespace created successfully: {}", Arrays.toString(namespace));
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {

    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
        return false;
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return new String[0][];
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        return new String[0][];
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {

        LOG.warn("Loading namespace metadata for: {}", Arrays.toString(namespace));

        // Since both Hudi (via Hive-Glue connector) and Iceberg (via direct Glue) access the same 
        // underlying Glue database, use Iceberg catalog as the primary source for namespace metadata
        // since it has direct Glue access and more complete metadata support.
        try {
            Map<String, String> metadata = icebergCatalog.loadNamespaceMetadata(namespace);
            LOG.warn("Found namespace metadata via Iceberg catalog for: {}", Arrays.toString(namespace));
            return metadata != null ? metadata : new HashMap<>();
        } catch (NoSuchNamespaceException e) {
            LOG.warn("Namespace not found via Iceberg catalog, trying Hudi catalog: {}", Arrays.toString(namespace));
            
            // Fall back to Hudi catalog if Iceberg fails
            try {
                Map<String, String> metadata = hudiCatalog.loadNamespaceMetadata(namespace);
                LOG.warn("Found namespace metadata via Hudi catalog for: {}", Arrays.toString(namespace));
                return metadata != null ? metadata : new HashMap<>();
            } catch (NoSuchNamespaceException hudiException) {
                LOG.error("Namespace not found in any catalog: {}", Arrays.toString(namespace));
                throw new NoSuchNamespaceException(namespace);
            } catch (Exception hudiException) {
                LOG.warn("Failed to load Hudi namespace metadata for: {}", Arrays.toString(namespace), hudiException);
                throw new NoSuchNamespaceException(namespace);
            }
        } catch (Exception e) {
            LOG.warn("Failed to load Iceberg namespace metadata for: {}", Arrays.toString(namespace), e);
            throw new NoSuchNamespaceException(namespace);
        }
    }

    /**
     * Determines the table format by inspecting Glue metadata.
     */
    private String getTableFormat(Identifier ident) throws NoSuchTableException {
        String database = ident.namespace()[0];
        String tableName = ident.name();

        LOG.warn("Attempting to determine table format for: database='{}', table='{}'", database, tableName);

        try {
            GetTableRequest.Builder requestBuilder = GetTableRequest.builder()
                    .databaseName(database)
                    .name(tableName);
            
            // Add catalog ID if specified in options
            if (options.containsKey("catalog-id")) {
                String catalogId = options.get("catalog-id");
                requestBuilder.catalogId(catalogId);
                LOG.warn("Using catalog ID: {}", catalogId);
            }
            
            GetTableRequest request = requestBuilder.build();
            LOG.warn("Calling Glue getTable with request: database='{}', table='{}', catalogId='{}'",
                     database, tableName, request.catalogId());

            GetTableResponse response = glueClient.getTable(request);
            software.amazon.awssdk.services.glue.model.Table glueTable = response.table();

            // Check table properties
            Map<String, String> parameters = glueTable.parameters();

            // Check for Iceberg
            if (parameters.containsKey(METADATA_LOCATION) ||
                    parameters.containsKey("table_type") &&
                            ICEBERG_TABLE_TYPE.equalsIgnoreCase(parameters.get("table_type"))) {
                return ICEBERG_TABLE_TYPE;
            }

            // Check for Hudi
            if (parameters.containsKey(SPARK_SQL_SOURCES_PROVIDER) &&
                    parameters.get(SPARK_SQL_SOURCES_PROVIDER).toLowerCase().contains("hudi")) {
                return HUDI_TABLE_TYPE;
            }

            // Check storage descriptor properties
            StorageDescriptor sd = glueTable.storageDescriptor();
            if (sd != null) {
                String inputFormat = sd.inputFormat();
                String outputFormat = sd.outputFormat();
                String serdeLib = sd.serdeInfo() != null ? sd.serdeInfo().serializationLibrary() : null;

                if (isHudiFormat(inputFormat, outputFormat, serdeLib)) {
                    return HUDI_TABLE_TYPE;
                }

                if (isIcebergFormat(inputFormat, outputFormat, serdeLib)) {
                    return ICEBERG_TABLE_TYPE;
                }
            }

            // Default detection failed - try loading from each catalog
            LOG.warn("Could not determine table format for: {}. Attempting both catalogs.", ident);

            try {
                icebergCatalog.loadTable(ident);
                return ICEBERG_TABLE_TYPE;
            } catch (NoSuchTableException e) {
                hudiCatalog.loadTable(ident);
                return HUDI_TABLE_TYPE;
            }

        } catch (GlueException e) {
            LOG.error("Failed to get table from Glue: {}", ident, e);
            throw new NoSuchTableException(ident);
        }
    }

    private boolean isHudiFormat(String inputFormat, String outputFormat, String serdeLib) {
        return (inputFormat != null && inputFormat.contains("hudi")) ||
                (outputFormat != null && outputFormat.contains("hudi")) ||
                (serdeLib != null && serdeLib.contains("hudi"));
    }

    private boolean isIcebergFormat(String inputFormat, String outputFormat, String serdeLib) {
        return (inputFormat != null && inputFormat.contains("iceberg")) ||
                (outputFormat != null && outputFormat.contains("iceberg")) ||
                (serdeLib != null && serdeLib.contains("iceberg"));
    }
}
