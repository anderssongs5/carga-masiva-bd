package com.arlsura.cargamasivabd.cargamasiva;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class CargaMasiva {

    private final String DRIVER_PROP = "dbdriver";
    private final String URL_PROP = "dburl";
    private final String USER_PROP = "dbuser";
    private final String PASSWORD_PROP = "dbpassword";
    private final String TABLE_PROP = "table";
    private final String FIELD_SEPARATOR_PROP = "fieldSeparator";
    private final String LINE_SEPARATOR_PROP = "lineSeparator";
    private final String FIELDS_PROP = "fields";
    private final String NULL_FIELDS_PROP = "nullFields";
    private final String FILE_FULL_PATH_PROP = "fileFullPaht";
    private final String COMMA = ",";
    private final String AT = "@";
    private static final Logger LOG = Logger.getLogger(CargaMasiva.class);
    private String query;
    private String driver;
    private String url;
    private String user;
    private String password;
    private String filePath;
    private String table;
    private String fieldSeparator;
    private String fields;
    private String lineSeparator;
    private String nullFields;
    private Connection connection;
    private Statement statement;

    public void cargarArchivoABd() {
        LOG.info("Inició! - " + (new Date()).toString());

        this.cargarConfiguracion();

        try {
            Class.forName(driver);

            LOG.info("Conectándose a la base de datos");
            this.connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();

            this.query = this.construirQuery();
            LOG.info("Query: " + this.query);

            LOG.info("Inicio de ejecución de carga masiva: " + (new Date()).toString());
            statement.executeUpdate(query);
            LOG.info("Fin de ejecución de carga masiva: " + (new Date()).toString());

        } catch (ClassNotFoundException e) {
            LOG.error("Error inicializando driver.", e);
        } catch (SQLException e) {
            LOG.error("Error SQL.", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.error("Error cerrando Statement", e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("Error cerrando Connection", e);
                }
            }
        }

        LOG.info("Fin! - " + (new Date()).toString());
    }

    private void cargarConfiguracion() {
        LOG.info("Cargando parámetros de configuración de base de datos");
        Util.cargarConfiguraciones();

        this.driver = Util.obtenerValorConf(DRIVER_PROP);
        this.url = Util.obtenerValorConf(URL_PROP);
        this.user = Util.obtenerValorConf(USER_PROP);
        this.password = Util.obtenerValorConf(PASSWORD_PROP);

        this.table = Util.obtenerValorConf(TABLE_PROP);
        this.fieldSeparator = Util.obtenerValorConf(FIELD_SEPARATOR_PROP);
        this.lineSeparator = Util.obtenerValorConf(LINE_SEPARATOR_PROP);
        this.fields = Util.obtenerValorConf(FIELDS_PROP);
        this.nullFields = Util.obtenerValorConf(NULL_FIELDS_PROP);
        this.filePath = Util.obtenerValorConf(FILE_FULL_PATH_PROP);
    }

    private String construirQuery() {
        StringBuilder stringBuilder = new StringBuilder("LOAD DATA LOCAL INFILE '");
        stringBuilder.append(this.filePath);
        stringBuilder.append("' INTO TABLE ");
        stringBuilder.append(this.table);
        stringBuilder.append(" FIELDS TERMINATED BY '");
        stringBuilder.append(this.fieldSeparator);
        stringBuilder.append("' LINES TERMINATED BY '");
        stringBuilder.append(lineSeparator);
        stringBuilder.append("' ");

        boolean hasFields = this.fields != null && !this.fields.trim().isEmpty();
        if (hasFields) {
            stringBuilder.append("(");
            stringBuilder.append(this.fields);
            stringBuilder.append(") ");
        }
        String query = stringBuilder.toString();

        boolean hasNullFields = this.nullFields != null && !this.nullFields.isEmpty();
        if (hasFields && hasNullFields) {
            query = query.concat("SET ");
            StringTokenizer token = new StringTokenizer(this.nullFields.trim(), COMMA);
            while (token.hasMoreElements()) {
                String nullField = (String) token.nextElement();
                nullField = nullField.trim();
                query = query.replace(nullField, AT + nullField);
                query = query.concat(nullField).concat(" = NULLIF(").concat(AT).concat(nullField).concat(COMMA)
                        .concat(" ''), ");
            }
        }

        return (hasFields && hasNullFields) ? query.substring(0, query.length() - 2)
                : query.substring(0, query.length() - 1);
    }
}
