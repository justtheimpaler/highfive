package highfive.commands;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.Identifier;
import highfive.model.Table;

public class ListColumnsAndCheckCommand extends DataSourceCommand {

  public ListColumnsAndCheckCommand(final String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("List Columns", datasourceName);
  }

  @Override
  public void execute()
      throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException, InvalidConfigurationException {

    // 1. list types

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();
    info(" ");
    info("Tables (" + tableNames.size() + "):");

    // 2. List columns

    Map<String, Integer> typeCount = new TreeMap<>();

    List<Table> tables = new ArrayList<>();
    for (Identifier tn : tableNames) {
      Table t = this.ds.getDialect().getTableMetaData(tn);
      tables.add(t);
      info(" ");
      info("Table " + tn.getGenericName() + " (" + t.getColumns().size() + " columns):");
      for (Column c : t.getColumns()) {
        String key = c.getRenderedType() + " [" + (c.getSerializer() == null ? "N/A" : c.getSerializer().getName())
            + "]";
        info("  " + c.getName() + ": " + key);
        Integer count = typeCount.get(key);
        count = count == null ? Integer.valueOf(1) : count + 1;
        typeCount.put(key, count);
      }
    }

    info(" ");
    info("Summary of types found (" + typeCount.size() + "):");
    for (String t : typeCount.keySet()) {
      info("  " + t + ": " + typeCount.get(t));
    }

    info(" ");
    info("Row Count:");
    for (Identifier tn : tableNames) {
      String tid = this.ds.getDialect().renderSQLTableIdentifier(tn);
      String sql = "select count(*) from " + tid;
      try (PreparedStatement ps = this.ds.getConnection().prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
        if (rs.next()) {
          long count = rs.getLong(1);
          info("  " + tn.getGenericName() + ": " + count + " rows");
        }
      }
    }

    // 3. Check if all tables have PK and all columns are supported

    checkIfHashingAndCopyingIsSupported(tables);

  }

}
