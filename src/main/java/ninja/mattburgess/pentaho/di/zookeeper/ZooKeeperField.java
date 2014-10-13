package ninja.mattburgess.pentaho.di.zookeeper;

import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperEntry;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperField {

  protected String fieldName;

  protected ZooKeeperEntry zooKeeperField;

  public ZooKeeperField() {

  }

  public ZooKeeperField( String fieldName ) {
    this.fieldName = fieldName;
  }

  public ZooKeeperField( String fieldName, ZooKeeperEntry entry ) {
    this( fieldName );
    zooKeeperField = entry;
  }

  public ZooKeeperField( String fieldName, String path, ValueMetaInterface type ) {
    this( fieldName, new ZooKeeperEntry( path, type ) );
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  public ZooKeeperEntry getZooKeeperField() {
    return zooKeeperField;
  }

  public void setZooKeeperField( ZooKeeperEntry zooKeeperField ) {
    this.zooKeeperField = zooKeeperField;
  }

  public String getPath() {
    return zooKeeperField == null ? null : zooKeeperField.getPath();
  }

  public ValueMetaInterface getType() {
    return zooKeeperField == null ? null : zooKeeperField.getType();
  }
}
