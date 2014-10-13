package ninja.mattburgess.pentaho.di.zookeeper;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperEntry {

  protected String path;

  protected ValueMetaInterface type;

  public ZooKeeperEntry() {

  }

  public ZooKeeperEntry( String path ) {
    this.path = path;
  }

  public ZooKeeperEntry( String path, ValueMetaInterface type ) {
    this( path );
    this.type = type;
  }

  public ZooKeeperEntry( String path, String typeName ) {
    this( path );
    try {
      type = ValueMetaFactory.createValueMeta( ValueMetaFactory.getIdForValueMeta( typeName ) );
    } catch ( Exception e ) {
      //TODO
    }
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  public ValueMetaInterface getType() {
    return type;
  }

  public void setType( ValueMetaInterface type ) {
    this.type = type;
  }

}
