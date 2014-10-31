package ninja.mattburgess.pentaho.di.zookeeper.zookeeperoutput;

import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperField;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperStepData;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperOutput extends BaseStep implements StepInterface {

  private static Class<?> PKG = ZooKeeperOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private ZooKeeperOutputMeta meta;
  private ZooKeeperStepData data;

  private ZooKeeper zk;

  private RowMetaInterface outputRowMeta;

  public ZooKeeperOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                          TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ZooKeeperOutputMeta) smi;
    data = (ZooKeeperStepData) sdi;

    try {
      if ( super.init( smi, sdi ) ) {
        zk = ZooKeeperUtils.connectToZooKeeper( ZooKeeperUtils.getZooKeeperConfig() );
      }
      return true;

    } catch ( Exception e ) {
      return false;
    }
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ZooKeeperOutputMeta) smi;
    data = (ZooKeeperStepData) sdi;

    if ( zk != null ) {
      try {
        zk.close();
      } catch ( Exception e ) {
        // Not much we can do here...
      }
    }
    super.dispose( smi, sdi );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] r = getRow(); // get row, set busy!
    // no more input to be expected...
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.setOutputRowMeta( getInputRowMeta().clone() );
    }

    RowMetaInterface outputRowMeta = data.getOutputRowMeta();
    meta.getFields( outputRowMeta, getStepname(), null, null, this, repository, metaStore );

    // Store the field values using the given paths
    try {
      List<ZooKeeperField> fields = meta.getFields();
      if ( fields != null ) {
        for ( ZooKeeperField field : fields ) {
          String fieldName = field.getFieldName();
          int rowIndex = outputRowMeta.indexOfValue( fieldName );
          if ( rowIndex == -1 ) {
            throw new KettleException( "Couldn't find field " + fieldName + " in row!" );
          }
          ValueMetaInterface type = field.getType();
          String valueString = type.getString( r[rowIndex] );
          if ( valueString != null ) {
            byte[] bytes = valueString.getBytes();
            if ( zk.exists( field.getPath(), false ) == null && meta.isCreatePaths() ) {
              // Need to recursively create a path. Could use Curator but I hear it's slow
              createPath( field.getPath() );

            }

            zk.setData( field.getPath(), bytes, -1 );

          } else {
            throw new KettleException( "Couldn't represent value in field " + fieldName + " as binary for ZK storage!" );
          }
        }
        incrementLinesOutput();
      }
    } catch ( KettleException ke ) {
      throw ke;
    } catch ( Exception e ) {
      throw new KettleException( e );
    }

    // Pass on the row(s)
    putRow( outputRowMeta, r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ZooKeeperOutput.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  protected void createPath( String fullPath ) throws Exception {
    if ( fullPath != null && !fullPath.isEmpty() ) {
      String[] pathSegments = fullPath.split( "/" );
      StringBuilder currentPath = new StringBuilder();
      // Skip the first (empty) path segment
      for ( int index = 1; index < pathSegments.length; index++ ) {
        currentPath.append( "/" );
        currentPath.append( pathSegments[index] );
        String currentPathString = currentPath.toString();
        if ( zk.exists( currentPathString, false ) == null ) {
          zk.create( currentPathString, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
        }
      }
    }
  }
}
