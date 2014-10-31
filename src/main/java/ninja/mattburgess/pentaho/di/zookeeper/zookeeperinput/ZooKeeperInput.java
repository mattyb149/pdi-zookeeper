package ninja.mattburgess.pentaho.di.zookeeper.zookeeperinput;

import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperField;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperStepData;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperUtils;
import org.apache.zookeeper.ZooKeeper;
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

import java.util.List;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperInput extends BaseStep implements StepInterface {

  private static Class<?> PKG = ZooKeeperInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private ZooKeeperInputMeta meta;
  private ZooKeeperStepData data;

  private ZooKeeper zk;

  public ZooKeeperInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                         TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ZooKeeperInputMeta) smi;
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
    meta = (ZooKeeperInputMeta) smi;
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
      // This is an input step, so if there are no incoming rows, we should still execute
      if ( !first ) {
        setOutputDone();
        return false;
      }
    }

    if ( first ) {
      first = false;

    }

    RowMetaInterface outputRowMeta = new RowMeta();
    meta.getFields( outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    Object[] outputRow = new Object[outputRowMeta.size()];

    // Fetch the field values using the given paths
    try {
      List<ZooKeeperField> fields = meta.getFields();
      if ( fields != null ) {
        int i = 0;
        for ( ZooKeeperField field : fields ) {
          Stat dataStat = new Stat();
          byte[] bytes = zk.getData( field.getPath(), false, dataStat );
          if ( bytes != null ) {
            // Get data as string and convert it to the specified type
            ValueMetaInterface type = field.getType();
            if(type.getStorageMetadata() == null) {
              type.setStorageMetadata( new ValueMetaString("data") );
            }
            outputRow[i] = type.convertBinaryStringToNativeType( bytes );
          }
          i++;
          incrementLinesInput();
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( e );
    }

    putRow( outputRowMeta, outputRow ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ZooKeeperInput.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }
}
