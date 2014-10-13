package ninja.mattburgess.pentaho.di.zookeeper;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperStepData extends BaseStepData implements StepDataInterface {

  protected RowMetaInterface outputRowMeta;
  protected String zooKeeperConfig;

  public RowMetaInterface getOutputRowMeta() {
    return outputRowMeta;
  }

  public void setOutputRowMeta( RowMetaInterface outputRowMeta ) {
    this.outputRowMeta = outputRowMeta;
  }

  public String getZooKeeperConfig() {
    return zooKeeperConfig;
  }

  public void setZooKeeperConfig( String zooKeeperConfig ) {
    this.zooKeeperConfig = zooKeeperConfig;
  }


}
