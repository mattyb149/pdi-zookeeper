package ninja.mattburgess.pentaho.di.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.pentaho.di.core.Const;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperUtils {

  public static final String CONFIG_FILENAME = "config.properties";

  public static final String DEFAULT_ZOOKEEPER = "localhost:2181";

  public static final String ZOOKEEPER_CONFIG_PROPERTY = "zk";

  public static final int DEFAULT_ZK_CONNECT_TIMEOUT_MSEC = 5000;

  public static String config = "";

  public static String getZooKeeperConfig() {
    String zookeeperList = DEFAULT_ZOOKEEPER;

    // Read in config properties
    File pluginFolderFile =
      new File( ZooKeeperUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath() ).getParentFile();

    // If set as a System property, just use that
    Properties props = new Properties( System.getProperties() );
    String zkConfigProp = props.getProperty( ZOOKEEPER_CONFIG_PROPERTY );
    if ( !Const.isEmpty( zkConfigProp ) ) {
      zookeeperList = zkConfigProp;
    } else {
      // Try the config file
      if ( pluginFolderFile != null && pluginFolderFile.exists() ) {
        File configFile = new File( pluginFolderFile, CONFIG_FILENAME );
        if ( configFile.exists() ) {
          try {

            props.load( new FileInputStream( configFile ) );
            zkConfigProp = props.getProperty( ZOOKEEPER_CONFIG_PROPERTY );
            if ( !Const.isEmpty( zkConfigProp ) ) {
              zookeeperList = zkConfigProp;
            }

          } catch ( Exception e ) {
            // Do nothing, default already set
          }
        }
      }
    }
    config = zookeeperList;
    return zookeeperList;
  }

  public static ZooKeeper connectToZooKeeper( String config ) throws IOException {
    return connectToZooKeeper( config, DEFAULT_ZK_CONNECT_TIMEOUT_MSEC );
  }

  public static ZooKeeper connectToZooKeeper( String config, int timeoutMsec ) throws IOException {
    return connectToZooKeeper( config, timeoutMsec, noOpWatcher );
  }

  public static ZooKeeper connectToZooKeeper( String config, int timeoutMsec, Watcher watcher ) throws IOException {
    return new ZooKeeper( config, timeoutMsec, watcher );
  }

  public static ZooKeeper reconnect() throws IOException {
    if ( config == null ) {
      getZooKeeperConfig();
    }
    if ( config != null ) {
      return connectToZooKeeper( config );
    }
    throw new IOException( "Couldn't reconnect to ZooKeeper quorum" );
  }

  private static Watcher noOpWatcher = new Watcher() {
    @Override
    public void process( WatchedEvent event ) {
      // Do nothing
    }
  };
}
