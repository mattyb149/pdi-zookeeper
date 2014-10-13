package ninja.mattburgess.pentaho.di.zookeeper.zooviewer;

import net.isammoc.zooviewer.App;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperUtils;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.*;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.ResourceBundle;

/**
 * Created by mburgess on 10/1/14.
 */
@SpoonPlugin( id = "ZooviewerSpoonPlugin", image = "" )
@SpoonPluginCategories( { "spoon" } )
public class ZooviewerSpoonPlugin extends AbstractXulEventHandler
    implements ISpoonMenuController, SpoonPluginInterface, SpoonLifecycleListener {

  ResourceBundle bundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString( ZooviewerSpoonPlugin.class, key );
    }
  };

  public ZooviewerSpoonPlugin() {
    Spoon spoon = ((Spoon) SpoonFactory.getInstance());
    spoon.addSpoonMenuController(this);
  }

  public String getName() {
    return "zooviewerSpoonPlugin"; //$NON-NLS-1$
  }

  @Override
  public void onEvent(SpoonLifeCycleEvent evt) {
    // Empty method
  }

  @Override
  public void applyToContainer(String category, XulDomContainer container) throws XulException {
    ClassLoader cl = getClass().getClassLoader();
    container.registerClassLoader(cl);
    if(category.equals("spoon")){
      container.loadOverlay("ninja/mattburgess/pentaho/di/zookeeper/zooviewer/spoon_overlays.xul", bundle);
      container.addEventHandler(this);
    }
    // force linking of console

  }

  @Override
  public SpoonLifecycleListener getLifecycleListener() {
    return this;
  }

  @Override
  public SpoonPerspective getPerspective() {
    return null;
  }

  @Override
  public void updateMenu(Document doc) {
    // Empty method
  }

  public void showZooviewer() {
    final Spoon spoon = Spoon.getInstance();


    final String zkList = ZooKeeperUtils.getZooKeeperConfig();
    IRunnableWithProgress op = new IRunnableWithProgress()
    {
      public void run(IProgressMonitor monitor) throws InvocationTargetException, InterruptedException
      {
        try {
          // TODO: spawn a thread to keep this from hanging Spoon
          App.main( new String[] { zkList } );
        }
        catch(Exception e)
        {
          throw new InvocationTargetException(e, "Error displaying Zooviewer: "+e.toString());
        }
      }
    };

    try
    {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog(spoon.getShell());

      pmd.run(true, true, op);
    }
    catch (Exception e)
    {
      showErrorDialog(e, "Error with Progress Monitor Dialog", "Error with Progress Monitor Dialog");
    }
  }

  /**
   * Show an error dialog
   *
   * @param e The exception to display
   * @param title The dialog title
   * @param message The message to display
   */
  private void showErrorDialog(Exception e, String title, String message)
  {
    new ErrorDialog(Spoon.getInstance().getShell(), title, message, e);
  }
}
