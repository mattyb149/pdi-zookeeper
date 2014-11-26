package ninja.mattburgess.pentaho.di.zookeeper.zookeeperinput;

import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperEntry;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperField;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by mburgess on 10/3/14.
 */
public class ZooKeeperInputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = ZooKeeperInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private ZooKeeperInputMeta meta;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  public ZooKeeperInputDialog( Shell parent, Object stepMeta, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) stepMeta, transMeta, stepname );
    meta = (ZooKeeperInputMeta) stepMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, meta );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        meta.setChanged();
      }
    };
    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ZooKeeperInputDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ZooKeeperInputDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    ColumnInfo[] colinf = new ColumnInfo[]
      {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ZooKeeperInputDialog.FieldsTable.FieldName.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ZooKeeperInputDialog.FieldsTable.ZooKeeperPath.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ZooKeeperInputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMeta.getTypes(),
          true )
      };

    Control lastControl = wStepname;

    // Fields
    wlFields = new Label( shell, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "ZooKeeperInputDialog.Input.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle / 4, -margin );
    fdlFields.top = new FormAttachment( lastControl, margin );
    wlFields.setLayoutData( fdlFields );

    wFields =
      new TableView( transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 5, lsMod,
        props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( middle / 4, 0 );
    fdFields.top = new FormAttachment( lastControl, margin * 2 );
    fdFields.right = new FormAttachment( 100, 0 );
    // fdFields.bottom = new FormAttachment( 100, 0 );
    wFields.setLayoutData( fdFields );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[]{ wOK, wCancel }, margin, wFields );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    meta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {

    int i = 0;
    List<ZooKeeperField> fields = meta.getFields();
    if ( fields != null ) {
      wFields.table.setItemCount( fields.size() );
      for ( ZooKeeperField field : fields ) {

        TableItem item = wFields.table.getItem( i );
        int col = 1;

        item.setText( col++, field.getFieldName() );
        item.setText( col++, field.getPath() );
        item.setText( col++, field.getType().getTypeDesc());
        i++;
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    meta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    int nrFields = wFields.nrNonEmpty();

    List<ZooKeeperField> fields = new ArrayList<ZooKeeperField>( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      try {
        TableItem item = wFields.getNonEmpty( i );

        ZooKeeperEntry entry = new ZooKeeperEntry( item.getText( 2 ),
          ValueMetaFactory.createValueMeta( "type", ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) ) ) );
        fields.add( new ZooKeeperField( item.getText( 1 ), entry ) );
      } catch ( Exception e ) {
        // TODO
      }
    }
    meta.setFields( fields );

    dispose();
  }

}
