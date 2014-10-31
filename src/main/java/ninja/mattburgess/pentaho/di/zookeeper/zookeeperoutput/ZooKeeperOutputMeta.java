package ninja.mattburgess.pentaho.di.zookeeper.zookeeperoutput;

import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperField;
import ninja.mattburgess.pentaho.di.zookeeper.ZooKeeperStepData;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mburgess on 10/3/14.
 */
@Step(id = "ZookeeperOutput",
  image = "zookeeper-output.png",
  name = "Zookeeper Output",
  description = "Stores data into Zookeeper",
  categoryDescription = "Output")
public class ZooKeeperOutputMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = ZooKeeperOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  protected List<ZooKeeperField> fields = new ArrayList<ZooKeeperField>();

  protected boolean createPaths = false;

  @Override
  public void setDefault() {

  }

  public List<ZooKeeperField> getFields() {
    return fields;
  }

  public void setFields( List<ZooKeeperField> fields ) {
    this.fields = fields;
  }

  public boolean isCreatePaths() {
    return createPaths;
  }

  public void setCreatePaths( boolean createPaths ) {
    this.createPaths = createPaths;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copy,
                                TransMeta transMeta, Trans trans ) {
    return new ZooKeeperOutput( stepMeta, stepDataInterface, copy, transMeta, trans );
  }

  @Override
  public String getDialogClassName() {
    return ZooKeeperOutputDialog.class.getName();
  }

  @Override
  public StepDataInterface getStepData() {
    return new ZooKeeperStepData();
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  private void readData( Node step ) throws KettleXMLException {
    try {
      setCreatePaths( "Y".equalsIgnoreCase( XMLHandler.getTagValue( step, "createPaths" ) ) );
      Node fieldsNode = XMLHandler.getSubNode( step, "fields" );

      int nrfields = XMLHandler.countNodes( fieldsNode, "field" );

      fields = new ArrayList<ZooKeeperField>( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node line = XMLHandler.getSubNodeByNr( fieldsNode, "field", i );
        String name = XMLHandler.getTagValue( line, "name" );
        String path = XMLHandler.getTagValue( line, "path" );
        ValueMetaInterface type = ValueMetaFactory.createValueMeta(
          ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( line, "type" ) ) );

        fields.add( new ZooKeeperField( name, path, type ) );
      }

    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
        PKG, "ZooKeeperOutputMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 300 );
    retval.append( "    " ).append( XMLHandler.addTagValue( "createPaths", isCreatePaths() ) );
    retval.append( "    <fields>" );
    if ( fields != null ) {
      for ( ZooKeeperField field : fields ) {
        retval.append( "      <field>" );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getPath() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getType().getTypeDesc() ) );
        retval.append( "      </field>" );
      }
    }

    retval.append( "    </fields>" );

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    try {
      setCreatePaths( rep.getStepAttributeBoolean( id_step, "createPaths" ) );
      int nrfields = rep.countNrStepAttributes( id_step, "field" );

      fields = new ArrayList<ZooKeeperField>( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {


        String name = rep.getStepAttributeString( id_step, i, "name" );
        String path = rep.getStepAttributeString( id_step, i, "path" );
        ValueMetaInterface type = ValueMetaFactory.createValueMeta(
          ValueMetaFactory.getIdForValueMeta( rep.getStepAttributeString( id_step, i, "type" ) ) );

        fields.add( new ZooKeeperField( name, path, type ) );

      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "ZooKeeperOutputMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "createPaths", isCreatePaths() );
      if ( fields != null ) {
        int i = 0;
        for ( ZooKeeperField field : fields ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getFieldName() );
          rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getPath() );
          rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getType().getName() );
          i++;
        }
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "ZooKeeperOutputMeta.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }
  }
}
