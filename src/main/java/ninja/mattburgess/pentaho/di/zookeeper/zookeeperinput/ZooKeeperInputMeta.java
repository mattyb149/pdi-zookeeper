package ninja.mattburgess.pentaho.di.zookeeper.zookeeperinput;

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
@Step( id = "ZookeeperInput",
  image = "zookeeper-input.png",
  name = "Zookeeper Input",
  description = "Reads configuration information from a Zookeeper cluster",
  categoryDescription = "Input" )
public class ZooKeeperInputMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = ZooKeeperInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  protected List<ZooKeeperField> fields = new ArrayList<ZooKeeperField>();

  @Override
  public void setDefault() {

  }

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the step
   * @param name         Name of the step to use as input for the origin field in the values
   * @param info         Fields used as extra lookup information
   * @param nextStep     the next step that is targeted
   * @param space        the space The variable space to use to replace variables
   * @param repository   the repository to use to load Kettle metadata objects impacting the output fields
   * @param metaStore    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws org.pentaho.di.core.exception.KettleStepException the kettle step exception
   */
  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    if ( fields != null ) {
      // Add the fields
      // TODO overwrite existing fields?
      for ( ZooKeeperField field : fields ) {
        ValueMetaInterface type = field.getType();
        type.setName( field.getFieldName() );
        inputRowMeta.addValueMeta( type );
      }
    }
  }

  public List<ZooKeeperField> getFields() {
    return fields;
  }

  public void setFields( List<ZooKeeperField> fields ) {
    this.fields = fields;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copy,
                                TransMeta transMeta, Trans trans ) {
    return new ZooKeeperInput( stepMeta, stepDataInterface, copy, transMeta, trans );
  }

  @Override
  public String getDialogClassName() {
    return ZooKeeperInputDialog.class.getName();
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
        PKG, "ZooKeeperInputMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 300 );

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
        PKG, "ZooKeeperInputMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
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
        PKG, "ZooKeeperInputMeta.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }

  }

}
