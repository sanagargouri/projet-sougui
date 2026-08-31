// ============================================================================
//
// Copyright (c) 2006-2015, Talend SA
//
// Ce code source a été automatiquement généré par_Talend Open Studio for Data Integration
// / Soumis à la Licence Apache, Version 2.0 (la "Licence") ;
// votre utilisation de ce fichier doit respecter les termes de la Licence.
// Vous pouvez obtenir une copie de la Licence sur
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Sauf lorsqu'explicitement prévu par la loi en vigueur ou accepté par écrit, le logiciel
// distribué sous la Licence est distribué "TEL QUEL",
// SANS GARANTIE OU CONDITION D'AUCUNE SORTE, expresse ou implicite.
// Consultez la Licence pour connaître la terminologie spécifique régissant les autorisations et
// les limites prévues par la Licence.


package pi_pa_mehdi.etl_fact_vente2_0_1;

import routines.Numeric;
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.StringHandling;
import routines.Relational;
import routines.TalendDate;
import routines.Mathematical;
import routines.ClientKeyGenerator;
import routines.SKUGenerator;
import routines.system.*;
import routines.system.api.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.math.BigDecimal;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.Comparator;
 





@SuppressWarnings("unused")

/**
 * Job: ETL_Fact_Vente2 Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ETL_Fact_Vente2 implements TalendJob {

protected static void logIgnoredError(String message, Throwable cause) {
       System.err.println(message);
       if (cause != null) {
               cause.printStackTrace();
       }

}


	public final Object obj = new Object();

	// for transmiting parameters purpose
	private Object valueObject = null;

	public Object getValueObject() {
		return this.valueObject;
	}

	public void setValueObject(Object valueObject) {
		this.valueObject = valueObject;
	}
	
	private final static String defaultCharset = java.nio.charset.Charset.defaultCharset().name();

	
	private final static String utf8Charset = "UTF-8";
	//contains type for every context property
	public class PropertiesWithType extends java.util.Properties {
		private static final long serialVersionUID = 1L;
		private java.util.Map<String,String> propertyTypes = new java.util.HashMap<>();
		
		public PropertiesWithType(java.util.Properties properties){
			super(properties);
		}
		public PropertiesWithType(){
			super();
		}
		
		public void setContextType(String key, String type) {
			propertyTypes.put(key,type);
		}
	
		public String getContextType(String key) {
			return propertyTypes.get(key);
		}
	}
	
	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();
	// create application properties with default
	public class ContextProperties extends PropertiesWithType {

		private static final long serialVersionUID = 1L;

		public ContextProperties(java.util.Properties properties){
			super(properties);
		}
		public ContextProperties(){
			super();
		}

		public void synchronizeContext(){
			
		}
		
		//if the stored or passed value is "<TALEND_NULL>" string, it mean null
		public String getStringValue(String key) {
			String origin_value = this.getProperty(key);
			if(NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY.equals(origin_value)) {
				return null;
			}
			return origin_value;
		}

	}
	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.
	public ContextProperties getContext() {
		return this.context;
	}
	private final String jobVersion = "0.1";
	private final String jobName = "ETL_Fact_Vente2";
	private final String projectName = "PI_PA_MEHDI";
	public Integer errorCode = null;
	private String currentComponent = "";
	
		private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
        private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();
	
		private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
		public  final java.util.List<String[]> globalBuffer = new java.util.ArrayList<String[]>();
	

private RunStat runStat = new RunStat();

	// OSGi DataSource
	private final static String KEY_DB_DATASOURCES = "KEY_DB_DATASOURCES";
	
	private final static String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

	public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		for (java.util.Map.Entry<String, javax.sql.DataSource> dataSourceEntry : dataSources.entrySet()) {
			talendDataSources.put(dataSourceEntry.getKey(), new routines.system.TalendDataSource(dataSourceEntry.getValue()));
		}
		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}
	
	public void setDataSourceReferences(List serviceReferences) throws Exception{
		
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		java.util.Map<String, javax.sql.DataSource> dataSources = new java.util.HashMap<String, javax.sql.DataSource>();
		
		for (java.util.Map.Entry<String, javax.sql.DataSource> entry : BundleUtils.getServices(serviceReferences,  javax.sql.DataSource.class).entrySet()) {
                    dataSources.put(entry.getKey(), entry.getValue());
                    talendDataSources.put(entry.getKey(), new routines.system.TalendDataSource(entry.getValue()));
		}

		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}


private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

public String getExceptionStackTrace() {
	if ("failure".equals(this.getStatus())) {
		errorMessagePS.flush();
		return baos.toString();
	}
	return null;
}

private Exception exception;

public Exception getException() {
	if ("failure".equals(this.getStatus())) {
		return this.exception;
	}
	return null;
}

private class TalendException extends Exception {

	private static final long serialVersionUID = 1L;

	private java.util.Map<String, Object> globalMap = null;
	private Exception e = null;
	private String currentComponent = null;
	private String virtualComponentName = null;
	
	public void setVirtualComponentName (String virtualComponentName){
		this.virtualComponentName = virtualComponentName;
	}

	private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
		this.currentComponent= errorComponent;
		this.globalMap = globalMap;
		this.e = e;
	}

	public Exception getException() {
		return this.e;
	}

	public String getCurrentComponent() {
		return this.currentComponent;
	}

	
    public String getExceptionCauseMessage(Exception e){
        Throwable cause = e;
        String message = null;
        int i = 10;
        while (null != cause && 0 < i--) {
            message = cause.getMessage();
            if (null == message) {
                cause = cause.getCause();
            } else {
                break;          
            }
        }
        if (null == message) {
            message = e.getClass().getName();
        }   
        return message;
    }

	@Override
	public void printStackTrace() {
		if (!(e instanceof TalendException || e instanceof TDieException)) {
			if(virtualComponentName!=null && currentComponent.indexOf(virtualComponentName+"_")==0){
				globalMap.put(virtualComponentName+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			}
			globalMap.put(currentComponent+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			System.err.println("Exception in component " + currentComponent + " (" + jobName + ")");
		}
		if (!(e instanceof TDieException)) {
			if(e instanceof TalendException){
				e.printStackTrace();
			} else {
				e.printStackTrace();
				e.printStackTrace(errorMessagePS);
				ETL_Fact_Vente2.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ETL_Fact_Vente2.this, new Object[] { e , currentComponent, globalMap});
					break;
				}
			}

			if(!(e instanceof TDieException)){
			}
		} catch (Exception e) {
			this.e.printStackTrace();
		}
		}
	}
}

			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBSCD_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_9_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_10_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_12_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_11_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row9_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row11_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row8_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row13_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row12_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class outfinalStruct implements routines.system.IPersistableRow<outfinalStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Vente_PK;

				public int getVente_PK () {
					return this.Vente_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Type_Vente_FK;

				public Integer getType_Vente_FK () {
					return this.Type_Vente_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Integer Categorie_FK;

				public Integer getCategorie_FK () {
					return this.Categorie_FK;
				}
				
			    public Integer Client_FK;

				public Integer getClient_FK () {
					return this.Client_FK;
				}
				
			    public Integer Geographie_FK;

				public Integer getGeographie_FK () {
					return this.Geographie_FK;
				}
				
			    public Integer Mode_Paiement_FK;

				public Integer getMode_Paiement_FK () {
					return this.Mode_Paiement_FK;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Livraison;

				public Double getMontant_Livraison () {
					return this.Montant_Livraison;
				}
				
			    public Double Montant_Remboursement;

				public Double getMontant_Remboursement () {
					return this.Montant_Remboursement;
				}
				
			    public Double Montant_de_la_commande;

				public Double getMontant_de_la_commande () {
					return this.Montant_de_la_commande;
				}
				
			    public Double Montant_Remise;

				public Double getMontant_Remise () {
					return this.Montant_Remise;
				}
				
			    public String Code_Promo;

				public String getCode_Promo () {
					return this.Code_Promo;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String Numero_Commande;

				public String getNumero_Commande () {
					return this.Numero_Commande;
				}
				
			    public Double Prix_Unite;

				public Double getPrix_Unite () {
					return this.Prix_Unite;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Vente_PK;
						
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final outfinalStruct other = (outfinalStruct) obj;
		
						if (this.Vente_PK != other.Vente_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(outfinalStruct other) {

		other.Vente_PK = this.Vente_PK;
	            other.Date_FK = this.Date_FK;
	            other.Type_Vente_FK = this.Type_Vente_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Categorie_FK = this.Categorie_FK;
	            other.Client_FK = this.Client_FK;
	            other.Geographie_FK = this.Geographie_FK;
	            other.Mode_Paiement_FK = this.Mode_Paiement_FK;
	            other.Quantite = this.Quantite;
	            other.Montant_Livraison = this.Montant_Livraison;
	            other.Montant_Remboursement = this.Montant_Remboursement;
	            other.Montant_de_la_commande = this.Montant_de_la_commande;
	            other.Montant_Remise = this.Montant_Remise;
	            other.Code_Promo = this.Code_Promo;
	            other.Numero_Facture = this.Numero_Facture;
	            other.Numero_Commande = this.Numero_Commande;
	            other.Prix_Unite = this.Prix_Unite;
	            
	}

	public void copyKeysDataTo(outfinalStruct other) {

		other.Vente_PK = this.Vente_PK;
	            	
	}



	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Vente_PK="+String.valueOf(Vente_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Type_Vente_FK="+String.valueOf(Type_Vente_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Categorie_FK="+String.valueOf(Categorie_FK));
		sb.append(",Client_FK="+String.valueOf(Client_FK));
		sb.append(",Geographie_FK="+String.valueOf(Geographie_FK));
		sb.append(",Mode_Paiement_FK="+String.valueOf(Mode_Paiement_FK));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Livraison="+String.valueOf(Montant_Livraison));
		sb.append(",Montant_Remboursement="+String.valueOf(Montant_Remboursement));
		sb.append(",Montant_de_la_commande="+String.valueOf(Montant_de_la_commande));
		sb.append(",Montant_Remise="+String.valueOf(Montant_Remise));
		sb.append(",Code_Promo="+Code_Promo);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",Numero_Commande="+Numero_Commande);
		sb.append(",Prix_Unite="+String.valueOf(Prix_Unite));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(outfinalStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Vente_PK, other.Vente_PK);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class client_rejectedStruct implements routines.system.IPersistableRow<client_rejectedStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public String Code_client;

				public String getCode_client () {
					return this.Code_client;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Code_client = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Code_client = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Code_client,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Code_client,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Code_client="+Code_client);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(client_rejectedStruct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class out_stateStruct implements routines.system.IPersistableRow<out_stateStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public int Vente_PK;

				public int getVente_PK () {
					return this.Vente_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Type_Vente_FK;

				public Integer getType_Vente_FK () {
					return this.Type_Vente_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Integer Categorie_FK;

				public Integer getCategorie_FK () {
					return this.Categorie_FK;
				}
				
			    public Integer Client_FK;

				public Integer getClient_FK () {
					return this.Client_FK;
				}
				
			    public Integer Geographie_FK;

				public Integer getGeographie_FK () {
					return this.Geographie_FK;
				}
				
			    public Integer Mode_Paiement_FK;

				public Integer getMode_Paiement_FK () {
					return this.Mode_Paiement_FK;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Livraison;

				public Double getMontant_Livraison () {
					return this.Montant_Livraison;
				}
				
			    public Double Montant_Remboursement;

				public Double getMontant_Remboursement () {
					return this.Montant_Remboursement;
				}
				
			    public Double Montant_de_la_commande;

				public Double getMontant_de_la_commande () {
					return this.Montant_de_la_commande;
				}
				
			    public Double Montant_Remise;

				public Double getMontant_Remise () {
					return this.Montant_Remise;
				}
				
			    public String Code_Promo;

				public String getCode_Promo () {
					return this.Code_Promo;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String Numero_Commande;

				public String getNumero_Commande () {
					return this.Numero_Commande;
				}
				
			    public Double Prix_Unite;

				public Double getPrix_Unite () {
					return this.Prix_Unite;
				}
				
			    public String Code_Client;

				public String getCode_Client () {
					return this.Code_Client;
				}
				
			    public java.util.Date Date_de_commande;

				public java.util.Date getDate_de_commande () {
					return this.Date_de_commande;
				}
				
			    public String Nom_de_l_element;

				public String getNom_de_l_element () {
					return this.Nom_de_l_element;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String TypeVente;

				public String getTypeVente () {
					return this.TypeVente;
				}
				
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				


	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Vente_PK="+String.valueOf(Vente_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Type_Vente_FK="+String.valueOf(Type_Vente_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Categorie_FK="+String.valueOf(Categorie_FK));
		sb.append(",Client_FK="+String.valueOf(Client_FK));
		sb.append(",Geographie_FK="+String.valueOf(Geographie_FK));
		sb.append(",Mode_Paiement_FK="+String.valueOf(Mode_Paiement_FK));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Livraison="+String.valueOf(Montant_Livraison));
		sb.append(",Montant_Remboursement="+String.valueOf(Montant_Remboursement));
		sb.append(",Montant_de_la_commande="+String.valueOf(Montant_de_la_commande));
		sb.append(",Montant_Remise="+String.valueOf(Montant_Remise));
		sb.append(",Code_Promo="+Code_Promo);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",Numero_Commande="+Numero_Commande);
		sb.append(",Prix_Unite="+String.valueOf(Prix_Unite));
		sb.append(",Code_Client="+Code_Client);
		sb.append(",Date_de_commande="+String.valueOf(Date_de_commande));
		sb.append(",Nom_de_l_element="+Nom_de_l_element);
		sb.append(",UGS="+UGS);
		sb.append(",Ville="+Ville);
		sb.append(",TypeVente="+TypeVente);
		sb.append(",Mode_Paiement="+Mode_Paiement);
		sb.append(",Categorie="+Categorie);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out_stateStruct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class row3Struct implements routines.system.IPersistableRow<row3Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public int Vente_PK;

				public int getVente_PK () {
					return this.Vente_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Type_Vente_FK;

				public Integer getType_Vente_FK () {
					return this.Type_Vente_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Integer Categorie_FK;

				public Integer getCategorie_FK () {
					return this.Categorie_FK;
				}
				
			    public Integer Client_FK;

				public Integer getClient_FK () {
					return this.Client_FK;
				}
				
			    public Integer Geographie_FK;

				public Integer getGeographie_FK () {
					return this.Geographie_FK;
				}
				
			    public Integer Mode_Paiement_FK;

				public Integer getMode_Paiement_FK () {
					return this.Mode_Paiement_FK;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Livraison;

				public Double getMontant_Livraison () {
					return this.Montant_Livraison;
				}
				
			    public Double Montant_Remboursement;

				public Double getMontant_Remboursement () {
					return this.Montant_Remboursement;
				}
				
			    public Double Montant_de_la_commande;

				public Double getMontant_de_la_commande () {
					return this.Montant_de_la_commande;
				}
				
			    public Double Montant_Remise;

				public Double getMontant_Remise () {
					return this.Montant_Remise;
				}
				
			    public String Code_Promo;

				public String getCode_Promo () {
					return this.Code_Promo;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String Numero_Commande;

				public String getNumero_Commande () {
					return this.Numero_Commande;
				}
				
			    public Double Prix_Unite;

				public Double getPrix_Unite () {
					return this.Prix_Unite;
				}
				
			    public String Code_Client;

				public String getCode_Client () {
					return this.Code_Client;
				}
				
			    public java.util.Date Date_de_commande;

				public java.util.Date getDate_de_commande () {
					return this.Date_de_commande;
				}
				
			    public String Nom_de_l_element;

				public String getNom_de_l_element () {
					return this.Nom_de_l_element;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String TypeVente;

				public String getTypeVente () {
					return this.TypeVente;
				}
				
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				


	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Vente_PK="+String.valueOf(Vente_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Type_Vente_FK="+String.valueOf(Type_Vente_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Categorie_FK="+String.valueOf(Categorie_FK));
		sb.append(",Client_FK="+String.valueOf(Client_FK));
		sb.append(",Geographie_FK="+String.valueOf(Geographie_FK));
		sb.append(",Mode_Paiement_FK="+String.valueOf(Mode_Paiement_FK));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Livraison="+String.valueOf(Montant_Livraison));
		sb.append(",Montant_Remboursement="+String.valueOf(Montant_Remboursement));
		sb.append(",Montant_de_la_commande="+String.valueOf(Montant_de_la_commande));
		sb.append(",Montant_Remise="+String.valueOf(Montant_Remise));
		sb.append(",Code_Promo="+Code_Promo);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",Numero_Commande="+Numero_Commande);
		sb.append(",Prix_Unite="+String.valueOf(Prix_Unite));
		sb.append(",Code_Client="+Code_Client);
		sb.append(",Date_de_commande="+String.valueOf(Date_de_commande));
		sb.append(",Nom_de_l_element="+Nom_de_l_element);
		sb.append(",UGS="+UGS);
		sb.append(",Ville="+Ville);
		sb.append(",TypeVente="+TypeVente);
		sb.append(",Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row3Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class out2Struct implements routines.system.IPersistableRow<out2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Vente_PK;

				public int getVente_PK () {
					return this.Vente_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Type_Vente_FK;

				public Integer getType_Vente_FK () {
					return this.Type_Vente_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Integer Categorie_FK;

				public Integer getCategorie_FK () {
					return this.Categorie_FK;
				}
				
			    public Integer Client_FK;

				public Integer getClient_FK () {
					return this.Client_FK;
				}
				
			    public Integer Geographie_FK;

				public Integer getGeographie_FK () {
					return this.Geographie_FK;
				}
				
			    public Integer Mode_Paiement_FK;

				public Integer getMode_Paiement_FK () {
					return this.Mode_Paiement_FK;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Livraison;

				public Double getMontant_Livraison () {
					return this.Montant_Livraison;
				}
				
			    public Double Montant_Remboursement;

				public Double getMontant_Remboursement () {
					return this.Montant_Remboursement;
				}
				
			    public Double Montant_de_la_commande;

				public Double getMontant_de_la_commande () {
					return this.Montant_de_la_commande;
				}
				
			    public Double Montant_Remise;

				public Double getMontant_Remise () {
					return this.Montant_Remise;
				}
				
			    public String Code_Promo;

				public String getCode_Promo () {
					return this.Code_Promo;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String Numero_Commande;

				public String getNumero_Commande () {
					return this.Numero_Commande;
				}
				
			    public Double Prix_Unite;

				public Double getPrix_Unite () {
					return this.Prix_Unite;
				}
				
			    public String Code_Client;

				public String getCode_Client () {
					return this.Code_Client;
				}
				
			    public java.util.Date Date_de_commande;

				public java.util.Date getDate_de_commande () {
					return this.Date_de_commande;
				}
				
			    public String Nom_de_l_element;

				public String getNom_de_l_element () {
					return this.Nom_de_l_element;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String TypeVente;

				public String getTypeVente () {
					return this.TypeVente;
				}
				
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Vente_PK;
						
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final out2Struct other = (out2Struct) obj;
		
						if (this.Vente_PK != other.Vente_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out2Struct other) {

		other.Vente_PK = this.Vente_PK;
	            other.Date_FK = this.Date_FK;
	            other.Type_Vente_FK = this.Type_Vente_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Categorie_FK = this.Categorie_FK;
	            other.Client_FK = this.Client_FK;
	            other.Geographie_FK = this.Geographie_FK;
	            other.Mode_Paiement_FK = this.Mode_Paiement_FK;
	            other.Quantite = this.Quantite;
	            other.Montant_Livraison = this.Montant_Livraison;
	            other.Montant_Remboursement = this.Montant_Remboursement;
	            other.Montant_de_la_commande = this.Montant_de_la_commande;
	            other.Montant_Remise = this.Montant_Remise;
	            other.Code_Promo = this.Code_Promo;
	            other.Numero_Facture = this.Numero_Facture;
	            other.Numero_Commande = this.Numero_Commande;
	            other.Prix_Unite = this.Prix_Unite;
	            other.Code_Client = this.Code_Client;
	            other.Date_de_commande = this.Date_de_commande;
	            other.Nom_de_l_element = this.Nom_de_l_element;
	            other.UGS = this.UGS;
	            other.Ville = this.Ville;
	            other.TypeVente = this.TypeVente;
	            other.Mode_Paiement = this.Mode_Paiement;
	            
	}

	public void copyKeysDataTo(out2Struct other) {

		other.Vente_PK = this.Vente_PK;
	            	
	}



	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Vente_PK="+String.valueOf(Vente_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Type_Vente_FK="+String.valueOf(Type_Vente_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Categorie_FK="+String.valueOf(Categorie_FK));
		sb.append(",Client_FK="+String.valueOf(Client_FK));
		sb.append(",Geographie_FK="+String.valueOf(Geographie_FK));
		sb.append(",Mode_Paiement_FK="+String.valueOf(Mode_Paiement_FK));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Livraison="+String.valueOf(Montant_Livraison));
		sb.append(",Montant_Remboursement="+String.valueOf(Montant_Remboursement));
		sb.append(",Montant_de_la_commande="+String.valueOf(Montant_de_la_commande));
		sb.append(",Montant_Remise="+String.valueOf(Montant_Remise));
		sb.append(",Code_Promo="+Code_Promo);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",Numero_Commande="+Numero_Commande);
		sb.append(",Prix_Unite="+String.valueOf(Prix_Unite));
		sb.append(",Code_Client="+Code_Client);
		sb.append(",Date_de_commande="+String.valueOf(Date_de_commande));
		sb.append(",Nom_de_l_element="+Nom_de_l_element);
		sb.append(",UGS="+UGS);
		sb.append(",Ville="+Ville);
		sb.append(",TypeVente="+TypeVente);
		sb.append(",Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out2Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Vente_PK, other.Vente_PK);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class row2Struct implements routines.system.IPersistableRow<row2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public String Reference;

				public String getReference () {
					return this.Reference;
				}
				
			    public String Description;

				public String getDescription () {
					return this.Description;
				}
				
			    public String PU_HT;

				public String getPU_HT () {
					return this.PU_HT;
				}
				
			    public String Quantite;

				public String getQuantite () {
					return this.Quantite;
				}
				
			    public String Prix_Total_HT;

				public String getPrix_Total_HT () {
					return this.Prix_Total_HT;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String id_Client;

				public String getId_Client () {
					return this.id_Client;
				}
				
			    public String Date;

				public String getDate () {
					return this.Date;
				}
				
			    public String Client_Nom;

				public String getClient_Nom () {
					return this.Client_Nom;
				}
				
			    public String Matricule_Fiscal;

				public String getMatricule_Fiscal () {
					return this.Matricule_Fiscal;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String TVA;

				public String getTVA () {
					return this.TVA;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.Prix_Total_HT = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.id_Client = readString(dis);
					
					this.Date = readString(dis);
					
					this.Client_Nom = readString(dis);
					
					this.Matricule_Fiscal = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.TVA = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.Prix_Total_HT = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.id_Client = readString(dis);
					
					this.Date = readString(dis);
					
					this.Client_Nom = readString(dis);
					
					this.Matricule_Fiscal = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.TVA = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.Prix_Total_HT,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.id_Client,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Client_Nom,dos);
					
					// String
				
						writeString(this.Matricule_Fiscal,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.Prix_Total_HT,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.id_Client,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Client_Nom,dos);
					
					// String
				
						writeString(this.Matricule_Fiscal,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Reference="+Reference);
		sb.append(",Description="+Description);
		sb.append(",PU_HT="+PU_HT);
		sb.append(",Quantite="+Quantite);
		sb.append(",Prix_Total_HT="+Prix_Total_HT);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",id_Client="+id_Client);
		sb.append(",Date="+Date);
		sb.append(",Client_Nom="+Client_Nom);
		sb.append(",Matricule_Fiscal="+Matricule_Fiscal);
		sb.append(",Adresse="+Adresse);
		sb.append(",TVA="+TVA);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row2Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class after_tDBInput_2Struct implements routines.system.IPersistableRow<after_tDBInput_2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public String Reference;

				public String getReference () {
					return this.Reference;
				}
				
			    public String Description;

				public String getDescription () {
					return this.Description;
				}
				
			    public String PU_HT;

				public String getPU_HT () {
					return this.PU_HT;
				}
				
			    public String Quantite;

				public String getQuantite () {
					return this.Quantite;
				}
				
			    public String Prix_Total_HT;

				public String getPrix_Total_HT () {
					return this.Prix_Total_HT;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String id_Client;

				public String getId_Client () {
					return this.id_Client;
				}
				
			    public String Date;

				public String getDate () {
					return this.Date;
				}
				
			    public String Client_Nom;

				public String getClient_Nom () {
					return this.Client_Nom;
				}
				
			    public String Matricule_Fiscal;

				public String getMatricule_Fiscal () {
					return this.Matricule_Fiscal;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String TVA;

				public String getTVA () {
					return this.TVA;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.Prix_Total_HT = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.id_Client = readString(dis);
					
					this.Date = readString(dis);
					
					this.Client_Nom = readString(dis);
					
					this.Matricule_Fiscal = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.TVA = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.Prix_Total_HT = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.id_Client = readString(dis);
					
					this.Date = readString(dis);
					
					this.Client_Nom = readString(dis);
					
					this.Matricule_Fiscal = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.TVA = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.Prix_Total_HT,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.id_Client,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Client_Nom,dos);
					
					// String
				
						writeString(this.Matricule_Fiscal,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.Prix_Total_HT,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.id_Client,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Client_Nom,dos);
					
					// String
				
						writeString(this.Matricule_Fiscal,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Reference="+Reference);
		sb.append(",Description="+Description);
		sb.append(",PU_HT="+PU_HT);
		sb.append(",Quantite="+Quantite);
		sb.append(",Prix_Total_HT="+Prix_Total_HT);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",id_Client="+id_Client);
		sb.append(",Date="+Date);
		sb.append(",Client_Nom="+Client_Nom);
		sb.append(",Matricule_Fiscal="+Matricule_Fiscal);
		sb.append(",Adresse="+Adresse);
		sb.append(",TVA="+TVA);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(after_tDBInput_2Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class out1Struct implements routines.system.IPersistableRow<out1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Vente_PK;

				public int getVente_PK () {
					return this.Vente_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Type_Vente_FK;

				public Integer getType_Vente_FK () {
					return this.Type_Vente_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Integer Categorie_FK;

				public Integer getCategorie_FK () {
					return this.Categorie_FK;
				}
				
			    public Integer Client_FK;

				public Integer getClient_FK () {
					return this.Client_FK;
				}
				
			    public Integer Geographie_FK;

				public Integer getGeographie_FK () {
					return this.Geographie_FK;
				}
				
			    public Integer Mode_Paiement_FK;

				public Integer getMode_Paiement_FK () {
					return this.Mode_Paiement_FK;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Livraison;

				public Double getMontant_Livraison () {
					return this.Montant_Livraison;
				}
				
			    public Double Montant_Remboursement;

				public Double getMontant_Remboursement () {
					return this.Montant_Remboursement;
				}
				
			    public Double Montant_de_la_commande;

				public Double getMontant_de_la_commande () {
					return this.Montant_de_la_commande;
				}
				
			    public Double Montant_Remise;

				public Double getMontant_Remise () {
					return this.Montant_Remise;
				}
				
			    public String Code_Promo;

				public String getCode_Promo () {
					return this.Code_Promo;
				}
				
			    public String Numero_Facture;

				public String getNumero_Facture () {
					return this.Numero_Facture;
				}
				
			    public String Numero_Commande;

				public String getNumero_Commande () {
					return this.Numero_Commande;
				}
				
			    public Double Prix_Unite;

				public Double getPrix_Unite () {
					return this.Prix_Unite;
				}
				
			    public String Code_Client;

				public String getCode_Client () {
					return this.Code_Client;
				}
				
			    public java.util.Date Date_de_commande;

				public java.util.Date getDate_de_commande () {
					return this.Date_de_commande;
				}
				
			    public String Nom_de_l_element;

				public String getNom_de_l_element () {
					return this.Nom_de_l_element;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String TypeVente;

				public String getTypeVente () {
					return this.TypeVente;
				}
				
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Vente_PK;
						
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final out1Struct other = (out1Struct) obj;
		
						if (this.Vente_PK != other.Vente_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out1Struct other) {

		other.Vente_PK = this.Vente_PK;
	            other.Date_FK = this.Date_FK;
	            other.Type_Vente_FK = this.Type_Vente_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Categorie_FK = this.Categorie_FK;
	            other.Client_FK = this.Client_FK;
	            other.Geographie_FK = this.Geographie_FK;
	            other.Mode_Paiement_FK = this.Mode_Paiement_FK;
	            other.Quantite = this.Quantite;
	            other.Montant_Livraison = this.Montant_Livraison;
	            other.Montant_Remboursement = this.Montant_Remboursement;
	            other.Montant_de_la_commande = this.Montant_de_la_commande;
	            other.Montant_Remise = this.Montant_Remise;
	            other.Code_Promo = this.Code_Promo;
	            other.Numero_Facture = this.Numero_Facture;
	            other.Numero_Commande = this.Numero_Commande;
	            other.Prix_Unite = this.Prix_Unite;
	            other.Code_Client = this.Code_Client;
	            other.Date_de_commande = this.Date_de_commande;
	            other.Nom_de_l_element = this.Nom_de_l_element;
	            other.UGS = this.UGS;
	            other.Ville = this.Ville;
	            other.TypeVente = this.TypeVente;
	            other.Mode_Paiement = this.Mode_Paiement;
	            
	}

	public void copyKeysDataTo(out1Struct other) {

		other.Vente_PK = this.Vente_PK;
	            	
	}



	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
			        this.Vente_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Type_Vente_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
						this.Categorie_FK = readInteger(dis);
					
						this.Client_FK = readInteger(dis);
					
						this.Geographie_FK = readInteger(dis);
					
						this.Mode_Paiement_FK = readInteger(dis);
					
						this.Quantite = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Livraison = null;
           				} else {
           			    	this.Montant_Livraison = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remboursement = null;
           				} else {
           			    	this.Montant_Remboursement = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_de_la_commande = null;
           				} else {
           			    	this.Montant_de_la_commande = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Remise = null;
           				} else {
           			    	this.Montant_Remise = dis.readDouble();
           				}
					
					this.Code_Promo = readString(dis);
					
					this.Numero_Facture = readString(dis);
					
					this.Numero_Commande = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_Unite = null;
           				} else {
           			    	this.Prix_Unite = dis.readDouble();
           				}
					
					this.Code_Client = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Nom_de_l_element = readString(dis);
					
					this.UGS = readString(dis);
					
					this.Ville = readString(dis);
					
					this.TypeVente = readString(dis);
					
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Vente_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Type_Vente_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Integer
				
						writeInteger(this.Categorie_FK,dos);
					
					// Integer
				
						writeInteger(this.Client_FK,dos);
					
					// Integer
				
						writeInteger(this.Geographie_FK,dos);
					
					// Integer
				
						writeInteger(this.Mode_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Double
				
						if(this.Montant_Livraison == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Livraison);
		            	}
					
					// Double
				
						if(this.Montant_Remboursement == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remboursement);
		            	}
					
					// Double
				
						if(this.Montant_de_la_commande == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_de_la_commande);
		            	}
					
					// Double
				
						if(this.Montant_Remise == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Remise);
		            	}
					
					// String
				
						writeString(this.Code_Promo,dos);
					
					// String
				
						writeString(this.Numero_Facture,dos);
					
					// String
				
						writeString(this.Numero_Commande,dos);
					
					// Double
				
						if(this.Prix_Unite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_Unite);
		            	}
					
					// String
				
						writeString(this.Code_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.TypeVente,dos);
					
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Vente_PK="+String.valueOf(Vente_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Type_Vente_FK="+String.valueOf(Type_Vente_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Categorie_FK="+String.valueOf(Categorie_FK));
		sb.append(",Client_FK="+String.valueOf(Client_FK));
		sb.append(",Geographie_FK="+String.valueOf(Geographie_FK));
		sb.append(",Mode_Paiement_FK="+String.valueOf(Mode_Paiement_FK));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Livraison="+String.valueOf(Montant_Livraison));
		sb.append(",Montant_Remboursement="+String.valueOf(Montant_Remboursement));
		sb.append(",Montant_de_la_commande="+String.valueOf(Montant_de_la_commande));
		sb.append(",Montant_Remise="+String.valueOf(Montant_Remise));
		sb.append(",Code_Promo="+Code_Promo);
		sb.append(",Numero_Facture="+Numero_Facture);
		sb.append(",Numero_Commande="+Numero_Commande);
		sb.append(",Prix_Unite="+String.valueOf(Prix_Unite));
		sb.append(",Code_Client="+Code_Client);
		sb.append(",Date_de_commande="+String.valueOf(Date_de_commande));
		sb.append(",Nom_de_l_element="+Nom_de_l_element);
		sb.append(",UGS="+UGS);
		sb.append(",Ville="+Ville);
		sb.append(",TypeVente="+TypeVente);
		sb.append(",Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out1Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Vente_PK, other.Vente_PK);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public static class row1Struct implements routines.system.IPersistableRow<row1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];

	
			    public String Numero_de_commande;

				public String getNumero_de_commande () {
					return this.Numero_de_commande;
				}
				
			    public String Etat_de_la_commande;

				public String getEtat_de_la_commande () {
					return this.Etat_de_la_commande;
				}
				
			    public java.util.Date Date_de_commande;

				public java.util.Date getDate_de_commande () {
					return this.Date_de_commande;
				}
				
			    public String Note_du_client;

				public String getNote_du_client () {
					return this.Note_du_client;
				}
				
			    public String Prenom__Facturation;

				public String getPrenom__Facturation () {
					return this.Prenom__Facturation;
				}
				
			    public String NOM_DE_FAMILLE__FACTURATION;

				public String getNOM_DE_FAMILLE__FACTURATION () {
					return this.NOM_DE_FAMILLE__FACTURATION;
				}
				
			    public String Societe__Facturation;

				public String getSociete__Facturation () {
					return this.Societe__Facturation;
				}
				
			    public String Adresse_1___2__Facturation;

				public String getAdresse_1___2__Facturation () {
					return this.Adresse_1___2__Facturation;
				}
				
			    public String Ville__Facturation;

				public String getVille__Facturation () {
					return this.Ville__Facturation;
				}
				
			    public String Code_de_l_etat__Facturation;

				public String getCode_de_l_etat__Facturation () {
					return this.Code_de_l_etat__Facturation;
				}
				
			    public String Code_postal__Facturation;

				public String getCode_postal__Facturation () {
					return this.Code_postal__Facturation;
				}
				
			    public String Code_du_pays__Facturation;

				public String getCode_du_pays__Facturation () {
					return this.Code_du_pays__Facturation;
				}
				
			    public String Prenom__Livraison;

				public String getPrenom__Livraison () {
					return this.Prenom__Livraison;
				}
				
			    public String NOM_DE_FAMILLE__LIVRAISON;

				public String getNOM_DE_FAMILLE__LIVRAISON () {
					return this.NOM_DE_FAMILLE__LIVRAISON;
				}
				
			    public String Adresse_1___2__Livraison;

				public String getAdresse_1___2__Livraison () {
					return this.Adresse_1___2__Livraison;
				}
				
			    public String Ville__Livraison;

				public String getVille__Livraison () {
					return this.Ville__Livraison;
				}
				
			    public String Code_de_l_etat__Livraison;

				public String getCode_de_l_etat__Livraison () {
					return this.Code_de_l_etat__Livraison;
				}
				
			    public String Code_postal__Livraison;

				public String getCode_postal__Livraison () {
					return this.Code_postal__Livraison;
				}
				
			    public String Code_du_pays__Livraison;

				public String getCode_du_pays__Livraison () {
					return this.Code_du_pays__Livraison;
				}
				
			    public String Titre_de_la_methode_de_paiement;

				public String getTitre_de_la_methode_de_paiement () {
					return this.Titre_de_la_methode_de_paiement;
				}
				
			    public BigDecimal Montant_de_la_remise_panier;

				public BigDecimal getMontant_de_la_remise_panier () {
					return this.Montant_de_la_remise_panier;
				}
				
			    public BigDecimal Cart_Discount_Amount_inc__tax;

				public BigDecimal getCart_Discount_Amount_inc__tax () {
					return this.Cart_Discount_Amount_inc__tax;
				}
				
			    public BigDecimal Sous_total_de_la_commande;

				public BigDecimal getSous_total_de_la_commande () {
					return this.Sous_total_de_la_commande;
				}
				
			    public String Titre_de_la_methode_de_livraison;

				public String getTitre_de_la_methode_de_livraison () {
					return this.Titre_de_la_methode_de_livraison;
				}
				
			    public BigDecimal Montant_de_la_livraison;

				public BigDecimal getMontant_de_la_livraison () {
					return this.Montant_de_la_livraison;
				}
				
			    public BigDecimal Montant_du_remboursement_de_la_commande;

				public BigDecimal getMontant_du_remboursement_de_la_commande () {
					return this.Montant_du_remboursement_de_la_commande;
				}
				
			    public BigDecimal Montant_total_de_la_commande;

				public BigDecimal getMontant_total_de_la_commande () {
					return this.Montant_total_de_la_commande;
				}
				
			    public BigDecimal Montant_total_de_la_taxe;

				public BigDecimal getMontant_total_de_la_taxe () {
					return this.Montant_total_de_la_taxe;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Article;

				public String getArticle () {
					return this.Article;
				}
				
			    public String Nom_de_l_element;

				public String getNom_de_l_element () {
					return this.Nom_de_l_element;
				}
				
			    public Integer Quantite____Remboursement;

				public Integer getQuantite____Remboursement () {
					return this.Quantite____Remboursement;
				}
				
			    public BigDecimal Prix_du_produit;

				public BigDecimal getPrix_du_produit () {
					return this.Prix_du_produit;
				}
				
			    public String Code_promo;

				public String getCode_promo () {
					return this.Code_promo;
				}
				
			    public BigDecimal Reduction;

				public BigDecimal getReduction () {
					return this.Reduction;
				}
				
			    public BigDecimal Taxe_de_la_reduction;

				public BigDecimal getTaxe_de_la_reduction () {
					return this.Taxe_de_la_reduction;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }
	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Numero_de_commande = readString(dis);
					
					this.Etat_de_la_commande = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Note_du_client = readString(dis);
					
					this.Prenom__Facturation = readString(dis);
					
					this.NOM_DE_FAMILLE__FACTURATION = readString(dis);
					
					this.Societe__Facturation = readString(dis);
					
					this.Adresse_1___2__Facturation = readString(dis);
					
					this.Ville__Facturation = readString(dis);
					
					this.Code_de_l_etat__Facturation = readString(dis);
					
					this.Code_postal__Facturation = readString(dis);
					
					this.Code_du_pays__Facturation = readString(dis);
					
					this.Prenom__Livraison = readString(dis);
					
					this.NOM_DE_FAMILLE__LIVRAISON = readString(dis);
					
					this.Adresse_1___2__Livraison = readString(dis);
					
					this.Ville__Livraison = readString(dis);
					
					this.Code_de_l_etat__Livraison = readString(dis);
					
					this.Code_postal__Livraison = readString(dis);
					
					this.Code_du_pays__Livraison = readString(dis);
					
					this.Titre_de_la_methode_de_paiement = readString(dis);
					
						this.Montant_de_la_remise_panier = (BigDecimal) dis.readObject();
					
						this.Cart_Discount_Amount_inc__tax = (BigDecimal) dis.readObject();
					
						this.Sous_total_de_la_commande = (BigDecimal) dis.readObject();
					
					this.Titre_de_la_methode_de_livraison = readString(dis);
					
						this.Montant_de_la_livraison = (BigDecimal) dis.readObject();
					
						this.Montant_du_remboursement_de_la_commande = (BigDecimal) dis.readObject();
					
						this.Montant_total_de_la_commande = (BigDecimal) dis.readObject();
					
						this.Montant_total_de_la_taxe = (BigDecimal) dis.readObject();
					
					this.UGS = readString(dis);
					
					this.Article = readString(dis);
					
					this.Nom_de_l_element = readString(dis);
					
						this.Quantite____Remboursement = readInteger(dis);
					
						this.Prix_du_produit = (BigDecimal) dis.readObject();
					
					this.Code_promo = readString(dis);
					
						this.Reduction = (BigDecimal) dis.readObject();
					
						this.Taxe_de_la_reduction = (BigDecimal) dis.readObject();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		
			} catch(ClassNotFoundException eCNFE) {
				 throw new RuntimeException(eCNFE);
		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Numero_de_commande = readString(dis);
					
					this.Etat_de_la_commande = readString(dis);
					
					this.Date_de_commande = readDate(dis);
					
					this.Note_du_client = readString(dis);
					
					this.Prenom__Facturation = readString(dis);
					
					this.NOM_DE_FAMILLE__FACTURATION = readString(dis);
					
					this.Societe__Facturation = readString(dis);
					
					this.Adresse_1___2__Facturation = readString(dis);
					
					this.Ville__Facturation = readString(dis);
					
					this.Code_de_l_etat__Facturation = readString(dis);
					
					this.Code_postal__Facturation = readString(dis);
					
					this.Code_du_pays__Facturation = readString(dis);
					
					this.Prenom__Livraison = readString(dis);
					
					this.NOM_DE_FAMILLE__LIVRAISON = readString(dis);
					
					this.Adresse_1___2__Livraison = readString(dis);
					
					this.Ville__Livraison = readString(dis);
					
					this.Code_de_l_etat__Livraison = readString(dis);
					
					this.Code_postal__Livraison = readString(dis);
					
					this.Code_du_pays__Livraison = readString(dis);
					
					this.Titre_de_la_methode_de_paiement = readString(dis);
					
						this.Montant_de_la_remise_panier = (BigDecimal) dis.readObject();
					
						this.Cart_Discount_Amount_inc__tax = (BigDecimal) dis.readObject();
					
						this.Sous_total_de_la_commande = (BigDecimal) dis.readObject();
					
					this.Titre_de_la_methode_de_livraison = readString(dis);
					
						this.Montant_de_la_livraison = (BigDecimal) dis.readObject();
					
						this.Montant_du_remboursement_de_la_commande = (BigDecimal) dis.readObject();
					
						this.Montant_total_de_la_commande = (BigDecimal) dis.readObject();
					
						this.Montant_total_de_la_taxe = (BigDecimal) dis.readObject();
					
					this.UGS = readString(dis);
					
					this.Article = readString(dis);
					
					this.Nom_de_l_element = readString(dis);
					
						this.Quantite____Remboursement = readInteger(dis);
					
						this.Prix_du_produit = (BigDecimal) dis.readObject();
					
					this.Code_promo = readString(dis);
					
						this.Reduction = (BigDecimal) dis.readObject();
					
						this.Taxe_de_la_reduction = (BigDecimal) dis.readObject();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		
			} catch(ClassNotFoundException eCNFE) {
				 throw new RuntimeException(eCNFE);
		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Numero_de_commande,dos);
					
					// String
				
						writeString(this.Etat_de_la_commande,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Note_du_client,dos);
					
					// String
				
						writeString(this.Prenom__Facturation,dos);
					
					// String
				
						writeString(this.NOM_DE_FAMILLE__FACTURATION,dos);
					
					// String
				
						writeString(this.Societe__Facturation,dos);
					
					// String
				
						writeString(this.Adresse_1___2__Facturation,dos);
					
					// String
				
						writeString(this.Ville__Facturation,dos);
					
					// String
				
						writeString(this.Code_de_l_etat__Facturation,dos);
					
					// String
				
						writeString(this.Code_postal__Facturation,dos);
					
					// String
				
						writeString(this.Code_du_pays__Facturation,dos);
					
					// String
				
						writeString(this.Prenom__Livraison,dos);
					
					// String
				
						writeString(this.NOM_DE_FAMILLE__LIVRAISON,dos);
					
					// String
				
						writeString(this.Adresse_1___2__Livraison,dos);
					
					// String
				
						writeString(this.Ville__Livraison,dos);
					
					// String
				
						writeString(this.Code_de_l_etat__Livraison,dos);
					
					// String
				
						writeString(this.Code_postal__Livraison,dos);
					
					// String
				
						writeString(this.Code_du_pays__Livraison,dos);
					
					// String
				
						writeString(this.Titre_de_la_methode_de_paiement,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_de_la_remise_panier);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Cart_Discount_Amount_inc__tax);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Sous_total_de_la_commande);
					
					// String
				
						writeString(this.Titre_de_la_methode_de_livraison,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_de_la_livraison);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_du_remboursement_de_la_commande);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_total_de_la_commande);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_total_de_la_taxe);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Article,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// Integer
				
						writeInteger(this.Quantite____Remboursement,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Prix_du_produit);
					
					// String
				
						writeString(this.Code_promo,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Reduction);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Taxe_de_la_reduction);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Numero_de_commande,dos);
					
					// String
				
						writeString(this.Etat_de_la_commande,dos);
					
					// java.util.Date
				
						writeDate(this.Date_de_commande,dos);
					
					// String
				
						writeString(this.Note_du_client,dos);
					
					// String
				
						writeString(this.Prenom__Facturation,dos);
					
					// String
				
						writeString(this.NOM_DE_FAMILLE__FACTURATION,dos);
					
					// String
				
						writeString(this.Societe__Facturation,dos);
					
					// String
				
						writeString(this.Adresse_1___2__Facturation,dos);
					
					// String
				
						writeString(this.Ville__Facturation,dos);
					
					// String
				
						writeString(this.Code_de_l_etat__Facturation,dos);
					
					// String
				
						writeString(this.Code_postal__Facturation,dos);
					
					// String
				
						writeString(this.Code_du_pays__Facturation,dos);
					
					// String
				
						writeString(this.Prenom__Livraison,dos);
					
					// String
				
						writeString(this.NOM_DE_FAMILLE__LIVRAISON,dos);
					
					// String
				
						writeString(this.Adresse_1___2__Livraison,dos);
					
					// String
				
						writeString(this.Ville__Livraison,dos);
					
					// String
				
						writeString(this.Code_de_l_etat__Livraison,dos);
					
					// String
				
						writeString(this.Code_postal__Livraison,dos);
					
					// String
				
						writeString(this.Code_du_pays__Livraison,dos);
					
					// String
				
						writeString(this.Titre_de_la_methode_de_paiement,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_de_la_remise_panier);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Cart_Discount_Amount_inc__tax);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Sous_total_de_la_commande);
					
					// String
				
						writeString(this.Titre_de_la_methode_de_livraison,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_de_la_livraison);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_du_remboursement_de_la_commande);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_total_de_la_commande);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Montant_total_de_la_taxe);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Article,dos);
					
					// String
				
						writeString(this.Nom_de_l_element,dos);
					
					// Integer
				
						writeInteger(this.Quantite____Remboursement,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Prix_du_produit);
					
					// String
				
						writeString(this.Code_promo,dos);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Reduction);
					
					// BigDecimal
				
       			    	dos.writeObject(this.Taxe_de_la_reduction);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Numero_de_commande="+Numero_de_commande);
		sb.append(",Etat_de_la_commande="+Etat_de_la_commande);
		sb.append(",Date_de_commande="+String.valueOf(Date_de_commande));
		sb.append(",Note_du_client="+Note_du_client);
		sb.append(",Prenom__Facturation="+Prenom__Facturation);
		sb.append(",NOM_DE_FAMILLE__FACTURATION="+NOM_DE_FAMILLE__FACTURATION);
		sb.append(",Societe__Facturation="+Societe__Facturation);
		sb.append(",Adresse_1___2__Facturation="+Adresse_1___2__Facturation);
		sb.append(",Ville__Facturation="+Ville__Facturation);
		sb.append(",Code_de_l_etat__Facturation="+Code_de_l_etat__Facturation);
		sb.append(",Code_postal__Facturation="+Code_postal__Facturation);
		sb.append(",Code_du_pays__Facturation="+Code_du_pays__Facturation);
		sb.append(",Prenom__Livraison="+Prenom__Livraison);
		sb.append(",NOM_DE_FAMILLE__LIVRAISON="+NOM_DE_FAMILLE__LIVRAISON);
		sb.append(",Adresse_1___2__Livraison="+Adresse_1___2__Livraison);
		sb.append(",Ville__Livraison="+Ville__Livraison);
		sb.append(",Code_de_l_etat__Livraison="+Code_de_l_etat__Livraison);
		sb.append(",Code_postal__Livraison="+Code_postal__Livraison);
		sb.append(",Code_du_pays__Livraison="+Code_du_pays__Livraison);
		sb.append(",Titre_de_la_methode_de_paiement="+Titre_de_la_methode_de_paiement);
		sb.append(",Montant_de_la_remise_panier="+String.valueOf(Montant_de_la_remise_panier));
		sb.append(",Cart_Discount_Amount_inc__tax="+String.valueOf(Cart_Discount_Amount_inc__tax));
		sb.append(",Sous_total_de_la_commande="+String.valueOf(Sous_total_de_la_commande));
		sb.append(",Titre_de_la_methode_de_livraison="+Titre_de_la_methode_de_livraison);
		sb.append(",Montant_de_la_livraison="+String.valueOf(Montant_de_la_livraison));
		sb.append(",Montant_du_remboursement_de_la_commande="+String.valueOf(Montant_du_remboursement_de_la_commande));
		sb.append(",Montant_total_de_la_commande="+String.valueOf(Montant_total_de_la_commande));
		sb.append(",Montant_total_de_la_taxe="+String.valueOf(Montant_total_de_la_taxe));
		sb.append(",UGS="+UGS);
		sb.append(",Article="+Article);
		sb.append(",Nom_de_l_element="+Nom_de_l_element);
		sb.append(",Quantite____Remboursement="+String.valueOf(Quantite____Remboursement));
		sb.append(",Prix_du_produit="+String.valueOf(Prix_du_produit));
		sb.append(",Code_promo="+Code_promo);
		sb.append(",Reduction="+String.valueOf(Reduction));
		sb.append(",Taxe_de_la_reduction="+String.valueOf(Taxe_de_la_reduction));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row1Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_2Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_2_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;


		tDBInput_3Process(globalMap);
		tDBInput_6Process(globalMap);
		tDBInput_4Process(globalMap);
		tDBInput_7Process(globalMap);
		tDBInput_9Process(globalMap);
		tDBInput_10Process(globalMap);
		tDBInput_5Process(globalMap);
		tDBInput_12Process(globalMap);
		tDBInput_11Process(globalMap);

		row2Struct row2 = new row2Struct();
out2Struct out2 = new out2Struct();

		row1Struct row1 = new row1Struct();
out1Struct out1 = new out1Struct();

			row3Struct row3 = new row3Struct();
out_stateStruct out_state = new out_stateStruct();
outfinalStruct outfinal = new outfinalStruct();
client_rejectedStruct client_rejected = new client_rejectedStruct();






	
	/**
	 * [tDBSCD_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBSCD_1", false);
		start_Hash.put("tDBSCD_1", System.currentTimeMillis());
		
	
	currentComponent="tDBSCD_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"outfinal");
					}
				
		int tos_count_tDBSCD_1 = 0;
		



        class SCDSK_tDBSCD_1 {
private int hashCode;
public boolean hashCodeDirty = true;
Integer Categorie_FK;
Integer Client_FK;
Integer Date_FK;
Integer Geographie_FK;
Integer Mode_Paiement_FK;
Integer Produit_FK;
Integer Type_Vente_FK;
public boolean equals(Object obj) {
if (this == obj) return true;
if (obj == null) return false;
if (getClass() != obj.getClass()) return false;
final SCDSK_tDBSCD_1 other = (SCDSK_tDBSCD_1) obj;
if (this.Categorie_FK == null) {
if (other.Categorie_FK!= null)
return false;
} else if (!this.Categorie_FK.equals(other.Categorie_FK))
return false;

if (this.Client_FK == null) {
if (other.Client_FK!= null)
return false;
} else if (!this.Client_FK.equals(other.Client_FK))
return false;

if (this.Date_FK == null) {
if (other.Date_FK!= null)
return false;
} else if (!this.Date_FK.equals(other.Date_FK))
return false;

if (this.Geographie_FK == null) {
if (other.Geographie_FK!= null)
return false;
} else if (!this.Geographie_FK.equals(other.Geographie_FK))
return false;

if (this.Mode_Paiement_FK == null) {
if (other.Mode_Paiement_FK!= null)
return false;
} else if (!this.Mode_Paiement_FK.equals(other.Mode_Paiement_FK))
return false;

if (this.Produit_FK == null) {
if (other.Produit_FK!= null)
return false;
} else if (!this.Produit_FK.equals(other.Produit_FK))
return false;

if (this.Type_Vente_FK == null) {
if (other.Type_Vente_FK!= null)
return false;
} else if (!this.Type_Vente_FK.equals(other.Type_Vente_FK))
return false;

return true;
}
public int hashCode() {
if(hashCodeDirty) {
int prime = 31;hashCode = prime * hashCode + (Categorie_FK == null ? 0 : Categorie_FK.hashCode());
hashCode = prime * hashCode + (Client_FK == null ? 0 : Client_FK.hashCode());
hashCode = prime * hashCode + (Date_FK == null ? 0 : Date_FK.hashCode());
hashCode = prime * hashCode + (Geographie_FK == null ? 0 : Geographie_FK.hashCode());
hashCode = prime * hashCode + (Mode_Paiement_FK == null ? 0 : Mode_Paiement_FK.hashCode());
hashCode = prime * hashCode + (Produit_FK == null ? 0 : Produit_FK.hashCode());
hashCode = prime * hashCode + (Type_Vente_FK == null ? 0 : Type_Vente_FK.hashCode());
hashCodeDirty = false;
}
return hashCode;
}
}

    class SCDStruct_tDBSCD_1 {
private String Code_Promo;
private Double Montant_de_la_commande;
private Double Montant_Livraison;
private Double Montant_Remboursement;
private Double Montant_Remise;
private String Numero_Commande;
private String Numero_Facture;
private Double Prix_Unité;
private Integer Quantite;
}

    int nb_line_update_tDBSCD_1 = 0;
    int nb_line_inserted_tDBSCD_1 = 0;
    int nb_line_rejected_tDBSCD_1 = 0;
    String tableName_tDBSCD_1 = null;
	String dbschema_tDBSCD_1 = null;
java.sql.Connection conn_tDBSCD_1 = null;
String dbUser_tDBSCD_1 = null;
    dbschema_tDBSCD_1 = "";
    String driverClass_tDBSCD_1 = "net.sourceforge.jtds.jdbc.Driver";
	
    java.lang.Class.forName(driverClass_tDBSCD_1);
    String port_tDBSCD_1 = "1433";
    String dbname_tDBSCD_1 = "DW_E-Commerce" ;
    String url_tDBSCD_1 = "jdbc:jtds:sqlserver://" + "localhost" ; 
    if (!"".equals(port_tDBSCD_1)) {
    	url_tDBSCD_1 += ":" + "1433";
    }
    if (!"".equals(dbname_tDBSCD_1)) {
				url_tDBSCD_1 += "//" + "DW_E-Commerce"; 
	
    }
    url_tDBSCD_1 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
    dbUser_tDBSCD_1 = "sa";

 
	final String decryptedPassword_tDBSCD_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:XyHFgRQ0VhnKbFUCZjXu3kSuUvYbAi9N0F5Dtud2BHsPS+u2uVI=");

    String dbPwd_tDBSCD_1 = decryptedPassword_tDBSCD_1;	
    conn_tDBSCD_1 = java.sql.DriverManager.getConnection(url_tDBSCD_1,dbUser_tDBSCD_1,dbPwd_tDBSCD_1);
	

    if(dbschema_tDBSCD_1 == null || dbschema_tDBSCD_1.trim().length() == 0) {
        tableName_tDBSCD_1 = "Fact_Ventes";
    } else {
        tableName_tDBSCD_1 = dbschema_tDBSCD_1 + "].[" + "Fact_Ventes";
    }
	org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBSCD_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();	
    String tmpValue_tDBSCD_1 = null;    
        String search_tDBSCD_1 = "SELECT [Categorie_FK], [Client_FK], [Date_FK], [Geographie_FK], [Mode_Paiement_FK], [Produit_FK], [Type_Vente_FK], [Code_Promo], [Montant_de_la_commande], [Montant_Livraison], [Montant_Remboursement], [Montant_Remise], [Numero_Commande], [Numero_Facture], [Prix_Unité], [Quantite] FROM [" + tableName_tDBSCD_1 + "]";
        java.sql.Statement statement_tDBSCD_1 = conn_tDBSCD_1.createStatement();
        java.sql.ResultSet resultSet_tDBSCD_1 = statement_tDBSCD_1.executeQuery(search_tDBSCD_1);
        java.util.Map<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1> cache_tDBSCD_1 = new java.util.HashMap<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1>();
        while(resultSet_tDBSCD_1.next()) {
            SCDSK_tDBSCD_1 sk_tDBSCD_1 = new SCDSK_tDBSCD_1();
            SCDStruct_tDBSCD_1 row_tDBSCD_1 = new SCDStruct_tDBSCD_1();
                    if(resultSet_tDBSCD_1.getObject(1) != null) {
                        sk_tDBSCD_1.Categorie_FK = resultSet_tDBSCD_1.getInt(1);
                    }
                    if(resultSet_tDBSCD_1.getObject(2) != null) {
                        sk_tDBSCD_1.Client_FK = resultSet_tDBSCD_1.getInt(2);
                    }
                    if(resultSet_tDBSCD_1.getObject(3) != null) {
                        sk_tDBSCD_1.Date_FK = resultSet_tDBSCD_1.getInt(3);
                    }
                    if(resultSet_tDBSCD_1.getObject(4) != null) {
                        sk_tDBSCD_1.Geographie_FK = resultSet_tDBSCD_1.getInt(4);
                    }
                    if(resultSet_tDBSCD_1.getObject(5) != null) {
                        sk_tDBSCD_1.Mode_Paiement_FK = resultSet_tDBSCD_1.getInt(5);
                    }
                    if(resultSet_tDBSCD_1.getObject(6) != null) {
                        sk_tDBSCD_1.Produit_FK = resultSet_tDBSCD_1.getInt(6);
                    }
                    if(resultSet_tDBSCD_1.getObject(7) != null) {
                        sk_tDBSCD_1.Type_Vente_FK = resultSet_tDBSCD_1.getInt(7);
                    }
                    if(resultSet_tDBSCD_1.getObject(8) != null) {
                        row_tDBSCD_1.Code_Promo = resultSet_tDBSCD_1.getString(8);
                    }
                    if(resultSet_tDBSCD_1.getObject(9) != null) {
                        row_tDBSCD_1.Montant_de_la_commande = resultSet_tDBSCD_1.getDouble(9);
                    }
                    if(resultSet_tDBSCD_1.getObject(10) != null) {
                        row_tDBSCD_1.Montant_Livraison = resultSet_tDBSCD_1.getDouble(10);
                    }
                    if(resultSet_tDBSCD_1.getObject(11) != null) {
                        row_tDBSCD_1.Montant_Remboursement = resultSet_tDBSCD_1.getDouble(11);
                    }
                    if(resultSet_tDBSCD_1.getObject(12) != null) {
                        row_tDBSCD_1.Montant_Remise = resultSet_tDBSCD_1.getDouble(12);
                    }
                    if(resultSet_tDBSCD_1.getObject(13) != null) {
                        row_tDBSCD_1.Numero_Commande = resultSet_tDBSCD_1.getString(13);
                    }
                    if(resultSet_tDBSCD_1.getObject(14) != null) {
                        row_tDBSCD_1.Numero_Facture = resultSet_tDBSCD_1.getString(14);
                    }
                    if(resultSet_tDBSCD_1.getObject(15) != null) {
                        row_tDBSCD_1.Prix_Unité = resultSet_tDBSCD_1.getDouble(15);
                    }
                    if(resultSet_tDBSCD_1.getObject(16) != null) {
                        row_tDBSCD_1.Quantite = resultSet_tDBSCD_1.getInt(16);
                    }
            cache_tDBSCD_1.put(sk_tDBSCD_1, row_tDBSCD_1);
        }
        resultSet_tDBSCD_1.close();
        statement_tDBSCD_1.close();
    String insertionSQL_tDBSCD_1 = "INSERT INTO [" + tableName_tDBSCD_1 + "]([Categorie_FK], [Client_FK], [Date_FK], [Geographie_FK], [Mode_Paiement_FK], [Produit_FK], [Type_Vente_FK], [Code_Promo], [Montant_de_la_commande], [Montant_Livraison], [Montant_Remboursement], [Montant_Remise], [Numero_Commande], [Numero_Facture], [Prix_Unité], [Quantite]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    java.sql.PreparedStatement insertionStatement_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(insertionSQL_tDBSCD_1);
        String updateSQLForType1_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [Code_Promo] = ?, [Montant_de_la_commande] = ?, [Montant_Livraison] = ?, [Montant_Remboursement] = ?, [Montant_Remise] = ?, [Numero_Commande] = ?, [Numero_Facture] = ?, [Prix_Unité] = ?, [Quantite] = ? WHERE [Categorie_FK] = ? AND [Client_FK] = ? AND [Date_FK] = ? AND [Geographie_FK] = ? AND [Mode_Paiement_FK] = ? AND [Produit_FK] = ? AND [Type_Vente_FK] = ?";
        java.sql.PreparedStatement updateForType1_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType1_tDBSCD_1);        
    
        SCDSK_tDBSCD_1 lookUpKey_tDBSCD_1 = null;        
    SCDStruct_tDBSCD_1 lookUpValue_tDBSCD_1 = null;

 



/**
 * [tDBSCD_1 begin ] stop
 */




	
	/**
	 * [tLogRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_1", false);
		start_Hash.put("tLogRow_1", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"client_rejected");
					}
				
		int tos_count_tLogRow_1 = 0;
		

	///////////////////////
	
         class Util_tLogRow_1 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[1];

        public void addRow(String[] row) {

            for (int i = 0; i < 1; i++) {
                if (row[i]!=null) {
                  colLengths[i] = Math.max(colLengths[i], row[i].length());
                }
            }
            list.add(row);
        }

        public void setTableName(String name) {

            this.name = name;
        }

            public StringBuilder format() {
            
                StringBuilder sb = new StringBuilder();
  
            
                    sb.append(print(des_top));
    
                    int totals = 0;
                    for (int i = 0; i < colLengths.length; i++) {
                        totals = totals + colLengths[i];
                    }
    
                    // name
                    sb.append("|");
                    int k = 0;
                    for (k = 0; k < (totals + 0 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 0 - name.length() - k; i++) {
                        sb.append(' ');
                    }
                    sb.append("|\n");

                    // head and rows
                    sb.append(print(des_head));
                    for (int i = 0; i < list.size(); i++) {
    
                        String[] row = list.get(i);
    
                        java.util.Formatter formatter = new java.util.Formatter(new StringBuilder());
                        
                        StringBuilder sbformat = new StringBuilder();                                             
        			        sbformat.append("|%1$-");
        			        sbformat.append(colLengths[0]);
        			        sbformat.append("s");
        			                      
                        sbformat.append("|\n");                    
       
                        formatter.format(sbformat.toString(), (Object[])row);	
                                
                        sb.append(formatter.toString());
                        if (i == 0)
                            sb.append(print(des_head)); // print the head
                    }
    
                    // end
                    sb.append(print(des_bottom));
                    return sb;
                }
            

            private StringBuilder print(String[] fillChars) {
                StringBuilder sb = new StringBuilder();
                //first column
                sb.append(fillChars[0]);	                

                  
                    //last column
                    for (int i = 0; i < colLengths[0] - fillChars[0].length() - fillChars[1].length()+2; i++) {
                        sb.append(fillChars[2]);
                    }         
                sb.append(fillChars[1]);
                sb.append("\n");               
                return sb;
            }
            
            public boolean isTableEmpty(){
            	if (list.size() > 1)
            		return false;
            	return true;
            }
        }
        Util_tLogRow_1 util_tLogRow_1 = new Util_tLogRow_1();
        util_tLogRow_1.setTableName("tLogRow_1");
        util_tLogRow_1.addRow(new String[]{"Code_client",});        
 		StringBuilder strBuffer_tLogRow_1 = null;
		int nb_line_tLogRow_1 = 0;
///////////////////////    			



 



/**
 * [tLogRow_1 begin ] stop
 */



	
	/**
	 * [tMap_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_3", false);
		start_Hash.put("tMap_3", System.currentTimeMillis());
		
	
	currentComponent="tMap_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out_state");
					}
				
		int tos_count_tMap_3 = 0;
		




// ###############################
// # Lookup's keys initialization
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row4Struct> tHash_Lookup_row4 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row4Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row4Struct>) 
					globalMap.get( "tHash_Lookup_row4" ))
					;					
					
	

row4Struct row4HashKey = new row4Struct();
row4Struct row4Default = new row4Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row5Struct> tHash_Lookup_row5 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row5Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row5Struct>) 
					globalMap.get( "tHash_Lookup_row5" ))
					;					
					
	

row5Struct row5HashKey = new row5Struct();
row5Struct row5Default = new row5Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct> tHash_Lookup_row6 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct>) 
					globalMap.get( "tHash_Lookup_row6" ))
					;					
					
	

row6Struct row6HashKey = new row6Struct();
row6Struct row6Default = new row6Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct> tHash_Lookup_row7 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct>) 
					globalMap.get( "tHash_Lookup_row7" ))
					;					
					
	

row7Struct row7HashKey = new row7Struct();
row7Struct row7Default = new row7Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row11Struct> tHash_Lookup_row11 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row11Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row11Struct>) 
					globalMap.get( "tHash_Lookup_row11" ))
					;					
					
	

row11Struct row11HashKey = new row11Struct();
row11Struct row11Default = new row11Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row8Struct> tHash_Lookup_row8 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row8Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row8Struct>) 
					globalMap.get( "tHash_Lookup_row8" ))
					;					
					
	

row8Struct row8HashKey = new row8Struct();
row8Struct row8Default = new row8Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct> tHash_Lookup_row13 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct>) 
					globalMap.get( "tHash_Lookup_row13" ))
					;					
					
	

row13Struct row13HashKey = new row13Struct();
row13Struct row13Default = new row13Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_3__Struct  {
}
Var__tMap_3__Struct Var__tMap_3 = new Var__tMap_3__Struct();
// ###############################

// ###############################
// # Outputs initialization
outfinalStruct outfinal_tmp = new outfinalStruct();
client_rejectedStruct client_rejected_tmp = new client_rejectedStruct();
// ###############################

        
        



        









 



/**
 * [tMap_3 begin ] stop
 */



	
	/**
	 * [tMap_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_5", false);
		start_Hash.put("tMap_5", System.currentTimeMillis());
		
	
	currentComponent="tMap_5";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row3");
					}
				
		int tos_count_tMap_5 = 0;
		




// ###############################
// # Lookup's keys initialization
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct> tHash_Lookup_row9 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct>) 
					globalMap.get( "tHash_Lookup_row9" ))
					;					
					
	

row9Struct row9HashKey = new row9Struct();
row9Struct row9Default = new row9Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct> tHash_Lookup_row12 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct>) 
					globalMap.get( "tHash_Lookup_row12" ))
					;					
					
	

row12Struct row12HashKey = new row12Struct();
row12Struct row12Default = new row12Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_5__Struct  {
}
Var__tMap_5__Struct Var__tMap_5 = new Var__tMap_5__Struct();
// ###############################

// ###############################
// # Outputs initialization
out_stateStruct out_state_tmp = new out_stateStruct();
// ###############################

        
        



        









 



/**
 * [tMap_5 begin ] stop
 */



	
	/**
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out2","out1");
					}
				
		int tos_count_tUnite_1 = 0;
		

int nb_line_tUnite_1 = 0;

 



/**
 * [tUnite_1 begin ] stop
 */

	
	/**
	 * [tMap_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_2", false);
		start_Hash.put("tMap_2", System.currentTimeMillis());
		
	
	currentComponent="tMap_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row2");
					}
				
		int tos_count_tMap_2 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_2__Struct  {
}
Var__tMap_2__Struct Var__tMap_2 = new Var__tMap_2__Struct();
// ###############################

// ###############################
// # Outputs initialization
out2Struct out2_tmp = new out2Struct();
// ###############################

        
        



        









 



/**
 * [tMap_2 begin ] stop
 */



	
	/**
	 * [tDBInput_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_2", false);
		start_Hash.put("tDBInput_2", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_2";

	
		int tos_count_tDBInput_2 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_2 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_2 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_2  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_2, talendToDBArray_tDBInput_2); 
		    int nb_line_tDBInput_2 = 0;
		    java.sql.Connection conn_tDBInput_2 = null;
				String driverClass_tDBInput_2 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_2 = java.lang.Class.forName(driverClass_tDBInput_2);
				String dbUser_tDBInput_2 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:wsHLpFbYSSaiUUY608j8dBkowQHG2BGWGhUSSEhJpQOWK137hX8=");
				
				String dbPwd_tDBInput_2 = decryptedPassword_tDBInput_2;
				
		    String port_tDBInput_2 = "1433";
		    String dbname_tDBInput_2 = "PA_BI_SA" ;
			String url_tDBInput_2 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_2)) {
		    	url_tDBInput_2 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_2)) {
				url_tDBInput_2 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_2 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_2 = "";
				
				conn_tDBInput_2 = java.sql.DriverManager.getConnection(url_tDBInput_2,dbUser_tDBInput_2,dbPwd_tDBInput_2);
		        
		    
			java.sql.Statement stmt_tDBInput_2 = conn_tDBInput_2.createStatement();

		    String dbquery_tDBInput_2 = "SELECT Factures_Ventes.Reference,\n		Factures_Ventes.Description,\n		Factures_Ventes.PU_HT,\n		Factures_Ventes.Quantite,\n	"
+"	Factures_Ventes.Prix_Total_HT,\n		Factures_Ventes.Numero_Facture,\n		Factures_Ventes.id_Client,\n		Factures_Ventes.Date,\n	"
+"	Factures_Ventes.Client_Nom,\n		Factures_Ventes.Matricule_Fiscal,\n		Factures_Ventes.Adresse,\n		Factures_Ventes.TVA\nFROM	F"
+"actures_Ventes";
		    

            	globalMap.put("tDBInput_2_QUERY",dbquery_tDBInput_2);
		    java.sql.ResultSet rs_tDBInput_2 = null;

		    try {
		    	rs_tDBInput_2 = stmt_tDBInput_2.executeQuery(dbquery_tDBInput_2);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_2 = rs_tDBInput_2.getMetaData();
		    	int colQtyInRs_tDBInput_2 = rsmd_tDBInput_2.getColumnCount();

		    String tmpContent_tDBInput_2 = null;
		    
		    
		    while (rs_tDBInput_2.next()) {
		        nb_line_tDBInput_2++;
		        
							if(colQtyInRs_tDBInput_2 < 1) {
								row2.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Reference = tmpContent_tDBInput_2;
                }
            } else {
                row2.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row2.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Description = tmpContent_tDBInput_2;
                }
            } else {
                row2.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row2.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(3);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.PU_HT = tmpContent_tDBInput_2;
                }
            } else {
                row2.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row2.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(4);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Quantite = tmpContent_tDBInput_2;
                }
            } else {
                row2.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row2.Prix_Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Prix_Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Prix_Total_HT = tmpContent_tDBInput_2;
                }
            } else {
                row2.Prix_Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row2.Numero_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(6);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Numero_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Numero_Facture = tmpContent_tDBInput_2;
                }
            } else {
                row2.Numero_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row2.id_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.id_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.id_Client = tmpContent_tDBInput_2;
                }
            } else {
                row2.id_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row2.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Date = tmpContent_tDBInput_2;
                }
            } else {
                row2.Date = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row2.Client_Nom = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Client_Nom = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Client_Nom = tmpContent_tDBInput_2;
                }
            } else {
                row2.Client_Nom = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row2.Matricule_Fiscal = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(10);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Matricule_Fiscal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Matricule_Fiscal = tmpContent_tDBInput_2;
                }
            } else {
                row2.Matricule_Fiscal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 11) {
								row2.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(11);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Adresse = tmpContent_tDBInput_2;
                }
            } else {
                row2.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 12) {
								row2.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(12);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.TVA = tmpContent_tDBInput_2;
                }
            } else {
                row2.TVA = null;
            }
		                    }
					





 



/**
 * [tDBInput_2 begin ] stop
 */
	
	/**
	 * [tDBInput_2 main ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 


	tos_count_tDBInput_2++;

/**
 * [tDBInput_2 main ] stop
 */
	
	/**
	 * [tDBInput_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 



/**
 * [tDBInput_2 process_data_begin ] stop
 */

	
	/**
	 * [tMap_2 main ] start
	 */

	

	
	
	currentComponent="tMap_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row2"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_2 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_2 = false;
		  boolean mainRowRejected_tMap_2 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_2__Struct Var = Var__tMap_2;// ###############################
        // ###############################
        // # Output tables

out2 = null;


// # Output table : 'out2'
out2_tmp.Vente_PK = 0;
out2_tmp.Date_FK = null;
out2_tmp.Type_Vente_FK = null;
out2_tmp.Produit_FK = null;
out2_tmp.Categorie_FK = null;
out2_tmp.Client_FK = null;
out2_tmp.Geographie_FK = null;
out2_tmp.Mode_Paiement_FK = null;
out2_tmp.Quantite = row2.Quantite != null ? (int) Double.parseDouble(row2.Quantite.replace(",", ".")) : null ;
out2_tmp.Montant_Livraison = 0.0;
out2_tmp.Montant_Remboursement = 0.0;
out2_tmp.Montant_de_la_commande = Double.parseDouble(
    row2.Prix_Total_HT.trim().replaceAll("[^0-9,\\.]", "").replace(',', '.')
) ;
out2_tmp.Montant_Remise = 0.0;
out2_tmp.Code_Promo = "Aucun";
out2_tmp.Numero_Facture = row2.Numero_Facture ;
out2_tmp.Numero_Commande = "Aucun";
out2_tmp.Prix_Unite = row2.PU_HT != null ? Double.parseDouble(row2.PU_HT.replace(",", ".")) : null ;
out2_tmp.Code_Client = row2.Matricule_Fiscal ;
out2_tmp.Date_de_commande = row2.Date.contains("/") 
? TalendDate.parseDate("dd/MM/yyyy", row2.Date)
: TalendDate.parseDate("yyyy-MM-dd", row2.Date.substring(0,10)) ;
out2_tmp.Nom_de_l_element = row2.Description != null && row2.Description.toLowerCase().contains("poste")? "Poste":row2.Description ;
out2_tmp.UGS = row2.Description != null && row2.Description.toLowerCase().contains("poste")
? "PT-123"
: row2.Reference ;
out2_tmp.Ville = row2.Adresse ;
out2_tmp.TypeVente = "Terrain";
out2_tmp.Mode_Paiement = "VIREMENT";
out2 = out2_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_2 = false;










 


	tos_count_tMap_2++;

/**
 * [tMap_2 main ] stop
 */
	
	/**
	 * [tMap_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 process_data_begin ] stop
 */
// Start of branch "out2"
if(out2 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out2"
									
							);
						}
						
//////////
 

// for output
			row3 = new row3Struct();
								
			row3.Vente_PK = out2.Vente_PK;								
			row3.Date_FK = out2.Date_FK;								
			row3.Type_Vente_FK = out2.Type_Vente_FK;								
			row3.Produit_FK = out2.Produit_FK;								
			row3.Categorie_FK = out2.Categorie_FK;								
			row3.Client_FK = out2.Client_FK;								
			row3.Geographie_FK = out2.Geographie_FK;								
			row3.Mode_Paiement_FK = out2.Mode_Paiement_FK;								
			row3.Quantite = out2.Quantite;								
			row3.Montant_Livraison = out2.Montant_Livraison;								
			row3.Montant_Remboursement = out2.Montant_Remboursement;								
			row3.Montant_de_la_commande = out2.Montant_de_la_commande;								
			row3.Montant_Remise = out2.Montant_Remise;								
			row3.Code_Promo = out2.Code_Promo;								
			row3.Numero_Facture = out2.Numero_Facture;								
			row3.Numero_Commande = out2.Numero_Commande;								
			row3.Prix_Unite = out2.Prix_Unite;								
			row3.Code_Client = out2.Code_Client;								
			row3.Date_de_commande = out2.Date_de_commande;								
			row3.Nom_de_l_element = out2.Nom_de_l_element;								
			row3.UGS = out2.UGS;								
			row3.Ville = out2.Ville;								
			row3.TypeVente = out2.TypeVente;								
			row3.Mode_Paiement = out2.Mode_Paiement;			

			nb_line_tUnite_1++;

//////////
 


	tos_count_tUnite_1++;

/**
 * [tUnite_1 main ] stop
 */
	
	/**
	 * [tUnite_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_begin ] stop
 */

	
	/**
	 * [tMap_5 main ] start
	 */

	

	
	
	currentComponent="tMap_5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_5 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_5 = false;
		  boolean mainRowRejected_tMap_5 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row9" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow9 = false;
       		  	    	
       		  	    	
 							row9Struct row9ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_5) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_5 = false;
								
                        		    		    row9HashKey.Input_Geo = row3.Ville ;
                        		    		

								
		                        	row9HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row9.lookup( row9HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row9.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_5 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row9 != null && tHash_Lookup_row9.getCount(row9HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row9' and it contains more one result from keys :  row9.Input_Geo = '" + row9HashKey.Input_Geo + "'");
								} // G 071
							

							row9Struct row9 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row9Struct fromLookup_row9 = null;
							row9 = row9Default;
										 
							
								 
							
							
								if (tHash_Lookup_row9 !=null && tHash_Lookup_row9.hasNext()) { // G 099
								
							
								
								fromLookup_row9 = tHash_Lookup_row9.next();

							
							
								} // G 099
							
							

							if(fromLookup_row9 != null) {
								row9 = fromLookup_row9;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row12" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow12 = false;
       		  	    	
       		  	    	
 							row12Struct row12ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_5) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_5 = false;
								
                        		    		    row12HashKey.Produit = row3.Nom_de_l_element ;
                        		    		

								
		                        	row12HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row12.lookup( row12HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row12.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_5 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row12 != null && tHash_Lookup_row12.getCount(row12HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row12' and it contains more one result from keys :  row12.Produit = '" + row12HashKey.Produit + "'");
								} // G 071
							

							row12Struct row12 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row12Struct fromLookup_row12 = null;
							row12 = row12Default;
										 
							
								 
							
							
								if (tHash_Lookup_row12 !=null && tHash_Lookup_row12.hasNext()) { // G 099
								
							
								
								fromLookup_row12 = tHash_Lookup_row12.next();

							
							
								} // G 099
							
							

							if(fromLookup_row12 != null) {
								row12 = fromLookup_row12;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_5__Struct Var = Var__tMap_5;// ###############################
        // ###############################
        // # Output tables

out_state = null;

if(!rejectedInnerJoin_tMap_5 ) {

// # Output table : 'out_state'
out_state_tmp.Vente_PK = row3.Vente_PK ;
out_state_tmp.Date_FK = row3.Date_FK ;
out_state_tmp.Type_Vente_FK = row3.Type_Vente_FK ;
out_state_tmp.Produit_FK = row3.Produit_FK ;
out_state_tmp.Categorie_FK = row3.Categorie_FK ;
out_state_tmp.Client_FK = row3.Client_FK ;
out_state_tmp.Geographie_FK = row3.Geographie_FK ;
out_state_tmp.Mode_Paiement_FK = row3.Mode_Paiement_FK ;
out_state_tmp.Quantite = row3.Quantite ;
out_state_tmp.Montant_Livraison = row3.Montant_Livraison ;
out_state_tmp.Montant_Remboursement = row3.Montant_Remboursement ;
out_state_tmp.Montant_de_la_commande = row3.Montant_de_la_commande ;
out_state_tmp.Montant_Remise = row3.Montant_Remise ;
out_state_tmp.Code_Promo = row3.Code_Promo ;
out_state_tmp.Numero_Facture = row3.Numero_Facture ;
out_state_tmp.Numero_Commande = row3.Numero_Commande ;
out_state_tmp.Prix_Unite = row3.Prix_Unite ;
out_state_tmp.Code_Client = row3.Code_Client ;
out_state_tmp.Date_de_commande = row3.Date_de_commande ;
out_state_tmp.Nom_de_l_element = row3.Nom_de_l_element ;
out_state_tmp.UGS = row3.UGS ;
out_state_tmp.Ville = row9.Ville ;
out_state_tmp.TypeVente = row3.TypeVente ;
out_state_tmp.Mode_Paiement = row3.Mode_Paiement ;
out_state_tmp.Categorie = row12.Categorie ;
out_state = out_state_tmp;
}  // closing inner join bracket (2)
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_5 = false;










 


	tos_count_tMap_5++;

/**
 * [tMap_5 main ] stop
 */
	
	/**
	 * [tMap_5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 process_data_begin ] stop
 */
// Start of branch "out_state"
if(out_state != null) { 



	
	/**
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_state"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_3 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_3 = false;
		  boolean mainRowRejected_tMap_3 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row4" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow4 = false;
       		  	    	
       		  	    	
 							row4Struct row4ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row4HashKey.Code_client = out_state.Code_Client ;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row4 != null && tHash_Lookup_row4.getCount(row4HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row4' and it contains more one result from keys :  row4.Code_client = '" + row4HashKey.Code_client + "'");
								} // G 071
							

							row4Struct row4 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								 
							
							
								if (tHash_Lookup_row4 !=null && tHash_Lookup_row4.hasNext()) { // G 099
								
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							
							
								} // G 099
							
							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row5" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow5 = false;
       		  	    	
       		  	    	
 							row5Struct row5ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row5HashKey.Type_Vente = out_state.TypeVente;
                        		    		

								
		                        	row5HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row5.lookup( row5HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row5 != null && tHash_Lookup_row5.getCount(row5HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row5' and it contains more one result from keys :  row5.Type_Vente = '" + row5HashKey.Type_Vente + "'");
								} // G 071
							

							row5Struct row5 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row5Struct fromLookup_row5 = null;
							row5 = row5Default;
										 
							
								 
							
							
								if (tHash_Lookup_row5 !=null && tHash_Lookup_row5.hasNext()) { // G 099
								
							
								
								fromLookup_row5 = tHash_Lookup_row5.next();

							
							
								} // G 099
							
							

							if(fromLookup_row5 != null) {
								row5 = fromLookup_row5;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row6HashKey.Type_paiement = out_state.Mode_Paiement;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Type_paiement = '" + row6HashKey.Type_paiement + "'");
								} // G 071
							

							row6Struct row6 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row6Struct fromLookup_row6 = null;
							row6 = row6Default;
										 
							
								 
							
							
								if (tHash_Lookup_row6 !=null && tHash_Lookup_row6.hasNext()) { // G 099
								
							
								
								fromLookup_row6 = tHash_Lookup_row6.next();

							
							
								} // G 099
							
							

							if(fromLookup_row6 != null) {
								row6 = fromLookup_row6;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row7" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow7 = false;
       		  	    	
       		  	    	
 							row7Struct row7ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row7HashKey.Date = out_state.Date_de_commande == null ? null : new java.util.Date(out_state.Date_de_commande.getTime());
                        		    		

								
		                        	row7HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row7.lookup( row7HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row7 != null && tHash_Lookup_row7.getCount(row7HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row7' and it contains more one result from keys :  row7.Date = '" + row7HashKey.Date + "'");
								} // G 071
							

							row7Struct row7 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row7Struct fromLookup_row7 = null;
							row7 = row7Default;
										 
							
								 
							
							
								if (tHash_Lookup_row7 !=null && tHash_Lookup_row7.hasNext()) { // G 099
								
							
								
								fromLookup_row7 = tHash_Lookup_row7.next();

							
							
								} // G 099
							
							

							if(fromLookup_row7 != null) {
								row7 = fromLookup_row7;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row11" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow11 = false;
       		  	    	
       		  	    	
 							row11Struct row11ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row11HashKey.Ville = out_state.Ville;
                        		    		

								
		                        	row11HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row11.lookup( row11HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row11 != null && tHash_Lookup_row11.getCount(row11HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row11' and it contains more one result from keys :  row11.Ville = '" + row11HashKey.Ville + "'");
								} // G 071
							

							row11Struct row11 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row11Struct fromLookup_row11 = null;
							row11 = row11Default;
										 
							
								 
							
							
								if (tHash_Lookup_row11 !=null && tHash_Lookup_row11.hasNext()) { // G 099
								
							
								
								fromLookup_row11 = tHash_Lookup_row11.next();

							
							
								} // G 099
							
							

							if(fromLookup_row11 != null) {
								row11 = fromLookup_row11;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row8" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow8 = false;
       		  	    	
       		  	    	
 							row8Struct row8ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row8HashKey.Nom_produit = out_state.Nom_de_l_element;
                        		    		

								
		                        	row8HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row8.lookup( row8HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row8 != null && tHash_Lookup_row8.getCount(row8HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row8' and it contains more one result from keys :  row8.Nom_produit = '" + row8HashKey.Nom_produit + "'");
								} // G 071
							

							row8Struct row8 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row8Struct fromLookup_row8 = null;
							row8 = row8Default;
										 
							
								 
							
							
								if (tHash_Lookup_row8 !=null && tHash_Lookup_row8.hasNext()) { // G 099
								
							
								
								fromLookup_row8 = tHash_Lookup_row8.next();

							
							
								} // G 099
							
							

							if(fromLookup_row8 != null) {
								row8 = fromLookup_row8;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row13" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow13 = false;
       		  	    	
       		  	    	
 							row13Struct row13ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row13HashKey.Categorie = out_state.Categorie;
                        		    		

								
		                        	row13HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row13.lookup( row13HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row13 != null && tHash_Lookup_row13.getCount(row13HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row13' and it contains more one result from keys :  row13.Categorie = '" + row13HashKey.Categorie + "'");
								} // G 071
							

							row13Struct row13 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row13Struct fromLookup_row13 = null;
							row13 = row13Default;
										 
							
								 
							
							
								if (tHash_Lookup_row13 !=null && tHash_Lookup_row13.hasNext()) { // G 099
								
							
								
								fromLookup_row13 = tHash_Lookup_row13.next();

							
							
								} // G 099
							
							

							if(fromLookup_row13 != null) {
								row13 = fromLookup_row13;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

outfinal = null;
client_rejected = null;


// # Output table : 'outfinal'
outfinal_tmp.Vente_PK = 0;
outfinal_tmp.Date_FK = row7.Date_PK ;
outfinal_tmp.Type_Vente_FK = row5.Type_Vente_PK ;
outfinal_tmp.Produit_FK = row8.Produit_PK ;
outfinal_tmp.Categorie_FK = row13.Categorie_PK ;
outfinal_tmp.Client_FK = row4.Client_PK ;
outfinal_tmp.Geographie_FK = row11.Geographie_PK ;
outfinal_tmp.Mode_Paiement_FK = row6.Mode_Paiement_PK ;
outfinal_tmp.Quantite = out_state.Quantite ;
outfinal_tmp.Montant_Livraison = out_state.Montant_Livraison ;
outfinal_tmp.Montant_Remboursement = out_state.Montant_Remboursement ;
outfinal_tmp.Montant_de_la_commande = out_state.Montant_de_la_commande ;
outfinal_tmp.Montant_Remise = out_state.Montant_Remise ;
outfinal_tmp.Code_Promo = out_state.Code_Promo ;
outfinal_tmp.Numero_Facture = out_state.Numero_Facture ;
outfinal_tmp.Numero_Commande = out_state.Numero_Commande ;
outfinal_tmp.Prix_Unite = out_state.Prix_Unite ;
outfinal = outfinal_tmp;

// # Output table : 'client_rejected'
// # Filter conditions 
if( 

row4.Code_client==null

 ) {
client_rejected_tmp.Code_client = out_state.Code_Client ;
client_rejected = client_rejected_tmp;
} // closing filter/reject
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_3 = false;










 


	tos_count_tMap_3++;

/**
 * [tMap_3 main ] stop
 */
	
	/**
	 * [tMap_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_begin ] stop
 */
// Start of branch "outfinal"
if(outfinal != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"outfinal"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Categorie_FK = outfinal.Categorie_FK;
            lookUpKey_tDBSCD_1.Client_FK = outfinal.Client_FK;
            lookUpKey_tDBSCD_1.Date_FK = outfinal.Date_FK;
            lookUpKey_tDBSCD_1.Geographie_FK = outfinal.Geographie_FK;
            lookUpKey_tDBSCD_1.Mode_Paiement_FK = outfinal.Mode_Paiement_FK;
            lookUpKey_tDBSCD_1.Produit_FK = outfinal.Produit_FK;
            lookUpKey_tDBSCD_1.Type_Vente_FK = outfinal.Type_Vente_FK;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(outfinal.Categorie_FK == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(1, outfinal.Categorie_FK);
}

                    if(outfinal.Client_FK == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(2, outfinal.Client_FK);
}

                    if(outfinal.Date_FK == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(3, outfinal.Date_FK);
}

                    if(outfinal.Geographie_FK == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(4, outfinal.Geographie_FK);
}

                    if(outfinal.Mode_Paiement_FK == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(5, outfinal.Mode_Paiement_FK);
}

                    if(outfinal.Produit_FK == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(6, outfinal.Produit_FK);
}

                    if(outfinal.Type_Vente_FK == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(7, outfinal.Type_Vente_FK);
}

                    if(outfinal.Code_Promo == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(8, outfinal.Code_Promo);
}

                    if(outfinal.Montant_de_la_commande == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, outfinal.Montant_de_la_commande);
}

                    if(outfinal.Montant_Livraison == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(10, outfinal.Montant_Livraison);
}

                    if(outfinal.Montant_Remboursement == null) {
insertionStatement_tDBSCD_1.setNull(11, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(11, outfinal.Montant_Remboursement);
}

                    if(outfinal.Montant_Remise == null) {
insertionStatement_tDBSCD_1.setNull(12, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(12, outfinal.Montant_Remise);
}

                    if(outfinal.Numero_Commande == null) {
insertionStatement_tDBSCD_1.setNull(13, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(13, outfinal.Numero_Commande);
}

                    if(outfinal.Numero_Facture == null) {
insertionStatement_tDBSCD_1.setNull(14, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(14, outfinal.Numero_Facture);
}

                    if(outfinal.Prix_Unite == null) {
insertionStatement_tDBSCD_1.setNull(15, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(15, outfinal.Prix_Unite);
}

                    if(outfinal.Quantite == null) {
insertionStatement_tDBSCD_1.setNull(16, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(16, outfinal.Quantite);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_Promo == null && outfinal.Code_Promo!= null) || (lookUpValue_tDBSCD_1.Code_Promo != null && !lookUpValue_tDBSCD_1.Code_Promo.equals(outfinal.Code_Promo)) || (lookUpValue_tDBSCD_1.Montant_de_la_commande == null && outfinal.Montant_de_la_commande!= null) || (lookUpValue_tDBSCD_1.Montant_de_la_commande != null && !lookUpValue_tDBSCD_1.Montant_de_la_commande.equals(outfinal.Montant_de_la_commande)) || (lookUpValue_tDBSCD_1.Montant_Livraison == null && outfinal.Montant_Livraison!= null) || (lookUpValue_tDBSCD_1.Montant_Livraison != null && !lookUpValue_tDBSCD_1.Montant_Livraison.equals(outfinal.Montant_Livraison)) || (lookUpValue_tDBSCD_1.Montant_Remboursement == null && outfinal.Montant_Remboursement!= null) || (lookUpValue_tDBSCD_1.Montant_Remboursement != null && !lookUpValue_tDBSCD_1.Montant_Remboursement.equals(outfinal.Montant_Remboursement)) || (lookUpValue_tDBSCD_1.Montant_Remise == null && outfinal.Montant_Remise!= null) || (lookUpValue_tDBSCD_1.Montant_Remise != null && !lookUpValue_tDBSCD_1.Montant_Remise.equals(outfinal.Montant_Remise)) || (lookUpValue_tDBSCD_1.Numero_Commande == null && outfinal.Numero_Commande!= null) || (lookUpValue_tDBSCD_1.Numero_Commande != null && !lookUpValue_tDBSCD_1.Numero_Commande.equals(outfinal.Numero_Commande)) || (lookUpValue_tDBSCD_1.Numero_Facture == null && outfinal.Numero_Facture!= null) || (lookUpValue_tDBSCD_1.Numero_Facture != null && !lookUpValue_tDBSCD_1.Numero_Facture.equals(outfinal.Numero_Facture)) || (lookUpValue_tDBSCD_1.Prix_Unité == null && outfinal.Prix_Unite!= null) || (lookUpValue_tDBSCD_1.Prix_Unité != null && !lookUpValue_tDBSCD_1.Prix_Unité.equals(outfinal.Prix_Unite)) || (lookUpValue_tDBSCD_1.Quantite == null && outfinal.Quantite!= null) || (lookUpValue_tDBSCD_1.Quantite != null && !lookUpValue_tDBSCD_1.Quantite.equals(outfinal.Quantite))) {
                    if(outfinal.Code_Promo == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, outfinal.Code_Promo);
}

                    if(outfinal.Montant_de_la_commande == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(2, outfinal.Montant_de_la_commande);
}

                    if(outfinal.Montant_Livraison == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(3, outfinal.Montant_Livraison);
}

                    if(outfinal.Montant_Remboursement == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(4, outfinal.Montant_Remboursement);
}

                    if(outfinal.Montant_Remise == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(5, outfinal.Montant_Remise);
}

                    if(outfinal.Numero_Commande == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, outfinal.Numero_Commande);
}

                    if(outfinal.Numero_Facture == null) {
updateForType1_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(7, outfinal.Numero_Facture);
}

                    if(outfinal.Prix_Unite == null) {
updateForType1_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(8, outfinal.Prix_Unite);
}

                    if(outfinal.Quantite == null) {
updateForType1_tDBSCD_1.setNull(9, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(9, outfinal.Quantite);
}

                    if(outfinal.Categorie_FK == null) {
updateForType1_tDBSCD_1.setNull(10, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(10, outfinal.Categorie_FK);
}

                    if(outfinal.Client_FK == null) {
updateForType1_tDBSCD_1.setNull(11, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(11, outfinal.Client_FK);
}

                    if(outfinal.Date_FK == null) {
updateForType1_tDBSCD_1.setNull(12, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(12, outfinal.Date_FK);
}

                    if(outfinal.Geographie_FK == null) {
updateForType1_tDBSCD_1.setNull(13, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(13, outfinal.Geographie_FK);
}

                    if(outfinal.Mode_Paiement_FK == null) {
updateForType1_tDBSCD_1.setNull(14, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(14, outfinal.Mode_Paiement_FK);
}

                    if(outfinal.Produit_FK == null) {
updateForType1_tDBSCD_1.setNull(15, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(15, outfinal.Produit_FK);
}

                    if(outfinal.Type_Vente_FK == null) {
updateForType1_tDBSCD_1.setNull(16, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(16, outfinal.Type_Vente_FK);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_Promo = outfinal.Code_Promo;
                lookUpValue_tDBSCD_1.Montant_de_la_commande = outfinal.Montant_de_la_commande;
                lookUpValue_tDBSCD_1.Montant_Livraison = outfinal.Montant_Livraison;
                lookUpValue_tDBSCD_1.Montant_Remboursement = outfinal.Montant_Remboursement;
                lookUpValue_tDBSCD_1.Montant_Remise = outfinal.Montant_Remise;
                lookUpValue_tDBSCD_1.Numero_Commande = outfinal.Numero_Commande;
                lookUpValue_tDBSCD_1.Numero_Facture = outfinal.Numero_Facture;
                lookUpValue_tDBSCD_1.Prix_Unité = outfinal.Prix_Unite;
                lookUpValue_tDBSCD_1.Quantite = outfinal.Quantite;
        cache_tDBSCD_1.put(lookUpKey_tDBSCD_1, lookUpValue_tDBSCD_1);


 


	tos_count_tDBSCD_1++;

/**
 * [tDBSCD_1 main ] stop
 */
	
	/**
	 * [tDBSCD_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBSCD_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 process_data_end ] stop
 */

} // End of branch "outfinal"




// Start of branch "client_rejected"
if(client_rejected != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"client_rejected"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[1];
   				
	    		if(client_rejected.Code_client != null) { //              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(client_rejected.Code_client)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 


	tos_count_tLogRow_1++;

/**
 * [tLogRow_1 main ] stop
 */
	
	/**
	 * [tLogRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_begin ] stop
 */
	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "client_rejected"




	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */

} // End of branch "out_state"




	
	/**
	 * [tMap_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out2"




	
	/**
	 * [tMap_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 



/**
 * [tDBInput_2 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_2 end ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

	}
}finally{
	if (rs_tDBInput_2 != null) {
		rs_tDBInput_2.close();
	}
	if (stmt_tDBInput_2 != null) {
		stmt_tDBInput_2.close();
	}
		if(conn_tDBInput_2 != null && !conn_tDBInput_2.isClosed()) {
			
			conn_tDBInput_2.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_2_NB_LINE",nb_line_tDBInput_2);

 

ok_Hash.put("tDBInput_2", true);
end_Hash.put("tDBInput_2", System.currentTimeMillis());




/**
 * [tDBInput_2 end ] stop
 */

	
	/**
	 * [tMap_2 end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row2");
			  	}
			  	
 

ok_Hash.put("tMap_2", true);
end_Hash.put("tMap_2", System.currentTimeMillis());




/**
 * [tMap_2 end ] stop
 */




	
	/**
	 * [tMap_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_1", false);
		start_Hash.put("tMap_1", System.currentTimeMillis());
		
	
	currentComponent="tMap_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row1");
					}
				
		int tos_count_tMap_1 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_1__Struct  {
}
Var__tMap_1__Struct Var__tMap_1 = new Var__tMap_1__Struct();
// ###############################

// ###############################
// # Outputs initialization
out1Struct out1_tmp = new out1Struct();
// ###############################

        
        



        









 



/**
 * [tMap_1 begin ] stop
 */



	
	/**
	 * [tDBInput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_1", false);
		start_Hash.put("tDBInput_1", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_1";

	
		int tos_count_tDBInput_1 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_1 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_1  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_1, talendToDBArray_tDBInput_1); 
		    int nb_line_tDBInput_1 = 0;
		    java.sql.Connection conn_tDBInput_1 = null;
				String driverClass_tDBInput_1 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_1 = java.lang.Class.forName(driverClass_tDBInput_1);
				String dbUser_tDBInput_1 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:2kW4XpgdepdsxRdn4f5FrqPOp1Yb4ETkYKXxgbu2tSmq0sckpTg=");
				
				String dbPwd_tDBInput_1 = decryptedPassword_tDBInput_1;
				
		    String port_tDBInput_1 = "1433";
		    String dbname_tDBInput_1 = "PA_BI_SA" ;
			String url_tDBInput_1 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_1)) {
		    	url_tDBInput_1 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_1)) {
				url_tDBInput_1 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_1 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_1 = "";
				
				conn_tDBInput_1 = java.sql.DriverManager.getConnection(url_tDBInput_1,dbUser_tDBInput_1,dbPwd_tDBInput_1);
		        
		    
			java.sql.Statement stmt_tDBInput_1 = conn_tDBInput_1.createStatement();

		    String dbquery_tDBInput_1 = "SELECT \"Commandes(produit_par_ligne)\".\"Numéro de commande\",\n		\"Commandes(produit_par_ligne)\".\"État de la command"
+"e\",\n		\"Commandes(produit_par_ligne)\".\"Date de commande\",\n		\"Commandes(produit_par_ligne)\".\"Note du client\",\n		"
+"\"Commandes(produit_par_ligne)\".Prenom__Facturation,\n		\"Commandes(produit_par_ligne)\".NOM_DE_FAMILLE__FACTURATION,\n		"
+"\"Commandes(produit_par_ligne)\".Societe__Facturation,\n		\"Commandes(produit_par_ligne)\".Adresse_1___2__Facturation,\n		"
+"\"Commandes(produit_par_ligne)\".Ville__Facturation,\n		\"Commandes(produit_par_ligne)\".\"Code de l’état (Facturation)\""
+",\n		\"Commandes(produit_par_ligne)\".Code_postal__Facturation,\n		\"Commandes(produit_par_ligne)\".\"Code du pays (Factur"
+"ation)\",\n		\"Commandes(produit_par_ligne)\".\"Prénom (Livraison)\",\n		\"Commandes(produit_par_ligne)\".\"NOM DE FAMILLE"
+" (LIVRAISON)\",\n		\"Commandes(produit_par_ligne)\".\"Adresse 1 & 2 (Livraison)\",\n		\"Commandes(produit_par_ligne)\".\"V"
+"ille (Livraison)\",\n		\"Commandes(produit_par_ligne)\".\"Code de l’état (Livraison)\",\n		\"Commandes(produit_par_ligne)"
+"\".\"Code postal (Livraison)\",\n		\"Commandes(produit_par_ligne)\".\"Code du pays (Livraison)\",\n		\"Commandes(produit_p"
+"ar_ligne)\".\"Titre de la méthode de paiement\",\n		\"Commandes(produit_par_ligne)\".\"Montant de la remise panier\",\n		"
+"\"Commandes(produit_par_ligne)\".\"Cart Discount Amount(inc. tax)\",\n		\"Commandes(produit_par_ligne)\".\"Sous-total de "
+"la commande\",\n		\"Commandes(produit_par_ligne)\".\"Titre de la méthode de livraison\",\n		\"Commandes(produit_par_ligne)"
+"\".\"Montant de la livraison\",\n		\"Commandes(produit_par_ligne)\".\"Montant du remboursement de la commande\",\n		\"Comm"
+"andes(produit_par_ligne)\".\"Montant total de la commande\",\n		\"Commandes(produit_par_ligne)\".\"Montant total de la ta"
+"xe\",\n		\"Commandes(produit_par_ligne)\".UGS,\n		\"Commandes(produit_par_ligne)\".\"Article #\",\n		\"Commandes(produit_pa"
+"r_ligne)\".\"Nom de l’élément\",\n		\"Commandes(produit_par_ligne)\".\"Quantité (- Remboursement)\",\n		\"Commandes(produi"
+"t_par_ligne)\".\"Prix du produit\",\n		\"Commandes(produit_par_ligne)\".\"Code promo\",\n		\"Commandes(produit_par_ligne)"
+"\".\"Réduction\",\n		\"Commandes(produit_par_ligne)\".\"Taxe de la réduction\"\nFROM	\"Commandes(produit_par_ligne)\"";
		    

            	globalMap.put("tDBInput_1_QUERY",dbquery_tDBInput_1);
		    java.sql.ResultSet rs_tDBInput_1 = null;

		    try {
		    	rs_tDBInput_1 = stmt_tDBInput_1.executeQuery(dbquery_tDBInput_1);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_1 = rs_tDBInput_1.getMetaData();
		    	int colQtyInRs_tDBInput_1 = rsmd_tDBInput_1.getColumnCount();

		    String tmpContent_tDBInput_1 = null;
		    
		    
		    while (rs_tDBInput_1.next()) {
		        nb_line_tDBInput_1++;
		        
							if(colQtyInRs_tDBInput_1 < 1) {
								row1.Numero_de_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(1);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Numero_de_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Numero_de_commande = tmpContent_tDBInput_1;
                }
            } else {
                row1.Numero_de_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 2) {
								row1.Etat_de_la_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(2);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Etat_de_la_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Etat_de_la_commande = tmpContent_tDBInput_1;
                }
            } else {
                row1.Etat_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 3) {
								row1.Date_de_commande = null;
							} else {
										
			row1.Date_de_commande = mssqlGTU_tDBInput_1.getDate(rsmd_tDBInput_1, rs_tDBInput_1, 3);
			
		                    }
							if(colQtyInRs_tDBInput_1 < 4) {
								row1.Note_du_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(4);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Note_du_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Note_du_client = tmpContent_tDBInput_1;
                }
            } else {
                row1.Note_du_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 5) {
								row1.Prenom__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(5);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Prenom__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Prenom__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Prenom__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 6) {
								row1.NOM_DE_FAMILLE__FACTURATION = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(6);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.NOM_DE_FAMILLE__FACTURATION = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.NOM_DE_FAMILLE__FACTURATION = tmpContent_tDBInput_1;
                }
            } else {
                row1.NOM_DE_FAMILLE__FACTURATION = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 7) {
								row1.Societe__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(7);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Societe__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Societe__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Societe__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 8) {
								row1.Adresse_1___2__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(8);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse_1___2__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Adresse_1___2__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Adresse_1___2__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 9) {
								row1.Ville__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(9);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Ville__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Ville__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Ville__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 10) {
								row1.Code_de_l_etat__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(10);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_de_l_etat__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_de_l_etat__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_de_l_etat__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 11) {
								row1.Code_postal__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(11);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_postal__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_postal__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_postal__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 12) {
								row1.Code_du_pays__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(12);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_du_pays__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_du_pays__Facturation = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_du_pays__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 13) {
								row1.Prenom__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(13);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Prenom__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Prenom__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Prenom__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 14) {
								row1.NOM_DE_FAMILLE__LIVRAISON = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(14);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.NOM_DE_FAMILLE__LIVRAISON = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.NOM_DE_FAMILLE__LIVRAISON = tmpContent_tDBInput_1;
                }
            } else {
                row1.NOM_DE_FAMILLE__LIVRAISON = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 15) {
								row1.Adresse_1___2__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(15);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse_1___2__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Adresse_1___2__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Adresse_1___2__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 16) {
								row1.Ville__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(16);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Ville__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Ville__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Ville__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 17) {
								row1.Code_de_l_etat__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(17);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_de_l_etat__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_de_l_etat__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_de_l_etat__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 18) {
								row1.Code_postal__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(18);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_postal__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_postal__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_postal__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 19) {
								row1.Code_du_pays__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(19);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_du_pays__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_du_pays__Livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_du_pays__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 20) {
								row1.Titre_de_la_methode_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(20);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Titre_de_la_methode_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Titre_de_la_methode_de_paiement = tmpContent_tDBInput_1;
                }
            } else {
                row1.Titre_de_la_methode_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 21) {
								row1.Montant_de_la_remise_panier = null;
							} else {
		                          
            row1.Montant_de_la_remise_panier = rs_tDBInput_1.getBigDecimal(21);
            if(rs_tDBInput_1.wasNull()){
                    row1.Montant_de_la_remise_panier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 22) {
								row1.Cart_Discount_Amount_inc__tax = null;
							} else {
		                          
            row1.Cart_Discount_Amount_inc__tax = rs_tDBInput_1.getBigDecimal(22);
            if(rs_tDBInput_1.wasNull()){
                    row1.Cart_Discount_Amount_inc__tax = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 23) {
								row1.Sous_total_de_la_commande = null;
							} else {
		                          
            row1.Sous_total_de_la_commande = rs_tDBInput_1.getBigDecimal(23);
            if(rs_tDBInput_1.wasNull()){
                    row1.Sous_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 24) {
								row1.Titre_de_la_methode_de_livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(24);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(24).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Titre_de_la_methode_de_livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Titre_de_la_methode_de_livraison = tmpContent_tDBInput_1;
                }
            } else {
                row1.Titre_de_la_methode_de_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 25) {
								row1.Montant_de_la_livraison = null;
							} else {
		                          
            row1.Montant_de_la_livraison = rs_tDBInput_1.getBigDecimal(25);
            if(rs_tDBInput_1.wasNull()){
                    row1.Montant_de_la_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 26) {
								row1.Montant_du_remboursement_de_la_commande = null;
							} else {
		                          
            row1.Montant_du_remboursement_de_la_commande = rs_tDBInput_1.getBigDecimal(26);
            if(rs_tDBInput_1.wasNull()){
                    row1.Montant_du_remboursement_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 27) {
								row1.Montant_total_de_la_commande = null;
							} else {
		                          
            row1.Montant_total_de_la_commande = rs_tDBInput_1.getBigDecimal(27);
            if(rs_tDBInput_1.wasNull()){
                    row1.Montant_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 28) {
								row1.Montant_total_de_la_taxe = null;
							} else {
		                          
            row1.Montant_total_de_la_taxe = rs_tDBInput_1.getBigDecimal(28);
            if(rs_tDBInput_1.wasNull()){
                    row1.Montant_total_de_la_taxe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 29) {
								row1.UGS = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(29);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(29).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.UGS = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.UGS = tmpContent_tDBInput_1;
                }
            } else {
                row1.UGS = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 30) {
								row1.Article = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(30);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(30).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Article = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Article = tmpContent_tDBInput_1;
                }
            } else {
                row1.Article = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 31) {
								row1.Nom_de_l_element = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(31);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(31).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Nom_de_l_element = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Nom_de_l_element = tmpContent_tDBInput_1;
                }
            } else {
                row1.Nom_de_l_element = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 32) {
								row1.Quantite____Remboursement = null;
							} else {
		                          
            row1.Quantite____Remboursement = rs_tDBInput_1.getInt(32);
            if(rs_tDBInput_1.wasNull()){
                    row1.Quantite____Remboursement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 33) {
								row1.Prix_du_produit = null;
							} else {
		                          
            row1.Prix_du_produit = rs_tDBInput_1.getBigDecimal(33);
            if(rs_tDBInput_1.wasNull()){
                    row1.Prix_du_produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 34) {
								row1.Code_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(34);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(34).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Code_promo = tmpContent_tDBInput_1;
                }
            } else {
                row1.Code_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 35) {
								row1.Reduction = null;
							} else {
		                          
            row1.Reduction = rs_tDBInput_1.getBigDecimal(35);
            if(rs_tDBInput_1.wasNull()){
                    row1.Reduction = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 36) {
								row1.Taxe_de_la_reduction = null;
							} else {
		                          
            row1.Taxe_de_la_reduction = rs_tDBInput_1.getBigDecimal(36);
            if(rs_tDBInput_1.wasNull()){
                    row1.Taxe_de_la_reduction = null;
            }
		                    }
					





 



/**
 * [tDBInput_1 begin ] stop
 */
	
	/**
	 * [tDBInput_1 main ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

 


	tos_count_tDBInput_1++;

/**
 * [tDBInput_1 main ] stop
 */
	
	/**
	 * [tDBInput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

 



/**
 * [tDBInput_1 process_data_begin ] stop
 */

	
	/**
	 * [tMap_1 main ] start
	 */

	

	
	
	currentComponent="tMap_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row1"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_1 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_1 = false;
		  boolean mainRowRejected_tMap_1 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
        // ###############################
        // # Output tables

out1 = null;


// # Output table : 'out1'
out1_tmp.Vente_PK = 0;
out1_tmp.Date_FK = null;
out1_tmp.Type_Vente_FK = null;
out1_tmp.Produit_FK = null;
out1_tmp.Categorie_FK = null;
out1_tmp.Client_FK = null;
out1_tmp.Geographie_FK = null;
out1_tmp.Mode_Paiement_FK = null;
out1_tmp.Quantite = row1.Quantite____Remboursement ;
out1_tmp.Montant_Livraison = row1.Montant_de_la_livraison
!= null ? row1.Montant_de_la_livraison
.doubleValue() : null ;
out1_tmp.Montant_Remboursement = 0.0;
out1_tmp.Montant_de_la_commande = row1.Sous_total_de_la_commande != null ? row1.Sous_total_de_la_commande .doubleValue() : null ;
out1_tmp.Montant_Remise = row1.Reduction != null ? row1.Reduction .doubleValue() : 0.0 ;
out1_tmp.Code_Promo = row1.Code_promo ==null || row1.Code_promo.equals("")? "Aucun":row1.Code_promo ;
out1_tmp.Numero_Facture = "Aucun";
out1_tmp.Numero_Commande = row1.Numero_de_commande ;
out1_tmp.Prix_Unite = row1.Prix_du_produit!= null ? row1.Prix_du_produit.doubleValue() : null ;
out1_tmp.Code_Client = ClientKeyGenerator.generateKey(
    row1.Prenom__Facturation,
    row1.Prenom__Livraison,
    row1.NOM_DE_FAMILLE__FACTURATION,
    row1.NOM_DE_FAMILLE__LIVRAISON,
    row1.Adresse_1___2__Facturation,
    row1.Adresse_1___2__Livraison,
    row1.Ville__Facturation,
    row1.Ville__Livraison,
    row1.Code_postal__Facturation,
    row1.Code_postal__Livraison,
    row1.Code_du_pays__Facturation,
    row1.Code_du_pays__Livraison
) ;
out1_tmp.Date_de_commande = row1.Date_de_commande== null ? null : TalendDate.parseDate("yyyy-MM-dd", TalendDate.formatDate("yyyy-MM-dd",row1.Date_de_commande )) ;
out1_tmp.Nom_de_l_element = row1.Nom_de_l_element;
out1_tmp.UGS = row1.UGS==null? SKUGenerator.generateSKU(row1.Nom_de_l_element):row1.UGS ;
out1_tmp.Ville = row1.Adresse_1___2__Livraison==null?row1.Adresse_1___2__Facturation==null?"Tunis":row1.Adresse_1___2__Facturation:row1.Adresse_1___2__Livraison ;
out1_tmp.TypeVente = "Site_Web";
out1_tmp.Mode_Paiement = row1.Titre_de_la_methode_de_paiement == null 
? "AUTRE" 
: StringHandling.TRIM(row1.Titre_de_la_methode_de_paiement).toUpperCase() ;
out1 = out1_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_1 = false;










 


	tos_count_tMap_1++;

/**
 * [tMap_1 main ] stop
 */
	
	/**
	 * [tMap_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 process_data_begin ] stop
 */
// Start of branch "out1"
if(out1 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out1"
									
							);
						}
						
//////////
 

// for output
			row3 = new row3Struct();
								
			row3.Vente_PK = out1.Vente_PK;								
			row3.Date_FK = out1.Date_FK;								
			row3.Type_Vente_FK = out1.Type_Vente_FK;								
			row3.Produit_FK = out1.Produit_FK;								
			row3.Categorie_FK = out1.Categorie_FK;								
			row3.Client_FK = out1.Client_FK;								
			row3.Geographie_FK = out1.Geographie_FK;								
			row3.Mode_Paiement_FK = out1.Mode_Paiement_FK;								
			row3.Quantite = out1.Quantite;								
			row3.Montant_Livraison = out1.Montant_Livraison;								
			row3.Montant_Remboursement = out1.Montant_Remboursement;								
			row3.Montant_de_la_commande = out1.Montant_de_la_commande;								
			row3.Montant_Remise = out1.Montant_Remise;								
			row3.Code_Promo = out1.Code_Promo;								
			row3.Numero_Facture = out1.Numero_Facture;								
			row3.Numero_Commande = out1.Numero_Commande;								
			row3.Prix_Unite = out1.Prix_Unite;								
			row3.Code_Client = out1.Code_Client;								
			row3.Date_de_commande = out1.Date_de_commande;								
			row3.Nom_de_l_element = out1.Nom_de_l_element;								
			row3.UGS = out1.UGS;								
			row3.Ville = out1.Ville;								
			row3.TypeVente = out1.TypeVente;								
			row3.Mode_Paiement = out1.Mode_Paiement;			

			nb_line_tUnite_1++;

//////////
 


	tos_count_tUnite_1++;

/**
 * [tUnite_1 main ] stop
 */
	
	/**
	 * [tUnite_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_begin ] stop
 */

	
	/**
	 * [tMap_5 main ] start
	 */

	

	
	
	currentComponent="tMap_5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_5 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_5 = false;
		  boolean mainRowRejected_tMap_5 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row9" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow9 = false;
       		  	    	
       		  	    	
 							row9Struct row9ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_5) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_5 = false;
								
                        		    		    row9HashKey.Input_Geo = row3.Ville ;
                        		    		

								
		                        	row9HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row9.lookup( row9HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row9.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_5 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row9 != null && tHash_Lookup_row9.getCount(row9HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row9' and it contains more one result from keys :  row9.Input_Geo = '" + row9HashKey.Input_Geo + "'");
								} // G 071
							

							row9Struct row9 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row9Struct fromLookup_row9 = null;
							row9 = row9Default;
										 
							
								 
							
							
								if (tHash_Lookup_row9 !=null && tHash_Lookup_row9.hasNext()) { // G 099
								
							
								
								fromLookup_row9 = tHash_Lookup_row9.next();

							
							
								} // G 099
							
							

							if(fromLookup_row9 != null) {
								row9 = fromLookup_row9;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row12" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow12 = false;
       		  	    	
       		  	    	
 							row12Struct row12ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_5) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_5 = false;
								
                        		    		    row12HashKey.Produit = row3.Nom_de_l_element ;
                        		    		

								
		                        	row12HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row12.lookup( row12HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row12.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_5 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row12 != null && tHash_Lookup_row12.getCount(row12HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row12' and it contains more one result from keys :  row12.Produit = '" + row12HashKey.Produit + "'");
								} // G 071
							

							row12Struct row12 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row12Struct fromLookup_row12 = null;
							row12 = row12Default;
										 
							
								 
							
							
								if (tHash_Lookup_row12 !=null && tHash_Lookup_row12.hasNext()) { // G 099
								
							
								
								fromLookup_row12 = tHash_Lookup_row12.next();

							
							
								} // G 099
							
							

							if(fromLookup_row12 != null) {
								row12 = fromLookup_row12;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_5__Struct Var = Var__tMap_5;// ###############################
        // ###############################
        // # Output tables

out_state = null;

if(!rejectedInnerJoin_tMap_5 ) {

// # Output table : 'out_state'
out_state_tmp.Vente_PK = row3.Vente_PK ;
out_state_tmp.Date_FK = row3.Date_FK ;
out_state_tmp.Type_Vente_FK = row3.Type_Vente_FK ;
out_state_tmp.Produit_FK = row3.Produit_FK ;
out_state_tmp.Categorie_FK = row3.Categorie_FK ;
out_state_tmp.Client_FK = row3.Client_FK ;
out_state_tmp.Geographie_FK = row3.Geographie_FK ;
out_state_tmp.Mode_Paiement_FK = row3.Mode_Paiement_FK ;
out_state_tmp.Quantite = row3.Quantite ;
out_state_tmp.Montant_Livraison = row3.Montant_Livraison ;
out_state_tmp.Montant_Remboursement = row3.Montant_Remboursement ;
out_state_tmp.Montant_de_la_commande = row3.Montant_de_la_commande ;
out_state_tmp.Montant_Remise = row3.Montant_Remise ;
out_state_tmp.Code_Promo = row3.Code_Promo ;
out_state_tmp.Numero_Facture = row3.Numero_Facture ;
out_state_tmp.Numero_Commande = row3.Numero_Commande ;
out_state_tmp.Prix_Unite = row3.Prix_Unite ;
out_state_tmp.Code_Client = row3.Code_Client ;
out_state_tmp.Date_de_commande = row3.Date_de_commande ;
out_state_tmp.Nom_de_l_element = row3.Nom_de_l_element ;
out_state_tmp.UGS = row3.UGS ;
out_state_tmp.Ville = row9.Ville ;
out_state_tmp.TypeVente = row3.TypeVente ;
out_state_tmp.Mode_Paiement = row3.Mode_Paiement ;
out_state_tmp.Categorie = row12.Categorie ;
out_state = out_state_tmp;
}  // closing inner join bracket (2)
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_5 = false;










 


	tos_count_tMap_5++;

/**
 * [tMap_5 main ] stop
 */
	
	/**
	 * [tMap_5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 process_data_begin ] stop
 */
// Start of branch "out_state"
if(out_state != null) { 



	
	/**
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_state"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_3 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_3 = false;
		  boolean mainRowRejected_tMap_3 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row4" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow4 = false;
       		  	    	
       		  	    	
 							row4Struct row4ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row4HashKey.Code_client = out_state.Code_Client ;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row4 != null && tHash_Lookup_row4.getCount(row4HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row4' and it contains more one result from keys :  row4.Code_client = '" + row4HashKey.Code_client + "'");
								} // G 071
							

							row4Struct row4 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								 
							
							
								if (tHash_Lookup_row4 !=null && tHash_Lookup_row4.hasNext()) { // G 099
								
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							
							
								} // G 099
							
							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row5" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow5 = false;
       		  	    	
       		  	    	
 							row5Struct row5ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row5HashKey.Type_Vente = out_state.TypeVente;
                        		    		

								
		                        	row5HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row5.lookup( row5HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row5 != null && tHash_Lookup_row5.getCount(row5HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row5' and it contains more one result from keys :  row5.Type_Vente = '" + row5HashKey.Type_Vente + "'");
								} // G 071
							

							row5Struct row5 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row5Struct fromLookup_row5 = null;
							row5 = row5Default;
										 
							
								 
							
							
								if (tHash_Lookup_row5 !=null && tHash_Lookup_row5.hasNext()) { // G 099
								
							
								
								fromLookup_row5 = tHash_Lookup_row5.next();

							
							
								} // G 099
							
							

							if(fromLookup_row5 != null) {
								row5 = fromLookup_row5;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row6HashKey.Type_paiement = out_state.Mode_Paiement;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Type_paiement = '" + row6HashKey.Type_paiement + "'");
								} // G 071
							

							row6Struct row6 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row6Struct fromLookup_row6 = null;
							row6 = row6Default;
										 
							
								 
							
							
								if (tHash_Lookup_row6 !=null && tHash_Lookup_row6.hasNext()) { // G 099
								
							
								
								fromLookup_row6 = tHash_Lookup_row6.next();

							
							
								} // G 099
							
							

							if(fromLookup_row6 != null) {
								row6 = fromLookup_row6;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row7" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow7 = false;
       		  	    	
       		  	    	
 							row7Struct row7ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row7HashKey.Date = out_state.Date_de_commande == null ? null : new java.util.Date(out_state.Date_de_commande.getTime());
                        		    		

								
		                        	row7HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row7.lookup( row7HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row7 != null && tHash_Lookup_row7.getCount(row7HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row7' and it contains more one result from keys :  row7.Date = '" + row7HashKey.Date + "'");
								} // G 071
							

							row7Struct row7 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row7Struct fromLookup_row7 = null;
							row7 = row7Default;
										 
							
								 
							
							
								if (tHash_Lookup_row7 !=null && tHash_Lookup_row7.hasNext()) { // G 099
								
							
								
								fromLookup_row7 = tHash_Lookup_row7.next();

							
							
								} // G 099
							
							

							if(fromLookup_row7 != null) {
								row7 = fromLookup_row7;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row11" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow11 = false;
       		  	    	
       		  	    	
 							row11Struct row11ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row11HashKey.Ville = out_state.Ville;
                        		    		

								
		                        	row11HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row11.lookup( row11HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row11 != null && tHash_Lookup_row11.getCount(row11HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row11' and it contains more one result from keys :  row11.Ville = '" + row11HashKey.Ville + "'");
								} // G 071
							

							row11Struct row11 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row11Struct fromLookup_row11 = null;
							row11 = row11Default;
										 
							
								 
							
							
								if (tHash_Lookup_row11 !=null && tHash_Lookup_row11.hasNext()) { // G 099
								
							
								
								fromLookup_row11 = tHash_Lookup_row11.next();

							
							
								} // G 099
							
							

							if(fromLookup_row11 != null) {
								row11 = fromLookup_row11;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row8" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow8 = false;
       		  	    	
       		  	    	
 							row8Struct row8ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row8HashKey.Nom_produit = out_state.Nom_de_l_element;
                        		    		

								
		                        	row8HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row8.lookup( row8HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row8 != null && tHash_Lookup_row8.getCount(row8HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row8' and it contains more one result from keys :  row8.Nom_produit = '" + row8HashKey.Nom_produit + "'");
								} // G 071
							

							row8Struct row8 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row8Struct fromLookup_row8 = null;
							row8 = row8Default;
										 
							
								 
							
							
								if (tHash_Lookup_row8 !=null && tHash_Lookup_row8.hasNext()) { // G 099
								
							
								
								fromLookup_row8 = tHash_Lookup_row8.next();

							
							
								} // G 099
							
							

							if(fromLookup_row8 != null) {
								row8 = fromLookup_row8;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	           	
	            	
	            	
	            

				///////////////////////////////////////////////
				// Starting Lookup Table "row13" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow13 = false;
       		  	    	
       		  	    	
 							row13Struct row13ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row13HashKey.Categorie = out_state.Categorie;
                        		    		

								
		                        	row13HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row13.lookup( row13HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row13 != null && tHash_Lookup_row13.getCount(row13HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row13' and it contains more one result from keys :  row13.Categorie = '" + row13HashKey.Categorie + "'");
								} // G 071
							

							row13Struct row13 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row13Struct fromLookup_row13 = null;
							row13 = row13Default;
										 
							
								 
							
							
								if (tHash_Lookup_row13 !=null && tHash_Lookup_row13.hasNext()) { // G 099
								
							
								
								fromLookup_row13 = tHash_Lookup_row13.next();

							
							
								} // G 099
							
							

							if(fromLookup_row13 != null) {
								row13 = fromLookup_row13;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

outfinal = null;
client_rejected = null;


// # Output table : 'outfinal'
outfinal_tmp.Vente_PK = 0;
outfinal_tmp.Date_FK = row7.Date_PK ;
outfinal_tmp.Type_Vente_FK = row5.Type_Vente_PK ;
outfinal_tmp.Produit_FK = row8.Produit_PK ;
outfinal_tmp.Categorie_FK = row13.Categorie_PK ;
outfinal_tmp.Client_FK = row4.Client_PK ;
outfinal_tmp.Geographie_FK = row11.Geographie_PK ;
outfinal_tmp.Mode_Paiement_FK = row6.Mode_Paiement_PK ;
outfinal_tmp.Quantite = out_state.Quantite ;
outfinal_tmp.Montant_Livraison = out_state.Montant_Livraison ;
outfinal_tmp.Montant_Remboursement = out_state.Montant_Remboursement ;
outfinal_tmp.Montant_de_la_commande = out_state.Montant_de_la_commande ;
outfinal_tmp.Montant_Remise = out_state.Montant_Remise ;
outfinal_tmp.Code_Promo = out_state.Code_Promo ;
outfinal_tmp.Numero_Facture = out_state.Numero_Facture ;
outfinal_tmp.Numero_Commande = out_state.Numero_Commande ;
outfinal_tmp.Prix_Unite = out_state.Prix_Unite ;
outfinal = outfinal_tmp;

// # Output table : 'client_rejected'
// # Filter conditions 
if( 

row4.Code_client==null

 ) {
client_rejected_tmp.Code_client = out_state.Code_Client ;
client_rejected = client_rejected_tmp;
} // closing filter/reject
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_3 = false;










 


	tos_count_tMap_3++;

/**
 * [tMap_3 main ] stop
 */
	
	/**
	 * [tMap_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_begin ] stop
 */
// Start of branch "outfinal"
if(outfinal != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"outfinal"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Categorie_FK = outfinal.Categorie_FK;
            lookUpKey_tDBSCD_1.Client_FK = outfinal.Client_FK;
            lookUpKey_tDBSCD_1.Date_FK = outfinal.Date_FK;
            lookUpKey_tDBSCD_1.Geographie_FK = outfinal.Geographie_FK;
            lookUpKey_tDBSCD_1.Mode_Paiement_FK = outfinal.Mode_Paiement_FK;
            lookUpKey_tDBSCD_1.Produit_FK = outfinal.Produit_FK;
            lookUpKey_tDBSCD_1.Type_Vente_FK = outfinal.Type_Vente_FK;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(outfinal.Categorie_FK == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(1, outfinal.Categorie_FK);
}

                    if(outfinal.Client_FK == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(2, outfinal.Client_FK);
}

                    if(outfinal.Date_FK == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(3, outfinal.Date_FK);
}

                    if(outfinal.Geographie_FK == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(4, outfinal.Geographie_FK);
}

                    if(outfinal.Mode_Paiement_FK == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(5, outfinal.Mode_Paiement_FK);
}

                    if(outfinal.Produit_FK == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(6, outfinal.Produit_FK);
}

                    if(outfinal.Type_Vente_FK == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(7, outfinal.Type_Vente_FK);
}

                    if(outfinal.Code_Promo == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(8, outfinal.Code_Promo);
}

                    if(outfinal.Montant_de_la_commande == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, outfinal.Montant_de_la_commande);
}

                    if(outfinal.Montant_Livraison == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(10, outfinal.Montant_Livraison);
}

                    if(outfinal.Montant_Remboursement == null) {
insertionStatement_tDBSCD_1.setNull(11, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(11, outfinal.Montant_Remboursement);
}

                    if(outfinal.Montant_Remise == null) {
insertionStatement_tDBSCD_1.setNull(12, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(12, outfinal.Montant_Remise);
}

                    if(outfinal.Numero_Commande == null) {
insertionStatement_tDBSCD_1.setNull(13, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(13, outfinal.Numero_Commande);
}

                    if(outfinal.Numero_Facture == null) {
insertionStatement_tDBSCD_1.setNull(14, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(14, outfinal.Numero_Facture);
}

                    if(outfinal.Prix_Unite == null) {
insertionStatement_tDBSCD_1.setNull(15, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(15, outfinal.Prix_Unite);
}

                    if(outfinal.Quantite == null) {
insertionStatement_tDBSCD_1.setNull(16, java.sql.Types.INTEGER);
} else {
insertionStatement_tDBSCD_1.setInt(16, outfinal.Quantite);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_Promo == null && outfinal.Code_Promo!= null) || (lookUpValue_tDBSCD_1.Code_Promo != null && !lookUpValue_tDBSCD_1.Code_Promo.equals(outfinal.Code_Promo)) || (lookUpValue_tDBSCD_1.Montant_de_la_commande == null && outfinal.Montant_de_la_commande!= null) || (lookUpValue_tDBSCD_1.Montant_de_la_commande != null && !lookUpValue_tDBSCD_1.Montant_de_la_commande.equals(outfinal.Montant_de_la_commande)) || (lookUpValue_tDBSCD_1.Montant_Livraison == null && outfinal.Montant_Livraison!= null) || (lookUpValue_tDBSCD_1.Montant_Livraison != null && !lookUpValue_tDBSCD_1.Montant_Livraison.equals(outfinal.Montant_Livraison)) || (lookUpValue_tDBSCD_1.Montant_Remboursement == null && outfinal.Montant_Remboursement!= null) || (lookUpValue_tDBSCD_1.Montant_Remboursement != null && !lookUpValue_tDBSCD_1.Montant_Remboursement.equals(outfinal.Montant_Remboursement)) || (lookUpValue_tDBSCD_1.Montant_Remise == null && outfinal.Montant_Remise!= null) || (lookUpValue_tDBSCD_1.Montant_Remise != null && !lookUpValue_tDBSCD_1.Montant_Remise.equals(outfinal.Montant_Remise)) || (lookUpValue_tDBSCD_1.Numero_Commande == null && outfinal.Numero_Commande!= null) || (lookUpValue_tDBSCD_1.Numero_Commande != null && !lookUpValue_tDBSCD_1.Numero_Commande.equals(outfinal.Numero_Commande)) || (lookUpValue_tDBSCD_1.Numero_Facture == null && outfinal.Numero_Facture!= null) || (lookUpValue_tDBSCD_1.Numero_Facture != null && !lookUpValue_tDBSCD_1.Numero_Facture.equals(outfinal.Numero_Facture)) || (lookUpValue_tDBSCD_1.Prix_Unité == null && outfinal.Prix_Unite!= null) || (lookUpValue_tDBSCD_1.Prix_Unité != null && !lookUpValue_tDBSCD_1.Prix_Unité.equals(outfinal.Prix_Unite)) || (lookUpValue_tDBSCD_1.Quantite == null && outfinal.Quantite!= null) || (lookUpValue_tDBSCD_1.Quantite != null && !lookUpValue_tDBSCD_1.Quantite.equals(outfinal.Quantite))) {
                    if(outfinal.Code_Promo == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, outfinal.Code_Promo);
}

                    if(outfinal.Montant_de_la_commande == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(2, outfinal.Montant_de_la_commande);
}

                    if(outfinal.Montant_Livraison == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(3, outfinal.Montant_Livraison);
}

                    if(outfinal.Montant_Remboursement == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(4, outfinal.Montant_Remboursement);
}

                    if(outfinal.Montant_Remise == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(5, outfinal.Montant_Remise);
}

                    if(outfinal.Numero_Commande == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, outfinal.Numero_Commande);
}

                    if(outfinal.Numero_Facture == null) {
updateForType1_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(7, outfinal.Numero_Facture);
}

                    if(outfinal.Prix_Unite == null) {
updateForType1_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
updateForType1_tDBSCD_1.setDouble(8, outfinal.Prix_Unite);
}

                    if(outfinal.Quantite == null) {
updateForType1_tDBSCD_1.setNull(9, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(9, outfinal.Quantite);
}

                    if(outfinal.Categorie_FK == null) {
updateForType1_tDBSCD_1.setNull(10, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(10, outfinal.Categorie_FK);
}

                    if(outfinal.Client_FK == null) {
updateForType1_tDBSCD_1.setNull(11, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(11, outfinal.Client_FK);
}

                    if(outfinal.Date_FK == null) {
updateForType1_tDBSCD_1.setNull(12, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(12, outfinal.Date_FK);
}

                    if(outfinal.Geographie_FK == null) {
updateForType1_tDBSCD_1.setNull(13, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(13, outfinal.Geographie_FK);
}

                    if(outfinal.Mode_Paiement_FK == null) {
updateForType1_tDBSCD_1.setNull(14, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(14, outfinal.Mode_Paiement_FK);
}

                    if(outfinal.Produit_FK == null) {
updateForType1_tDBSCD_1.setNull(15, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(15, outfinal.Produit_FK);
}

                    if(outfinal.Type_Vente_FK == null) {
updateForType1_tDBSCD_1.setNull(16, java.sql.Types.INTEGER);
} else {
updateForType1_tDBSCD_1.setInt(16, outfinal.Type_Vente_FK);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_Promo = outfinal.Code_Promo;
                lookUpValue_tDBSCD_1.Montant_de_la_commande = outfinal.Montant_de_la_commande;
                lookUpValue_tDBSCD_1.Montant_Livraison = outfinal.Montant_Livraison;
                lookUpValue_tDBSCD_1.Montant_Remboursement = outfinal.Montant_Remboursement;
                lookUpValue_tDBSCD_1.Montant_Remise = outfinal.Montant_Remise;
                lookUpValue_tDBSCD_1.Numero_Commande = outfinal.Numero_Commande;
                lookUpValue_tDBSCD_1.Numero_Facture = outfinal.Numero_Facture;
                lookUpValue_tDBSCD_1.Prix_Unité = outfinal.Prix_Unite;
                lookUpValue_tDBSCD_1.Quantite = outfinal.Quantite;
        cache_tDBSCD_1.put(lookUpKey_tDBSCD_1, lookUpValue_tDBSCD_1);


 


	tos_count_tDBSCD_1++;

/**
 * [tDBSCD_1 main ] stop
 */
	
	/**
	 * [tDBSCD_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBSCD_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 process_data_end ] stop
 */

} // End of branch "outfinal"




// Start of branch "client_rejected"
if(client_rejected != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"client_rejected"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[1];
   				
	    		if(client_rejected.Code_client != null) { //              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(client_rejected.Code_client)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 


	tos_count_tLogRow_1++;

/**
 * [tLogRow_1 main ] stop
 */
	
	/**
	 * [tLogRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_begin ] stop
 */
	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "client_rejected"




	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */

} // End of branch "out_state"




	
	/**
	 * [tMap_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out1"




	
	/**
	 * [tMap_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

 



/**
 * [tDBInput_1 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_1 end ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

	}
}finally{
	if (rs_tDBInput_1 != null) {
		rs_tDBInput_1.close();
	}
	if (stmt_tDBInput_1 != null) {
		stmt_tDBInput_1.close();
	}
		if(conn_tDBInput_1 != null && !conn_tDBInput_1.isClosed()) {
			
			conn_tDBInput_1.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_1_NB_LINE",nb_line_tDBInput_1);

 

ok_Hash.put("tDBInput_1", true);
end_Hash.put("tDBInput_1", System.currentTimeMillis());




/**
 * [tDBInput_1 end ] stop
 */

	
	/**
	 * [tMap_1 end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row1");
			  	}
			  	
 

ok_Hash.put("tMap_1", true);
end_Hash.put("tMap_1", System.currentTimeMillis());




/**
 * [tMap_1 end ] stop
 */



	
	/**
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out2","out1");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
 */

	
	/**
	 * [tMap_5 end ] start
	 */

	

	
	
	currentComponent="tMap_5";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row9 != null) {
						tHash_Lookup_row9.endGet();
					}
					globalMap.remove( "tHash_Lookup_row9" );

					
					
				
					if(tHash_Lookup_row12 != null) {
						tHash_Lookup_row12.endGet();
					}
					globalMap.remove( "tHash_Lookup_row12" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row3");
			  	}
			  	
 

ok_Hash.put("tMap_5", true);
end_Hash.put("tMap_5", System.currentTimeMillis());




/**
 * [tMap_5 end ] stop
 */

	
	/**
	 * [tMap_3 end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row4 != null) {
						tHash_Lookup_row4.endGet();
					}
					globalMap.remove( "tHash_Lookup_row4" );

					
					
				
					if(tHash_Lookup_row5 != null) {
						tHash_Lookup_row5.endGet();
					}
					globalMap.remove( "tHash_Lookup_row5" );

					
					
				
					if(tHash_Lookup_row6 != null) {
						tHash_Lookup_row6.endGet();
					}
					globalMap.remove( "tHash_Lookup_row6" );

					
					
				
					if(tHash_Lookup_row7 != null) {
						tHash_Lookup_row7.endGet();
					}
					globalMap.remove( "tHash_Lookup_row7" );

					
					
				
					if(tHash_Lookup_row11 != null) {
						tHash_Lookup_row11.endGet();
					}
					globalMap.remove( "tHash_Lookup_row11" );

					
					
				
					if(tHash_Lookup_row8 != null) {
						tHash_Lookup_row8.endGet();
					}
					globalMap.remove( "tHash_Lookup_row8" );

					
					
				
					if(tHash_Lookup_row13 != null) {
						tHash_Lookup_row13.endGet();
					}
					globalMap.remove( "tHash_Lookup_row13" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out_state");
			  	}
			  	
 

ok_Hash.put("tMap_3", true);
end_Hash.put("tMap_3", System.currentTimeMillis());




/**
 * [tMap_3 end ] stop
 */

	
	/**
	 * [tDBSCD_1 end ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

    insertionStatement_tDBSCD_1.close();
        updateForType1_tDBSCD_1.close();
    
    if(conn_tDBSCD_1 != null && !conn_tDBSCD_1.isClosed()) {
        conn_tDBSCD_1.close();
    }    
    
    globalMap.put("tDBSCD_1_NB_LINE_UPDATED", nb_line_update_tDBSCD_1);
    globalMap.put("tDBSCD_1_NB_LINE_INSERTED", nb_line_inserted_tDBSCD_1);
    globalMap.put("tDBSCD_1_NB_LINE_REJECTED",nb_line_rejected_tDBSCD_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"outfinal");
			  	}
			  	
 

ok_Hash.put("tDBSCD_1", true);
end_Hash.put("tDBSCD_1", System.currentTimeMillis());




/**
 * [tDBSCD_1 end ] stop
 */




	
	/**
	 * [tLogRow_1 end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	


//////

                    
                    java.io.PrintStream consoleOut_tLogRow_1 = null;
                    if (globalMap.get("tLogRow_CONSOLE")!=null)
                    {
                    	consoleOut_tLogRow_1 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
                    }
                    else
                    {
                    	consoleOut_tLogRow_1 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                    	globalMap.put("tLogRow_CONSOLE",consoleOut_tLogRow_1);
                    }
                    
                    consoleOut_tLogRow_1.println(util_tLogRow_1.format().toString());
                    consoleOut_tLogRow_1.flush();
//////
globalMap.put("tLogRow_1_NB_LINE",nb_line_tLogRow_1);

///////////////////////    			

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"client_rejected");
			  	}
			  	
 

ok_Hash.put("tLogRow_1", true);
end_Hash.put("tLogRow_1", System.currentTimeMillis());




/**
 * [tLogRow_1 end ] stop
 */









				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row4"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row5"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row6"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row7"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row11"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row8"); 
				     			
					     			//free memory for "tMap_3"
					     			globalMap.remove("tHash_Lookup_row13"); 
				     			
					     			//free memory for "tMap_5"
					     			globalMap.remove("tHash_Lookup_row9"); 
				     			
					     			//free memory for "tMap_5"
					     			globalMap.remove("tHash_Lookup_row12"); 
				     			
				try{
					
	
	/**
	 * [tDBInput_2 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 



/**
 * [tDBInput_2 finally ] stop
 */

	
	/**
	 * [tMap_2 finally ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 finally ] stop
 */



	
	/**
	 * [tDBInput_1 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

 



/**
 * [tDBInput_1 finally ] stop
 */

	
	/**
	 * [tMap_1 finally ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 finally ] stop
 */



	
	/**
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tMap_5 finally ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 finally ] stop
 */

	
	/**
	 * [tMap_3 finally ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 finally ] stop
 */

	
	/**
	 * [tDBSCD_1 finally ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 finally ] stop
 */




	
	/**
	 * [tLogRow_1 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 finally ] stop
 */









				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_2_SUBPROCESS_STATE", 1);
	}
	


public static class row4Struct implements routines.system.IPersistableComparableLookupRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Client_PK;

				public int getClient_PK () {
					return this.Client_PK;
				}
				
			    public String Code_client;

				public String getCode_client () {
					return this.Code_client;
				}
				
			    public String Nom_client;

				public String getNom_client () {
					return this.Nom_client;
				}
				
			    public String Prenom_client;

				public String getPrenom_client () {
					return this.Prenom_client;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String Type_client;

				public String getType_client () {
					return this.Type_client;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Code_client == null) ? 0 : this.Code_client.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row4Struct other = (row4Struct) obj;
		
						if (this.Code_client == null) {
							if (other.Code_client != null)
								return false;
						
						} else if (!this.Code_client.equals(other.Code_client))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row4Struct other) {

		other.Client_PK = this.Client_PK;
	            other.Code_client = this.Code_client;
	            other.Nom_client = this.Nom_client;
	            other.Prenom_client = this.Prenom_client;
	            other.Ville = this.Ville;
	            other.Type_client = this.Type_client;
	            
	}

	public void copyKeysDataTo(row4Struct other) {

		other.Code_client = this.Code_client;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Code_client = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Code_client = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Code_client,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Code_client,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Client_PK = dis.readInt();
					
						this.Nom_client = readString(dis,ois);
					
						this.Prenom_client = readString(dis,ois);
					
						this.Ville = readString(dis,ois);
					
						this.Type_client = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Client_PK = objectIn.readInt();
					
						this.Nom_client = readString(dis,objectIn);
					
						this.Prenom_client = readString(dis,objectIn);
					
						this.Ville = readString(dis,objectIn);
					
						this.Type_client = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Client_PK);
					
						writeString(this.Nom_client, dos, oos);
					
						writeString(this.Prenom_client, dos, oos);
					
						writeString(this.Ville, dos, oos);
					
						writeString(this.Type_client, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Client_PK);
					
						writeString(this.Nom_client, dos, objectOut);
					
						writeString(this.Prenom_client, dos, objectOut);
					
						writeString(this.Ville, dos, objectOut);
					
						writeString(this.Type_client, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Client_PK="+String.valueOf(Client_PK));
		sb.append(",Code_client="+Code_client);
		sb.append(",Nom_client="+Nom_client);
		sb.append(",Prenom_client="+Prenom_client);
		sb.append(",Ville="+Ville);
		sb.append(",Type_client="+Type_client);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row4Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Code_client, other.Code_client);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_3Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_3_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row4Struct row4 = new row4Struct();




	
	/**
	 * [tAdvancedHash_row4 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row4", false);
		start_Hash.put("tAdvancedHash_row4", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row4");
					}
				
		int tos_count_tAdvancedHash_row4 = 0;
		

			   		// connection name:row4
			   		// source node:tDBInput_3 - inputs:(after_tDBInput_2) outputs:(row4,row4) | target node:tAdvancedHash_row4 - inputs:(row4) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row4 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row4Struct> tHash_Lookup_row4 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row4Struct>getLookup(matchingModeEnum_row4);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row4", tHash_Lookup_row4);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row4 begin ] stop
 */



	
	/**
	 * [tDBInput_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_3", false);
		start_Hash.put("tDBInput_3", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_3";

	
		int tos_count_tDBInput_3 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_3 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_3 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_3  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_3, talendToDBArray_tDBInput_3); 
		    int nb_line_tDBInput_3 = 0;
		    java.sql.Connection conn_tDBInput_3 = null;
				String driverClass_tDBInput_3 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_3 = java.lang.Class.forName(driverClass_tDBInput_3);
				String dbUser_tDBInput_3 = "sa";
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:dXMJFcUxbfTB6XFRHOZuWV9nzLTbibTljGPJC9IzINX/DhFBYp8=");
				
				String dbPwd_tDBInput_3 = decryptedPassword_tDBInput_3;
				
		    String port_tDBInput_3 = "1433";
		    String dbname_tDBInput_3 = "DW_E-Commerce" ;
			String url_tDBInput_3 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_3)) {
		    	url_tDBInput_3 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_3)) {
				url_tDBInput_3 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_3 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_3 = "";
				
				conn_tDBInput_3 = java.sql.DriverManager.getConnection(url_tDBInput_3,dbUser_tDBInput_3,dbPwd_tDBInput_3);
		        
		    
			java.sql.Statement stmt_tDBInput_3 = conn_tDBInput_3.createStatement();

		    String dbquery_tDBInput_3 = "SELECT Dim_Client.Client_PK,\n		Dim_Client.Code_client,\n		Dim_Client.Nom_client,\n		Dim_Client.Prenom_client,\n		Dim_Clien"
+"t.Ville,\n		Dim_Client.Type_client\nFROM	Dim_Client";
		    

            	globalMap.put("tDBInput_3_QUERY",dbquery_tDBInput_3);
		    java.sql.ResultSet rs_tDBInput_3 = null;

		    try {
		    	rs_tDBInput_3 = stmt_tDBInput_3.executeQuery(dbquery_tDBInput_3);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_3 = rs_tDBInput_3.getMetaData();
		    	int colQtyInRs_tDBInput_3 = rsmd_tDBInput_3.getColumnCount();

		    String tmpContent_tDBInput_3 = null;
		    
		    
		    while (rs_tDBInput_3.next()) {
		        nb_line_tDBInput_3++;
		        
							if(colQtyInRs_tDBInput_3 < 1) {
								row4.Client_PK = 0;
							} else {
		                          
            row4.Client_PK = rs_tDBInput_3.getInt(1);
            if(rs_tDBInput_3.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row4.Code_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_client = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row4.Nom_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(3);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Nom_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Nom_client = tmpContent_tDBInput_3;
                }
            } else {
                row4.Nom_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row4.Prenom_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Prenom_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Prenom_client = tmpContent_tDBInput_3;
                }
            } else {
                row4.Prenom_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 5) {
								row4.Ville = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(5);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Ville = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Ville = tmpContent_tDBInput_3;
                }
            } else {
                row4.Ville = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 6) {
								row4.Type_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(6);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Type_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Type_client = tmpContent_tDBInput_3;
                }
            } else {
                row4.Type_client = null;
            }
		                    }
					





 



/**
 * [tDBInput_3 begin ] stop
 */
	
	/**
	 * [tDBInput_3 main ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 


	tos_count_tDBInput_3++;

/**
 * [tDBInput_3 main ] stop
 */
	
	/**
	 * [tDBInput_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 



/**
 * [tDBInput_3 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row4 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					


			   
			   

					row4Struct row4_HashRow = new row4Struct();
		   	   	   
				
				row4_HashRow.Client_PK = row4.Client_PK;
				
				row4_HashRow.Code_client = row4.Code_client;
				
				row4_HashRow.Nom_client = row4.Nom_client;
				
				row4_HashRow.Prenom_client = row4.Prenom_client;
				
				row4_HashRow.Ville = row4.Ville;
				
				row4_HashRow.Type_client = row4.Type_client;
				
			tHash_Lookup_row4.put(row4_HashRow);
			
            




 


	tos_count_tAdvancedHash_row4++;

/**
 * [tAdvancedHash_row4 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row4";

	

 



/**
 * [tAdvancedHash_row4 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row4 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row4";

	

 



/**
 * [tAdvancedHash_row4 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 



/**
 * [tDBInput_3 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_3 end ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

	}
}finally{
	if (rs_tDBInput_3 != null) {
		rs_tDBInput_3.close();
	}
	if (stmt_tDBInput_3 != null) {
		stmt_tDBInput_3.close();
	}
		if(conn_tDBInput_3 != null && !conn_tDBInput_3.isClosed()) {
			
			conn_tDBInput_3.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_3_NB_LINE",nb_line_tDBInput_3);

 

ok_Hash.put("tDBInput_3", true);
end_Hash.put("tDBInput_3", System.currentTimeMillis());




/**
 * [tDBInput_3 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row4 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row4";

	

tHash_Lookup_row4.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row4");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row4", true);
end_Hash.put("tAdvancedHash_row4", System.currentTimeMillis());




/**
 * [tAdvancedHash_row4 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_3 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 



/**
 * [tDBInput_3 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row4 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row4";

	

 



/**
 * [tAdvancedHash_row4 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_3_SUBPROCESS_STATE", 1);
	}
	


public static class row5Struct implements routines.system.IPersistableComparableLookupRow<row5Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Type_Vente_PK;

				public int getType_Vente_PK () {
					return this.Type_Vente_PK;
				}
				
			    public String Type_Vente;

				public String getType_Vente () {
					return this.Type_Vente;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Type_Vente == null) ? 0 : this.Type_Vente.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row5Struct other = (row5Struct) obj;
		
						if (this.Type_Vente == null) {
							if (other.Type_Vente != null)
								return false;
						
						} else if (!this.Type_Vente.equals(other.Type_Vente))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row5Struct other) {

		other.Type_Vente_PK = this.Type_Vente_PK;
	            other.Type_Vente = this.Type_Vente;
	            
	}

	public void copyKeysDataTo(row5Struct other) {

		other.Type_Vente = this.Type_Vente;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Type_Vente = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Type_Vente = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Type_Vente,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Type_Vente,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Type_Vente_PK = dis.readInt();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Type_Vente_PK = objectIn.readInt();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Type_Vente_PK);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Type_Vente_PK);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Type_Vente_PK="+String.valueOf(Type_Vente_PK));
		sb.append(",Type_Vente="+Type_Vente);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row5Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Type_Vente, other.Type_Vente);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_6Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_6_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row5Struct row5 = new row5Struct();




	
	/**
	 * [tAdvancedHash_row5 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row5", false);
		start_Hash.put("tAdvancedHash_row5", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row5";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
					}
				
		int tos_count_tAdvancedHash_row5 = 0;
		

			   		// connection name:row5
			   		// source node:tDBInput_6 - inputs:(after_tDBInput_2) outputs:(row5,row5) | target node:tAdvancedHash_row5 - inputs:(row5) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row5 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row5Struct> tHash_Lookup_row5 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row5Struct>getLookup(matchingModeEnum_row5);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row5", tHash_Lookup_row5);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row5 begin ] stop
 */



	
	/**
	 * [tDBInput_6 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_6", false);
		start_Hash.put("tDBInput_6", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_6";

	
		int tos_count_tDBInput_6 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_6 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_6 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_6  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_6, talendToDBArray_tDBInput_6); 
		    int nb_line_tDBInput_6 = 0;
		    java.sql.Connection conn_tDBInput_6 = null;
				String driverClass_tDBInput_6 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_6 = java.lang.Class.forName(driverClass_tDBInput_6);
				String dbUser_tDBInput_6 = "sa";
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:oYneQmcPveiDlAI9aSxrnq/PLKuXUDdrriOHDVutc+hL6RoE2YI=");
				
				String dbPwd_tDBInput_6 = decryptedPassword_tDBInput_6;
				
		    String port_tDBInput_6 = "1433";
		    String dbname_tDBInput_6 = "DW_E-Commerce" ;
			String url_tDBInput_6 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_6)) {
		    	url_tDBInput_6 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_6)) {
				url_tDBInput_6 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_6 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_6 = "";
				
				conn_tDBInput_6 = java.sql.DriverManager.getConnection(url_tDBInput_6,dbUser_tDBInput_6,dbPwd_tDBInput_6);
		        
		    
			java.sql.Statement stmt_tDBInput_6 = conn_tDBInput_6.createStatement();

		    String dbquery_tDBInput_6 = "SELECT Dim_Type_Vente.Type_Vente_PK,\n		Dim_Type_Vente.Type_Vente\nFROM	Dim_Type_Vente";
		    

            	globalMap.put("tDBInput_6_QUERY",dbquery_tDBInput_6);
		    java.sql.ResultSet rs_tDBInput_6 = null;

		    try {
		    	rs_tDBInput_6 = stmt_tDBInput_6.executeQuery(dbquery_tDBInput_6);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_6 = rs_tDBInput_6.getMetaData();
		    	int colQtyInRs_tDBInput_6 = rsmd_tDBInput_6.getColumnCount();

		    String tmpContent_tDBInput_6 = null;
		    
		    
		    while (rs_tDBInput_6.next()) {
		        nb_line_tDBInput_6++;
		        
							if(colQtyInRs_tDBInput_6 < 1) {
								row5.Type_Vente_PK = 0;
							} else {
		                          
            row5.Type_Vente_PK = rs_tDBInput_6.getInt(1);
            if(rs_tDBInput_6.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row5.Type_Vente = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row5.Type_Vente = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row5.Type_Vente = tmpContent_tDBInput_6;
                }
            } else {
                row5.Type_Vente = null;
            }
		                    }
					





 



/**
 * [tDBInput_6 begin ] stop
 */
	
	/**
	 * [tDBInput_6 main ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 


	tos_count_tDBInput_6++;

/**
 * [tDBInput_6 main ] stop
 */
	
	/**
	 * [tDBInput_6 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row5 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					


			   
			   

					row5Struct row5_HashRow = new row5Struct();
		   	   	   
				
				row5_HashRow.Type_Vente_PK = row5.Type_Vente_PK;
				
				row5_HashRow.Type_Vente = row5.Type_Vente;
				
			tHash_Lookup_row5.put(row5_HashRow);
			
            




 


	tos_count_tAdvancedHash_row5++;

/**
 * [tAdvancedHash_row5 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	

 



/**
 * [tAdvancedHash_row5 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row5 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	

 



/**
 * [tAdvancedHash_row5 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_6 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_6 end ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

	}
}finally{
	if (rs_tDBInput_6 != null) {
		rs_tDBInput_6.close();
	}
	if (stmt_tDBInput_6 != null) {
		stmt_tDBInput_6.close();
	}
		if(conn_tDBInput_6 != null && !conn_tDBInput_6.isClosed()) {
			
			conn_tDBInput_6.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_6_NB_LINE",nb_line_tDBInput_6);

 

ok_Hash.put("tDBInput_6", true);
end_Hash.put("tDBInput_6", System.currentTimeMillis());




/**
 * [tDBInput_6 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row5 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	

tHash_Lookup_row5.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row5", true);
end_Hash.put("tAdvancedHash_row5", System.currentTimeMillis());




/**
 * [tAdvancedHash_row5 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row5 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	

 



/**
 * [tAdvancedHash_row5 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_6_SUBPROCESS_STATE", 1);
	}
	


public static class row6Struct implements routines.system.IPersistableComparableLookupRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Mode_Paiement_PK;

				public int getMode_Paiement_PK () {
					return this.Mode_Paiement_PK;
				}
				
			    public String Type_paiement;

				public String getType_paiement () {
					return this.Type_paiement;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Type_paiement == null) ? 0 : this.Type_paiement.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row6Struct other = (row6Struct) obj;
		
						if (this.Type_paiement == null) {
							if (other.Type_paiement != null)
								return false;
						
						} else if (!this.Type_paiement.equals(other.Type_paiement))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row6Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(row6Struct other) {

		other.Type_paiement = this.Type_paiement;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Mode_Paiement_PK = dis.readInt();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Mode_Paiement_PK = objectIn.readInt();
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Mode_Paiement_PK);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Mode_Paiement_PK);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Mode_Paiement_PK="+String.valueOf(Mode_Paiement_PK));
		sb.append(",Type_paiement="+Type_paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row6Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Type_paiement, other.Type_paiement);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_4Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_4_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row6Struct row6 = new row6Struct();




	
	/**
	 * [tAdvancedHash_row6 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row6", false);
		start_Hash.put("tAdvancedHash_row6", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row6";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row6");
					}
				
		int tos_count_tAdvancedHash_row6 = 0;
		

			   		// connection name:row6
			   		// source node:tDBInput_4 - inputs:(after_tDBInput_2) outputs:(row6,row6) | target node:tAdvancedHash_row6 - inputs:(row6) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row6 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct> tHash_Lookup_row6 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row6Struct>getLookup(matchingModeEnum_row6);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row6", tHash_Lookup_row6);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row6 begin ] stop
 */



	
	/**
	 * [tDBInput_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_4", false);
		start_Hash.put("tDBInput_4", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_4";

	
		int tos_count_tDBInput_4 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_4 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_4 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_4  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_4, talendToDBArray_tDBInput_4); 
		    int nb_line_tDBInput_4 = 0;
		    java.sql.Connection conn_tDBInput_4 = null;
				String driverClass_tDBInput_4 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_4 = java.lang.Class.forName(driverClass_tDBInput_4);
				String dbUser_tDBInput_4 = "sa";
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:Hd02BV0soRYK+Jt/iQ5gHycnuv5S0fmNLfq4nVfgs1/U6hE5xqI=");
				
				String dbPwd_tDBInput_4 = decryptedPassword_tDBInput_4;
				
		    String port_tDBInput_4 = "1433";
		    String dbname_tDBInput_4 = "DW_E-Commerce" ;
			String url_tDBInput_4 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_4)) {
		    	url_tDBInput_4 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_4)) {
				url_tDBInput_4 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_4 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_4 = "";
				
				conn_tDBInput_4 = java.sql.DriverManager.getConnection(url_tDBInput_4,dbUser_tDBInput_4,dbPwd_tDBInput_4);
		        
		    
			java.sql.Statement stmt_tDBInput_4 = conn_tDBInput_4.createStatement();

		    String dbquery_tDBInput_4 = "SELECT Dim_Mode_Paiement.Mode_Paiement_PK,\n		Dim_Mode_Paiement.Type_paiement\nFROM	Dim_Mode_Paiement";
		    

            	globalMap.put("tDBInput_4_QUERY",dbquery_tDBInput_4);
		    java.sql.ResultSet rs_tDBInput_4 = null;

		    try {
		    	rs_tDBInput_4 = stmt_tDBInput_4.executeQuery(dbquery_tDBInput_4);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_4 = rs_tDBInput_4.getMetaData();
		    	int colQtyInRs_tDBInput_4 = rsmd_tDBInput_4.getColumnCount();

		    String tmpContent_tDBInput_4 = null;
		    
		    
		    while (rs_tDBInput_4.next()) {
		        nb_line_tDBInput_4++;
		        
							if(colQtyInRs_tDBInput_4 < 1) {
								row6.Mode_Paiement_PK = 0;
							} else {
		                          
            row6.Mode_Paiement_PK = rs_tDBInput_4.getInt(1);
            if(rs_tDBInput_4.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row6.Type_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Type_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row6.Type_paiement = tmpContent_tDBInput_4;
                }
            } else {
                row6.Type_paiement = null;
            }
		                    }
					





 



/**
 * [tDBInput_4 begin ] stop
 */
	
	/**
	 * [tDBInput_4 main ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 


	tos_count_tDBInput_4++;

/**
 * [tDBInput_4 main ] stop
 */
	
	/**
	 * [tDBInput_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row6 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					


			   
			   

					row6Struct row6_HashRow = new row6Struct();
		   	   	   
				
				row6_HashRow.Mode_Paiement_PK = row6.Mode_Paiement_PK;
				
				row6_HashRow.Type_paiement = row6.Type_paiement;
				
			tHash_Lookup_row6.put(row6_HashRow);
			
            




 


	tos_count_tAdvancedHash_row6++;

/**
 * [tAdvancedHash_row6 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row6 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	

 



/**
 * [tAdvancedHash_row6 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row6 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	

 



/**
 * [tAdvancedHash_row6 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_4 end ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

	}
}finally{
	if (rs_tDBInput_4 != null) {
		rs_tDBInput_4.close();
	}
	if (stmt_tDBInput_4 != null) {
		stmt_tDBInput_4.close();
	}
		if(conn_tDBInput_4 != null && !conn_tDBInput_4.isClosed()) {
			
			conn_tDBInput_4.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_4_NB_LINE",nb_line_tDBInput_4);

 

ok_Hash.put("tDBInput_4", true);
end_Hash.put("tDBInput_4", System.currentTimeMillis());




/**
 * [tDBInput_4 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row6 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	

tHash_Lookup_row6.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row6");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row6", true);
end_Hash.put("tAdvancedHash_row6", System.currentTimeMillis());




/**
 * [tAdvancedHash_row6 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_4 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row6 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	

 



/**
 * [tAdvancedHash_row6 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_4_SUBPROCESS_STATE", 1);
	}
	


public static class row7Struct implements routines.system.IPersistableComparableLookupRow<row7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Date_PK;

				public int getDate_PK () {
					return this.Date_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Jour_Moi_Annee;

				public String getJour_Moi_Annee () {
					return this.Jour_Moi_Annee;
				}
				
			    public Integer Annee;

				public Integer getAnnee () {
					return this.Annee;
				}
				
			    public String ID_Semestre;

				public String getID_Semestre () {
					return this.ID_Semestre;
				}
				
			    public String Semestre;

				public String getSemestre () {
					return this.Semestre;
				}
				
			    public String ID_Trimestre;

				public String getID_Trimestre () {
					return this.ID_Trimestre;
				}
				
			    public String Trimestre;

				public String getTrimestre () {
					return this.Trimestre;
				}
				
			    public Integer ID_Mois;

				public Integer getID_Mois () {
					return this.ID_Mois;
				}
				
			    public Integer Mois;

				public Integer getMois () {
					return this.Mois;
				}
				
			    public String Lib_Mois;

				public String getLib_Mois () {
					return this.Lib_Mois;
				}
				
			    public Integer Jour;

				public Integer getJour () {
					return this.Jour;
				}
				
			    public Integer Id_Lib_Jour;

				public Integer getId_Lib_Jour () {
					return this.Id_Lib_Jour;
				}
				
			    public String Lib_Jour;

				public String getLib_Jour () {
					return this.Lib_Jour;
				}
				
			    public Integer Semaine;

				public Integer getSemaine () {
					return this.Semaine;
				}
				
			    public Integer JourDeAnnee;

				public Integer getJourDeAnnee () {
					return this.JourDeAnnee;
				}
				
			    public String Jour_mois_lettre;

				public String getJour_mois_lettre () {
					return this.Jour_mois_lettre;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Date == null) ? 0 : this.Date.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row7Struct other = (row7Struct) obj;
		
						if (this.Date == null) {
							if (other.Date != null)
								return false;
						
						} else if (!this.Date.equals(other.Date))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row7Struct other) {

		other.Date_PK = this.Date_PK;
	            other.Date = this.Date;
	            other.Jour_Moi_Annee = this.Jour_Moi_Annee;
	            other.Annee = this.Annee;
	            other.ID_Semestre = this.ID_Semestre;
	            other.Semestre = this.Semestre;
	            other.ID_Trimestre = this.ID_Trimestre;
	            other.Trimestre = this.Trimestre;
	            other.ID_Mois = this.ID_Mois;
	            other.Mois = this.Mois;
	            other.Lib_Mois = this.Lib_Mois;
	            other.Jour = this.Jour;
	            other.Id_Lib_Jour = this.Id_Lib_Jour;
	            other.Lib_Jour = this.Lib_Jour;
	            other.Semaine = this.Semaine;
	            other.JourDeAnnee = this.JourDeAnnee;
	            other.Jour_mois_lettre = this.Jour_mois_lettre;
	            
	}

	public void copyKeysDataTo(row7Struct other) {

		other.Date = this.Date;
	            	
	}




	private java.util.Date readDate(ObjectInputStream dis) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		java.util.Date dateReturn = null;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

    private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
    }
    
    private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}
	private Integer readInteger(DataInputStream dis, ObjectInputStream ois) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
			intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		Integer intReturn;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
			intReturn = unmarshaller.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, DataOutputStream dos,org.jboss.marshalling.Marshaller marshaller ) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Date = readDate(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Date = readDate(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// java.util.Date
				
						writeDate(this.Date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// java.util.Date
				
						writeDate(this.Date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Date_PK = dis.readInt();
					
						this.Jour_Moi_Annee = readString(dis,ois);
					
						this.Annee = readInteger(dis,ois);
					
						this.ID_Semestre = readString(dis,ois);
					
						this.Semestre = readString(dis,ois);
					
						this.ID_Trimestre = readString(dis,ois);
					
						this.Trimestre = readString(dis,ois);
					
						this.ID_Mois = readInteger(dis,ois);
					
						this.Mois = readInteger(dis,ois);
					
						this.Lib_Mois = readString(dis,ois);
					
						this.Jour = readInteger(dis,ois);
					
						this.Id_Lib_Jour = readInteger(dis,ois);
					
						this.Lib_Jour = readString(dis,ois);
					
						this.Semaine = readInteger(dis,ois);
					
						this.JourDeAnnee = readInteger(dis,ois);
					
						this.Jour_mois_lettre = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Date_PK = objectIn.readInt();
					
						this.Jour_Moi_Annee = readString(dis,objectIn);
					
						this.Annee = readInteger(dis,objectIn);
					
						this.ID_Semestre = readString(dis,objectIn);
					
						this.Semestre = readString(dis,objectIn);
					
						this.ID_Trimestre = readString(dis,objectIn);
					
						this.Trimestre = readString(dis,objectIn);
					
						this.ID_Mois = readInteger(dis,objectIn);
					
						this.Mois = readInteger(dis,objectIn);
					
						this.Lib_Mois = readString(dis,objectIn);
					
						this.Jour = readInteger(dis,objectIn);
					
						this.Id_Lib_Jour = readInteger(dis,objectIn);
					
						this.Lib_Jour = readString(dis,objectIn);
					
						this.Semaine = readInteger(dis,objectIn);
					
						this.JourDeAnnee = readInteger(dis,objectIn);
					
						this.Jour_mois_lettre = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Date_PK);
					
						writeString(this.Jour_Moi_Annee, dos, oos);
					
					writeInteger(this.Annee, dos, oos);
					
						writeString(this.ID_Semestre, dos, oos);
					
						writeString(this.Semestre, dos, oos);
					
						writeString(this.ID_Trimestre, dos, oos);
					
						writeString(this.Trimestre, dos, oos);
					
					writeInteger(this.ID_Mois, dos, oos);
					
					writeInteger(this.Mois, dos, oos);
					
						writeString(this.Lib_Mois, dos, oos);
					
					writeInteger(this.Jour, dos, oos);
					
					writeInteger(this.Id_Lib_Jour, dos, oos);
					
						writeString(this.Lib_Jour, dos, oos);
					
					writeInteger(this.Semaine, dos, oos);
					
					writeInteger(this.JourDeAnnee, dos, oos);
					
						writeString(this.Jour_mois_lettre, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Date_PK);
					
						writeString(this.Jour_Moi_Annee, dos, objectOut);
					
					writeInteger(this.Annee, dos, objectOut);
					
						writeString(this.ID_Semestre, dos, objectOut);
					
						writeString(this.Semestre, dos, objectOut);
					
						writeString(this.ID_Trimestre, dos, objectOut);
					
						writeString(this.Trimestre, dos, objectOut);
					
					writeInteger(this.ID_Mois, dos, objectOut);
					
					writeInteger(this.Mois, dos, objectOut);
					
						writeString(this.Lib_Mois, dos, objectOut);
					
					writeInteger(this.Jour, dos, objectOut);
					
					writeInteger(this.Id_Lib_Jour, dos, objectOut);
					
						writeString(this.Lib_Jour, dos, objectOut);
					
					writeInteger(this.Semaine, dos, objectOut);
					
					writeInteger(this.JourDeAnnee, dos, objectOut);
					
						writeString(this.Jour_mois_lettre, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Date_PK="+String.valueOf(Date_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Jour_Moi_Annee="+Jour_Moi_Annee);
		sb.append(",Annee="+String.valueOf(Annee));
		sb.append(",ID_Semestre="+ID_Semestre);
		sb.append(",Semestre="+Semestre);
		sb.append(",ID_Trimestre="+ID_Trimestre);
		sb.append(",Trimestre="+Trimestre);
		sb.append(",ID_Mois="+String.valueOf(ID_Mois));
		sb.append(",Mois="+String.valueOf(Mois));
		sb.append(",Lib_Mois="+Lib_Mois);
		sb.append(",Jour="+String.valueOf(Jour));
		sb.append(",Id_Lib_Jour="+String.valueOf(Id_Lib_Jour));
		sb.append(",Lib_Jour="+Lib_Jour);
		sb.append(",Semaine="+String.valueOf(Semaine));
		sb.append(",JourDeAnnee="+String.valueOf(JourDeAnnee));
		sb.append(",Jour_mois_lettre="+Jour_mois_lettre);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row7Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Date, other.Date);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_7Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_7_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row7Struct row7 = new row7Struct();




	
	/**
	 * [tAdvancedHash_row7 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row7", false);
		start_Hash.put("tAdvancedHash_row7", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row7";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row7");
					}
				
		int tos_count_tAdvancedHash_row7 = 0;
		

			   		// connection name:row7
			   		// source node:tDBInput_7 - inputs:(after_tDBInput_2) outputs:(row7,row7) | target node:tAdvancedHash_row7 - inputs:(row7) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row7 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct> tHash_Lookup_row7 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row7Struct>getLookup(matchingModeEnum_row7);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row7", tHash_Lookup_row7);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row7 begin ] stop
 */



	
	/**
	 * [tDBInput_7 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_7", false);
		start_Hash.put("tDBInput_7", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_7";

	
		int tos_count_tDBInput_7 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_7 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_7 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_7  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_7, talendToDBArray_tDBInput_7); 
		    int nb_line_tDBInput_7 = 0;
		    java.sql.Connection conn_tDBInput_7 = null;
				String driverClass_tDBInput_7 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_7 = java.lang.Class.forName(driverClass_tDBInput_7);
				String dbUser_tDBInput_7 = "sa";
				
				 
	final String decryptedPassword_tDBInput_7 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:iYQD4/nM2IdqTzDI4AZ3RKZjvVjQY0HTgnOllsq3QOPoWKil5fs=");
				
				String dbPwd_tDBInput_7 = decryptedPassword_tDBInput_7;
				
		    String port_tDBInput_7 = "1433";
		    String dbname_tDBInput_7 = "DW_E-Commerce" ;
			String url_tDBInput_7 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_7)) {
		    	url_tDBInput_7 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_7)) {
				url_tDBInput_7 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_7 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_7 = "";
				
				conn_tDBInput_7 = java.sql.DriverManager.getConnection(url_tDBInput_7,dbUser_tDBInput_7,dbPwd_tDBInput_7);
		        
		    
			java.sql.Statement stmt_tDBInput_7 = conn_tDBInput_7.createStatement();

		    String dbquery_tDBInput_7 = "SELECT DimDates.Date_PK,\n		DimDates.Date,\n		DimDates.Jour_Moi_Annee,\n		DimDates.Annee,\n		DimDates.ID_Semestre,\n		DimDat"
+"es.Semestre,\n		DimDates.ID_Trimestre,\n		DimDates.Trimestre,\n		DimDates.ID_Mois,\n		DimDates.Mois,\n		DimDates.Lib_Mois,\n		"
+"DimDates.Jour,\n		DimDates.Id_Lib_Jour,\n		DimDates.Lib_Jour,\n		DimDates.Semaine,\n		DimDates.JourDeAnnee,\n		DimDates.Jour_"
+"mois_lettre\nFROM	DimDates";
		    

            	globalMap.put("tDBInput_7_QUERY",dbquery_tDBInput_7);
		    java.sql.ResultSet rs_tDBInput_7 = null;

		    try {
		    	rs_tDBInput_7 = stmt_tDBInput_7.executeQuery(dbquery_tDBInput_7);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_7 = rs_tDBInput_7.getMetaData();
		    	int colQtyInRs_tDBInput_7 = rsmd_tDBInput_7.getColumnCount();

		    String tmpContent_tDBInput_7 = null;
		    
		    
		    while (rs_tDBInput_7.next()) {
		        nb_line_tDBInput_7++;
		        
							if(colQtyInRs_tDBInput_7 < 1) {
								row7.Date_PK = 0;
							} else {
		                          
            row7.Date_PK = rs_tDBInput_7.getInt(1);
            if(rs_tDBInput_7.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 2) {
								row7.Date = null;
							} else {
										
			row7.Date = mssqlGTU_tDBInput_7.getDate(rsmd_tDBInput_7, rs_tDBInput_7, 2);
			
		                    }
							if(colQtyInRs_tDBInput_7 < 3) {
								row7.Jour_Moi_Annee = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(3);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Jour_Moi_Annee = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Jour_Moi_Annee = tmpContent_tDBInput_7;
                }
            } else {
                row7.Jour_Moi_Annee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 4) {
								row7.Annee = null;
							} else {
		                          
            row7.Annee = rs_tDBInput_7.getInt(4);
            if(rs_tDBInput_7.wasNull()){
                    row7.Annee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 5) {
								row7.ID_Semestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(5);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.ID_Semestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.ID_Semestre = tmpContent_tDBInput_7;
                }
            } else {
                row7.ID_Semestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 6) {
								row7.Semestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(6);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Semestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Semestre = tmpContent_tDBInput_7;
                }
            } else {
                row7.Semestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 7) {
								row7.ID_Trimestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(7);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.ID_Trimestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.ID_Trimestre = tmpContent_tDBInput_7;
                }
            } else {
                row7.ID_Trimestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 8) {
								row7.Trimestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(8);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Trimestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Trimestre = tmpContent_tDBInput_7;
                }
            } else {
                row7.Trimestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 9) {
								row7.ID_Mois = null;
							} else {
		                          
            row7.ID_Mois = rs_tDBInput_7.getInt(9);
            if(rs_tDBInput_7.wasNull()){
                    row7.ID_Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 10) {
								row7.Mois = null;
							} else {
		                          
            row7.Mois = rs_tDBInput_7.getInt(10);
            if(rs_tDBInput_7.wasNull()){
                    row7.Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 11) {
								row7.Lib_Mois = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(11);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Lib_Mois = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Lib_Mois = tmpContent_tDBInput_7;
                }
            } else {
                row7.Lib_Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 12) {
								row7.Jour = null;
							} else {
		                          
            row7.Jour = rs_tDBInput_7.getInt(12);
            if(rs_tDBInput_7.wasNull()){
                    row7.Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 13) {
								row7.Id_Lib_Jour = null;
							} else {
		                          
            row7.Id_Lib_Jour = rs_tDBInput_7.getInt(13);
            if(rs_tDBInput_7.wasNull()){
                    row7.Id_Lib_Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 14) {
								row7.Lib_Jour = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(14);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Lib_Jour = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Lib_Jour = tmpContent_tDBInput_7;
                }
            } else {
                row7.Lib_Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 15) {
								row7.Semaine = null;
							} else {
		                          
            row7.Semaine = rs_tDBInput_7.getInt(15);
            if(rs_tDBInput_7.wasNull()){
                    row7.Semaine = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 16) {
								row7.JourDeAnnee = null;
							} else {
		                          
            row7.JourDeAnnee = rs_tDBInput_7.getInt(16);
            if(rs_tDBInput_7.wasNull()){
                    row7.JourDeAnnee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 17) {
								row7.Jour_mois_lettre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(17);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Jour_mois_lettre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row7.Jour_mois_lettre = tmpContent_tDBInput_7;
                }
            } else {
                row7.Jour_mois_lettre = null;
            }
		                    }
					





 



/**
 * [tDBInput_7 begin ] stop
 */
	
	/**
	 * [tDBInput_7 main ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 


	tos_count_tDBInput_7++;

/**
 * [tDBInput_7 main ] stop
 */
	
	/**
	 * [tDBInput_7 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row7 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					


			   
			   

					row7Struct row7_HashRow = new row7Struct();
		   	   	   
				
				row7_HashRow.Date_PK = row7.Date_PK;
				
				row7_HashRow.Date = row7.Date;
				
				row7_HashRow.Jour_Moi_Annee = row7.Jour_Moi_Annee;
				
				row7_HashRow.Annee = row7.Annee;
				
				row7_HashRow.ID_Semestre = row7.ID_Semestre;
				
				row7_HashRow.Semestre = row7.Semestre;
				
				row7_HashRow.ID_Trimestre = row7.ID_Trimestre;
				
				row7_HashRow.Trimestre = row7.Trimestre;
				
				row7_HashRow.ID_Mois = row7.ID_Mois;
				
				row7_HashRow.Mois = row7.Mois;
				
				row7_HashRow.Lib_Mois = row7.Lib_Mois;
				
				row7_HashRow.Jour = row7.Jour;
				
				row7_HashRow.Id_Lib_Jour = row7.Id_Lib_Jour;
				
				row7_HashRow.Lib_Jour = row7.Lib_Jour;
				
				row7_HashRow.Semaine = row7.Semaine;
				
				row7_HashRow.JourDeAnnee = row7.JourDeAnnee;
				
				row7_HashRow.Jour_mois_lettre = row7.Jour_mois_lettre;
				
			tHash_Lookup_row7.put(row7_HashRow);
			
            




 


	tos_count_tAdvancedHash_row7++;

/**
 * [tAdvancedHash_row7 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row7 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	

 



/**
 * [tAdvancedHash_row7 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row7 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	

 



/**
 * [tAdvancedHash_row7 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_7 end ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

	}
}finally{
	if (rs_tDBInput_7 != null) {
		rs_tDBInput_7.close();
	}
	if (stmt_tDBInput_7 != null) {
		stmt_tDBInput_7.close();
	}
		if(conn_tDBInput_7 != null && !conn_tDBInput_7.isClosed()) {
			
			conn_tDBInput_7.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_7_NB_LINE",nb_line_tDBInput_7);

 

ok_Hash.put("tDBInput_7", true);
end_Hash.put("tDBInput_7", System.currentTimeMillis());




/**
 * [tDBInput_7 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row7 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	

tHash_Lookup_row7.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row7");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row7", true);
end_Hash.put("tAdvancedHash_row7", System.currentTimeMillis());




/**
 * [tAdvancedHash_row7 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_7 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row7 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	

 



/**
 * [tAdvancedHash_row7 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_7_SUBPROCESS_STATE", 1);
	}
	


public static class row9Struct implements routines.system.IPersistableComparableLookupRow<row9Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public String Input_Geo;

				public String getInput_Geo () {
					return this.Input_Geo;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String Pays;

				public String getPays () {
					return this.Pays;
				}
				
			    public String Code_Postale;

				public String getCode_Postale () {
					return this.Code_Postale;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Input_Geo == null) ? 0 : this.Input_Geo.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row9Struct other = (row9Struct) obj;
		
						if (this.Input_Geo == null) {
							if (other.Input_Geo != null)
								return false;
						
						} else if (!this.Input_Geo.equals(other.Input_Geo))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row9Struct other) {

		other.Input_Geo = this.Input_Geo;
	            other.Ville = this.Ville;
	            other.Pays = this.Pays;
	            other.Code_Postale = this.Code_Postale;
	            
	}

	public void copyKeysDataTo(row9Struct other) {

		other.Input_Geo = this.Input_Geo;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Input_Geo = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Input_Geo = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Input_Geo,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Input_Geo,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
						this.Ville = readString(dis,ois);
					
						this.Pays = readString(dis,ois);
					
						this.Code_Postale = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
						this.Ville = readString(dis,objectIn);
					
						this.Pays = readString(dis,objectIn);
					
						this.Code_Postale = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
						writeString(this.Ville, dos, oos);
					
						writeString(this.Pays, dos, oos);
					
						writeString(this.Code_Postale, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
						writeString(this.Ville, dos, objectOut);
					
						writeString(this.Pays, dos, objectOut);
					
						writeString(this.Code_Postale, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Input_Geo="+Input_Geo);
		sb.append(",Ville="+Ville);
		sb.append(",Pays="+Pays);
		sb.append(",Code_Postale="+Code_Postale);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row9Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Input_Geo, other.Input_Geo);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_9Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_9_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row9Struct row9 = new row9Struct();




	
	/**
	 * [tAdvancedHash_row9 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row9", false);
		start_Hash.put("tAdvancedHash_row9", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row9";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row9");
					}
				
		int tos_count_tAdvancedHash_row9 = 0;
		

			   		// connection name:row9
			   		// source node:tDBInput_9 - inputs:(after_tDBInput_2) outputs:(row9,row9) | target node:tAdvancedHash_row9 - inputs:(row9) outputs:()
			   		// linked node: tMap_5 - inputs:(row3,row9,row12) outputs:(out_state)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row9 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct> tHash_Lookup_row9 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row9Struct>getLookup(matchingModeEnum_row9);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row9", tHash_Lookup_row9);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row9 begin ] stop
 */



	
	/**
	 * [tDBInput_9 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_9", false);
		start_Hash.put("tDBInput_9", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_9";

	
		int tos_count_tDBInput_9 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_9 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_9 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_9  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_9, talendToDBArray_tDBInput_9); 
		    int nb_line_tDBInput_9 = 0;
		    java.sql.Connection conn_tDBInput_9 = null;
				String driverClass_tDBInput_9 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_9 = java.lang.Class.forName(driverClass_tDBInput_9);
				String dbUser_tDBInput_9 = "sa";
				
				 
	final String decryptedPassword_tDBInput_9 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:7zLOXC19gD+E1N2fT5DpLnJHtyeN2T/qdAQLC3DgWg43LSTr9m8=");
				
				String dbPwd_tDBInput_9 = decryptedPassword_tDBInput_9;
				
		    String port_tDBInput_9 = "1433";
		    String dbname_tDBInput_9 = "DW_E-Commerce" ;
			String url_tDBInput_9 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_9)) {
		    	url_tDBInput_9 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_9)) {
				url_tDBInput_9 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_9 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_9 = "";
				
				conn_tDBInput_9 = java.sql.DriverManager.getConnection(url_tDBInput_9,dbUser_tDBInput_9,dbPwd_tDBInput_9);
		        
		    
			java.sql.Statement stmt_tDBInput_9 = conn_tDBInput_9.createStatement();

		    String dbquery_tDBInput_9 = "SELECT Dictionnaire_Geographie.Input_Geo,\n		Dictionnaire_Geographie.Ville,\n		Dictionnaire_Geographie.Pays,\n		Dictionnai"
+"re_Geographie.Code_Postale\nFROM	Dictionnaire_Geographie";
		    

            	globalMap.put("tDBInput_9_QUERY",dbquery_tDBInput_9);
		    java.sql.ResultSet rs_tDBInput_9 = null;

		    try {
		    	rs_tDBInput_9 = stmt_tDBInput_9.executeQuery(dbquery_tDBInput_9);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_9 = rs_tDBInput_9.getMetaData();
		    	int colQtyInRs_tDBInput_9 = rsmd_tDBInput_9.getColumnCount();

		    String tmpContent_tDBInput_9 = null;
		    
		    
		    while (rs_tDBInput_9.next()) {
		        nb_line_tDBInput_9++;
		        
							if(colQtyInRs_tDBInput_9 < 1) {
								row9.Input_Geo = null;
							} else {
	                         		
           		tmpContent_tDBInput_9 = rs_tDBInput_9.getString(1);
            if(tmpContent_tDBInput_9 != null) {
            	if (talendToDBList_tDBInput_9 .contains(rsmd_tDBInput_9.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Input_Geo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_9);
            	} else {
                	row9.Input_Geo = tmpContent_tDBInput_9;
                }
            } else {
                row9.Input_Geo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_9 < 2) {
								row9.Ville = null;
							} else {
	                         		
           		tmpContent_tDBInput_9 = rs_tDBInput_9.getString(2);
            if(tmpContent_tDBInput_9 != null) {
            	if (talendToDBList_tDBInput_9 .contains(rsmd_tDBInput_9.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Ville = FormatterUtils.formatUnwithE(tmpContent_tDBInput_9);
            	} else {
                	row9.Ville = tmpContent_tDBInput_9;
                }
            } else {
                row9.Ville = null;
            }
		                    }
							if(colQtyInRs_tDBInput_9 < 3) {
								row9.Pays = null;
							} else {
	                         		
           		tmpContent_tDBInput_9 = rs_tDBInput_9.getString(3);
            if(tmpContent_tDBInput_9 != null) {
            	if (talendToDBList_tDBInput_9 .contains(rsmd_tDBInput_9.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Pays = FormatterUtils.formatUnwithE(tmpContent_tDBInput_9);
            	} else {
                	row9.Pays = tmpContent_tDBInput_9;
                }
            } else {
                row9.Pays = null;
            }
		                    }
							if(colQtyInRs_tDBInput_9 < 4) {
								row9.Code_Postale = null;
							} else {
	                         		
           		tmpContent_tDBInput_9 = rs_tDBInput_9.getString(4);
            if(tmpContent_tDBInput_9 != null) {
            	if (talendToDBList_tDBInput_9 .contains(rsmd_tDBInput_9.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Code_Postale = FormatterUtils.formatUnwithE(tmpContent_tDBInput_9);
            	} else {
                	row9.Code_Postale = tmpContent_tDBInput_9;
                }
            } else {
                row9.Code_Postale = null;
            }
		                    }
					





 



/**
 * [tDBInput_9 begin ] stop
 */
	
	/**
	 * [tDBInput_9 main ] start
	 */

	

	
	
	currentComponent="tDBInput_9";

	

 


	tos_count_tDBInput_9++;

/**
 * [tDBInput_9 main ] stop
 */
	
	/**
	 * [tDBInput_9 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_9";

	

 



/**
 * [tDBInput_9 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row9 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					


			   
			   

					row9Struct row9_HashRow = new row9Struct();
		   	   	   
				
				row9_HashRow.Input_Geo = row9.Input_Geo;
				
				row9_HashRow.Ville = row9.Ville;
				
				row9_HashRow.Pays = row9.Pays;
				
				row9_HashRow.Code_Postale = row9.Code_Postale;
				
			tHash_Lookup_row9.put(row9_HashRow);
			
            




 


	tos_count_tAdvancedHash_row9++;

/**
 * [tAdvancedHash_row9 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row9 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	

 



/**
 * [tAdvancedHash_row9 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row9 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	

 



/**
 * [tAdvancedHash_row9 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_9 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_9";

	

 



/**
 * [tDBInput_9 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_9 end ] start
	 */

	

	
	
	currentComponent="tDBInput_9";

	

	}
}finally{
	if (rs_tDBInput_9 != null) {
		rs_tDBInput_9.close();
	}
	if (stmt_tDBInput_9 != null) {
		stmt_tDBInput_9.close();
	}
		if(conn_tDBInput_9 != null && !conn_tDBInput_9.isClosed()) {
			
			conn_tDBInput_9.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_9_NB_LINE",nb_line_tDBInput_9);

 

ok_Hash.put("tDBInput_9", true);
end_Hash.put("tDBInput_9", System.currentTimeMillis());




/**
 * [tDBInput_9 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row9 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	

tHash_Lookup_row9.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row9");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row9", true);
end_Hash.put("tAdvancedHash_row9", System.currentTimeMillis());




/**
 * [tAdvancedHash_row9 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_9 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_9";

	

 



/**
 * [tDBInput_9 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row9 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	

 



/**
 * [tAdvancedHash_row9 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_9_SUBPROCESS_STATE", 1);
	}
	


public static class row11Struct implements routines.system.IPersistableComparableLookupRow<row11Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Geographie_PK;

				public int getGeographie_PK () {
					return this.Geographie_PK;
				}
				
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				
			    public String Code_postal;

				public String getCode_postal () {
					return this.Code_postal;
				}
				
			    public String Pays;

				public String getPays () {
					return this.Pays;
				}
				
			    public String Code_pays;

				public String getCode_pays () {
					return this.Code_pays;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Ville == null) ? 0 : this.Ville.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row11Struct other = (row11Struct) obj;
		
						if (this.Ville == null) {
							if (other.Ville != null)
								return false;
						
						} else if (!this.Ville.equals(other.Ville))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row11Struct other) {

		other.Geographie_PK = this.Geographie_PK;
	            other.Ville = this.Ville;
	            other.Code_postal = this.Code_postal;
	            other.Pays = this.Pays;
	            other.Code_pays = this.Code_pays;
	            
	}

	public void copyKeysDataTo(row11Struct other) {

		other.Ville = this.Ville;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Geographie_PK = dis.readInt();
					
						this.Code_postal = readString(dis,ois);
					
						this.Pays = readString(dis,ois);
					
						this.Code_pays = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Geographie_PK = objectIn.readInt();
					
						this.Code_postal = readString(dis,objectIn);
					
						this.Pays = readString(dis,objectIn);
					
						this.Code_pays = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Geographie_PK);
					
						writeString(this.Code_postal, dos, oos);
					
						writeString(this.Pays, dos, oos);
					
						writeString(this.Code_pays, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Geographie_PK);
					
						writeString(this.Code_postal, dos, objectOut);
					
						writeString(this.Pays, dos, objectOut);
					
						writeString(this.Code_pays, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Geographie_PK="+String.valueOf(Geographie_PK));
		sb.append(",Ville="+Ville);
		sb.append(",Code_postal="+Code_postal);
		sb.append(",Pays="+Pays);
		sb.append(",Code_pays="+Code_pays);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row11Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Ville, other.Ville);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_10Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_10_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row11Struct row11 = new row11Struct();




	
	/**
	 * [tAdvancedHash_row11 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row11", false);
		start_Hash.put("tAdvancedHash_row11", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row11";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row11");
					}
				
		int tos_count_tAdvancedHash_row11 = 0;
		

			   		// connection name:row11
			   		// source node:tDBInput_10 - inputs:(after_tDBInput_2) outputs:(row11,row11) | target node:tAdvancedHash_row11 - inputs:(row11) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row11 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row11Struct> tHash_Lookup_row11 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row11Struct>getLookup(matchingModeEnum_row11);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row11", tHash_Lookup_row11);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row11 begin ] stop
 */



	
	/**
	 * [tDBInput_10 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_10", false);
		start_Hash.put("tDBInput_10", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_10";

	
		int tos_count_tDBInput_10 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_10 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_10 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_10  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_10, talendToDBArray_tDBInput_10); 
		    int nb_line_tDBInput_10 = 0;
		    java.sql.Connection conn_tDBInput_10 = null;
				String driverClass_tDBInput_10 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_10 = java.lang.Class.forName(driverClass_tDBInput_10);
				String dbUser_tDBInput_10 = "sa";
				
				 
	final String decryptedPassword_tDBInput_10 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:uBpWV71uXpmaY7brUUGS2F9q+W4gTPU1SCh3sDv5ntWsgJw3/q8=");
				
				String dbPwd_tDBInput_10 = decryptedPassword_tDBInput_10;
				
		    String port_tDBInput_10 = "1433";
		    String dbname_tDBInput_10 = "DW_E-Commerce" ;
			String url_tDBInput_10 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_10)) {
		    	url_tDBInput_10 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_10)) {
				url_tDBInput_10 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_10 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_10 = "";
				
				conn_tDBInput_10 = java.sql.DriverManager.getConnection(url_tDBInput_10,dbUser_tDBInput_10,dbPwd_tDBInput_10);
		        
		    
			java.sql.Statement stmt_tDBInput_10 = conn_tDBInput_10.createStatement();

		    String dbquery_tDBInput_10 = "SELECT Dim_Geographie.Geographie_PK,\n		Dim_Geographie.Ville,\n		Dim_Geographie.Code_postal,\n		Dim_Geographie.Pays,\n		Dim"
+"_Geographie.Code_pays\nFROM	Dim_Geographie";
		    

            	globalMap.put("tDBInput_10_QUERY",dbquery_tDBInput_10);
		    java.sql.ResultSet rs_tDBInput_10 = null;

		    try {
		    	rs_tDBInput_10 = stmt_tDBInput_10.executeQuery(dbquery_tDBInput_10);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_10 = rs_tDBInput_10.getMetaData();
		    	int colQtyInRs_tDBInput_10 = rsmd_tDBInput_10.getColumnCount();

		    String tmpContent_tDBInput_10 = null;
		    
		    
		    while (rs_tDBInput_10.next()) {
		        nb_line_tDBInput_10++;
		        
							if(colQtyInRs_tDBInput_10 < 1) {
								row11.Geographie_PK = 0;
							} else {
		                          
            row11.Geographie_PK = rs_tDBInput_10.getInt(1);
            if(rs_tDBInput_10.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_10 < 2) {
								row11.Ville = null;
							} else {
	                         		
           		tmpContent_tDBInput_10 = rs_tDBInput_10.getString(2);
            if(tmpContent_tDBInput_10 != null) {
            	if (talendToDBList_tDBInput_10 .contains(rsmd_tDBInput_10.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Ville = FormatterUtils.formatUnwithE(tmpContent_tDBInput_10);
            	} else {
                	row11.Ville = tmpContent_tDBInput_10;
                }
            } else {
                row11.Ville = null;
            }
		                    }
							if(colQtyInRs_tDBInput_10 < 3) {
								row11.Code_postal = null;
							} else {
	                         		
           		tmpContent_tDBInput_10 = rs_tDBInput_10.getString(3);
            if(tmpContent_tDBInput_10 != null) {
            	if (talendToDBList_tDBInput_10 .contains(rsmd_tDBInput_10.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Code_postal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_10);
            	} else {
                	row11.Code_postal = tmpContent_tDBInput_10;
                }
            } else {
                row11.Code_postal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_10 < 4) {
								row11.Pays = null;
							} else {
	                         		
           		tmpContent_tDBInput_10 = rs_tDBInput_10.getString(4);
            if(tmpContent_tDBInput_10 != null) {
            	if (talendToDBList_tDBInput_10 .contains(rsmd_tDBInput_10.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Pays = FormatterUtils.formatUnwithE(tmpContent_tDBInput_10);
            	} else {
                	row11.Pays = tmpContent_tDBInput_10;
                }
            } else {
                row11.Pays = null;
            }
		                    }
							if(colQtyInRs_tDBInput_10 < 5) {
								row11.Code_pays = null;
							} else {
	                         		
           		tmpContent_tDBInput_10 = rs_tDBInput_10.getString(5);
            if(tmpContent_tDBInput_10 != null) {
            	if (talendToDBList_tDBInput_10 .contains(rsmd_tDBInput_10.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Code_pays = FormatterUtils.formatUnwithE(tmpContent_tDBInput_10);
            	} else {
                	row11.Code_pays = tmpContent_tDBInput_10;
                }
            } else {
                row11.Code_pays = null;
            }
		                    }
					





 



/**
 * [tDBInput_10 begin ] stop
 */
	
	/**
	 * [tDBInput_10 main ] start
	 */

	

	
	
	currentComponent="tDBInput_10";

	

 


	tos_count_tDBInput_10++;

/**
 * [tDBInput_10 main ] stop
 */
	
	/**
	 * [tDBInput_10 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_10";

	

 



/**
 * [tDBInput_10 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row11 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row11";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					


			   
			   

					row11Struct row11_HashRow = new row11Struct();
		   	   	   
				
				row11_HashRow.Geographie_PK = row11.Geographie_PK;
				
				row11_HashRow.Ville = row11.Ville;
				
				row11_HashRow.Code_postal = row11.Code_postal;
				
				row11_HashRow.Pays = row11.Pays;
				
				row11_HashRow.Code_pays = row11.Code_pays;
				
			tHash_Lookup_row11.put(row11_HashRow);
			
            




 


	tos_count_tAdvancedHash_row11++;

/**
 * [tAdvancedHash_row11 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row11 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row11";

	

 



/**
 * [tAdvancedHash_row11 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row11 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row11";

	

 



/**
 * [tAdvancedHash_row11 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_10 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_10";

	

 



/**
 * [tDBInput_10 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_10 end ] start
	 */

	

	
	
	currentComponent="tDBInput_10";

	

	}
}finally{
	if (rs_tDBInput_10 != null) {
		rs_tDBInput_10.close();
	}
	if (stmt_tDBInput_10 != null) {
		stmt_tDBInput_10.close();
	}
		if(conn_tDBInput_10 != null && !conn_tDBInput_10.isClosed()) {
			
			conn_tDBInput_10.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_10_NB_LINE",nb_line_tDBInput_10);

 

ok_Hash.put("tDBInput_10", true);
end_Hash.put("tDBInput_10", System.currentTimeMillis());




/**
 * [tDBInput_10 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row11 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row11";

	

tHash_Lookup_row11.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row11");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row11", true);
end_Hash.put("tAdvancedHash_row11", System.currentTimeMillis());




/**
 * [tAdvancedHash_row11 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_10 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_10";

	

 



/**
 * [tDBInput_10 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row11 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row11";

	

 



/**
 * [tAdvancedHash_row11 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_10_SUBPROCESS_STATE", 1);
	}
	


public static class row8Struct implements routines.system.IPersistableComparableLookupRow<row8Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Produit_PK;

				public int getProduit_PK () {
					return this.Produit_PK;
				}
				
			    public String UGS;

				public String getUGS () {
					return this.UGS;
				}
				
			    public String Nom_produit;

				public String getNom_produit () {
					return this.Nom_produit;
				}
				
			    public String Description;

				public String getDescription () {
					return this.Description;
				}
				
			    public String Url;

				public String getUrl () {
					return this.Url;
				}
				
			    public Boolean Visibilite;

				public Boolean getVisibilite () {
					return this.Visibilite;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Prix_en_promo;

				public Double getPrix_en_promo () {
					return this.Prix_en_promo;
				}
				
			    public Boolean En_Promo;

				public Boolean getEn_Promo () {
					return this.En_Promo;
				}
				
			    public String Canal;

				public String getCanal () {
					return this.Canal;
				}
				
			    public String Source;

				public String getSource () {
					return this.Source;
				}
				
			    public java.util.Date date_debut;

				public java.util.Date getDate_debut () {
					return this.date_debut;
				}
				
			    public java.util.Date date_fin;

				public java.util.Date getDate_fin () {
					return this.date_fin;
				}
				
			    public Integer actif;

				public Integer getActif () {
					return this.actif;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Nom_produit == null) ? 0 : this.Nom_produit.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row8Struct other = (row8Struct) obj;
		
						if (this.Nom_produit == null) {
							if (other.Nom_produit != null)
								return false;
						
						} else if (!this.Nom_produit.equals(other.Nom_produit))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row8Struct other) {

		other.Produit_PK = this.Produit_PK;
	            other.UGS = this.UGS;
	            other.Nom_produit = this.Nom_produit;
	            other.Description = this.Description;
	            other.Url = this.Url;
	            other.Visibilite = this.Visibilite;
	            other.Prix = this.Prix;
	            other.Prix_en_promo = this.Prix_en_promo;
	            other.En_Promo = this.En_Promo;
	            other.Canal = this.Canal;
	            other.Source = this.Source;
	            other.date_debut = this.date_debut;
	            other.date_fin = this.date_fin;
	            other.actif = this.actif;
	            
	}

	public void copyKeysDataTo(row8Struct other) {

		other.Nom_produit = this.Nom_produit;
	            	
	}



	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

	private java.util.Date readDate(DataInputStream dis, ObjectInputStream ois) throws IOException{
		java.util.Date dateReturn = null;
		int length = 0;
        length = dis.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(dis.readLong());
		}
		return dateReturn;
	}
	
	private java.util.Date readDate(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller ) throws IOException{
		java.util.Date dateReturn = null;
		int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			dateReturn = null;
		} else {
	    	dateReturn = new Date(unmarshaller.readLong());
		}
		return dateReturn;
	}

	private void writeDate(java.util.Date date1, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(date1 == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeLong(date1.getTime());
    	}
	}
	
	private void writeDate(java.util.Date date1, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(date1 == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeLong(date1.getTime());
    	}
	}
	private Integer readInteger(DataInputStream dis, ObjectInputStream ois) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
			intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		Integer intReturn;
        int length = 0;
        length = unmarshaller.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
			intReturn = unmarshaller.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, DataOutputStream dos,org.jboss.marshalling.Marshaller marshaller ) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Nom_produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Nom_produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Nom_produit,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Nom_produit,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Produit_PK = dis.readInt();
					
						this.UGS = readString(dis,ois);
					
						this.Description = readString(dis,ois);
					
						this.Url = readString(dis,ois);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Visibilite = null;
           				} else {
           			    	this.Visibilite = dis.readBoolean();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix_en_promo = null;
           				} else {
           			    	this.Prix_en_promo = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.En_Promo = null;
           				} else {
           			    	this.En_Promo = dis.readBoolean();
           				}
					
						this.Canal = readString(dis,ois);
					
						this.Source = readString(dis,ois);
					
						this.date_debut = readDate(dis,ois);
					
						this.date_fin = readDate(dis,ois);
					
						this.actif = readInteger(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Produit_PK = objectIn.readInt();
					
						this.UGS = readString(dis,objectIn);
					
						this.Description = readString(dis,objectIn);
					
						this.Url = readString(dis,objectIn);
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Visibilite = null;
           				} else {
           			    	this.Visibilite = objectIn.readBoolean();
           				}
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = objectIn.readDouble();
           				}
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Prix_en_promo = null;
           				} else {
           			    	this.Prix_en_promo = objectIn.readDouble();
           				}
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.En_Promo = null;
           				} else {
           			    	this.En_Promo = objectIn.readBoolean();
           				}
					
						this.Canal = readString(dis,objectIn);
					
						this.Source = readString(dis,objectIn);
					
						this.date_debut = readDate(dis,objectIn);
					
						this.date_fin = readDate(dis,objectIn);
					
						this.actif = readInteger(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Produit_PK);
					
						writeString(this.UGS, dos, oos);
					
						writeString(this.Description, dos, oos);
					
						writeString(this.Url, dos, oos);
					
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
						writeString(this.Canal, dos, oos);
					
						writeString(this.Source, dos, oos);
					
						writeDate(this.date_debut, dos, oos);
					
						writeDate(this.date_fin, dos, oos);
					
					writeInteger(this.actif, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Produit_PK);
					
						writeString(this.UGS, dos, objectOut);
					
						writeString(this.Description, dos, objectOut);
					
						writeString(this.Url, dos, objectOut);
					
						if(this.Visibilite == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeBoolean(this.Visibilite);
		            	}
					
						if(this.Prix == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeDouble(this.Prix);
		            	}
					
						if(this.Prix_en_promo == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeDouble(this.Prix_en_promo);
		            	}
					
						if(this.En_Promo == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeBoolean(this.En_Promo);
		            	}
					
						writeString(this.Canal, dos, objectOut);
					
						writeString(this.Source, dos, objectOut);
					
						writeDate(this.date_debut, dos, objectOut);
					
						writeDate(this.date_fin, dos, objectOut);
					
					writeInteger(this.actif, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit_PK="+String.valueOf(Produit_PK));
		sb.append(",UGS="+UGS);
		sb.append(",Nom_produit="+Nom_produit);
		sb.append(",Description="+Description);
		sb.append(",Url="+Url);
		sb.append(",Visibilite="+String.valueOf(Visibilite));
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Prix_en_promo="+String.valueOf(Prix_en_promo));
		sb.append(",En_Promo="+String.valueOf(En_Promo));
		sb.append(",Canal="+Canal);
		sb.append(",Source="+Source);
		sb.append(",date_debut="+String.valueOf(date_debut));
		sb.append(",date_fin="+String.valueOf(date_fin));
		sb.append(",actif="+String.valueOf(actif));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row8Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Nom_produit, other.Nom_produit);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_5Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_5_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row8Struct row8 = new row8Struct();




	
	/**
	 * [tAdvancedHash_row8 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row8", false);
		start_Hash.put("tAdvancedHash_row8", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row8";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row8");
					}
				
		int tos_count_tAdvancedHash_row8 = 0;
		

			   		// connection name:row8
			   		// source node:tDBInput_5 - inputs:(after_tDBInput_2) outputs:(row8,row8) | target node:tAdvancedHash_row8 - inputs:(row8) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row8 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row8Struct> tHash_Lookup_row8 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row8Struct>getLookup(matchingModeEnum_row8);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row8", tHash_Lookup_row8);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row8 begin ] stop
 */



	
	/**
	 * [tDBInput_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_5", false);
		start_Hash.put("tDBInput_5", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_5";

	
		int tos_count_tDBInput_5 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_5 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_5 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_5  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_5, talendToDBArray_tDBInput_5); 
		    int nb_line_tDBInput_5 = 0;
		    java.sql.Connection conn_tDBInput_5 = null;
				String driverClass_tDBInput_5 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_5 = java.lang.Class.forName(driverClass_tDBInput_5);
				String dbUser_tDBInput_5 = "sa";
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:5ndBJ1yeKTgVApEGse7VAzZAuFKE76UuBCgXKggsEKqvTveh3xM=");
				
				String dbPwd_tDBInput_5 = decryptedPassword_tDBInput_5;
				
		    String port_tDBInput_5 = "1433";
		    String dbname_tDBInput_5 = "DW_E-Commerce" ;
			String url_tDBInput_5 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_5)) {
		    	url_tDBInput_5 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_5)) {
				url_tDBInput_5 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_5 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_5 = "";
				
				conn_tDBInput_5 = java.sql.DriverManager.getConnection(url_tDBInput_5,dbUser_tDBInput_5,dbPwd_tDBInput_5);
		        
		    
			java.sql.Statement stmt_tDBInput_5 = conn_tDBInput_5.createStatement();

		    String dbquery_tDBInput_5 = "SELECT Dim_Produit.Produit_PK,\n		Dim_Produit.UGS,\n		Dim_Produit.Nom_produit,\n		Dim_Produit.Description,\n		Dim_Produit.U"
+"rl,\n		Dim_Produit.Visibilite,\n		Dim_Produit.Prix,\n		Dim_Produit.Prix_en_promo,\n		Dim_Produit.En_promo,\n		Dim_Produit.Can"
+"al,\n		Dim_Produit.Source,\n		Dim_Produit.date_debut,\n		Dim_Produit.date_fin,\n		Dim_Produit.actif\nFROM	Dim_Produit";
		    

            	globalMap.put("tDBInput_5_QUERY",dbquery_tDBInput_5);
		    java.sql.ResultSet rs_tDBInput_5 = null;

		    try {
		    	rs_tDBInput_5 = stmt_tDBInput_5.executeQuery(dbquery_tDBInput_5);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_5 = rs_tDBInput_5.getMetaData();
		    	int colQtyInRs_tDBInput_5 = rsmd_tDBInput_5.getColumnCount();

		    String tmpContent_tDBInput_5 = null;
		    
		    
		    while (rs_tDBInput_5.next()) {
		        nb_line_tDBInput_5++;
		        
							if(colQtyInRs_tDBInput_5 < 1) {
								row8.Produit_PK = 0;
							} else {
		                          
            row8.Produit_PK = rs_tDBInput_5.getInt(1);
            if(rs_tDBInput_5.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 2) {
								row8.UGS = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(2);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.UGS = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.UGS = tmpContent_tDBInput_5;
                }
            } else {
                row8.UGS = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 3) {
								row8.Nom_produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(3);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Nom_produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Nom_produit = tmpContent_tDBInput_5;
                }
            } else {
                row8.Nom_produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 4) {
								row8.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(4);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Description = tmpContent_tDBInput_5;
                }
            } else {
                row8.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 5) {
								row8.Url = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(5);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Url = tmpContent_tDBInput_5;
                }
            } else {
                row8.Url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 6) {
								row8.Visibilite = null;
							} else {
	                         		
            row8.Visibilite = rs_tDBInput_5.getBoolean(6);
            if(rs_tDBInput_5.wasNull()){
                    row8.Visibilite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 7) {
								row8.Prix = null;
							} else {
	                         		
            row8.Prix = rs_tDBInput_5.getDouble(7);
            if(rs_tDBInput_5.wasNull()){
                    row8.Prix = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 8) {
								row8.Prix_en_promo = null;
							} else {
	                         		
            row8.Prix_en_promo = rs_tDBInput_5.getDouble(8);
            if(rs_tDBInput_5.wasNull()){
                    row8.Prix_en_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 9) {
								row8.En_Promo = null;
							} else {
	                         		
            row8.En_Promo = rs_tDBInput_5.getBoolean(9);
            if(rs_tDBInput_5.wasNull()){
                    row8.En_Promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 10) {
								row8.Canal = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(10);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Canal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Canal = tmpContent_tDBInput_5;
                }
            } else {
                row8.Canal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 11) {
								row8.Source = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(11);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Source = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Source = tmpContent_tDBInput_5;
                }
            } else {
                row8.Source = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 12) {
								row8.date_debut = null;
							} else {
										
			row8.date_debut = mssqlGTU_tDBInput_5.getDate(rsmd_tDBInput_5, rs_tDBInput_5, 12);
			
		                    }
							if(colQtyInRs_tDBInput_5 < 13) {
								row8.date_fin = null;
							} else {
										
			row8.date_fin = mssqlGTU_tDBInput_5.getDate(rsmd_tDBInput_5, rs_tDBInput_5, 13);
			
		                    }
							if(colQtyInRs_tDBInput_5 < 14) {
								row8.actif = null;
							} else {
		                          
            row8.actif = rs_tDBInput_5.getInt(14);
            if(rs_tDBInput_5.wasNull()){
                    row8.actif = null;
            }
		                    }
					





 



/**
 * [tDBInput_5 begin ] stop
 */
	
	/**
	 * [tDBInput_5 main ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 


	tos_count_tDBInput_5++;

/**
 * [tDBInput_5 main ] stop
 */
	
	/**
	 * [tDBInput_5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 



/**
 * [tDBInput_5 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row8 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row8";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row8"
						
						);
					}
					


			   
			   

					row8Struct row8_HashRow = new row8Struct();
		   	   	   
				
				row8_HashRow.Produit_PK = row8.Produit_PK;
				
				row8_HashRow.UGS = row8.UGS;
				
				row8_HashRow.Nom_produit = row8.Nom_produit;
				
				row8_HashRow.Description = row8.Description;
				
				row8_HashRow.Url = row8.Url;
				
				row8_HashRow.Visibilite = row8.Visibilite;
				
				row8_HashRow.Prix = row8.Prix;
				
				row8_HashRow.Prix_en_promo = row8.Prix_en_promo;
				
				row8_HashRow.En_Promo = row8.En_Promo;
				
				row8_HashRow.Canal = row8.Canal;
				
				row8_HashRow.Source = row8.Source;
				
				row8_HashRow.date_debut = row8.date_debut;
				
				row8_HashRow.date_fin = row8.date_fin;
				
				row8_HashRow.actif = row8.actif;
				
			tHash_Lookup_row8.put(row8_HashRow);
			
            




 


	tos_count_tAdvancedHash_row8++;

/**
 * [tAdvancedHash_row8 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row8 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row8";

	

 



/**
 * [tAdvancedHash_row8 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row8 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row8";

	

 



/**
 * [tAdvancedHash_row8 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 



/**
 * [tDBInput_5 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_5 end ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

	}
}finally{
	if (rs_tDBInput_5 != null) {
		rs_tDBInput_5.close();
	}
	if (stmt_tDBInput_5 != null) {
		stmt_tDBInput_5.close();
	}
		if(conn_tDBInput_5 != null && !conn_tDBInput_5.isClosed()) {
			
			conn_tDBInput_5.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_5_NB_LINE",nb_line_tDBInput_5);

 

ok_Hash.put("tDBInput_5", true);
end_Hash.put("tDBInput_5", System.currentTimeMillis());




/**
 * [tDBInput_5 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row8 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row8";

	

tHash_Lookup_row8.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row8");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row8", true);
end_Hash.put("tAdvancedHash_row8", System.currentTimeMillis());




/**
 * [tAdvancedHash_row8 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_5 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 



/**
 * [tDBInput_5 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row8 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row8";

	

 



/**
 * [tAdvancedHash_row8 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_5_SUBPROCESS_STATE", 1);
	}
	


public static class row13Struct implements routines.system.IPersistableComparableLookupRow<row13Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Categorie_PK;

				public int getCategorie_PK () {
					return this.Categorie_PK;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				
			    public String Source;

				public String getSource () {
					return this.Source;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Categorie == null) ? 0 : this.Categorie.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row13Struct other = (row13Struct) obj;
		
						if (this.Categorie == null) {
							if (other.Categorie != null)
								return false;
						
						} else if (!this.Categorie.equals(other.Categorie))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row13Struct other) {

		other.Categorie_PK = this.Categorie_PK;
	            other.Categorie = this.Categorie;
	            other.Source = this.Source;
	            
	}

	public void copyKeysDataTo(row13Struct other) {

		other.Categorie = this.Categorie;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Categorie,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Categorie,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
			            this.Categorie_PK = dis.readInt();
					
						this.Source = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Categorie_PK = objectIn.readInt();
					
						this.Source = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Categorie_PK);
					
						writeString(this.Source, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Categorie_PK);
					
						writeString(this.Source, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Categorie_PK="+String.valueOf(Categorie_PK));
		sb.append(",Categorie="+Categorie);
		sb.append(",Source="+Source);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row13Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Categorie, other.Categorie);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_12Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_12_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row13Struct row13 = new row13Struct();




	
	/**
	 * [tAdvancedHash_row13 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row13", false);
		start_Hash.put("tAdvancedHash_row13", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row13";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row13");
					}
				
		int tos_count_tAdvancedHash_row13 = 0;
		

			   		// connection name:row13
			   		// source node:tDBInput_12 - inputs:(after_tDBInput_2) outputs:(row13,row13) | target node:tAdvancedHash_row13 - inputs:(row13) outputs:()
			   		// linked node: tMap_3 - inputs:(out_state,row4,row5,row6,row7,row11,row8,row13) outputs:(outfinal,client_rejected)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row13 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct> tHash_Lookup_row13 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row13Struct>getLookup(matchingModeEnum_row13);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row13", tHash_Lookup_row13);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row13 begin ] stop
 */



	
	/**
	 * [tDBInput_12 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_12", false);
		start_Hash.put("tDBInput_12", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_12";

	
		int tos_count_tDBInput_12 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_12 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_12 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_12  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_12, talendToDBArray_tDBInput_12); 
		    int nb_line_tDBInput_12 = 0;
		    java.sql.Connection conn_tDBInput_12 = null;
				String driverClass_tDBInput_12 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_12 = java.lang.Class.forName(driverClass_tDBInput_12);
				String dbUser_tDBInput_12 = "sa";
				
				 
	final String decryptedPassword_tDBInput_12 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:yKt8cC1iznFJbPWOvQKTTqP8Y1lTFKDPyUgBJHq3oXxJHuBJLVo=");
				
				String dbPwd_tDBInput_12 = decryptedPassword_tDBInput_12;
				
		    String port_tDBInput_12 = "1433";
		    String dbname_tDBInput_12 = "DW_E-Commerce" ;
			String url_tDBInput_12 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_12)) {
		    	url_tDBInput_12 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_12)) {
				url_tDBInput_12 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_12 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_12 = "";
				
				conn_tDBInput_12 = java.sql.DriverManager.getConnection(url_tDBInput_12,dbUser_tDBInput_12,dbPwd_tDBInput_12);
		        
		    
			java.sql.Statement stmt_tDBInput_12 = conn_tDBInput_12.createStatement();

		    String dbquery_tDBInput_12 = "SELECT Dim_Categorie.Categorie_PK,\n		Dim_Categorie.Categorie,\n		Dim_Categorie.Source\nFROM	Dim_Categorie";
		    

            	globalMap.put("tDBInput_12_QUERY",dbquery_tDBInput_12);
		    java.sql.ResultSet rs_tDBInput_12 = null;

		    try {
		    	rs_tDBInput_12 = stmt_tDBInput_12.executeQuery(dbquery_tDBInput_12);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_12 = rs_tDBInput_12.getMetaData();
		    	int colQtyInRs_tDBInput_12 = rsmd_tDBInput_12.getColumnCount();

		    String tmpContent_tDBInput_12 = null;
		    
		    
		    while (rs_tDBInput_12.next()) {
		        nb_line_tDBInput_12++;
		        
							if(colQtyInRs_tDBInput_12 < 1) {
								row13.Categorie_PK = 0;
							} else {
		                          
            row13.Categorie_PK = rs_tDBInput_12.getInt(1);
            if(rs_tDBInput_12.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_12 < 2) {
								row13.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_12 = rs_tDBInput_12.getString(2);
            if(tmpContent_tDBInput_12 != null) {
            	if (talendToDBList_tDBInput_12 .contains(rsmd_tDBInput_12.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_12);
            	} else {
                	row13.Categorie = tmpContent_tDBInput_12;
                }
            } else {
                row13.Categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_12 < 3) {
								row13.Source = null;
							} else {
	                         		
           		tmpContent_tDBInput_12 = rs_tDBInput_12.getString(3);
            if(tmpContent_tDBInput_12 != null) {
            	if (talendToDBList_tDBInput_12 .contains(rsmd_tDBInput_12.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.Source = FormatterUtils.formatUnwithE(tmpContent_tDBInput_12);
            	} else {
                	row13.Source = tmpContent_tDBInput_12;
                }
            } else {
                row13.Source = null;
            }
		                    }
					





 



/**
 * [tDBInput_12 begin ] stop
 */
	
	/**
	 * [tDBInput_12 main ] start
	 */

	

	
	
	currentComponent="tDBInput_12";

	

 


	tos_count_tDBInput_12++;

/**
 * [tDBInput_12 main ] stop
 */
	
	/**
	 * [tDBInput_12 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_12";

	

 



/**
 * [tDBInput_12 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row13 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row13"
						
						);
					}
					


			   
			   

					row13Struct row13_HashRow = new row13Struct();
		   	   	   
				
				row13_HashRow.Categorie_PK = row13.Categorie_PK;
				
				row13_HashRow.Categorie = row13.Categorie;
				
				row13_HashRow.Source = row13.Source;
				
			tHash_Lookup_row13.put(row13_HashRow);
			
            




 


	tos_count_tAdvancedHash_row13++;

/**
 * [tAdvancedHash_row13 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row13 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	

 



/**
 * [tAdvancedHash_row13 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row13 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	

 



/**
 * [tAdvancedHash_row13 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_12 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_12";

	

 



/**
 * [tDBInput_12 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_12 end ] start
	 */

	

	
	
	currentComponent="tDBInput_12";

	

	}
}finally{
	if (rs_tDBInput_12 != null) {
		rs_tDBInput_12.close();
	}
	if (stmt_tDBInput_12 != null) {
		stmt_tDBInput_12.close();
	}
		if(conn_tDBInput_12 != null && !conn_tDBInput_12.isClosed()) {
			
			conn_tDBInput_12.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_12_NB_LINE",nb_line_tDBInput_12);

 

ok_Hash.put("tDBInput_12", true);
end_Hash.put("tDBInput_12", System.currentTimeMillis());




/**
 * [tDBInput_12 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row13 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	

tHash_Lookup_row13.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row13");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row13", true);
end_Hash.put("tAdvancedHash_row13", System.currentTimeMillis());




/**
 * [tAdvancedHash_row13 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_12 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_12";

	

 



/**
 * [tDBInput_12 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row13 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	

 



/**
 * [tAdvancedHash_row13 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_12_SUBPROCESS_STATE", 1);
	}
	


public static class row12Struct implements routines.system.IPersistableComparableLookupRow<row12Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Produit == null) ? 0 : this.Produit.hashCode());
					
    		this.hashCode = result;
    		this.hashCodeDirty = false;
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final row12Struct other = (row12Struct) obj;
		
						if (this.Produit == null) {
							if (other.Produit != null)
								return false;
						
						} else if (!this.Produit.equals(other.Produit))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row12Struct other) {

		other.Produit = this.Produit;
	            other.Categorie = this.Categorie;
	            
	}

	public void copyKeysDataTo(row12Struct other) {

		other.Produit = this.Produit;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Vente2, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }
	
	private String readString(DataInputStream dis, ObjectInputStream ois) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			dis.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(DataInputStream dis, org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			byte[] byteArray = new byte[length];
			unmarshaller.read(byteArray);
			strReturn = new String(byteArray, utf8Charset);
		}
		return strReturn;
	}
	
	private void writeString(String str, DataOutputStream dos, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
	}

	private void writeString(String str, DataOutputStream dos, ObjectOutputStream oos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
	}

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Vente2) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }



    /**
     * Fill Values data by reading ObjectInputStream.
     */
    public void readValuesData(DataInputStream dis, ObjectInputStream ois) {
        try {

			int length = 0;
		
						this.Categorie = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
						this.Categorie = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
						writeString(this.Categorie, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
						writeString(this.Categorie, dos, objectOut);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}
    }


    
    public boolean supportMarshaller(){
        return true;
    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Categorie="+Categorie);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row12Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit, other.Produit);
						if(returnValue != 0) {
							return returnValue;
						}

					
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tDBInput_11Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_11_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row12Struct row12 = new row12Struct();




	
	/**
	 * [tAdvancedHash_row12 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row12", false);
		start_Hash.put("tAdvancedHash_row12", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row12";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row12");
					}
				
		int tos_count_tAdvancedHash_row12 = 0;
		

			   		// connection name:row12
			   		// source node:tDBInput_11 - inputs:(after_tDBInput_2) outputs:(row12,row12) | target node:tAdvancedHash_row12 - inputs:(row12) outputs:()
			   		// linked node: tMap_5 - inputs:(row3,row9,row12) outputs:(out_state)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row12 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct> tHash_Lookup_row12 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row12Struct>getLookup(matchingModeEnum_row12);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row12", tHash_Lookup_row12);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row12 begin ] stop
 */



	
	/**
	 * [tDBInput_11 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_11", false);
		start_Hash.put("tDBInput_11", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_11";

	
		int tos_count_tDBInput_11 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_11 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_11 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_11  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_11, talendToDBArray_tDBInput_11); 
		    int nb_line_tDBInput_11 = 0;
		    java.sql.Connection conn_tDBInput_11 = null;
				String driverClass_tDBInput_11 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_11 = java.lang.Class.forName(driverClass_tDBInput_11);
				String dbUser_tDBInput_11 = "sa";
				
				 
	final String decryptedPassword_tDBInput_11 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:x73o/6+97ICkIFnDnELCoCIQ/Gzi/Lqpw8LX+kw1EyTjki3leYc=");
				
				String dbPwd_tDBInput_11 = decryptedPassword_tDBInput_11;
				
		    String port_tDBInput_11 = "1433";
		    String dbname_tDBInput_11 = "DW_E-Commerce" ;
			String url_tDBInput_11 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_11)) {
		    	url_tDBInput_11 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_11)) {
				url_tDBInput_11 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_11 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_11 = "";
				
				conn_tDBInput_11 = java.sql.DriverManager.getConnection(url_tDBInput_11,dbUser_tDBInput_11,dbPwd_tDBInput_11);
		        
		    
			java.sql.Statement stmt_tDBInput_11 = conn_tDBInput_11.createStatement();

		    String dbquery_tDBInput_11 = "SELECT Dictionnaire_Categorie.Produit,\n		Dictionnaire_Categorie.Categorie\nFROM	Dictionnaire_Categorie";
		    

            	globalMap.put("tDBInput_11_QUERY",dbquery_tDBInput_11);
		    java.sql.ResultSet rs_tDBInput_11 = null;

		    try {
		    	rs_tDBInput_11 = stmt_tDBInput_11.executeQuery(dbquery_tDBInput_11);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_11 = rs_tDBInput_11.getMetaData();
		    	int colQtyInRs_tDBInput_11 = rsmd_tDBInput_11.getColumnCount();

		    String tmpContent_tDBInput_11 = null;
		    
		    
		    while (rs_tDBInput_11.next()) {
		        nb_line_tDBInput_11++;
		        
							if(colQtyInRs_tDBInput_11 < 1) {
								row12.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_11 = rs_tDBInput_11.getString(1);
            if(tmpContent_tDBInput_11 != null) {
            	if (talendToDBList_tDBInput_11 .contains(rsmd_tDBInput_11.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row12.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_11);
            	} else {
                	row12.Produit = tmpContent_tDBInput_11;
                }
            } else {
                row12.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_11 < 2) {
								row12.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_11 = rs_tDBInput_11.getString(2);
            if(tmpContent_tDBInput_11 != null) {
            	if (talendToDBList_tDBInput_11 .contains(rsmd_tDBInput_11.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row12.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_11);
            	} else {
                	row12.Categorie = tmpContent_tDBInput_11;
                }
            } else {
                row12.Categorie = null;
            }
		                    }
					





 



/**
 * [tDBInput_11 begin ] stop
 */
	
	/**
	 * [tDBInput_11 main ] start
	 */

	

	
	
	currentComponent="tDBInput_11";

	

 


	tos_count_tDBInput_11++;

/**
 * [tDBInput_11 main ] stop
 */
	
	/**
	 * [tDBInput_11 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_11";

	

 



/**
 * [tDBInput_11 process_data_begin ] stop
 */

	
	/**
	 * [tAdvancedHash_row12 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row12";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					


			   
			   

					row12Struct row12_HashRow = new row12Struct();
		   	   	   
				
				row12_HashRow.Produit = row12.Produit;
				
				row12_HashRow.Categorie = row12.Categorie;
				
			tHash_Lookup_row12.put(row12_HashRow);
			
            




 


	tos_count_tAdvancedHash_row12++;

/**
 * [tAdvancedHash_row12 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row12 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row12";

	

 



/**
 * [tAdvancedHash_row12 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row12 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row12";

	

 



/**
 * [tAdvancedHash_row12 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_11 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_11";

	

 



/**
 * [tDBInput_11 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_11 end ] start
	 */

	

	
	
	currentComponent="tDBInput_11";

	

	}
}finally{
	if (rs_tDBInput_11 != null) {
		rs_tDBInput_11.close();
	}
	if (stmt_tDBInput_11 != null) {
		stmt_tDBInput_11.close();
	}
		if(conn_tDBInput_11 != null && !conn_tDBInput_11.isClosed()) {
			
			conn_tDBInput_11.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_11_NB_LINE",nb_line_tDBInput_11);

 

ok_Hash.put("tDBInput_11", true);
end_Hash.put("tDBInput_11", System.currentTimeMillis());




/**
 * [tDBInput_11 end ] stop
 */

	
	/**
	 * [tAdvancedHash_row12 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row12";

	

tHash_Lookup_row12.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row12");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row12", true);
end_Hash.put("tAdvancedHash_row12", System.currentTimeMillis());




/**
 * [tAdvancedHash_row12 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBInput_11 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_11";

	

 



/**
 * [tDBInput_11 finally ] stop
 */

	
	/**
	 * [tAdvancedHash_row12 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row12";

	

 



/**
 * [tAdvancedHash_row12 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_11_SUBPROCESS_STATE", 1);
	}
	
    public String resuming_logs_dir_path = null;
    public String resuming_checkpoint_path = null;
    public String parent_part_launcher = null;
    private String resumeEntryMethodName = null;
    private boolean globalResumeTicket = false;

    public boolean watch = false;
    // portStats is null, it means don't execute the statistics
    public Integer portStats = null;
    public int portTraces = 4334;
    public String clientHost;
    public String defaultClientHost = "localhost";
    public String contextStr = "Default";
    public boolean isDefaultContext = true;
    public String pid = "0";
    public String rootPid = null;
    public String fatherPid = null;
    public String fatherNode = null;
    public long startTime = 0;
    public boolean isChildJob = false;
    public String log4jLevel = "";
    
    private boolean enableLogStash;

    private boolean execStat = true;

    private ThreadLocal<java.util.Map<String, String>> threadLocal = new ThreadLocal<java.util.Map<String, String>>() {
        protected java.util.Map<String, String> initialValue() {
            java.util.Map<String,String> threadRunResultMap = new java.util.HashMap<String, String>();
            threadRunResultMap.put("errorCode", null);
            threadRunResultMap.put("status", "");
            return threadRunResultMap;
        };
    };


    protected PropertiesWithType context_param = new PropertiesWithType();
    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

    public String status= "";
    

    public static void main(String[] args){
        final ETL_Fact_Vente2 ETL_Fact_Vente2Class = new ETL_Fact_Vente2();

        int exitCode = ETL_Fact_Vente2Class.runJobInTOS(args);

        System.exit(exitCode);
    }


    public String[][] runJob(String[] args) {

        int exitCode = runJobInTOS(args);
        String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };

        return bufferValue;
    }

    public boolean hastBufferOutputComponent() {
		boolean hastBufferOutput = false;
    	
        return hastBufferOutput;
    }

    public int runJobInTOS(String[] args) {
	   	// reset status
	   	status = "";
	   	
        String lastStr = "";
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--context_param")) {
                lastStr = arg;
            } else if (lastStr.equals("")) {
                evalParam(arg);
            } else {
                evalParam(lastStr + " " + arg);
                lastStr = "";
            }
        }
        enableLogStash = "true".equalsIgnoreCase(System.getProperty("audit.enabled"));

    	
    	

        if(clientHost == null) {
            clientHost = defaultClientHost;
        }

        if(pid == null || "0".equals(pid)) {
            pid = TalendString.getAsciiRandomString(6);
        }

        if (rootPid==null) {
            rootPid = pid;
        }
        if (fatherPid==null) {
            fatherPid = pid;
        }else{
            isChildJob = true;
        }

        if (portStats != null) {
            // portStats = -1; //for testing
            if (portStats < 0 || portStats > 65535) {
                // issue:10869, the portStats is invalid, so this client socket can't open
                System.err.println("The statistics socket port " + portStats + " is invalid.");
                execStat = false;
            }
        } else {
            execStat = false;
        }
        boolean inOSGi = routines.system.BundleUtils.inOSGi();

        if (inOSGi) {
            java.util.Dictionary<String, Object> jobProperties = routines.system.BundleUtils.getJobProperties(jobName);

            if (jobProperties != null && jobProperties.get("context") != null) {
                contextStr = (String)jobProperties.get("context");
            }
        }

        try {
            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
            java.io.InputStream inContext = ETL_Fact_Vente2.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/etl_fact_vente2_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ETL_Fact_Vente2.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
            }
            if (inContext != null) {
                try {
                    //defaultProps is in order to keep the original context value
                    if(context != null && context.isEmpty()) {
	                defaultProps.load(inContext);
	                context = new ContextProperties(defaultProps);
                    }
                } finally {
                    inContext.close();
                }
            } else if (!isDefaultContext) {
                //print info and job continue to run, for case: context_param is not empty.
                System.err.println("Could not find the context " + contextStr);
            }

            if(!context_param.isEmpty()) {
                context.putAll(context_param);
				//set types for params from parentJobs
				for (Object key: context_param.keySet()){
					String context_key = key.toString();
					String context_type = context_param.getContextType(context_key);
					context.setContextType(context_key, context_type);

				}
            }
            class ContextProcessing {
                private void processContext_0() {
                } 
                public void processAllContext() {
                        processContext_0();
                }
            }

            new ContextProcessing().processAllContext();
        } catch (java.io.IOException ie) {
            System.err.println("Could not load context "+contextStr);
            ie.printStackTrace();
        }

        // get context value from parent directly
        if (parentContextMap != null && !parentContextMap.isEmpty()) {
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
        //Resume: jobStart
        resumeUtil.addLog("JOB_STARTED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","","","",resumeUtil.convertToJsonText(context,parametersToEncrypt));

if(execStat) {
    try {
        runStat.openSocket(!isChildJob);
        runStat.setAllPID(rootPid, fatherPid, pid, jobName);
        runStat.startThreadStat(clientHost, portStats);
        runStat.updateStatOnJob(RunStat.JOBSTART, fatherNode);
    } catch (java.io.IOException ioException) {
        ioException.printStackTrace();
    }
}



	
	    java.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap = new java.util.concurrent.ConcurrentHashMap<Object, Object>();
	    globalMap.put("concurrentHashMap", concurrentHashMap);
	

    long startUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long endUsedMemory = 0;
    long end = 0;

    startTime = System.currentTimeMillis();


this.globalResumeTicket = true;//to run tPreJob





this.globalResumeTicket = false;//to run others jobs

try {
errorCode = null;tDBInput_2Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tDBInput_2) {
globalMap.put("tDBInput_2_SUBPROCESS_STATE", -1);

e_tDBInput_2.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ETL_Fact_Vente2");
        }



if (execStat) {
    runStat.updateStatOnJob(RunStat.JOBEND, fatherNode);
    runStat.stopThreadStat();
}
    int returnCode = 0;


    if(errorCode == null) {
         returnCode = status != null && status.equals("failure") ? 1 : 0;
    } else {
         returnCode = errorCode.intValue();
    }
    resumeUtil.addLog("JOB_ENDED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","" + returnCode,"","","");

    return returnCode;

  }

    // only for OSGi env
    public void destroy() {


    }














    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();






        return connections;
    }

    private void evalParam(String arg) {
        if (arg.startsWith("--resuming_logs_dir_path")) {
            resuming_logs_dir_path = arg.substring(25);
        } else if (arg.startsWith("--resuming_checkpoint_path")) {
            resuming_checkpoint_path = arg.substring(27);
        } else if (arg.startsWith("--parent_part_launcher")) {
            parent_part_launcher = arg.substring(23);
        } else if (arg.startsWith("--watch")) {
            watch = true;
        } else if (arg.startsWith("--stat_port=")) {
            String portStatsStr = arg.substring(12);
            if (portStatsStr != null && !portStatsStr.equals("null")) {
                portStats = Integer.parseInt(portStatsStr);
            }
        } else if (arg.startsWith("--trace_port=")) {
            portTraces = Integer.parseInt(arg.substring(13));
        } else if (arg.startsWith("--client_host=")) {
            clientHost = arg.substring(14);
        } else if (arg.startsWith("--context=")) {
            contextStr = arg.substring(10);
            isDefaultContext = false;
        } else if (arg.startsWith("--father_pid=")) {
            fatherPid = arg.substring(13);
        } else if (arg.startsWith("--root_pid=")) {
            rootPid = arg.substring(11);
        } else if (arg.startsWith("--father_node=")) {
            fatherNode = arg.substring(14);
        } else if (arg.startsWith("--pid=")) {
            pid = arg.substring(6);
        } else if (arg.startsWith("--context_type")) {
            String keyValue = arg.substring(15);
			int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.setContextType(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.setContextType(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }

            }

		} else if (arg.startsWith("--context_param")) {
            String keyValue = arg.substring(16);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.put(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }
            }
        } else if (arg.startsWith("--log4jLevel=")) {
            log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--audit.enabled") && arg.contains("=")) {//for trunjob call
		    final int equal = arg.indexOf('=');
			final String key = arg.substring("--".length(), equal);
			System.setProperty(key, arg.substring(equal + 1));
		}
    }
    
    private static final String NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY = "<TALEND_NULL>";

    private final String[][] escapeChars = {
        {"\\\\","\\"},{"\\n","\n"},{"\\'","\'"},{"\\r","\r"},
        {"\\f","\f"},{"\\b","\b"},{"\\t","\t"}
        };
    private String replaceEscapeChars (String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0],currIndex);
				if (index>=0) {

					result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left into the result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
    }

    public Integer getErrorCode() {
        return errorCode;
    }


    public String getStatus() {
        return status;
    }

    ResumeUtil resumeUtil = null;
}
/************************************************************************************************
 *     553721 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:46 CET
 ************************************************************************************************/