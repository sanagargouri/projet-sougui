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


package pi_pa_mehdi.etl_dim_produits_0_1;

import routines.parseTVA;
import routines.Numeric;
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.StringHandling;
import routines.Relational;
import routines.TalendDate;
import routines.Mathematical;
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
 * Job: ETL_Dim_Produits Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ETL_Dim_Produits implements TalendJob {

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
	private final String jobName = "ETL_Dim_Produits";
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
				ETL_Dim_Produits.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ETL_Dim_Produits.this, new Object[] { e , currentComponent, globalMap});
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

			public void tDBInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBSCD_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFilterRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_8_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_8_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_8_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_9_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class row11Struct implements routines.system.IPersistableRow<row11Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row11Struct other) {

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
	            
	}

	public void copyKeysDataTo(row11Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row11Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row10Struct implements routines.system.IPersistableRow<row10Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row10Struct other = (row10Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row10Struct other) {

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
	            
	}

	public void copyKeysDataTo(row10Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row10Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row9Struct implements routines.system.IPersistableRow<row9Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row9Struct other) {

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
	            
	}

	public void copyKeysDataTo(row9Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row9Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row6Struct implements routines.system.IPersistableRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row6Struct other) {

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
	            
	}

	public void copyKeysDataTo(row6Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row6Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row2Struct other = (row2Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row2Struct other) {

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
	            
	}

	public void copyKeysDataTo(row2Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row2Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class WebScrappingStruct implements routines.system.IPersistableRow<WebScrappingStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final WebScrappingStruct other = (WebScrappingStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(WebScrappingStruct other) {

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
	            
	}

	public void copyKeysDataTo(WebScrappingStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(WebScrappingStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String titre;

				public String getTitre () {
					return this.titre;
				}
				
			    public String url;

				public String getUrl () {
					return this.url;
				}
				
			    public String pack;

				public String getPack () {
					return this.pack;
				}
				
			    public String prix_avant_promo;

				public String getPrix_avant_promo () {
					return this.prix_avant_promo;
				}
				
			    public String prix_apres_promo;

				public String getPrix_apres_promo () {
					return this.prix_apres_promo;
				}
				
			    public String categories;

				public String getCategories () {
					return this.categories;
				}
				
			    public String image;

				public String getImage () {
					return this.image;
				}
				
			    public String en_promotion;

				public String getEn_promotion () {
					return this.en_promotion;
				}
				
			    public String sku;

				public String getSku () {
					return this.sku;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.titre = readString(dis);
					
					this.url = readString(dis);
					
					this.pack = readString(dis);
					
					this.prix_avant_promo = readString(dis);
					
					this.prix_apres_promo = readString(dis);
					
					this.categories = readString(dis);
					
					this.image = readString(dis);
					
					this.en_promotion = readString(dis);
					
					this.sku = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.titre = readString(dis);
					
					this.url = readString(dis);
					
					this.pack = readString(dis);
					
					this.prix_avant_promo = readString(dis);
					
					this.prix_apres_promo = readString(dis);
					
					this.categories = readString(dis);
					
					this.image = readString(dis);
					
					this.en_promotion = readString(dis);
					
					this.sku = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.titre,dos);
					
					// String
				
						writeString(this.url,dos);
					
					// String
				
						writeString(this.pack,dos);
					
					// String
				
						writeString(this.prix_avant_promo,dos);
					
					// String
				
						writeString(this.prix_apres_promo,dos);
					
					// String
				
						writeString(this.categories,dos);
					
					// String
				
						writeString(this.image,dos);
					
					// String
				
						writeString(this.en_promotion,dos);
					
					// String
				
						writeString(this.sku,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.titre,dos);
					
					// String
				
						writeString(this.url,dos);
					
					// String
				
						writeString(this.pack,dos);
					
					// String
				
						writeString(this.prix_avant_promo,dos);
					
					// String
				
						writeString(this.prix_apres_promo,dos);
					
					// String
				
						writeString(this.categories,dos);
					
					// String
				
						writeString(this.image,dos);
					
					// String
				
						writeString(this.en_promotion,dos);
					
					// String
				
						writeString(this.sku,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("titre="+titre);
		sb.append(",url="+url);
		sb.append(",pack="+pack);
		sb.append(",prix_avant_promo="+prix_avant_promo);
		sb.append(",prix_apres_promo="+prix_apres_promo);
		sb.append(",categories="+categories);
		sb.append(",image="+image);
		sb.append(",en_promotion="+en_promotion);
		sb.append(",sku="+sku);
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

public static class row8Struct implements routines.system.IPersistableRow<row8Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
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
	            
	}

	public void copyKeysDataTo(row8Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row8Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row5Struct implements routines.system.IPersistableRow<row5Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row5Struct other) {

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
	            
	}

	public void copyKeysDataTo(row5Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row5Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class ProduitsCommandeVenteStruct implements routines.system.IPersistableRow<ProduitsCommandeVenteStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final ProduitsCommandeVenteStruct other = (ProduitsCommandeVenteStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(ProduitsCommandeVenteStruct other) {

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
	            
	}

	public void copyKeysDataTo(ProduitsCommandeVenteStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(ProduitsCommandeVenteStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row4Struct implements routines.system.IPersistableRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

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
    public int compareTo(row4Struct other) {

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

public static class row7Struct implements routines.system.IPersistableRow<row7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row7Struct other) {

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
	            
	}

	public void copyKeysDataTo(row7Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row7Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class ProduitsInventaireStruct implements routines.system.IPersistableRow<ProduitsInventaireStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final ProduitsInventaireStruct other = (ProduitsInventaireStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(ProduitsInventaireStruct other) {

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
	            
	}

	public void copyKeysDataTo(ProduitsInventaireStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(ProduitsInventaireStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row12Struct implements routines.system.IPersistableRow<row12Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String Reference_Produit;

				public String getReference_Produit () {
					return this.Reference_Produit;
				}
				
			    public String Nom_Produit;

				public String getNom_Produit () {
					return this.Nom_Produit;
				}
				
			    public Integer Stock_Initial;

				public Integer getStock_Initial () {
					return this.Stock_Initial;
				}
				
			    public Integer Entrees;

				public Integer getEntrees () {
					return this.Entrees;
				}
				
			    public Integer Sorties;

				public Integer getSorties () {
					return this.Sorties;
				}
				
			    public Integer Stock_Actuel;

				public Integer getStock_Actuel () {
					return this.Stock_Actuel;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Prix_achat;

				public String getPrix_achat () {
					return this.Prix_achat;
				}
				
			    public String Prix_unitaire;

				public String getPrix_unitaire () {
					return this.Prix_unitaire;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Reference_Produit = readString(dis);
					
					this.Nom_Produit = readString(dis);
					
						this.Stock_Initial = readInteger(dis);
					
						this.Entrees = readInteger(dis);
					
						this.Sorties = readInteger(dis);
					
						this.Stock_Actuel = readInteger(dis);
					
					this.Categorie = readString(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Prix_achat = readString(dis);
					
					this.Prix_unitaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Reference_Produit = readString(dis);
					
					this.Nom_Produit = readString(dis);
					
						this.Stock_Initial = readInteger(dis);
					
						this.Entrees = readInteger(dis);
					
						this.Sorties = readInteger(dis);
					
						this.Stock_Actuel = readInteger(dis);
					
					this.Categorie = readString(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Prix_achat = readString(dis);
					
					this.Prix_unitaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Reference_Produit,dos);
					
					// String
				
						writeString(this.Nom_Produit,dos);
					
					// Integer
				
						writeInteger(this.Stock_Initial,dos);
					
					// Integer
				
						writeInteger(this.Entrees,dos);
					
					// Integer
				
						writeInteger(this.Sorties,dos);
					
					// Integer
				
						writeInteger(this.Stock_Actuel,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Prix_achat,dos);
					
					// String
				
						writeString(this.Prix_unitaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Reference_Produit,dos);
					
					// String
				
						writeString(this.Nom_Produit,dos);
					
					// Integer
				
						writeInteger(this.Stock_Initial,dos);
					
					// Integer
				
						writeInteger(this.Entrees,dos);
					
					// Integer
				
						writeInteger(this.Sorties,dos);
					
					// Integer
				
						writeInteger(this.Stock_Actuel,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Prix_achat,dos);
					
					// String
				
						writeString(this.Prix_unitaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Reference_Produit="+Reference_Produit);
		sb.append(",Nom_Produit="+Nom_Produit);
		sb.append(",Stock_Initial="+String.valueOf(Stock_Initial));
		sb.append(",Entrees="+String.valueOf(Entrees));
		sb.append(",Sorties="+String.valueOf(Sorties));
		sb.append(",Stock_Actuel="+String.valueOf(Stock_Actuel));
		sb.append(",Categorie="+Categorie);
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Prix_achat="+Prix_achat);
		sb.append(",Prix_unitaire="+Prix_unitaire);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row12Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String Reference_Produit;

				public String getReference_Produit () {
					return this.Reference_Produit;
				}
				
			    public String Nom_Produit;

				public String getNom_Produit () {
					return this.Nom_Produit;
				}
				
			    public Integer Stock_Initial;

				public Integer getStock_Initial () {
					return this.Stock_Initial;
				}
				
			    public Integer Entrees;

				public Integer getEntrees () {
					return this.Entrees;
				}
				
			    public Integer Sorties;

				public Integer getSorties () {
					return this.Sorties;
				}
				
			    public Integer Stock_Actuel;

				public Integer getStock_Actuel () {
					return this.Stock_Actuel;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Prix_achat;

				public String getPrix_achat () {
					return this.Prix_achat;
				}
				
			    public String Prix_unitaire;

				public String getPrix_unitaire () {
					return this.Prix_unitaire;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Reference_Produit = readString(dis);
					
					this.Nom_Produit = readString(dis);
					
						this.Stock_Initial = readInteger(dis);
					
						this.Entrees = readInteger(dis);
					
						this.Sorties = readInteger(dis);
					
						this.Stock_Actuel = readInteger(dis);
					
					this.Categorie = readString(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Prix_achat = readString(dis);
					
					this.Prix_unitaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Reference_Produit = readString(dis);
					
					this.Nom_Produit = readString(dis);
					
						this.Stock_Initial = readInteger(dis);
					
						this.Entrees = readInteger(dis);
					
						this.Sorties = readInteger(dis);
					
						this.Stock_Actuel = readInteger(dis);
					
					this.Categorie = readString(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Prix_achat = readString(dis);
					
					this.Prix_unitaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Reference_Produit,dos);
					
					// String
				
						writeString(this.Nom_Produit,dos);
					
					// Integer
				
						writeInteger(this.Stock_Initial,dos);
					
					// Integer
				
						writeInteger(this.Entrees,dos);
					
					// Integer
				
						writeInteger(this.Sorties,dos);
					
					// Integer
				
						writeInteger(this.Stock_Actuel,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Prix_achat,dos);
					
					// String
				
						writeString(this.Prix_unitaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Reference_Produit,dos);
					
					// String
				
						writeString(this.Nom_Produit,dos);
					
					// Integer
				
						writeInteger(this.Stock_Initial,dos);
					
					// Integer
				
						writeInteger(this.Entrees,dos);
					
					// Integer
				
						writeInteger(this.Sorties,dos);
					
					// Integer
				
						writeInteger(this.Stock_Actuel,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Prix_achat,dos);
					
					// String
				
						writeString(this.Prix_unitaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Reference_Produit="+Reference_Produit);
		sb.append(",Nom_Produit="+Nom_Produit);
		sb.append(",Stock_Initial="+String.valueOf(Stock_Initial));
		sb.append(",Entrees="+String.valueOf(Entrees));
		sb.append(",Sorties="+String.valueOf(Sorties));
		sb.append(",Stock_Actuel="+String.valueOf(Stock_Actuel));
		sb.append(",Categorie="+Categorie);
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Prix_achat="+Prix_achat);
		sb.append(",Prix_unitaire="+Prix_unitaire);
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

public static class row15Struct implements routines.system.IPersistableRow<row15Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row15Struct other = (row15Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row15Struct other) {

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
	            
	}

	public void copyKeysDataTo(row15Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row15Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class factureVenteStruct implements routines.system.IPersistableRow<factureVenteStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final factureVenteStruct other = (factureVenteStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(factureVenteStruct other) {

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
	            
	}

	public void copyKeysDataTo(factureVenteStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(factureVenteStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row14Struct implements routines.system.IPersistableRow<row14Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

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
    public int compareTo(row14Struct other) {

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

public static class row16Struct implements routines.system.IPersistableRow<row16Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row16Struct other = (row16Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row16Struct other) {

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
	            
	}

	public void copyKeysDataTo(row16Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row16Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class out_EdaytnaStruct implements routines.system.IPersistableRow<out_EdaytnaStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final out_EdaytnaStruct other = (out_EdaytnaStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out_EdaytnaStruct other) {

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
	            
	}

	public void copyKeysDataTo(out_EdaytnaStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out_EdaytnaStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row13Struct implements routines.system.IPersistableRow<row13Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String titre;

				public String getTitre () {
					return this.titre;
				}
				
			    public String categorie;

				public String getCategorie () {
					return this.categorie;
				}
				
			    public String prix_avant;

				public String getPrix_avant () {
					return this.prix_avant;
				}
				
			    public String prix_apres;

				public String getPrix_apres () {
					return this.prix_apres;
				}
				
			    public String promotion;

				public String getPromotion () {
					return this.promotion;
				}
				
			    public String url;

				public String getUrl () {
					return this.url;
				}
				
			    public String image;

				public String getImage () {
					return this.image;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.titre = readString(dis);
					
					this.categorie = readString(dis);
					
					this.prix_avant = readString(dis);
					
					this.prix_apres = readString(dis);
					
					this.promotion = readString(dis);
					
					this.url = readString(dis);
					
					this.image = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.titre = readString(dis);
					
					this.categorie = readString(dis);
					
					this.prix_avant = readString(dis);
					
					this.prix_apres = readString(dis);
					
					this.promotion = readString(dis);
					
					this.url = readString(dis);
					
					this.image = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.titre,dos);
					
					// String
				
						writeString(this.categorie,dos);
					
					// String
				
						writeString(this.prix_avant,dos);
					
					// String
				
						writeString(this.prix_apres,dos);
					
					// String
				
						writeString(this.promotion,dos);
					
					// String
				
						writeString(this.url,dos);
					
					// String
				
						writeString(this.image,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.titre,dos);
					
					// String
				
						writeString(this.categorie,dos);
					
					// String
				
						writeString(this.prix_avant,dos);
					
					// String
				
						writeString(this.prix_apres,dos);
					
					// String
				
						writeString(this.promotion,dos);
					
					// String
				
						writeString(this.url,dos);
					
					// String
				
						writeString(this.image,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("titre="+titre);
		sb.append(",categorie="+categorie);
		sb.append(",prix_avant="+prix_avant);
		sb.append(",prix_apres="+prix_apres);
		sb.append(",promotion="+promotion);
		sb.append(",url="+url);
		sb.append(",image="+image);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row13Struct other) {

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

public static class row18Struct implements routines.system.IPersistableRow<row18Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row18Struct other = (row18Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row18Struct other) {

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
	            
	}

	public void copyKeysDataTo(row18Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row18Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class out_KalysStruct implements routines.system.IPersistableRow<out_KalysStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final out_KalysStruct other = (out_KalysStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out_KalysStruct other) {

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
	            
	}

	public void copyKeysDataTo(out_KalysStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out_KalysStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row17Struct implements routines.system.IPersistableRow<row17Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				
			    public String Sous_categorie;

				public String getSous_categorie () {
					return this.Sous_categorie;
				}
				
			    public String Categories_supplementaires;

				public String getCategories_supplementaires () {
					return this.Categories_supplementaires;
				}
				
			    public String Prix_avant_promo__TND;

				public String getPrix_avant_promo__TND () {
					return this.Prix_avant_promo__TND;
				}
				
			    public String Prix_apres_promo__TND;

				public String getPrix_apres_promo__TND () {
					return this.Prix_apres_promo__TND;
				}
				
			    public String En_promotion;

				public String getEn_promotion () {
					return this.En_promotion;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
					this.Sous_categorie = readString(dis);
					
					this.Categories_supplementaires = readString(dis);
					
					this.Prix_avant_promo__TND = readString(dis);
					
					this.Prix_apres_promo__TND = readString(dis);
					
					this.En_promotion = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
					this.Sous_categorie = readString(dis);
					
					this.Categories_supplementaires = readString(dis);
					
					this.Prix_avant_promo__TND = readString(dis);
					
					this.Prix_apres_promo__TND = readString(dis);
					
					this.En_promotion = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Sous_categorie,dos);
					
					// String
				
						writeString(this.Categories_supplementaires,dos);
					
					// String
				
						writeString(this.Prix_avant_promo__TND,dos);
					
					// String
				
						writeString(this.Prix_apres_promo__TND,dos);
					
					// String
				
						writeString(this.En_promotion,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Categorie,dos);
					
					// String
				
						writeString(this.Sous_categorie,dos);
					
					// String
				
						writeString(this.Categories_supplementaires,dos);
					
					// String
				
						writeString(this.Prix_avant_promo__TND,dos);
					
					// String
				
						writeString(this.Prix_apres_promo__TND,dos);
					
					// String
				
						writeString(this.En_promotion,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Categorie="+Categorie);
		sb.append(",Sous_categorie="+Sous_categorie);
		sb.append(",Categories_supplementaires="+Categories_supplementaires);
		sb.append(",Prix_avant_promo__TND="+Prix_avant_promo__TND);
		sb.append(",Prix_apres_promo__TND="+Prix_apres_promo__TND);
		sb.append(",En_promotion="+En_promotion);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row17Struct other) {

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

public static class row20Struct implements routines.system.IPersistableRow<row20Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row20Struct other = (row20Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row20Struct other) {

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
	            
	}

	public void copyKeysDataTo(row20Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row20Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class copyOfout_KalysStruct implements routines.system.IPersistableRow<copyOfout_KalysStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final copyOfout_KalysStruct other = (copyOfout_KalysStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(copyOfout_KalysStruct other) {

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
	            
	}

	public void copyKeysDataTo(copyOfout_KalysStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfout_KalysStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row19Struct implements routines.system.IPersistableRow<row19Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String Nom_du_fichier;

				public String getNom_du_fichier () {
					return this.Nom_du_fichier;
				}
				
			    public String N__Facture;

				public String getN__Facture () {
					return this.N__Facture;
				}
				
			    public String Date_Facture;

				public String getDate_Facture () {
					return this.Date_Facture;
				}
				
			    public String Nom_du_vendeur;

				public String getNom_du_vendeur () {
					return this.Nom_du_vendeur;
				}
				
			    public String Adresse_du_vendeur;

				public String getAdresse_du_vendeur () {
					return this.Adresse_du_vendeur;
				}
				
			    public String Matricule_Fiscale_du_vendeur;

				public String getMatricule_Fiscale_du_vendeur () {
					return this.Matricule_Fiscale_du_vendeur;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Reference;

				public String getReference () {
					return this.Reference;
				}
				
			    public String Quantite;

				public String getQuantite () {
					return this.Quantite;
				}
				
			    public String PU_HT;

				public String getPU_HT () {
					return this.PU_HT;
				}
				
			    public String TVA;

				public String getTVA () {
					return this.TVA;
				}
				
			    public String Total_HT;

				public String getTotal_HT () {
					return this.Total_HT;
				}
				
			    public String Montant_Total_net_a_payer;

				public String getMontant_Total_net_a_payer () {
					return this.Montant_Total_net_a_payer;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Nom_du_fichier = readString(dis);
					
					this.N__Facture = readString(dis);
					
					this.Date_Facture = readString(dis);
					
					this.Nom_du_vendeur = readString(dis);
					
					this.Adresse_du_vendeur = readString(dis);
					
					this.Matricule_Fiscale_du_vendeur = readString(dis);
					
					this.Produit = readString(dis);
					
					this.Reference = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.TVA = readString(dis);
					
					this.Total_HT = readString(dis);
					
					this.Montant_Total_net_a_payer = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Nom_du_fichier = readString(dis);
					
					this.N__Facture = readString(dis);
					
					this.Date_Facture = readString(dis);
					
					this.Nom_du_vendeur = readString(dis);
					
					this.Adresse_du_vendeur = readString(dis);
					
					this.Matricule_Fiscale_du_vendeur = readString(dis);
					
					this.Produit = readString(dis);
					
					this.Reference = readString(dis);
					
					this.Quantite = readString(dis);
					
					this.PU_HT = readString(dis);
					
					this.TVA = readString(dis);
					
					this.Total_HT = readString(dis);
					
					this.Montant_Total_net_a_payer = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Nom_du_fichier,dos);
					
					// String
				
						writeString(this.N__Facture,dos);
					
					// String
				
						writeString(this.Date_Facture,dos);
					
					// String
				
						writeString(this.Nom_du_vendeur,dos);
					
					// String
				
						writeString(this.Adresse_du_vendeur,dos);
					
					// String
				
						writeString(this.Matricule_Fiscale_du_vendeur,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
					// String
				
						writeString(this.Total_HT,dos);
					
					// String
				
						writeString(this.Montant_Total_net_a_payer,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Nom_du_fichier,dos);
					
					// String
				
						writeString(this.N__Facture,dos);
					
					// String
				
						writeString(this.Date_Facture,dos);
					
					// String
				
						writeString(this.Nom_du_vendeur,dos);
					
					// String
				
						writeString(this.Adresse_du_vendeur,dos);
					
					// String
				
						writeString(this.Matricule_Fiscale_du_vendeur,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Quantite,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// String
				
						writeString(this.TVA,dos);
					
					// String
				
						writeString(this.Total_HT,dos);
					
					// String
				
						writeString(this.Montant_Total_net_a_payer,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Nom_du_fichier="+Nom_du_fichier);
		sb.append(",N__Facture="+N__Facture);
		sb.append(",Date_Facture="+Date_Facture);
		sb.append(",Nom_du_vendeur="+Nom_du_vendeur);
		sb.append(",Adresse_du_vendeur="+Adresse_du_vendeur);
		sb.append(",Matricule_Fiscale_du_vendeur="+Matricule_Fiscale_du_vendeur);
		sb.append(",Produit="+Produit);
		sb.append(",Reference="+Reference);
		sb.append(",Quantite="+Quantite);
		sb.append(",PU_HT="+PU_HT);
		sb.append(",TVA="+TVA);
		sb.append(",Total_HT="+Total_HT);
		sb.append(",Montant_Total_net_a_payer="+Montant_Total_net_a_payer);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row19Struct other) {

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

public static class row22Struct implements routines.system.IPersistableRow<row22Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final row22Struct other = (row22Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row22Struct other) {

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
	            
	}

	public void copyKeysDataTo(row22Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row22Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class copyOfcopyOfout_KalysStruct implements routines.system.IPersistableRow<copyOfcopyOfout_KalysStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
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
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Produit_PK;
						
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
		final copyOfcopyOfout_KalysStruct other = (copyOfcopyOfout_KalysStruct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(copyOfcopyOfout_KalysStruct other) {

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
	            
	}

	public void copyKeysDataTo(copyOfcopyOfout_KalysStruct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
			        this.Produit_PK = dis.readInt();
					
					this.UGS = readString(dis);
					
					this.Nom_produit = readString(dis);
					
					this.Description = readString(dis);
					
					this.Url = readString(dis);
					
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
					
					this.Canal = readString(dis);
					
					this.Source = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Produit_PK);
					
					// String
				
						writeString(this.UGS,dos);
					
					// String
				
						writeString(this.Nom_produit,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Url,dos);
					
					// Boolean
				
						if(this.Visibilite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.Visibilite);
		            	}
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Prix_en_promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix_en_promo);
		            	}
					
					// Boolean
				
						if(this.En_Promo == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeBoolean(this.En_Promo);
		            	}
					
					// String
				
						writeString(this.Canal,dos);
					
					// String
				
						writeString(this.Source,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfcopyOfout_KalysStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Produit_PK, other.Produit_PK);
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

public static class row21Struct implements routines.system.IPersistableRow<row21Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[0];

	
			    public String Societe;

				public String getSociete () {
					return this.Societe;
				}
				
			    public String Nom_du_Fournisseur;

				public String getNom_du_Fournisseur () {
					return this.Nom_du_Fournisseur;
				}
				
			    public String N_Facture;

				public String getN_Facture () {
					return this.N_Facture;
				}
				
			    public String Date_de_facture;

				public String getDate_de_facture () {
					return this.Date_de_facture;
				}
				
			    public String Montant_HT;

				public String getMontant_HT () {
					return this.Montant_HT;
				}
				
			    public String Taux_de_TVA;

				public String getTaux_de_TVA () {
					return this.Taux_de_TVA;
				}
				
			    public String Timbre;

				public String getTimbre () {
					return this.Timbre;
				}
				
			    public String Montant_de_la_TVA;

				public String getMontant_de_la_TVA () {
					return this.Montant_de_la_TVA;
				}
				
			    public String FODEC;

				public String getFODEC () {
					return this.FODEC;
				}
				
			    public String Montant_TTC;

				public String getMontant_TTC () {
					return this.Montant_TTC;
				}
				
			    public String Retenue;

				public String getRetenue () {
					return this.Retenue;
				}
				
			    public String Versements_effectues_par_SOUGUI;

				public String getVersements_effectues_par_SOUGUI () {
					return this.Versements_effectues_par_SOUGUI;
				}
				
			    public String Reste_du;

				public String getReste_du () {
					return this.Reste_du;
				}
				
			    public String Action_a_effectuer;

				public String getAction_a_effectuer () {
					return this.Action_a_effectuer;
				}
				
			    public String Date_de_paiement_total_de_la_facture;

				public String getDate_de_paiement_total_de_la_facture () {
					return this.Date_de_paiement_total_de_la_facture;
				}
				
			    public String mode_de_reglement;

				public String getMode_de_reglement () {
					return this.mode_de_reglement;
				}
				
			    public String Date_de_la_premiere_relance;

				public String getDate_de_la_premiere_relance () {
					return this.Date_de_la_premiere_relance;
				}
				
			    public String Date_de_la_deuxieme_relance;

				public String getDate_de_la_deuxieme_relance () {
					return this.Date_de_la_deuxieme_relance;
				}
				
			    public String Penalite_et_majoration_de_paiement;

				public String getPenalite_et_majoration_de_paiement () {
					return this.Penalite_et_majoration_de_paiement;
				}
				
			    public String Commentaire;

				public String getCommentaire () {
					return this.Commentaire;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Produits, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Societe = readString(dis);
					
					this.Nom_du_Fournisseur = readString(dis);
					
					this.N_Facture = readString(dis);
					
					this.Date_de_facture = readString(dis);
					
					this.Montant_HT = readString(dis);
					
					this.Taux_de_TVA = readString(dis);
					
					this.Timbre = readString(dis);
					
					this.Montant_de_la_TVA = readString(dis);
					
					this.FODEC = readString(dis);
					
					this.Montant_TTC = readString(dis);
					
					this.Retenue = readString(dis);
					
					this.Versements_effectues_par_SOUGUI = readString(dis);
					
					this.Reste_du = readString(dis);
					
					this.Action_a_effectuer = readString(dis);
					
					this.Date_de_paiement_total_de_la_facture = readString(dis);
					
					this.mode_de_reglement = readString(dis);
					
					this.Date_de_la_premiere_relance = readString(dis);
					
					this.Date_de_la_deuxieme_relance = readString(dis);
					
					this.Penalite_et_majoration_de_paiement = readString(dis);
					
					this.Commentaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Produits) {

        	try {

        		int length = 0;
		
					this.Societe = readString(dis);
					
					this.Nom_du_Fournisseur = readString(dis);
					
					this.N_Facture = readString(dis);
					
					this.Date_de_facture = readString(dis);
					
					this.Montant_HT = readString(dis);
					
					this.Taux_de_TVA = readString(dis);
					
					this.Timbre = readString(dis);
					
					this.Montant_de_la_TVA = readString(dis);
					
					this.FODEC = readString(dis);
					
					this.Montant_TTC = readString(dis);
					
					this.Retenue = readString(dis);
					
					this.Versements_effectues_par_SOUGUI = readString(dis);
					
					this.Reste_du = readString(dis);
					
					this.Action_a_effectuer = readString(dis);
					
					this.Date_de_paiement_total_de_la_facture = readString(dis);
					
					this.mode_de_reglement = readString(dis);
					
					this.Date_de_la_premiere_relance = readString(dis);
					
					this.Date_de_la_deuxieme_relance = readString(dis);
					
					this.Penalite_et_majoration_de_paiement = readString(dis);
					
					this.Commentaire = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Societe,dos);
					
					// String
				
						writeString(this.Nom_du_Fournisseur,dos);
					
					// String
				
						writeString(this.N_Facture,dos);
					
					// String
				
						writeString(this.Date_de_facture,dos);
					
					// String
				
						writeString(this.Montant_HT,dos);
					
					// String
				
						writeString(this.Taux_de_TVA,dos);
					
					// String
				
						writeString(this.Timbre,dos);
					
					// String
				
						writeString(this.Montant_de_la_TVA,dos);
					
					// String
				
						writeString(this.FODEC,dos);
					
					// String
				
						writeString(this.Montant_TTC,dos);
					
					// String
				
						writeString(this.Retenue,dos);
					
					// String
				
						writeString(this.Versements_effectues_par_SOUGUI,dos);
					
					// String
				
						writeString(this.Reste_du,dos);
					
					// String
				
						writeString(this.Action_a_effectuer,dos);
					
					// String
				
						writeString(this.Date_de_paiement_total_de_la_facture,dos);
					
					// String
				
						writeString(this.mode_de_reglement,dos);
					
					// String
				
						writeString(this.Date_de_la_premiere_relance,dos);
					
					// String
				
						writeString(this.Date_de_la_deuxieme_relance,dos);
					
					// String
				
						writeString(this.Penalite_et_majoration_de_paiement,dos);
					
					// String
				
						writeString(this.Commentaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Societe,dos);
					
					// String
				
						writeString(this.Nom_du_Fournisseur,dos);
					
					// String
				
						writeString(this.N_Facture,dos);
					
					// String
				
						writeString(this.Date_de_facture,dos);
					
					// String
				
						writeString(this.Montant_HT,dos);
					
					// String
				
						writeString(this.Taux_de_TVA,dos);
					
					// String
				
						writeString(this.Timbre,dos);
					
					// String
				
						writeString(this.Montant_de_la_TVA,dos);
					
					// String
				
						writeString(this.FODEC,dos);
					
					// String
				
						writeString(this.Montant_TTC,dos);
					
					// String
				
						writeString(this.Retenue,dos);
					
					// String
				
						writeString(this.Versements_effectues_par_SOUGUI,dos);
					
					// String
				
						writeString(this.Reste_du,dos);
					
					// String
				
						writeString(this.Action_a_effectuer,dos);
					
					// String
				
						writeString(this.Date_de_paiement_total_de_la_facture,dos);
					
					// String
				
						writeString(this.mode_de_reglement,dos);
					
					// String
				
						writeString(this.Date_de_la_premiere_relance,dos);
					
					// String
				
						writeString(this.Date_de_la_deuxieme_relance,dos);
					
					// String
				
						writeString(this.Penalite_et_majoration_de_paiement,dos);
					
					// String
				
						writeString(this.Commentaire,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Societe="+Societe);
		sb.append(",Nom_du_Fournisseur="+Nom_du_Fournisseur);
		sb.append(",N_Facture="+N_Facture);
		sb.append(",Date_de_facture="+Date_de_facture);
		sb.append(",Montant_HT="+Montant_HT);
		sb.append(",Taux_de_TVA="+Taux_de_TVA);
		sb.append(",Timbre="+Timbre);
		sb.append(",Montant_de_la_TVA="+Montant_de_la_TVA);
		sb.append(",FODEC="+FODEC);
		sb.append(",Montant_TTC="+Montant_TTC);
		sb.append(",Retenue="+Retenue);
		sb.append(",Versements_effectues_par_SOUGUI="+Versements_effectues_par_SOUGUI);
		sb.append(",Reste_du="+Reste_du);
		sb.append(",Action_a_effectuer="+Action_a_effectuer);
		sb.append(",Date_de_paiement_total_de_la_facture="+Date_de_paiement_total_de_la_facture);
		sb.append(",mode_de_reglement="+mode_de_reglement);
		sb.append(",Date_de_la_premiere_relance="+Date_de_la_premiere_relance);
		sb.append(",Date_de_la_deuxieme_relance="+Date_de_la_deuxieme_relance);
		sb.append(",Penalite_et_majoration_de_paiement="+Penalite_et_majoration_de_paiement);
		sb.append(",Commentaire="+Commentaire);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row21Struct other) {

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
public void tDBInput_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_1_SUBPROCESS_STATE", 0);

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



		row1Struct row1 = new row1Struct();
WebScrappingStruct WebScrapping = new WebScrappingStruct();
row2Struct row2 = new row2Struct();
row2Struct row6 = row2;

		row3Struct row3 = new row3Struct();
row12Struct row12 = new row12Struct();
ProduitsInventaireStruct ProduitsInventaire = new ProduitsInventaireStruct();
row7Struct row7 = new row7Struct();

		row4Struct row4 = new row4Struct();
ProduitsCommandeVenteStruct ProduitsCommandeVente = new ProduitsCommandeVenteStruct();
row5Struct row5 = new row5Struct();
row5Struct row8 = row5;

		row14Struct row14 = new row14Struct();
factureVenteStruct factureVente = new factureVenteStruct();
row15Struct row15 = new row15Struct();

		row13Struct row13 = new row13Struct();
out_EdaytnaStruct out_Edaytna = new out_EdaytnaStruct();
row16Struct row16 = new row16Struct();

		row17Struct row17 = new row17Struct();
out_KalysStruct out_Kalys = new out_KalysStruct();
row18Struct row18 = new row18Struct();

		row19Struct row19 = new row19Struct();
copyOfout_KalysStruct copyOfout_Kalys = new copyOfout_KalysStruct();
row20Struct row20 = new row20Struct();

		row21Struct row21 = new row21Struct();
copyOfcopyOfout_KalysStruct copyOfcopyOfout_Kalys = new copyOfcopyOfout_KalysStruct();
row22Struct row22 = new row22Struct();

			row9Struct row9 = new row9Struct();
row10Struct row10 = new row10Struct();
row10Struct row11 = row10;






	
	/**
	 * [tDBSCD_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBSCD_1", false);
		start_Hash.put("tDBSCD_1", System.currentTimeMillis());
		
	
	currentComponent="tDBSCD_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row11");
					}
				
		int tos_count_tDBSCD_1 = 0;
		



        class SCDSK_tDBSCD_1 {
private int hashCode;
public boolean hashCodeDirty = true;
String Nom_produit;
String UGS;
public boolean equals(Object obj) {
if (this == obj) return true;
if (obj == null) return false;
if (getClass() != obj.getClass()) return false;
final SCDSK_tDBSCD_1 other = (SCDSK_tDBSCD_1) obj;
if (this.Nom_produit == null) {
if (other.Nom_produit!= null)
return false;
} else if (!this.Nom_produit.equals(other.Nom_produit))
return false;

if (this.UGS == null) {
if (other.UGS!= null)
return false;
} else if (!this.UGS.equals(other.UGS))
return false;

return true;
}
public int hashCode() {
if(hashCodeDirty) {
int prime = 31;hashCode = prime * hashCode + (Nom_produit == null ? 0 : Nom_produit.hashCode());
hashCode = prime * hashCode + (UGS == null ? 0 : UGS.hashCode());
hashCodeDirty = false;
}
return hashCode;
}
}

    class SCDStruct_tDBSCD_1 {
private String Description;
private Boolean En_Promo;
private String Url;
private Boolean Visibilite;
private String Canal;
private Double Prix;
private Double Prix_en_promo;
private String Source;
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

 
	final String decryptedPassword_tDBSCD_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:boxQec1mqhEtFgYpgBb0zfGUX77V18hBRL/o1+nkJvwdauLks4w=");

    String dbPwd_tDBSCD_1 = decryptedPassword_tDBSCD_1;	
    conn_tDBSCD_1 = java.sql.DriverManager.getConnection(url_tDBSCD_1,dbUser_tDBSCD_1,dbPwd_tDBSCD_1);
	

    if(dbschema_tDBSCD_1 == null || dbschema_tDBSCD_1.trim().length() == 0) {
        tableName_tDBSCD_1 = "Dim_Produit";
    } else {
        tableName_tDBSCD_1 = dbschema_tDBSCD_1 + "].[" + "Dim_Produit";
    }
	org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBSCD_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();	
    String tmpValue_tDBSCD_1 = null;    
        String search_tDBSCD_1 = "SELECT [Nom_produit], [UGS], [Description], [En_Promo], [Url], [Visibilite], [Canal], [Prix], [Prix_en_promo], [Source] FROM [" + tableName_tDBSCD_1 + "] WHERE [date_fin] IS NULL";
        java.sql.Statement statement_tDBSCD_1 = conn_tDBSCD_1.createStatement();
        java.sql.ResultSet resultSet_tDBSCD_1 = statement_tDBSCD_1.executeQuery(search_tDBSCD_1);
        java.util.Map<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1> cache_tDBSCD_1 = new java.util.HashMap<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1>();
        while(resultSet_tDBSCD_1.next()) {
            SCDSK_tDBSCD_1 sk_tDBSCD_1 = new SCDSK_tDBSCD_1();
            SCDStruct_tDBSCD_1 row_tDBSCD_1 = new SCDStruct_tDBSCD_1();
                    if(resultSet_tDBSCD_1.getObject(1) != null) {
                        sk_tDBSCD_1.Nom_produit = resultSet_tDBSCD_1.getString(1);
                    }
                    if(resultSet_tDBSCD_1.getObject(2) != null) {
                        sk_tDBSCD_1.UGS = resultSet_tDBSCD_1.getString(2);
                    }
                    if(resultSet_tDBSCD_1.getObject(3) != null) {
                        row_tDBSCD_1.Description = resultSet_tDBSCD_1.getString(3);
                    }
                    if(resultSet_tDBSCD_1.getObject(4) != null) {
                        row_tDBSCD_1.En_Promo = resultSet_tDBSCD_1.getBoolean(4);
                    }
                    if(resultSet_tDBSCD_1.getObject(5) != null) {
                        row_tDBSCD_1.Url = resultSet_tDBSCD_1.getString(5);
                    }
                    if(resultSet_tDBSCD_1.getObject(6) != null) {
                        row_tDBSCD_1.Visibilite = resultSet_tDBSCD_1.getBoolean(6);
                    }
                    if(resultSet_tDBSCD_1.getObject(7) != null) {
                        row_tDBSCD_1.Canal = resultSet_tDBSCD_1.getString(7);
                    }
                    if(resultSet_tDBSCD_1.getObject(8) != null) {
                        row_tDBSCD_1.Prix = resultSet_tDBSCD_1.getDouble(8);
                    }
                    if(resultSet_tDBSCD_1.getObject(9) != null) {
                        row_tDBSCD_1.Prix_en_promo = resultSet_tDBSCD_1.getDouble(9);
                    }
                    if(resultSet_tDBSCD_1.getObject(10) != null) {
                        row_tDBSCD_1.Source = resultSet_tDBSCD_1.getString(10);
                    }
            cache_tDBSCD_1.put(sk_tDBSCD_1, row_tDBSCD_1);
        }
        resultSet_tDBSCD_1.close();
        statement_tDBSCD_1.close();
    String insertionSQL_tDBSCD_1 = "INSERT INTO [" + tableName_tDBSCD_1 + "]([Nom_produit], [UGS], [Description], [En_Promo], [Url], [Visibilite], [Canal], [Prix], [Prix_en_promo], [Source], [actif], [date_debut], [date_fin]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)";
    java.sql.PreparedStatement insertionStatement_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(insertionSQL_tDBSCD_1);
            insertionStatement_tDBSCD_1.setTimestamp(11, new java.sql.Timestamp(start_Hash.get("tDBSCD_1")));
            insertionStatement_tDBSCD_1.setNull(12, java.sql.Types.DATE);
        String updateSQLForType1_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [Description] = ?, [En_Promo] = ?, [Url] = ?, [Visibilite] = ? WHERE [Nom_produit] = ? AND [UGS] = ?";
        java.sql.PreparedStatement updateForType1_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType1_tDBSCD_1);        
        String updateSQLForType2_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [date_fin] = ?, [actif] = 0 WHERE [Nom_produit] = ? AND [UGS] = ? AND [date_fin] IS NULL";
        java.sql.PreparedStatement updateForType2_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType2_tDBSCD_1);
            updateForType2_tDBSCD_1.setTimestamp(1, new java.sql.Timestamp(start_Hash.get("tDBSCD_1")));
    
        SCDSK_tDBSCD_1 lookUpKey_tDBSCD_1 = null;        
    SCDStruct_tDBSCD_1 lookUpValue_tDBSCD_1 = null;

 



/**
 * [tDBSCD_1 begin ] stop
 */



	
	/**
	 * [tLogRow_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_4", false);
		start_Hash.put("tLogRow_4", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row10");
					}
				
		int tos_count_tLogRow_4 = 0;
		

	///////////////////////
	
         class Util_tLogRow_4 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[11];

        public void addRow(String[] row) {

            for (int i = 0; i < 11; i++) {
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
                    for (k = 0; k < (totals + 10 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 10 - name.length() - k; i++) {
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
        			              
        			        sbformat.append("|%2$-");
        			        sbformat.append(colLengths[1]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%3$-");
        			        sbformat.append(colLengths[2]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%4$-");
        			        sbformat.append(colLengths[3]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%5$-");
        			        sbformat.append(colLengths[4]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%6$-");
        			        sbformat.append(colLengths[5]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%7$-");
        			        sbformat.append(colLengths[6]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%8$-");
        			        sbformat.append(colLengths[7]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%9$-");
        			        sbformat.append(colLengths[8]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%10$-");
        			        sbformat.append(colLengths[9]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%11$-");
        			        sbformat.append(colLengths[10]);
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
                    for (int i = 0; i < colLengths[0] - fillChars[0].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);	                

                    for (int i = 0; i < colLengths[1] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[2] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[3] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[4] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[5] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[6] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[7] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[8] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[9] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                
                    //last column
                    for (int i = 0; i < colLengths[10] - fillChars[1].length() + 1; i++) {
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
        Util_tLogRow_4 util_tLogRow_4 = new Util_tLogRow_4();
        util_tLogRow_4.setTableName("tLogRow_4");
        util_tLogRow_4.addRow(new String[]{"Produit_PK","UGS","Nom_produit","Description","Url","Visibilite","Prix","Prix_en_promo","En_Promo","Canal","Source",});        
 		StringBuilder strBuffer_tLogRow_4 = null;
		int nb_line_tLogRow_4 = 0;
///////////////////////    			



 



/**
 * [tLogRow_4 begin ] stop
 */



	
	/**
	 * [tUniqRow_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_4", false);
		start_Hash.put("tUniqRow_4", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row9");
					}
				
		int tos_count_tUniqRow_4 = 0;
		

	
		class KeyStruct_tUniqRow_4 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Nom_produit;        
	        
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
				final KeyStruct_tUniqRow_4 other = (KeyStruct_tUniqRow_4) obj;
				
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_4 = 0;
int nb_duplicates_tUniqRow_4 = 0;
KeyStruct_tUniqRow_4 finder_tUniqRow_4 = new KeyStruct_tUniqRow_4();
java.util.Set<KeyStruct_tUniqRow_4> keystUniqRow_4 = new java.util.HashSet<KeyStruct_tUniqRow_4>(); 

 



/**
 * [tUniqRow_4 begin ] stop
 */



	
	/**
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row8","row18","row16","row20","row6","row15","row7","row22");
					}
				
		int tos_count_tUnite_1 = 0;
		

int nb_line_tUnite_1 = 0;

 



/**
 * [tUnite_1 begin ] stop
 */



	
	/**
	 * [tLogRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_1", false);
		start_Hash.put("tLogRow_1", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row2");
					}
				
		int tos_count_tLogRow_1 = 0;
		

	///////////////////////
	
         class Util_tLogRow_1 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[11];

        public void addRow(String[] row) {

            for (int i = 0; i < 11; i++) {
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
                    for (k = 0; k < (totals + 10 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 10 - name.length() - k; i++) {
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
        			              
        			        sbformat.append("|%2$-");
        			        sbformat.append(colLengths[1]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%3$-");
        			        sbformat.append(colLengths[2]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%4$-");
        			        sbformat.append(colLengths[3]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%5$-");
        			        sbformat.append(colLengths[4]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%6$-");
        			        sbformat.append(colLengths[5]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%7$-");
        			        sbformat.append(colLengths[6]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%8$-");
        			        sbformat.append(colLengths[7]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%9$-");
        			        sbformat.append(colLengths[8]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%10$-");
        			        sbformat.append(colLengths[9]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%11$-");
        			        sbformat.append(colLengths[10]);
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
                    for (int i = 0; i < colLengths[0] - fillChars[0].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);	                

                    for (int i = 0; i < colLengths[1] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[2] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[3] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[4] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[5] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[6] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[7] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[8] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[9] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                
                    //last column
                    for (int i = 0; i < colLengths[10] - fillChars[1].length() + 1; i++) {
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
        util_tLogRow_1.addRow(new String[]{"Produit_PK","UGS","Nom_produit","Description","Url","Visibilite","Prix","Prix_en_promo","En_Promo","Canal","Source",});        
 		StringBuilder strBuffer_tLogRow_1 = null;
		int nb_line_tLogRow_1 = 0;
///////////////////////    			



 



/**
 * [tLogRow_1 begin ] stop
 */



	
	/**
	 * [tUniqRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_1", false);
		start_Hash.put("tUniqRow_1", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"WebScrapping");
					}
				
		int tos_count_tUniqRow_1 = 0;
		

	
		class KeyStruct_tUniqRow_1 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_1 other = (KeyStruct_tUniqRow_1) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_1 = 0;
int nb_duplicates_tUniqRow_1 = 0;
KeyStruct_tUniqRow_1 finder_tUniqRow_1 = new KeyStruct_tUniqRow_1();
java.util.Set<KeyStruct_tUniqRow_1> keystUniqRow_1 = new java.util.HashSet<KeyStruct_tUniqRow_1>(); 

 



/**
 * [tUniqRow_1 begin ] stop
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
WebScrappingStruct WebScrapping_tmp = new WebScrappingStruct();
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
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:UKoZRu5Cz0kJGE+ZkkbSv9YbwEQeDNK3xkWnnPuv+1GEN3Bdh78=");
				
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

		    String dbquery_tDBInput_1 = "SELECT produits_sougui_webscraping.titre,\n		produits_sougui_webscraping.url,\n		produits_sougui_webscraping.pack,\n		prod"
+"uits_sougui_webscraping.prix_avant_promo,\n		produits_sougui_webscraping.prix_apres_promo,\n		produits_sougui_webscraping."
+"categories,\n		produits_sougui_webscraping.image,\n		produits_sougui_webscraping.en_promotion,\n		produits_sougui_webscrapi"
+"ng.sku\nFROM	produits_sougui_webscraping";
		    

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
								row1.titre = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(1);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.titre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.titre = tmpContent_tDBInput_1;
                }
            } else {
                row1.titre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 2) {
								row1.url = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(2);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.url = tmpContent_tDBInput_1;
                }
            } else {
                row1.url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 3) {
								row1.pack = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(3);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.pack = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.pack = tmpContent_tDBInput_1;
                }
            } else {
                row1.pack = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 4) {
								row1.prix_avant_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(4);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.prix_avant_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.prix_avant_promo = tmpContent_tDBInput_1;
                }
            } else {
                row1.prix_avant_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 5) {
								row1.prix_apres_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(5);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.prix_apres_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.prix_apres_promo = tmpContent_tDBInput_1;
                }
            } else {
                row1.prix_apres_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 6) {
								row1.categories = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(6);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.categories = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.categories = tmpContent_tDBInput_1;
                }
            } else {
                row1.categories = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 7) {
								row1.image = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(7);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.image = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.image = tmpContent_tDBInput_1;
                }
            } else {
                row1.image = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 8) {
								row1.en_promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(8);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.en_promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.en_promotion = tmpContent_tDBInput_1;
                }
            } else {
                row1.en_promotion = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 9) {
								row1.sku = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(9);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.sku = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.sku = tmpContent_tDBInput_1;
                }
            } else {
                row1.sku = null;
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

WebScrapping = null;


// # Output table : 'WebScrapping'
WebScrapping_tmp.Produit_PK = 0;
WebScrapping_tmp.UGS = row1.sku==null? SKUGenerator.generateSKU(row1.titre):
row1.pack.replace(" ", "").equals("N/A") ?row1.sku : row1.sku+"-"+row1.pack.replace(" ", "") ;
WebScrapping_tmp.Nom_produit = row1.pack.replace(" ","" ).equals("N/A") ? 
    (row1.titre != null ? row1.titre.replace("?", "") : "") : 
    (row1.titre != null ? row1.titre.replace("?", "") + "-" + row1.pack.replace(" " ,"" ) : "-" + row1.pack) ;
WebScrapping_tmp.Description = "unknown";
WebScrapping_tmp.Url = row1.url ;
WebScrapping_tmp.Visibilite = true;
WebScrapping_tmp.Prix = Double.parseDouble(
    row1.prix_avant_promo.replaceAll("[^0-9,\\.]", "").replace(',', '.')
) ;
WebScrapping_tmp.Prix_en_promo = Double.parseDouble(
    row1.prix_apres_promo.replaceAll("[^0-9,\\.]", "").replace(',', '.')
) ;
WebScrapping_tmp.En_Promo = Double.parseDouble(
    row1.prix_avant_promo.replaceAll("[^0-9,\\.]", "").replace(',', '.')
)==Double.parseDouble(
    row1.prix_apres_promo.replaceAll("[^0-9,\\.]", "").replace(',', '.')
)?false:true ;
WebScrapping_tmp.Canal = "Site Web";
WebScrapping_tmp.Source = "Sougui";
WebScrapping = WebScrapping_tmp;
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
// Start of branch "WebScrapping"
if(WebScrapping != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"WebScrapping"
						
						);
					}
					
row2 = null;			
if(WebScrapping.UGS == null){
	finder_tUniqRow_1.UGS = null;
}else{
	finder_tUniqRow_1.UGS = WebScrapping.UGS.toLowerCase();
}
if(WebScrapping.Nom_produit == null){
	finder_tUniqRow_1.Nom_produit = null;
}else{
	finder_tUniqRow_1.Nom_produit = WebScrapping.Nom_produit.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(WebScrapping.UGS == null){
	new_tUniqRow_1.UGS = null;
}else{
	new_tUniqRow_1.UGS = WebScrapping.UGS.toLowerCase();
}
if(WebScrapping.Nom_produit == null){
	new_tUniqRow_1.Nom_produit = null;
}else{
	new_tUniqRow_1.Nom_produit = WebScrapping.Nom_produit.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row2 == null){ 
	
	row2 = new row2Struct();
}row2.Produit_PK = WebScrapping.Produit_PK;			row2.UGS = WebScrapping.UGS;			row2.Nom_produit = WebScrapping.Nom_produit;			row2.Description = WebScrapping.Description;			row2.Url = WebScrapping.Url;			row2.Visibilite = WebScrapping.Visibilite;			row2.Prix = WebScrapping.Prix;			row2.Prix_en_promo = WebScrapping.Prix_en_promo;			row2.En_Promo = WebScrapping.En_Promo;			row2.Canal = WebScrapping.Canal;			row2.Source = WebScrapping.Source;					
		nb_uniques_tUniqRow_1++;
	} else {
	  nb_duplicates_tUniqRow_1++;
	}

 


	tos_count_tUniqRow_1++;

/**
 * [tUniqRow_1 main ] stop
 */
	
	/**
	 * [tUniqRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_begin ] stop
 */
// Start of branch "row2"
if(row2 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row2"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[11];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(row2.Produit_PK)			
					          ;	
										
    			   				
	    		if(row2.UGS != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(row2.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Nom_produit != null) { //              
                 row_tLogRow_1[2]=    						    
				                String.valueOf(row2.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Description != null) { //              
                 row_tLogRow_1[3]=    						    
				                String.valueOf(row2.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Url != null) { //              
                 row_tLogRow_1[4]=    						    
				                String.valueOf(row2.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Visibilite != null) { //              
                 row_tLogRow_1[5]=    						    
				                String.valueOf(row2.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Prix != null) { //              
                 row_tLogRow_1[6]=    						
								FormatterUtils.formatUnwithE(row2.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Prix_en_promo != null) { //              
                 row_tLogRow_1[7]=    						
								FormatterUtils.formatUnwithE(row2.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row2.En_Promo != null) { //              
                 row_tLogRow_1[8]=    						    
				                String.valueOf(row2.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Canal != null) { //              
                 row_tLogRow_1[9]=    						    
				                String.valueOf(row2.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row2.Source != null) { //              
                 row_tLogRow_1[10]=    						    
				                String.valueOf(row2.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 
     row6 = row2;


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
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row6"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row6.Produit_PK;								
			row9.UGS = row6.UGS;								
			row9.Nom_produit = row6.Nom_produit;								
			row9.Description = row6.Description;								
			row9.Url = row6.Url;								
			row9.Visibilite = row6.Visibilite;								
			row9.Prix = row6.Prix;								
			row9.Prix_en_promo = row6.Prix_en_promo;								
			row9.En_Promo = row6.En_Promo;								
			row9.Canal = row6.Canal;								
			row9.Source = row6.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "row2"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "WebScrapping"




	
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
	 * [tUniqRow_1 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

globalMap.put("tUniqRow_1_NB_UNIQUES",nb_uniques_tUniqRow_1);
globalMap.put("tUniqRow_1_NB_DUPLICATES",nb_duplicates_tUniqRow_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"WebScrapping");
			  	}
			  	
 

ok_Hash.put("tUniqRow_1", true);
end_Hash.put("tUniqRow_1", System.currentTimeMillis());




/**
 * [tUniqRow_1 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row2");
			  	}
			  	
 

ok_Hash.put("tLogRow_1", true);
end_Hash.put("tLogRow_1", System.currentTimeMillis());




/**
 * [tLogRow_1 end ] stop
 */












	
	/**
	 * [tUniqRow_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_2", false);
		start_Hash.put("tUniqRow_2", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"ProduitsInventaire");
					}
				
		int tos_count_tUniqRow_2 = 0;
		

	
		class KeyStruct_tUniqRow_2 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_2 other = (KeyStruct_tUniqRow_2) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_2 = 0;
int nb_duplicates_tUniqRow_2 = 0;
KeyStruct_tUniqRow_2 finder_tUniqRow_2 = new KeyStruct_tUniqRow_2();
java.util.Set<KeyStruct_tUniqRow_2> keystUniqRow_2 = new java.util.HashSet<KeyStruct_tUniqRow_2>(); 

 



/**
 * [tUniqRow_2 begin ] stop
 */



	
	/**
	 * [tMap_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_2", false);
		start_Hash.put("tMap_2", System.currentTimeMillis());
		
	
	currentComponent="tMap_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row12");
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
ProduitsInventaireStruct ProduitsInventaire_tmp = new ProduitsInventaireStruct();
// ###############################

        
        



        









 



/**
 * [tMap_2 begin ] stop
 */



	
	/**
	 * [tFilterRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tFilterRow_1", false);
		start_Hash.put("tFilterRow_1", System.currentTimeMillis());
		
	
	currentComponent="tFilterRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row3");
					}
				
		int tos_count_tFilterRow_1 = 0;
		
    int nb_line_tFilterRow_1 = 0;
    int nb_line_ok_tFilterRow_1 = 0;
    int nb_line_reject_tFilterRow_1 = 0;

    class Operator_tFilterRow_1 {
      private String sErrorMsg = "";
      private boolean bMatchFlag = true;
      private String sUnionFlag = "&&";

      public Operator_tFilterRow_1(String unionFlag){
        sUnionFlag = unionFlag;
        bMatchFlag =  "||".equals(unionFlag) ? false : true;
      }

      public String getErrorMsg() {
        if (sErrorMsg != null && sErrorMsg.length() > 1)
          return sErrorMsg.substring(1);
        else 
          return null;
      }

      public boolean getMatchFlag() {
        return bMatchFlag;
      }

      public void matches(boolean partMatched, String reason) {
        // no need to care about the next judgement
        if ("||".equals(sUnionFlag) && bMatchFlag){
          return;
        }

        if (!partMatched) {
          sErrorMsg += "|" + reason;
        }

        if ("||".equals(sUnionFlag))
          bMatchFlag = bMatchFlag || partMatched;
        else
          bMatchFlag = bMatchFlag && partMatched;
      }
    }

 



/**
 * [tFilterRow_1 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:+b093ylE7eBOSkv477Y5oU3wNHYPC4vng+YbU42TfU+kL0cOO3o=");
				
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

		    String dbquery_tDBInput_2 = "SELECT ERP_Inventaire.Reference_Produit,\n		ERP_Inventaire.Nom_Produit,\n		ERP_Inventaire.Stock_Initial,\n		ERP_Inventaire"
+".Entrees,\n		ERP_Inventaire.Sorties,\n		ERP_Inventaire.Stock_Actuel,\n		ERP_Inventaire.Categorie,\n		ERP_Inventaire.Fourniss"
+"eur,\n		ERP_Inventaire.Prix_achat,\n		ERP_Inventaire.Prix_unitaire\nFROM	ERP_Inventaire";
		    

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
								row3.Reference_Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Reference_Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Reference_Produit = tmpContent_tDBInput_2;
                }
            } else {
                row3.Reference_Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row3.Nom_Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Nom_Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Nom_Produit = tmpContent_tDBInput_2;
                }
            } else {
                row3.Nom_Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row3.Stock_Initial = null;
							} else {
		                          
            row3.Stock_Initial = rs_tDBInput_2.getInt(3);
            if(rs_tDBInput_2.wasNull()){
                    row3.Stock_Initial = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row3.Entrees = null;
							} else {
		                          
            row3.Entrees = rs_tDBInput_2.getInt(4);
            if(rs_tDBInput_2.wasNull()){
                    row3.Entrees = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row3.Sorties = null;
							} else {
		                          
            row3.Sorties = rs_tDBInput_2.getInt(5);
            if(rs_tDBInput_2.wasNull()){
                    row3.Sorties = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row3.Stock_Actuel = null;
							} else {
		                          
            row3.Stock_Actuel = rs_tDBInput_2.getInt(6);
            if(rs_tDBInput_2.wasNull()){
                    row3.Stock_Actuel = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row3.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Categorie = tmpContent_tDBInput_2;
                }
            } else {
                row3.Categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row3.Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Fournisseur = tmpContent_tDBInput_2;
                }
            } else {
                row3.Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row3.Prix_achat = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Prix_achat = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Prix_achat = tmpContent_tDBInput_2;
                }
            } else {
                row3.Prix_achat = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row3.Prix_unitaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(10);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.Prix_unitaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row3.Prix_unitaire = tmpContent_tDBInput_2;
                }
            } else {
                row3.Prix_unitaire = null;
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
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
						);
					}
					

          row12 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("||");
	        ope_tFilterRow_1.matches((row3.Reference_Produit != null)
	                       , "Reference_Produit!=null failed");
	        ope_tFilterRow_1.matches((row3.Nom_Produit != null)
	                       , "Nom_Produit!=null failed");
	        ope_tFilterRow_1.matches((row3.Reference_Produit != null)
	                       , "Reference_Produit!=null failed");
	        ope_tFilterRow_1.matches((row3.Stock_Initial != null)
	                       , "Stock_Initial!=null failed");
	        ope_tFilterRow_1.matches((row3.Entrees != null)
	                       , "Entrees!=null failed");
	        ope_tFilterRow_1.matches((row3.Sorties != null)
	                       , "Sorties!=null failed");
	        ope_tFilterRow_1.matches((row3.Stock_Actuel != null)
	                       , "Stock_Actuel!=null failed");
	        ope_tFilterRow_1.matches((row3.Categorie != null)
	                       , "Categorie!=null failed");
	        ope_tFilterRow_1.matches((row3.Fournisseur != null)
	                       , "Fournisseur!=null failed");
	        ope_tFilterRow_1.matches((row3.Prix_achat != null)
	                       , "Prix_achat!=null failed");
	        ope_tFilterRow_1.matches((row3.Prix_unitaire != null)
	                       , "Prix_unitaire!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row12 == null){ 
                row12 = new row12Struct();
              }
               row12.Reference_Produit = row3.Reference_Produit;
               row12.Nom_Produit = row3.Nom_Produit;
               row12.Stock_Initial = row3.Stock_Initial;
               row12.Entrees = row3.Entrees;
               row12.Sorties = row3.Sorties;
               row12.Stock_Actuel = row3.Stock_Actuel;
               row12.Categorie = row3.Categorie;
               row12.Fournisseur = row3.Fournisseur;
               row12.Prix_achat = row3.Prix_achat;
               row12.Prix_unitaire = row3.Prix_unitaire;    
      nb_line_ok_tFilterRow_1++;
    } else {
      nb_line_reject_tFilterRow_1++;
    }

nb_line_tFilterRow_1++;

 


	tos_count_tFilterRow_1++;

/**
 * [tFilterRow_1 main ] stop
 */
	
	/**
	 * [tFilterRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_begin ] stop
 */
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tMap_2 main ] start
	 */

	

	
	
	currentComponent="tMap_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
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

ProduitsInventaire = null;


// # Output table : 'ProduitsInventaire'
ProduitsInventaire_tmp.Produit_PK = 0;
ProduitsInventaire_tmp.UGS = row12.Reference_Produit==null? SKUGenerator.generateSKU(row12.Nom_Produit):row12.Reference_Produit ;
ProduitsInventaire_tmp.Nom_produit = row12.Nom_Produit ;
ProduitsInventaire_tmp.Description = "unknown";
ProduitsInventaire_tmp.Url = "unknown";
ProduitsInventaire_tmp.Visibilite = false;
ProduitsInventaire_tmp.Prix = row12.Prix_unitaire != null && !row12.Prix_unitaire.trim().isEmpty() ? 
    Double.parseDouble(row12.Prix_unitaire.replace(",", ".")) : 
    0.0 ;
ProduitsInventaire_tmp.Prix_en_promo = row12.Prix_unitaire != null && !row12.Prix_unitaire.trim().isEmpty() ? 
    Double.parseDouble(row12.Prix_unitaire.replace(",", ".")) : 
    0.0 ;
ProduitsInventaire_tmp.En_Promo = false;
ProduitsInventaire_tmp.Canal = "Site Web";
ProduitsInventaire_tmp.Source = "Sougui";
ProduitsInventaire = ProduitsInventaire_tmp;
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
// Start of branch "ProduitsInventaire"
if(ProduitsInventaire != null) { 



	
	/**
	 * [tUniqRow_2 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"ProduitsInventaire"
						
						);
					}
					
row7 = null;			
if(ProduitsInventaire.UGS == null){
	finder_tUniqRow_2.UGS = null;
}else{
	finder_tUniqRow_2.UGS = ProduitsInventaire.UGS.toLowerCase();
}
if(ProduitsInventaire.Nom_produit == null){
	finder_tUniqRow_2.Nom_produit = null;
}else{
	finder_tUniqRow_2.Nom_produit = ProduitsInventaire.Nom_produit.toLowerCase();
}	
finder_tUniqRow_2.hashCodeDirty = true;
if (!keystUniqRow_2.contains(finder_tUniqRow_2)) {
		KeyStruct_tUniqRow_2 new_tUniqRow_2 = new KeyStruct_tUniqRow_2();

		
if(ProduitsInventaire.UGS == null){
	new_tUniqRow_2.UGS = null;
}else{
	new_tUniqRow_2.UGS = ProduitsInventaire.UGS.toLowerCase();
}
if(ProduitsInventaire.Nom_produit == null){
	new_tUniqRow_2.Nom_produit = null;
}else{
	new_tUniqRow_2.Nom_produit = ProduitsInventaire.Nom_produit.toLowerCase();
}
		
		keystUniqRow_2.add(new_tUniqRow_2);if(row7 == null){ 
	
	row7 = new row7Struct();
}row7.Produit_PK = ProduitsInventaire.Produit_PK;			row7.UGS = ProduitsInventaire.UGS;			row7.Nom_produit = ProduitsInventaire.Nom_produit;			row7.Description = ProduitsInventaire.Description;			row7.Url = ProduitsInventaire.Url;			row7.Visibilite = ProduitsInventaire.Visibilite;			row7.Prix = ProduitsInventaire.Prix;			row7.Prix_en_promo = ProduitsInventaire.Prix_en_promo;			row7.En_Promo = ProduitsInventaire.En_Promo;			row7.Canal = ProduitsInventaire.Canal;			row7.Source = ProduitsInventaire.Source;					
		nb_uniques_tUniqRow_2++;
	} else {
	  nb_duplicates_tUniqRow_2++;
	}

 


	tos_count_tUniqRow_2++;

/**
 * [tUniqRow_2 main ] stop
 */
	
	/**
	 * [tUniqRow_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

 



/**
 * [tUniqRow_2 process_data_begin ] stop
 */
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row7"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row7.Produit_PK;								
			row9.UGS = row7.UGS;								
			row9.Nom_produit = row7.Nom_produit;								
			row9.Description = row7.Description;								
			row9.Url = row7.Url;								
			row9.Visibilite = row7.Visibilite;								
			row9.Prix = row7.Prix;								
			row9.Prix_en_promo = row7.Prix_en_promo;								
			row9.En_Promo = row7.En_Promo;								
			row9.Canal = row7.Canal;								
			row9.Source = row7.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tUniqRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

 



/**
 * [tUniqRow_2 process_data_end ] stop
 */

} // End of branch "ProduitsInventaire"




	
	/**
	 * [tMap_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 process_data_end ] stop
 */

} // End of branch "row12"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
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
	 * [tFilterRow_1 end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
    globalMap.put("tFilterRow_1_NB_LINE", nb_line_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_OK", nb_line_ok_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_REJECT", nb_line_reject_tFilterRow_1);
    

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row3");
			  	}
			  	
 

ok_Hash.put("tFilterRow_1", true);
end_Hash.put("tFilterRow_1", System.currentTimeMillis());




/**
 * [tFilterRow_1 end ] stop
 */

	
	/**
	 * [tMap_2 end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row12");
			  	}
			  	
 

ok_Hash.put("tMap_2", true);
end_Hash.put("tMap_2", System.currentTimeMillis());




/**
 * [tMap_2 end ] stop
 */

	
	/**
	 * [tUniqRow_2 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

globalMap.put("tUniqRow_2_NB_UNIQUES",nb_uniques_tUniqRow_2);
globalMap.put("tUniqRow_2_NB_DUPLICATES",nb_duplicates_tUniqRow_2);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"ProduitsInventaire");
			  	}
			  	
 

ok_Hash.put("tUniqRow_2", true);
end_Hash.put("tUniqRow_2", System.currentTimeMillis());




/**
 * [tUniqRow_2 end ] stop
 */












	
	/**
	 * [tLogRow_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_2", false);
		start_Hash.put("tLogRow_2", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
					}
				
		int tos_count_tLogRow_2 = 0;
		

	///////////////////////
	
         class Util_tLogRow_2 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[11];

        public void addRow(String[] row) {

            for (int i = 0; i < 11; i++) {
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
                    for (k = 0; k < (totals + 10 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 10 - name.length() - k; i++) {
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
        			              
        			        sbformat.append("|%2$-");
        			        sbformat.append(colLengths[1]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%3$-");
        			        sbformat.append(colLengths[2]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%4$-");
        			        sbformat.append(colLengths[3]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%5$-");
        			        sbformat.append(colLengths[4]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%6$-");
        			        sbformat.append(colLengths[5]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%7$-");
        			        sbformat.append(colLengths[6]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%8$-");
        			        sbformat.append(colLengths[7]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%9$-");
        			        sbformat.append(colLengths[8]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%10$-");
        			        sbformat.append(colLengths[9]);
        			        sbformat.append("s");
        			              
        			        sbformat.append("|%11$-");
        			        sbformat.append(colLengths[10]);
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
                    for (int i = 0; i < colLengths[0] - fillChars[0].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);	                

                    for (int i = 0; i < colLengths[1] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[2] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[3] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[4] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[5] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[6] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[7] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[8] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                    for (int i = 0; i < colLengths[9] - fillChars[3].length() + 1; i++) {
                        sb.append(fillChars[2]);
                    }
                    sb.append(fillChars[3]);
                
                    //last column
                    for (int i = 0; i < colLengths[10] - fillChars[1].length() + 1; i++) {
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
        Util_tLogRow_2 util_tLogRow_2 = new Util_tLogRow_2();
        util_tLogRow_2.setTableName("tLogRow_2");
        util_tLogRow_2.addRow(new String[]{"Produit_PK","UGS","Nom_produit","Description","Url","Visibilite","Prix","Prix_en_promo","En_Promo","Canal","Source",});        
 		StringBuilder strBuffer_tLogRow_2 = null;
		int nb_line_tLogRow_2 = 0;
///////////////////////    			



 



/**
 * [tLogRow_2 begin ] stop
 */



	
	/**
	 * [tUniqRow_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_3", false);
		start_Hash.put("tUniqRow_3", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"ProduitsCommandeVente");
					}
				
		int tos_count_tUniqRow_3 = 0;
		

	
		class KeyStruct_tUniqRow_3 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_3 other = (KeyStruct_tUniqRow_3) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_3 = 0;
int nb_duplicates_tUniqRow_3 = 0;
KeyStruct_tUniqRow_3 finder_tUniqRow_3 = new KeyStruct_tUniqRow_3();
java.util.Set<KeyStruct_tUniqRow_3> keystUniqRow_3 = new java.util.HashSet<KeyStruct_tUniqRow_3>(); 

 



/**
 * [tUniqRow_3 begin ] stop
 */



	
	/**
	 * [tMap_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_3", false);
		start_Hash.put("tMap_3", System.currentTimeMillis());
		
	
	currentComponent="tMap_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row4");
					}
				
		int tos_count_tMap_3 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_3__Struct  {
}
Var__tMap_3__Struct Var__tMap_3 = new Var__tMap_3__Struct();
// ###############################

// ###############################
// # Outputs initialization
ProduitsCommandeVenteStruct ProduitsCommandeVente_tmp = new ProduitsCommandeVenteStruct();
// ###############################

        
        



        









 



/**
 * [tMap_3 begin ] stop
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
				String dbUser_tDBInput_3 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:mXFf7Yr1u4UnzqUFmr8eoEd4QowUzpLtpTNA8ThQWtubbIye+YM=");
				
				String dbPwd_tDBInput_3 = decryptedPassword_tDBInput_3;
				
		    String port_tDBInput_3 = "1433";
		    String dbname_tDBInput_3 = "PA_BI_SA" ;
			String url_tDBInput_3 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_3)) {
		    	url_tDBInput_3 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_3)) {
				url_tDBInput_3 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_3 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_3 = "";
				
				conn_tDBInput_3 = java.sql.DriverManager.getConnection(url_tDBInput_3,dbUser_tDBInput_3,dbPwd_tDBInput_3);
		        
		    
			java.sql.Statement stmt_tDBInput_3 = conn_tDBInput_3.createStatement();

		    String dbquery_tDBInput_3 = "SELECT \"Commandes(produit_par_ligne)\".\"Numéro de commande\",\n		\"Commandes(produit_par_ligne)\".\"État de la command"
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
								row4.Numero_de_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(1);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Numero_de_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Numero_de_commande = tmpContent_tDBInput_3;
                }
            } else {
                row4.Numero_de_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row4.Etat_de_la_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Etat_de_la_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Etat_de_la_commande = tmpContent_tDBInput_3;
                }
            } else {
                row4.Etat_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row4.Date_de_commande = null;
							} else {
										
			row4.Date_de_commande = mssqlGTU_tDBInput_3.getDate(rsmd_tDBInput_3, rs_tDBInput_3, 3);
			
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row4.Note_du_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Note_du_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Note_du_client = tmpContent_tDBInput_3;
                }
            } else {
                row4.Note_du_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 5) {
								row4.Prenom__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(5);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Prenom__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Prenom__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Prenom__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 6) {
								row4.NOM_DE_FAMILLE__FACTURATION = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(6);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.NOM_DE_FAMILLE__FACTURATION = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.NOM_DE_FAMILLE__FACTURATION = tmpContent_tDBInput_3;
                }
            } else {
                row4.NOM_DE_FAMILLE__FACTURATION = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 7) {
								row4.Societe__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(7);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Societe__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Societe__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Societe__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 8) {
								row4.Adresse_1___2__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(8);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Adresse_1___2__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Adresse_1___2__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Adresse_1___2__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 9) {
								row4.Ville__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(9);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Ville__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Ville__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Ville__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 10) {
								row4.Code_de_l_etat__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(10);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_de_l_etat__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_de_l_etat__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_de_l_etat__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 11) {
								row4.Code_postal__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(11);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_postal__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_postal__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_postal__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 12) {
								row4.Code_du_pays__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(12);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_du_pays__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_du_pays__Facturation = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_du_pays__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 13) {
								row4.Prenom__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(13);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Prenom__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Prenom__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Prenom__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 14) {
								row4.NOM_DE_FAMILLE__LIVRAISON = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(14);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.NOM_DE_FAMILLE__LIVRAISON = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.NOM_DE_FAMILLE__LIVRAISON = tmpContent_tDBInput_3;
                }
            } else {
                row4.NOM_DE_FAMILLE__LIVRAISON = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 15) {
								row4.Adresse_1___2__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(15);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Adresse_1___2__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Adresse_1___2__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Adresse_1___2__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 16) {
								row4.Ville__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(16);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Ville__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Ville__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Ville__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 17) {
								row4.Code_de_l_etat__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(17);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_de_l_etat__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_de_l_etat__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_de_l_etat__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 18) {
								row4.Code_postal__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(18);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_postal__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_postal__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_postal__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 19) {
								row4.Code_du_pays__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(19);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_du_pays__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_du_pays__Livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_du_pays__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 20) {
								row4.Titre_de_la_methode_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(20);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Titre_de_la_methode_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Titre_de_la_methode_de_paiement = tmpContent_tDBInput_3;
                }
            } else {
                row4.Titre_de_la_methode_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 21) {
								row4.Montant_de_la_remise_panier = null;
							} else {
		                          
            row4.Montant_de_la_remise_panier = rs_tDBInput_3.getBigDecimal(21);
            if(rs_tDBInput_3.wasNull()){
                    row4.Montant_de_la_remise_panier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 22) {
								row4.Cart_Discount_Amount_inc__tax = null;
							} else {
		                          
            row4.Cart_Discount_Amount_inc__tax = rs_tDBInput_3.getBigDecimal(22);
            if(rs_tDBInput_3.wasNull()){
                    row4.Cart_Discount_Amount_inc__tax = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 23) {
								row4.Sous_total_de_la_commande = null;
							} else {
		                          
            row4.Sous_total_de_la_commande = rs_tDBInput_3.getBigDecimal(23);
            if(rs_tDBInput_3.wasNull()){
                    row4.Sous_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 24) {
								row4.Titre_de_la_methode_de_livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(24);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(24).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Titre_de_la_methode_de_livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Titre_de_la_methode_de_livraison = tmpContent_tDBInput_3;
                }
            } else {
                row4.Titre_de_la_methode_de_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 25) {
								row4.Montant_de_la_livraison = null;
							} else {
		                          
            row4.Montant_de_la_livraison = rs_tDBInput_3.getBigDecimal(25);
            if(rs_tDBInput_3.wasNull()){
                    row4.Montant_de_la_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 26) {
								row4.Montant_du_remboursement_de_la_commande = null;
							} else {
		                          
            row4.Montant_du_remboursement_de_la_commande = rs_tDBInput_3.getBigDecimal(26);
            if(rs_tDBInput_3.wasNull()){
                    row4.Montant_du_remboursement_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 27) {
								row4.Montant_total_de_la_commande = null;
							} else {
		                          
            row4.Montant_total_de_la_commande = rs_tDBInput_3.getBigDecimal(27);
            if(rs_tDBInput_3.wasNull()){
                    row4.Montant_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 28) {
								row4.Montant_total_de_la_taxe = null;
							} else {
		                          
            row4.Montant_total_de_la_taxe = rs_tDBInput_3.getBigDecimal(28);
            if(rs_tDBInput_3.wasNull()){
                    row4.Montant_total_de_la_taxe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 29) {
								row4.UGS = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(29);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(29).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.UGS = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.UGS = tmpContent_tDBInput_3;
                }
            } else {
                row4.UGS = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 30) {
								row4.Article = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(30);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(30).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Article = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Article = tmpContent_tDBInput_3;
                }
            } else {
                row4.Article = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 31) {
								row4.Nom_de_l_element = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(31);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(31).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Nom_de_l_element = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Nom_de_l_element = tmpContent_tDBInput_3;
                }
            } else {
                row4.Nom_de_l_element = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 32) {
								row4.Quantite____Remboursement = null;
							} else {
		                          
            row4.Quantite____Remboursement = rs_tDBInput_3.getInt(32);
            if(rs_tDBInput_3.wasNull()){
                    row4.Quantite____Remboursement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 33) {
								row4.Prix_du_produit = null;
							} else {
		                          
            row4.Prix_du_produit = rs_tDBInput_3.getBigDecimal(33);
            if(rs_tDBInput_3.wasNull()){
                    row4.Prix_du_produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 34) {
								row4.Code_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(34);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(34).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Code_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Code_promo = tmpContent_tDBInput_3;
                }
            } else {
                row4.Code_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 35) {
								row4.Reduction = null;
							} else {
		                          
            row4.Reduction = rs_tDBInput_3.getBigDecimal(35);
            if(rs_tDBInput_3.wasNull()){
                    row4.Reduction = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 36) {
								row4.Taxe_de_la_reduction = null;
							} else {
		                          
            row4.Taxe_de_la_reduction = rs_tDBInput_3.getBigDecimal(36);
            if(rs_tDBInput_3.wasNull()){
                    row4.Taxe_de_la_reduction = null;
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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_3 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_3 = false;
		  boolean mainRowRejected_tMap_3 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

ProduitsCommandeVente = null;


// # Output table : 'ProduitsCommandeVente'
ProduitsCommandeVente_tmp.Produit_PK = 0;
ProduitsCommandeVente_tmp.UGS = row4.UGS==null? SKUGenerator.generateSKU(row4.Nom_de_l_element):row4.UGS ;
ProduitsCommandeVente_tmp.Nom_produit = row4.Nom_de_l_element ;
ProduitsCommandeVente_tmp.Description = "unknown";
ProduitsCommandeVente_tmp.Url = "unknown";
ProduitsCommandeVente_tmp.Visibilite = //manrech
true ;
ProduitsCommandeVente_tmp.Prix = row4.Prix_du_produit != null ? row4.Prix_du_produit.doubleValue() : 0.0 ;
ProduitsCommandeVente_tmp.Prix_en_promo = row4.Prix_du_produit != null ? row4.Prix_du_produit.doubleValue() : 0.0 ;
ProduitsCommandeVente_tmp.En_Promo = false;
ProduitsCommandeVente_tmp.Canal = "Site Web";
ProduitsCommandeVente_tmp.Source = "Sougui";
ProduitsCommandeVente = ProduitsCommandeVente_tmp;
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
// Start of branch "ProduitsCommandeVente"
if(ProduitsCommandeVente != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"ProduitsCommandeVente"
						
						);
					}
					
row5 = null;			
if(ProduitsCommandeVente.UGS == null){
	finder_tUniqRow_3.UGS = null;
}else{
	finder_tUniqRow_3.UGS = ProduitsCommandeVente.UGS.toLowerCase();
}
if(ProduitsCommandeVente.Nom_produit == null){
	finder_tUniqRow_3.Nom_produit = null;
}else{
	finder_tUniqRow_3.Nom_produit = ProduitsCommandeVente.Nom_produit.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(ProduitsCommandeVente.UGS == null){
	new_tUniqRow_3.UGS = null;
}else{
	new_tUniqRow_3.UGS = ProduitsCommandeVente.UGS.toLowerCase();
}
if(ProduitsCommandeVente.Nom_produit == null){
	new_tUniqRow_3.Nom_produit = null;
}else{
	new_tUniqRow_3.Nom_produit = ProduitsCommandeVente.Nom_produit.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Produit_PK = ProduitsCommandeVente.Produit_PK;			row5.UGS = ProduitsCommandeVente.UGS;			row5.Nom_produit = ProduitsCommandeVente.Nom_produit;			row5.Description = ProduitsCommandeVente.Description;			row5.Url = ProduitsCommandeVente.Url;			row5.Visibilite = ProduitsCommandeVente.Visibilite;			row5.Prix = ProduitsCommandeVente.Prix;			row5.Prix_en_promo = ProduitsCommandeVente.Prix_en_promo;			row5.En_Promo = ProduitsCommandeVente.En_Promo;			row5.Canal = ProduitsCommandeVente.Canal;			row5.Source = ProduitsCommandeVente.Source;					
		nb_uniques_tUniqRow_3++;
	} else {
	  nb_duplicates_tUniqRow_3++;
	}

 


	tos_count_tUniqRow_3++;

/**
 * [tUniqRow_3 main ] stop
 */
	
	/**
	 * [tUniqRow_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_begin ] stop
 */
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tLogRow_2 main ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_2 = new String[11];
              
                 row_tLogRow_2[0]=    						    
				                String.valueOf(row5.Produit_PK)			
					          ;	
										
    			   				
	    		if(row5.UGS != null) { //              
                 row_tLogRow_2[1]=    						    
				                String.valueOf(row5.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Nom_produit != null) { //              
                 row_tLogRow_2[2]=    						    
				                String.valueOf(row5.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Description != null) { //              
                 row_tLogRow_2[3]=    						    
				                String.valueOf(row5.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Url != null) { //              
                 row_tLogRow_2[4]=    						    
				                String.valueOf(row5.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Visibilite != null) { //              
                 row_tLogRow_2[5]=    						    
				                String.valueOf(row5.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Prix != null) { //              
                 row_tLogRow_2[6]=    						
								FormatterUtils.formatUnwithE(row5.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Prix_en_promo != null) { //              
                 row_tLogRow_2[7]=    						
								FormatterUtils.formatUnwithE(row5.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row5.En_Promo != null) { //              
                 row_tLogRow_2[8]=    						    
				                String.valueOf(row5.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Canal != null) { //              
                 row_tLogRow_2[9]=    						    
				                String.valueOf(row5.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row5.Source != null) { //              
                 row_tLogRow_2[10]=    						    
				                String.valueOf(row5.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_2.addRow(row_tLogRow_2);	
				nb_line_tLogRow_2++;
//////

//////                    
                    
///////////////////////    			

 
     row8 = row5;


	tos_count_tLogRow_2++;

/**
 * [tLogRow_2 main ] stop
 */
	
	/**
	 * [tLogRow_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 process_data_begin ] stop
 */

	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row8"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row8.Produit_PK;								
			row9.UGS = row8.UGS;								
			row9.Nom_produit = row8.Nom_produit;								
			row9.Description = row8.Description;								
			row9.Url = row8.Url;								
			row9.Visibilite = row8.Visibilite;								
			row9.Prix = row8.Prix;								
			row9.Prix_en_promo = row8.Prix_en_promo;								
			row9.En_Promo = row8.En_Promo;								
			row9.Canal = row8.Canal;								
			row9.Source = row8.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "ProduitsCommandeVente"




	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
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
	 * [tMap_3 end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row4");
			  	}
			  	
 

ok_Hash.put("tMap_3", true);
end_Hash.put("tMap_3", System.currentTimeMillis());




/**
 * [tMap_3 end ] stop
 */

	
	/**
	 * [tUniqRow_3 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

globalMap.put("tUniqRow_3_NB_UNIQUES",nb_uniques_tUniqRow_3);
globalMap.put("tUniqRow_3_NB_DUPLICATES",nb_duplicates_tUniqRow_3);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"ProduitsCommandeVente");
			  	}
			  	
 

ok_Hash.put("tUniqRow_3", true);
end_Hash.put("tUniqRow_3", System.currentTimeMillis());




/**
 * [tUniqRow_3 end ] stop
 */

	
	/**
	 * [tLogRow_2 end ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	


//////

                    
                    java.io.PrintStream consoleOut_tLogRow_2 = null;
                    if (globalMap.get("tLogRow_CONSOLE")!=null)
                    {
                    	consoleOut_tLogRow_2 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
                    }
                    else
                    {
                    	consoleOut_tLogRow_2 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                    	globalMap.put("tLogRow_CONSOLE",consoleOut_tLogRow_2);
                    }
                    
                    consoleOut_tLogRow_2.println(util_tLogRow_2.format().toString());
                    consoleOut_tLogRow_2.flush();
//////
globalMap.put("tLogRow_2_NB_LINE",nb_line_tLogRow_2);

///////////////////////    			

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
			  	}
			  	
 

ok_Hash.put("tLogRow_2", true);
end_Hash.put("tLogRow_2", System.currentTimeMillis());




/**
 * [tLogRow_2 end ] stop
 */











	
	/**
	 * [tUniqRow_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_5", false);
		start_Hash.put("tUniqRow_5", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_5";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"factureVente");
					}
				
		int tos_count_tUniqRow_5 = 0;
		

	
		class KeyStruct_tUniqRow_5 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_5 other = (KeyStruct_tUniqRow_5) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_5 = 0;
int nb_duplicates_tUniqRow_5 = 0;
KeyStruct_tUniqRow_5 finder_tUniqRow_5 = new KeyStruct_tUniqRow_5();
java.util.Set<KeyStruct_tUniqRow_5> keystUniqRow_5 = new java.util.HashSet<KeyStruct_tUniqRow_5>(); 

 



/**
 * [tUniqRow_5 begin ] stop
 */



	
	/**
	 * [tMap_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_5", false);
		start_Hash.put("tMap_5", System.currentTimeMillis());
		
	
	currentComponent="tMap_5";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row14");
					}
				
		int tos_count_tMap_5 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_5__Struct  {
}
Var__tMap_5__Struct Var__tMap_5 = new Var__tMap_5__Struct();
// ###############################

// ###############################
// # Outputs initialization
factureVenteStruct factureVente_tmp = new factureVenteStruct();
// ###############################

        
        



        









 



/**
 * [tMap_5 begin ] stop
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
				String dbUser_tDBInput_5 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:VmCjflAi2gg5/OHE6dS0U99nsBQXrRcrJnaFFGn5B4B7SeWfp5s=");
				
				String dbPwd_tDBInput_5 = decryptedPassword_tDBInput_5;
				
		    String port_tDBInput_5 = "1433";
		    String dbname_tDBInput_5 = "PA_BI_SA" ;
			String url_tDBInput_5 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_5)) {
		    	url_tDBInput_5 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_5)) {
				url_tDBInput_5 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_5 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_5 = "";
				
				conn_tDBInput_5 = java.sql.DriverManager.getConnection(url_tDBInput_5,dbUser_tDBInput_5,dbPwd_tDBInput_5);
		        
		    
			java.sql.Statement stmt_tDBInput_5 = conn_tDBInput_5.createStatement();

		    String dbquery_tDBInput_5 = "SELECT Factures_Ventes.Reference,\n		Factures_Ventes.Description,\n		Factures_Ventes.PU_HT,\n		Factures_Ventes.Quantite,\n	"
+"	Factures_Ventes.Prix_Total_HT,\n		Factures_Ventes.Numero_Facture,\n		Factures_Ventes.id_Client,\n		Factures_Ventes.Date,\n	"
+"	Factures_Ventes.Client_Nom,\n		Factures_Ventes.Matricule_Fiscal,\n		Factures_Ventes.Adresse,\n		Factures_Ventes.TVA\nFROM	F"
+"actures_Ventes";
		    

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
								row14.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(1);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Reference = tmpContent_tDBInput_5;
                }
            } else {
                row14.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 2) {
								row14.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(2);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Description = tmpContent_tDBInput_5;
                }
            } else {
                row14.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 3) {
								row14.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(3);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.PU_HT = tmpContent_tDBInput_5;
                }
            } else {
                row14.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 4) {
								row14.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(4);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Quantite = tmpContent_tDBInput_5;
                }
            } else {
                row14.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 5) {
								row14.Prix_Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(5);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Prix_Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Prix_Total_HT = tmpContent_tDBInput_5;
                }
            } else {
                row14.Prix_Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 6) {
								row14.Numero_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(6);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Numero_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Numero_Facture = tmpContent_tDBInput_5;
                }
            } else {
                row14.Numero_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 7) {
								row14.id_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(7);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.id_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.id_Client = tmpContent_tDBInput_5;
                }
            } else {
                row14.id_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 8) {
								row14.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(8);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Date = tmpContent_tDBInput_5;
                }
            } else {
                row14.Date = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 9) {
								row14.Client_Nom = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(9);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Client_Nom = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Client_Nom = tmpContent_tDBInput_5;
                }
            } else {
                row14.Client_Nom = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 10) {
								row14.Matricule_Fiscal = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(10);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Matricule_Fiscal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Matricule_Fiscal = tmpContent_tDBInput_5;
                }
            } else {
                row14.Matricule_Fiscal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 11) {
								row14.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(11);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.Adresse = tmpContent_tDBInput_5;
                }
            } else {
                row14.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 12) {
								row14.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(12);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row14.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row14.TVA = tmpContent_tDBInput_5;
                }
            } else {
                row14.TVA = null;
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
	 * [tMap_5 main ] start
	 */

	

	
	
	currentComponent="tMap_5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row14"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_5 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_5 = false;
		  boolean mainRowRejected_tMap_5 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_5__Struct Var = Var__tMap_5;// ###############################
        // ###############################
        // # Output tables

factureVente = null;


// # Output table : 'factureVente'
factureVente_tmp.Produit_PK = 0;
factureVente_tmp.UGS = row14.Description != null && row14.Description.toLowerCase().contains("poste")
? "PT-123"
: row14.Reference ;
factureVente_tmp.Nom_produit = row14.Description != null && row14.Description.toLowerCase().contains("poste")? "Poste":row14.Description ;
factureVente_tmp.Description = "Unknown";
factureVente_tmp.Url = "Unknown";
factureVente_tmp.Visibilite = false;
factureVente_tmp.Prix = row14.PU_HT != null && !row14.PU_HT.trim().isEmpty()
    ? Double.parseDouble(row14.PU_HT.replace(",", "."))
        * (1 + routines.parseTVA.parseTVA(row14.TVA) / 100.0)
    : 0 ;
factureVente_tmp.Prix_en_promo = row14.PU_HT != null && !row14.PU_HT.trim().isEmpty()
    ? Double.parseDouble(row14.PU_HT.replace(",", "."))
        * (1 + routines.parseTVA.parseTVA(row14.TVA) / 100.0)
    : 0 ;
factureVente_tmp.En_Promo = false;
factureVente_tmp.Canal = "Terrain";
factureVente_tmp.Source = "Sougui";
factureVente = factureVente_tmp;
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
// Start of branch "factureVente"
if(factureVente != null) { 



	
	/**
	 * [tUniqRow_5 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"factureVente"
						
						);
					}
					
row15 = null;			
if(factureVente.UGS == null){
	finder_tUniqRow_5.UGS = null;
}else{
	finder_tUniqRow_5.UGS = factureVente.UGS.toLowerCase();
}
if(factureVente.Nom_produit == null){
	finder_tUniqRow_5.Nom_produit = null;
}else{
	finder_tUniqRow_5.Nom_produit = factureVente.Nom_produit.toLowerCase();
}	
finder_tUniqRow_5.hashCodeDirty = true;
if (!keystUniqRow_5.contains(finder_tUniqRow_5)) {
		KeyStruct_tUniqRow_5 new_tUniqRow_5 = new KeyStruct_tUniqRow_5();

		
if(factureVente.UGS == null){
	new_tUniqRow_5.UGS = null;
}else{
	new_tUniqRow_5.UGS = factureVente.UGS.toLowerCase();
}
if(factureVente.Nom_produit == null){
	new_tUniqRow_5.Nom_produit = null;
}else{
	new_tUniqRow_5.Nom_produit = factureVente.Nom_produit.toLowerCase();
}
		
		keystUniqRow_5.add(new_tUniqRow_5);if(row15 == null){ 
	
	row15 = new row15Struct();
}row15.Produit_PK = factureVente.Produit_PK;			row15.UGS = factureVente.UGS;			row15.Nom_produit = factureVente.Nom_produit;			row15.Description = factureVente.Description;			row15.Url = factureVente.Url;			row15.Visibilite = factureVente.Visibilite;			row15.Prix = factureVente.Prix;			row15.Prix_en_promo = factureVente.Prix_en_promo;			row15.En_Promo = factureVente.En_Promo;			row15.Canal = factureVente.Canal;			row15.Source = factureVente.Source;					
		nb_uniques_tUniqRow_5++;
	} else {
	  nb_duplicates_tUniqRow_5++;
	}

 


	tos_count_tUniqRow_5++;

/**
 * [tUniqRow_5 main ] stop
 */
	
	/**
	 * [tUniqRow_5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_5";

	

 



/**
 * [tUniqRow_5 process_data_begin ] stop
 */
// Start of branch "row15"
if(row15 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row15"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row15.Produit_PK;								
			row9.UGS = row15.UGS;								
			row9.Nom_produit = row15.Nom_produit;								
			row9.Description = row15.Description;								
			row9.Url = row15.Url;								
			row9.Visibilite = row15.Visibilite;								
			row9.Prix = row15.Prix;								
			row9.Prix_en_promo = row15.Prix_en_promo;								
			row9.En_Promo = row15.En_Promo;								
			row9.Canal = row15.Canal;								
			row9.Source = row15.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row15"




	
	/**
	 * [tUniqRow_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_5";

	

 



/**
 * [tUniqRow_5 process_data_end ] stop
 */

} // End of branch "factureVente"




	
	/**
	 * [tMap_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 process_data_end ] stop
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
	 * [tMap_5 end ] start
	 */

	

	
	
	currentComponent="tMap_5";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row14");
			  	}
			  	
 

ok_Hash.put("tMap_5", true);
end_Hash.put("tMap_5", System.currentTimeMillis());




/**
 * [tMap_5 end ] stop
 */

	
	/**
	 * [tUniqRow_5 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_5";

	

globalMap.put("tUniqRow_5_NB_UNIQUES",nb_uniques_tUniqRow_5);
globalMap.put("tUniqRow_5_NB_DUPLICATES",nb_duplicates_tUniqRow_5);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"factureVente");
			  	}
			  	
 

ok_Hash.put("tUniqRow_5", true);
end_Hash.put("tUniqRow_5", System.currentTimeMillis());




/**
 * [tUniqRow_5 end ] stop
 */








	
	/**
	 * [tUniqRow_6 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_6", false);
		start_Hash.put("tUniqRow_6", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_6";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out_Edaytna");
					}
				
		int tos_count_tUniqRow_6 = 0;
		

	
		class KeyStruct_tUniqRow_6 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_6 other = (KeyStruct_tUniqRow_6) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_6 = 0;
int nb_duplicates_tUniqRow_6 = 0;
KeyStruct_tUniqRow_6 finder_tUniqRow_6 = new KeyStruct_tUniqRow_6();
java.util.Set<KeyStruct_tUniqRow_6> keystUniqRow_6 = new java.util.HashSet<KeyStruct_tUniqRow_6>(); 

 



/**
 * [tUniqRow_6 begin ] stop
 */



	
	/**
	 * [tMap_6 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_6", false);
		start_Hash.put("tMap_6", System.currentTimeMillis());
		
	
	currentComponent="tMap_6";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row13");
					}
				
		int tos_count_tMap_6 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_6__Struct  {
}
Var__tMap_6__Struct Var__tMap_6 = new Var__tMap_6__Struct();
// ###############################

// ###############################
// # Outputs initialization
out_EdaytnaStruct out_Edaytna_tmp = new out_EdaytnaStruct();
// ###############################

        
        



        









 



/**
 * [tMap_6 begin ] stop
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
				String dbUser_tDBInput_4 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:feKuKOYb5Wpgrjp2ex52QooaMdTgmQc/7MXOyk0EZcZYk0JBpHw=");
				
				String dbPwd_tDBInput_4 = decryptedPassword_tDBInput_4;
				
		    String port_tDBInput_4 = "1433";
		    String dbname_tDBInput_4 = "PA_BI_SA" ;
			String url_tDBInput_4 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_4)) {
		    	url_tDBInput_4 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_4)) {
				url_tDBInput_4 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_4 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_4 = "";
				
				conn_tDBInput_4 = java.sql.DriverManager.getConnection(url_tDBInput_4,dbUser_tDBInput_4,dbPwd_tDBInput_4);
		        
		    
			java.sql.Statement stmt_tDBInput_4 = conn_tDBInput_4.createStatement();

		    String dbquery_tDBInput_4 = "SELECT produits_edayetna_webscraping.titre,\n		produits_edayetna_webscraping.categorie,\n		produits_edayetna_webscraping."
+"prix_avant,\n		produits_edayetna_webscraping.prix_apres,\n		produits_edayetna_webscraping.promotion,\n		produits_edayetna_w"
+"ebscraping.url,\n		produits_edayetna_webscraping.image\nFROM	produits_edayetna_webscraping";
		    

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
								row13.titre = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(1);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.titre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.titre = tmpContent_tDBInput_4;
                }
            } else {
                row13.titre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row13.categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.categorie = tmpContent_tDBInput_4;
                }
            } else {
                row13.categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 3) {
								row13.prix_avant = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(3);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.prix_avant = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.prix_avant = tmpContent_tDBInput_4;
                }
            } else {
                row13.prix_avant = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 4) {
								row13.prix_apres = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(4);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.prix_apres = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.prix_apres = tmpContent_tDBInput_4;
                }
            } else {
                row13.prix_apres = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 5) {
								row13.promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(5);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.promotion = tmpContent_tDBInput_4;
                }
            } else {
                row13.promotion = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 6) {
								row13.url = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(6);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.url = tmpContent_tDBInput_4;
                }
            } else {
                row13.url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 7) {
								row13.image = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(7);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.image = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row13.image = tmpContent_tDBInput_4;
                }
            } else {
                row13.image = null;
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
	 * [tMap_6 main ] start
	 */

	

	
	
	currentComponent="tMap_6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row13"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_6 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_6 = false;
		  boolean mainRowRejected_tMap_6 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_6__Struct Var = Var__tMap_6;// ###############################
        // ###############################
        // # Output tables

out_Edaytna = null;


// # Output table : 'out_Edaytna'
out_Edaytna_tmp.Produit_PK = 0;
out_Edaytna_tmp.UGS = SKUGenerator.generateSKU(row13.titre);
out_Edaytna_tmp.Nom_produit = row13.titre ;
out_Edaytna_tmp.Description = "Unknown";
out_Edaytna_tmp.Url = "Unknown";
out_Edaytna_tmp.Visibilite = true;
out_Edaytna_tmp.Prix = row13.prix_avant != null && !row13.prix_avant.trim().isEmpty()
? Double.parseDouble(row13.prix_avant.replace(",", "."))
: 0 ;
out_Edaytna_tmp.Prix_en_promo = row13.prix_apres != null && !row13.prix_apres.trim().isEmpty()
? Double.parseDouble(row13.prix_apres.replace(",", "."))
: 0 ;
out_Edaytna_tmp.En_Promo = row13.prix_apres.equals(row13.prix_avant)?false:true ;
out_Edaytna_tmp.Canal = "Site Web";
out_Edaytna_tmp.Source = "Edaytna";
out_Edaytna = out_Edaytna_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_6 = false;










 


	tos_count_tMap_6++;

/**
 * [tMap_6 main ] stop
 */
	
	/**
	 * [tMap_6 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 process_data_begin ] stop
 */
// Start of branch "out_Edaytna"
if(out_Edaytna != null) { 



	
	/**
	 * [tUniqRow_6 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_Edaytna"
						
						);
					}
					
row16 = null;			
if(out_Edaytna.UGS == null){
	finder_tUniqRow_6.UGS = null;
}else{
	finder_tUniqRow_6.UGS = out_Edaytna.UGS.toLowerCase();
}
if(out_Edaytna.Nom_produit == null){
	finder_tUniqRow_6.Nom_produit = null;
}else{
	finder_tUniqRow_6.Nom_produit = out_Edaytna.Nom_produit.toLowerCase();
}	
finder_tUniqRow_6.hashCodeDirty = true;
if (!keystUniqRow_6.contains(finder_tUniqRow_6)) {
		KeyStruct_tUniqRow_6 new_tUniqRow_6 = new KeyStruct_tUniqRow_6();

		
if(out_Edaytna.UGS == null){
	new_tUniqRow_6.UGS = null;
}else{
	new_tUniqRow_6.UGS = out_Edaytna.UGS.toLowerCase();
}
if(out_Edaytna.Nom_produit == null){
	new_tUniqRow_6.Nom_produit = null;
}else{
	new_tUniqRow_6.Nom_produit = out_Edaytna.Nom_produit.toLowerCase();
}
		
		keystUniqRow_6.add(new_tUniqRow_6);if(row16 == null){ 
	
	row16 = new row16Struct();
}row16.Produit_PK = out_Edaytna.Produit_PK;			row16.UGS = out_Edaytna.UGS;			row16.Nom_produit = out_Edaytna.Nom_produit;			row16.Description = out_Edaytna.Description;			row16.Url = out_Edaytna.Url;			row16.Visibilite = out_Edaytna.Visibilite;			row16.Prix = out_Edaytna.Prix;			row16.Prix_en_promo = out_Edaytna.Prix_en_promo;			row16.En_Promo = out_Edaytna.En_Promo;			row16.Canal = out_Edaytna.Canal;			row16.Source = out_Edaytna.Source;					
		nb_uniques_tUniqRow_6++;
	} else {
	  nb_duplicates_tUniqRow_6++;
	}

 


	tos_count_tUniqRow_6++;

/**
 * [tUniqRow_6 main ] stop
 */
	
	/**
	 * [tUniqRow_6 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_6";

	

 



/**
 * [tUniqRow_6 process_data_begin ] stop
 */
// Start of branch "row16"
if(row16 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row16"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row16.Produit_PK;								
			row9.UGS = row16.UGS;								
			row9.Nom_produit = row16.Nom_produit;								
			row9.Description = row16.Description;								
			row9.Url = row16.Url;								
			row9.Visibilite = row16.Visibilite;								
			row9.Prix = row16.Prix;								
			row9.Prix_en_promo = row16.Prix_en_promo;								
			row9.En_Promo = row16.En_Promo;								
			row9.Canal = row16.Canal;								
			row9.Source = row16.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row16"




	
	/**
	 * [tUniqRow_6 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_6";

	

 



/**
 * [tUniqRow_6 process_data_end ] stop
 */

} // End of branch "out_Edaytna"




	
	/**
	 * [tMap_6 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 process_data_end ] stop
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
	 * [tMap_6 end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row13");
			  	}
			  	
 

ok_Hash.put("tMap_6", true);
end_Hash.put("tMap_6", System.currentTimeMillis());




/**
 * [tMap_6 end ] stop
 */

	
	/**
	 * [tUniqRow_6 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_6";

	

globalMap.put("tUniqRow_6_NB_UNIQUES",nb_uniques_tUniqRow_6);
globalMap.put("tUniqRow_6_NB_DUPLICATES",nb_duplicates_tUniqRow_6);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out_Edaytna");
			  	}
			  	
 

ok_Hash.put("tUniqRow_6", true);
end_Hash.put("tUniqRow_6", System.currentTimeMillis());




/**
 * [tUniqRow_6 end ] stop
 */








	
	/**
	 * [tUniqRow_8 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_8", false);
		start_Hash.put("tUniqRow_8", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_8";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out_Kalys");
					}
				
		int tos_count_tUniqRow_8 = 0;
		

	
		class KeyStruct_tUniqRow_8 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String UGS;
					String Nom_produit;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.UGS == null) ? 0 : this.UGS.hashCode());
								
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
				final KeyStruct_tUniqRow_8 other = (KeyStruct_tUniqRow_8) obj;
				
									if (this.UGS == null) {
										if (other.UGS != null) 
											return false;
								
									} else if (!this.UGS.equals(other.UGS))
								 
										return false;
								
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_8 = 0;
int nb_duplicates_tUniqRow_8 = 0;
KeyStruct_tUniqRow_8 finder_tUniqRow_8 = new KeyStruct_tUniqRow_8();
java.util.Set<KeyStruct_tUniqRow_8> keystUniqRow_8 = new java.util.HashSet<KeyStruct_tUniqRow_8>(); 

 



/**
 * [tUniqRow_8 begin ] stop
 */



	
	/**
	 * [tMap_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_4", false);
		start_Hash.put("tMap_4", System.currentTimeMillis());
		
	
	currentComponent="tMap_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row17");
					}
				
		int tos_count_tMap_4 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_4__Struct  {
}
Var__tMap_4__Struct Var__tMap_4 = new Var__tMap_4__Struct();
// ###############################

// ###############################
// # Outputs initialization
out_KalysStruct out_Kalys_tmp = new out_KalysStruct();
// ###############################

        
        



        









 



/**
 * [tMap_4 begin ] stop
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
				String dbUser_tDBInput_6 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:vFFvI5JhQbbd8PrDebyuZFn5miFmWT6XDzQ4UfeoPL+Nc/z8ZUY=");
				
				String dbPwd_tDBInput_6 = decryptedPassword_tDBInput_6;
				
		    String port_tDBInput_6 = "1433";
		    String dbname_tDBInput_6 = "PA_BI_SA" ;
			String url_tDBInput_6 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_6)) {
		    	url_tDBInput_6 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_6)) {
				url_tDBInput_6 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_6 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_6 = "";
				
				conn_tDBInput_6 = java.sql.DriverManager.getConnection(url_tDBInput_6,dbUser_tDBInput_6,dbPwd_tDBInput_6);
		        
		    
			java.sql.Statement stmt_tDBInput_6 = conn_tDBInput_6.createStatement();

		    String dbquery_tDBInput_6 = "SELECT produits_kalys_webscraping.Produit,\n		produits_kalys_webscraping.Categorie,\n		produits_kalys_webscraping.Sous_ca"
+"tegorie,\n		produits_kalys_webscraping.Categories_supplementaires,\n		produits_kalys_webscraping.Prix_avant_promo__TND,\n		"
+"produits_kalys_webscraping.Prix_apres_promo__TND,\n		produits_kalys_webscraping.En_promotion\nFROM	produits_kalys_webscrap"
+"ing";
		    

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
								row17.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(1);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Produit = tmpContent_tDBInput_6;
                }
            } else {
                row17.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row17.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Categorie = tmpContent_tDBInput_6;
                }
            } else {
                row17.Categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 3) {
								row17.Sous_categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(3);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Sous_categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Sous_categorie = tmpContent_tDBInput_6;
                }
            } else {
                row17.Sous_categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 4) {
								row17.Categories_supplementaires = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(4);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Categories_supplementaires = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Categories_supplementaires = tmpContent_tDBInput_6;
                }
            } else {
                row17.Categories_supplementaires = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 5) {
								row17.Prix_avant_promo__TND = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(5);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Prix_avant_promo__TND = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Prix_avant_promo__TND = tmpContent_tDBInput_6;
                }
            } else {
                row17.Prix_avant_promo__TND = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 6) {
								row17.Prix_apres_promo__TND = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(6);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.Prix_apres_promo__TND = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.Prix_apres_promo__TND = tmpContent_tDBInput_6;
                }
            } else {
                row17.Prix_apres_promo__TND = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 7) {
								row17.En_promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(7);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row17.En_promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row17.En_promotion = tmpContent_tDBInput_6;
                }
            } else {
                row17.En_promotion = null;
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
	 * [tMap_4 main ] start
	 */

	

	
	
	currentComponent="tMap_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row17"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_4 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_4 = false;
		  boolean mainRowRejected_tMap_4 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_4__Struct Var = Var__tMap_4;// ###############################
        // ###############################
        // # Output tables

out_Kalys = null;


// # Output table : 'out_Kalys'
out_Kalys_tmp.Produit_PK = 0;
out_Kalys_tmp.UGS = SKUGenerator.generateSKU(row17.Produit) ;
out_Kalys_tmp.Nom_produit = row17.Produit ;
out_Kalys_tmp.Description = "Unknown";
out_Kalys_tmp.Url = "Unknown";
out_Kalys_tmp.Visibilite = true;
out_Kalys_tmp.Prix = row17.Prix_avant_promo__TND != null && !row17.Prix_avant_promo__TND.trim().isEmpty()
? Double.parseDouble(row17.Prix_avant_promo__TND.replace(",", "."))
: 0 ;
out_Kalys_tmp.Prix_en_promo = row17.Prix_apres_promo__TND!= null && !row17.Prix_apres_promo__TND.trim().isEmpty()
? Double.parseDouble(row17.Prix_apres_promo__TND.replace(",", "."))
: Double.parseDouble(row17.Prix_avant_promo__TND.replace(",", ".")) ;
out_Kalys_tmp.En_Promo = row17.Prix_apres_promo__TND!=null?row17.Prix_apres_promo__TND.trim().isEmpty()?false:true:true ;
out_Kalys_tmp.Canal = "Site Web";
out_Kalys_tmp.Source = "Kalys";
out_Kalys = out_Kalys_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_4 = false;










 


	tos_count_tMap_4++;

/**
 * [tMap_4 main ] stop
 */
	
	/**
	 * [tMap_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 process_data_begin ] stop
 */
// Start of branch "out_Kalys"
if(out_Kalys != null) { 



	
	/**
	 * [tUniqRow_8 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_8";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_Kalys"
						
						);
					}
					
row18 = null;			
if(out_Kalys.UGS == null){
	finder_tUniqRow_8.UGS = null;
}else{
	finder_tUniqRow_8.UGS = out_Kalys.UGS.toLowerCase();
}
if(out_Kalys.Nom_produit == null){
	finder_tUniqRow_8.Nom_produit = null;
}else{
	finder_tUniqRow_8.Nom_produit = out_Kalys.Nom_produit.toLowerCase();
}	
finder_tUniqRow_8.hashCodeDirty = true;
if (!keystUniqRow_8.contains(finder_tUniqRow_8)) {
		KeyStruct_tUniqRow_8 new_tUniqRow_8 = new KeyStruct_tUniqRow_8();

		
if(out_Kalys.UGS == null){
	new_tUniqRow_8.UGS = null;
}else{
	new_tUniqRow_8.UGS = out_Kalys.UGS.toLowerCase();
}
if(out_Kalys.Nom_produit == null){
	new_tUniqRow_8.Nom_produit = null;
}else{
	new_tUniqRow_8.Nom_produit = out_Kalys.Nom_produit.toLowerCase();
}
		
		keystUniqRow_8.add(new_tUniqRow_8);if(row18 == null){ 
	
	row18 = new row18Struct();
}row18.Produit_PK = out_Kalys.Produit_PK;			row18.UGS = out_Kalys.UGS;			row18.Nom_produit = out_Kalys.Nom_produit;			row18.Description = out_Kalys.Description;			row18.Url = out_Kalys.Url;			row18.Visibilite = out_Kalys.Visibilite;			row18.Prix = out_Kalys.Prix;			row18.Prix_en_promo = out_Kalys.Prix_en_promo;			row18.En_Promo = out_Kalys.En_Promo;			row18.Canal = out_Kalys.Canal;			row18.Source = out_Kalys.Source;					
		nb_uniques_tUniqRow_8++;
	} else {
	  nb_duplicates_tUniqRow_8++;
	}

 


	tos_count_tUniqRow_8++;

/**
 * [tUniqRow_8 main ] stop
 */
	
	/**
	 * [tUniqRow_8 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_8";

	

 



/**
 * [tUniqRow_8 process_data_begin ] stop
 */
// Start of branch "row18"
if(row18 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row18"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row18.Produit_PK;								
			row9.UGS = row18.UGS;								
			row9.Nom_produit = row18.Nom_produit;								
			row9.Description = row18.Description;								
			row9.Url = row18.Url;								
			row9.Visibilite = row18.Visibilite;								
			row9.Prix = row18.Prix;								
			row9.Prix_en_promo = row18.Prix_en_promo;								
			row9.En_Promo = row18.En_Promo;								
			row9.Canal = row18.Canal;								
			row9.Source = row18.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row18"




	
	/**
	 * [tUniqRow_8 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_8";

	

 



/**
 * [tUniqRow_8 process_data_end ] stop
 */

} // End of branch "out_Kalys"




	
	/**
	 * [tMap_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 process_data_end ] stop
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
	 * [tMap_4 end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row17");
			  	}
			  	
 

ok_Hash.put("tMap_4", true);
end_Hash.put("tMap_4", System.currentTimeMillis());




/**
 * [tMap_4 end ] stop
 */

	
	/**
	 * [tUniqRow_8 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_8";

	

globalMap.put("tUniqRow_8_NB_UNIQUES",nb_uniques_tUniqRow_8);
globalMap.put("tUniqRow_8_NB_DUPLICATES",nb_duplicates_tUniqRow_8);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out_Kalys");
			  	}
			  	
 

ok_Hash.put("tUniqRow_8", true);
end_Hash.put("tUniqRow_8", System.currentTimeMillis());




/**
 * [tUniqRow_8 end ] stop
 */








	
	/**
	 * [tUniqRow_7 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_7", false);
		start_Hash.put("tUniqRow_7", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_7";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"copyOfout_Kalys");
					}
				
		int tos_count_tUniqRow_7 = 0;
		

	
		class KeyStruct_tUniqRow_7 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Nom_produit;        
	        
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
				final KeyStruct_tUniqRow_7 other = (KeyStruct_tUniqRow_7) obj;
				
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_7 = 0;
int nb_duplicates_tUniqRow_7 = 0;
KeyStruct_tUniqRow_7 finder_tUniqRow_7 = new KeyStruct_tUniqRow_7();
java.util.Set<KeyStruct_tUniqRow_7> keystUniqRow_7 = new java.util.HashSet<KeyStruct_tUniqRow_7>(); 

 



/**
 * [tUniqRow_7 begin ] stop
 */



	
	/**
	 * [tMap_7 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_7", false);
		start_Hash.put("tMap_7", System.currentTimeMillis());
		
	
	currentComponent="tMap_7";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row19");
					}
				
		int tos_count_tMap_7 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_7__Struct  {
}
Var__tMap_7__Struct Var__tMap_7 = new Var__tMap_7__Struct();
// ###############################

// ###############################
// # Outputs initialization
copyOfout_KalysStruct copyOfout_Kalys_tmp = new copyOfout_KalysStruct();
// ###############################

        
        



        









 



/**
 * [tMap_7 begin ] stop
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
				String dbUser_tDBInput_7 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_7 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:5yQcjMy/FW4isVzyLMsLl4wHAsmsJD71YakYJ3E1R+UiMlogi8c=");
				
				String dbPwd_tDBInput_7 = decryptedPassword_tDBInput_7;
				
		    String port_tDBInput_7 = "1433";
		    String dbname_tDBInput_7 = "PA_BI_SA" ;
			String url_tDBInput_7 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_7)) {
		    	url_tDBInput_7 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_7)) {
				url_tDBInput_7 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_7 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_7 = "";
				
				conn_tDBInput_7 = java.sql.DriverManager.getConnection(url_tDBInput_7,dbUser_tDBInput_7,dbPwd_tDBInput_7);
		        
		    
			java.sql.Statement stmt_tDBInput_7 = conn_tDBInput_7.createStatement();

		    String dbquery_tDBInput_7 = "SELECT Factures_achat.Nom_du_fichier,\n		Factures_achat.N__Facture,\n		Factures_achat.Date_Facture,\n		Factures_achat.Nom_"
+"du_vendeur,\n		Factures_achat.Adresse_du_vendeur,\n		Factures_achat.Matricule_Fiscale_du_vendeur,\n		Factures_achat.Produit"
+",\n		Factures_achat.Reference,\n		Factures_achat.Quantite,\n		Factures_achat.PU_HT,\n		Factures_achat.TVA,\n		Factures_achat."
+"Total_HT,\n		Factures_achat.Montant_Total_net_a_payer\nFROM	Factures_achat";
		    

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
								row19.Nom_du_fichier = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(1);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Nom_du_fichier = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Nom_du_fichier = tmpContent_tDBInput_7;
                }
            } else {
                row19.Nom_du_fichier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 2) {
								row19.N__Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(2);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.N__Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.N__Facture = tmpContent_tDBInput_7;
                }
            } else {
                row19.N__Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 3) {
								row19.Date_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(3);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Date_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Date_Facture = tmpContent_tDBInput_7;
                }
            } else {
                row19.Date_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 4) {
								row19.Nom_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(4);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Nom_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Nom_du_vendeur = tmpContent_tDBInput_7;
                }
            } else {
                row19.Nom_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 5) {
								row19.Adresse_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(5);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Adresse_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Adresse_du_vendeur = tmpContent_tDBInput_7;
                }
            } else {
                row19.Adresse_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 6) {
								row19.Matricule_Fiscale_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(6);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Matricule_Fiscale_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Matricule_Fiscale_du_vendeur = tmpContent_tDBInput_7;
                }
            } else {
                row19.Matricule_Fiscale_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 7) {
								row19.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(7);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Produit = tmpContent_tDBInput_7;
                }
            } else {
                row19.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 8) {
								row19.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(8);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Reference = tmpContent_tDBInput_7;
                }
            } else {
                row19.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 9) {
								row19.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(9);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Quantite = tmpContent_tDBInput_7;
                }
            } else {
                row19.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 10) {
								row19.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(10);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.PU_HT = tmpContent_tDBInput_7;
                }
            } else {
                row19.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 11) {
								row19.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(11);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.TVA = tmpContent_tDBInput_7;
                }
            } else {
                row19.TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 12) {
								row19.Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(12);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Total_HT = tmpContent_tDBInput_7;
                }
            } else {
                row19.Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 13) {
								row19.Montant_Total_net_a_payer = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(13);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row19.Montant_Total_net_a_payer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row19.Montant_Total_net_a_payer = tmpContent_tDBInput_7;
                }
            } else {
                row19.Montant_Total_net_a_payer = null;
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
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row19"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

copyOfout_Kalys = null;


// # Output table : 'copyOfout_Kalys'
copyOfout_Kalys_tmp.Produit_PK = 0;
copyOfout_Kalys_tmp.UGS = row19.Reference==null || row19.Reference.trim().isEmpty() ?SKUGenerator.generateSKU(row19.Produit):row19.Reference ;
copyOfout_Kalys_tmp.Nom_produit = row19.Produit == null || row19.Produit.length() == 0
    ? "Unknown"
    : row19.Produit.substring(0,1).toUpperCase() 
      + row19.Produit.substring(1).toLowerCase() ;
copyOfout_Kalys_tmp.Description = "Unknown";
copyOfout_Kalys_tmp.Url = "Unknown";
copyOfout_Kalys_tmp.Visibilite = false;
copyOfout_Kalys_tmp.Prix = row19.PU_HT!= null && !row19.PU_HT.trim().isEmpty()
? Double.parseDouble(row19.PU_HT.replace(",", "."))
: 0 ;
copyOfout_Kalys_tmp.Prix_en_promo = row19.PU_HT!= null && !row19.PU_HT.trim().isEmpty()
? Double.parseDouble(row19.PU_HT.replace(",", "."))
: 0 ;
copyOfout_Kalys_tmp.En_Promo = false;
copyOfout_Kalys_tmp.Canal = "Achat";
copyOfout_Kalys_tmp.Source = row19.Nom_du_vendeur ;
copyOfout_Kalys = copyOfout_Kalys_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_7 = false;










 


	tos_count_tMap_7++;

/**
 * [tMap_7 main ] stop
 */
	
	/**
	 * [tMap_7 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_begin ] stop
 */
// Start of branch "copyOfout_Kalys"
if(copyOfout_Kalys != null) { 



	
	/**
	 * [tUniqRow_7 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"copyOfout_Kalys"
						
						);
					}
					
row20 = null;			
if(copyOfout_Kalys.Nom_produit == null){
	finder_tUniqRow_7.Nom_produit = null;
}else{
	finder_tUniqRow_7.Nom_produit = copyOfout_Kalys.Nom_produit.toLowerCase();
}	
finder_tUniqRow_7.hashCodeDirty = true;
if (!keystUniqRow_7.contains(finder_tUniqRow_7)) {
		KeyStruct_tUniqRow_7 new_tUniqRow_7 = new KeyStruct_tUniqRow_7();

		
if(copyOfout_Kalys.Nom_produit == null){
	new_tUniqRow_7.Nom_produit = null;
}else{
	new_tUniqRow_7.Nom_produit = copyOfout_Kalys.Nom_produit.toLowerCase();
}
		
		keystUniqRow_7.add(new_tUniqRow_7);if(row20 == null){ 
	
	row20 = new row20Struct();
}row20.Produit_PK = copyOfout_Kalys.Produit_PK;			row20.UGS = copyOfout_Kalys.UGS;			row20.Nom_produit = copyOfout_Kalys.Nom_produit;			row20.Description = copyOfout_Kalys.Description;			row20.Url = copyOfout_Kalys.Url;			row20.Visibilite = copyOfout_Kalys.Visibilite;			row20.Prix = copyOfout_Kalys.Prix;			row20.Prix_en_promo = copyOfout_Kalys.Prix_en_promo;			row20.En_Promo = copyOfout_Kalys.En_Promo;			row20.Canal = copyOfout_Kalys.Canal;			row20.Source = copyOfout_Kalys.Source;					
		nb_uniques_tUniqRow_7++;
	} else {
	  nb_duplicates_tUniqRow_7++;
	}

 


	tos_count_tUniqRow_7++;

/**
 * [tUniqRow_7 main ] stop
 */
	
	/**
	 * [tUniqRow_7 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_7";

	

 



/**
 * [tUniqRow_7 process_data_begin ] stop
 */
// Start of branch "row20"
if(row20 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row20"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row20.Produit_PK;								
			row9.UGS = row20.UGS;								
			row9.Nom_produit = row20.Nom_produit;								
			row9.Description = row20.Description;								
			row9.Url = row20.Url;								
			row9.Visibilite = row20.Visibilite;								
			row9.Prix = row20.Prix;								
			row9.Prix_en_promo = row20.Prix_en_promo;								
			row9.En_Promo = row20.En_Promo;								
			row9.Canal = row20.Canal;								
			row9.Source = row20.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row20"




	
	/**
	 * [tUniqRow_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_7";

	

 



/**
 * [tUniqRow_7 process_data_end ] stop
 */

} // End of branch "copyOfout_Kalys"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
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
	 * [tMap_7 end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row19");
			  	}
			  	
 

ok_Hash.put("tMap_7", true);
end_Hash.put("tMap_7", System.currentTimeMillis());




/**
 * [tMap_7 end ] stop
 */

	
	/**
	 * [tUniqRow_7 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_7";

	

globalMap.put("tUniqRow_7_NB_UNIQUES",nb_uniques_tUniqRow_7);
globalMap.put("tUniqRow_7_NB_DUPLICATES",nb_duplicates_tUniqRow_7);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"copyOfout_Kalys");
			  	}
			  	
 

ok_Hash.put("tUniqRow_7", true);
end_Hash.put("tUniqRow_7", System.currentTimeMillis());




/**
 * [tUniqRow_7 end ] stop
 */








	
	/**
	 * [tUniqRow_9 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_9", false);
		start_Hash.put("tUniqRow_9", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_9";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"copyOfcopyOfout_Kalys");
					}
				
		int tos_count_tUniqRow_9 = 0;
		

	
		class KeyStruct_tUniqRow_9 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Nom_produit;        
	        
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
				final KeyStruct_tUniqRow_9 other = (KeyStruct_tUniqRow_9) obj;
				
									if (this.Nom_produit == null) {
										if (other.Nom_produit != null) 
											return false;
								
									} else if (!this.Nom_produit.equals(other.Nom_produit))
								 
										return false;
								
				
				return true;
			}
	  
	        
		}

	
int nb_uniques_tUniqRow_9 = 0;
int nb_duplicates_tUniqRow_9 = 0;
KeyStruct_tUniqRow_9 finder_tUniqRow_9 = new KeyStruct_tUniqRow_9();
java.util.Set<KeyStruct_tUniqRow_9> keystUniqRow_9 = new java.util.HashSet<KeyStruct_tUniqRow_9>(); 

 



/**
 * [tUniqRow_9 begin ] stop
 */



	
	/**
	 * [tMap_8 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_8", false);
		start_Hash.put("tMap_8", System.currentTimeMillis());
		
	
	currentComponent="tMap_8";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row21");
					}
				
		int tos_count_tMap_8 = 0;
		




// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_8__Struct  {
}
Var__tMap_8__Struct Var__tMap_8 = new Var__tMap_8__Struct();
// ###############################

// ###############################
// # Outputs initialization
copyOfcopyOfout_KalysStruct copyOfcopyOfout_Kalys_tmp = new copyOfcopyOfout_KalysStruct();
// ###############################

        
        



        









 



/**
 * [tMap_8 begin ] stop
 */



	
	/**
	 * [tDBInput_8 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBInput_8", false);
		start_Hash.put("tDBInput_8", System.currentTimeMillis());
		
	
	currentComponent="tDBInput_8";

	
		int tos_count_tDBInput_8 = 0;
		
	
    
	
			org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_8 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();
			
			java.util.List<String> talendToDBList_tDBInput_8 = new java.util.ArrayList();
			String[] talendToDBArray_tDBInput_8  = new String[]{"FLOAT","NUMERIC","NUMERIC IDENTITY","DECIMAL","DECIMAL IDENTITY","REAL"}; 
			java.util.Collections.addAll(talendToDBList_tDBInput_8, talendToDBArray_tDBInput_8); 
		    int nb_line_tDBInput_8 = 0;
		    java.sql.Connection conn_tDBInput_8 = null;
				String driverClass_tDBInput_8 = "net.sourceforge.jtds.jdbc.Driver";
			    java.lang.Class jdbcclazz_tDBInput_8 = java.lang.Class.forName(driverClass_tDBInput_8);
				String dbUser_tDBInput_8 = "sa1";
				
				 
	final String decryptedPassword_tDBInput_8 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:htb+FDw4mv3Dl3a32FcY3u0ZqsyWe0aXcIP17fLyLpXn4MLzJ8w=");
				
				String dbPwd_tDBInput_8 = decryptedPassword_tDBInput_8;
				
		    String port_tDBInput_8 = "1433";
		    String dbname_tDBInput_8 = "PA_BI_SA" ;
			String url_tDBInput_8 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_8)) {
		    	url_tDBInput_8 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_8)) {
				url_tDBInput_8 += "//" + "PA_BI_SA"; 
		    }
		    url_tDBInput_8 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_8 = "";
				
				conn_tDBInput_8 = java.sql.DriverManager.getConnection(url_tDBInput_8,dbUser_tDBInput_8,dbPwd_tDBInput_8);
		        
		    
			java.sql.Statement stmt_tDBInput_8 = conn_tDBInput_8.createStatement();

		    String dbquery_tDBInput_8 = "SELECT etats_achats_ventes_Suivi_achats.Societe,\n		etats_achats_ventes_Suivi_achats.Nom_du_Fournisseur,\n		etats_achats_"
+"ventes_Suivi_achats.N_Facture,\n		etats_achats_ventes_Suivi_achats.Date_de_facture,\n		etats_achats_ventes_Suivi_achats.Mo"
+"ntant_HT,\n		etats_achats_ventes_Suivi_achats.Taux_de_TVA,\n		etats_achats_ventes_Suivi_achats.Timbre,\n		etats_achats_vent"
+"es_Suivi_achats.Montant_de_la_TVA,\n		etats_achats_ventes_Suivi_achats.FODEC,\n		etats_achats_ventes_Suivi_achats.Montant_"
+"TTC,\n		etats_achats_ventes_Suivi_achats.Retenue,\n		etats_achats_ventes_Suivi_achats.Versements_effectues_par_SOUGUI,\n		e"
+"tats_achats_ventes_Suivi_achats.Reste_du,\n		etats_achats_ventes_Suivi_achats.Action_a_effectuer,\n		etats_achats_ventes_S"
+"uivi_achats.Date_de_paiement_total_de_la_facture,\n		etats_achats_ventes_Suivi_achats.mode_de_reglement,\n		etats_achats_v"
+"entes_Suivi_achats.Date_de_la_premiere_relance,\n		etats_achats_ventes_Suivi_achats.Date_de_la_deuxieme_relance,\n		etats_"
+"achats_ventes_Suivi_achats.Penalite_et_majoration_de_paiement,\n		etats_achats_ventes_Suivi_achats.Commentaire\nFROM	etats"
+"_achats_ventes_Suivi_achats";
		    

            	globalMap.put("tDBInput_8_QUERY",dbquery_tDBInput_8);
		    java.sql.ResultSet rs_tDBInput_8 = null;

		    try {
		    	rs_tDBInput_8 = stmt_tDBInput_8.executeQuery(dbquery_tDBInput_8);
		    	java.sql.ResultSetMetaData rsmd_tDBInput_8 = rs_tDBInput_8.getMetaData();
		    	int colQtyInRs_tDBInput_8 = rsmd_tDBInput_8.getColumnCount();

		    String tmpContent_tDBInput_8 = null;
		    
		    
		    while (rs_tDBInput_8.next()) {
		        nb_line_tDBInput_8++;
		        
							if(colQtyInRs_tDBInput_8 < 1) {
								row21.Societe = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(1);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Societe = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Societe = tmpContent_tDBInput_8;
                }
            } else {
                row21.Societe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 2) {
								row21.Nom_du_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(2);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Nom_du_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Nom_du_Fournisseur = tmpContent_tDBInput_8;
                }
            } else {
                row21.Nom_du_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 3) {
								row21.N_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(3);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.N_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.N_Facture = tmpContent_tDBInput_8;
                }
            } else {
                row21.N_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 4) {
								row21.Date_de_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(4);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Date_de_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Date_de_facture = tmpContent_tDBInput_8;
                }
            } else {
                row21.Date_de_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 5) {
								row21.Montant_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(5);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Montant_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Montant_HT = tmpContent_tDBInput_8;
                }
            } else {
                row21.Montant_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 6) {
								row21.Taux_de_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(6);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Taux_de_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Taux_de_TVA = tmpContent_tDBInput_8;
                }
            } else {
                row21.Taux_de_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 7) {
								row21.Timbre = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(7);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Timbre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Timbre = tmpContent_tDBInput_8;
                }
            } else {
                row21.Timbre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 8) {
								row21.Montant_de_la_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(8);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Montant_de_la_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Montant_de_la_TVA = tmpContent_tDBInput_8;
                }
            } else {
                row21.Montant_de_la_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 9) {
								row21.FODEC = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(9);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.FODEC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.FODEC = tmpContent_tDBInput_8;
                }
            } else {
                row21.FODEC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 10) {
								row21.Montant_TTC = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(10);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Montant_TTC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Montant_TTC = tmpContent_tDBInput_8;
                }
            } else {
                row21.Montant_TTC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 11) {
								row21.Retenue = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(11);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Retenue = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Retenue = tmpContent_tDBInput_8;
                }
            } else {
                row21.Retenue = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 12) {
								row21.Versements_effectues_par_SOUGUI = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(12);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Versements_effectues_par_SOUGUI = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Versements_effectues_par_SOUGUI = tmpContent_tDBInput_8;
                }
            } else {
                row21.Versements_effectues_par_SOUGUI = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 13) {
								row21.Reste_du = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(13);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Reste_du = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Reste_du = tmpContent_tDBInput_8;
                }
            } else {
                row21.Reste_du = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 14) {
								row21.Action_a_effectuer = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(14);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Action_a_effectuer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Action_a_effectuer = tmpContent_tDBInput_8;
                }
            } else {
                row21.Action_a_effectuer = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 15) {
								row21.Date_de_paiement_total_de_la_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(15);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Date_de_paiement_total_de_la_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Date_de_paiement_total_de_la_facture = tmpContent_tDBInput_8;
                }
            } else {
                row21.Date_de_paiement_total_de_la_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 16) {
								row21.mode_de_reglement = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(16);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.mode_de_reglement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.mode_de_reglement = tmpContent_tDBInput_8;
                }
            } else {
                row21.mode_de_reglement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 17) {
								row21.Date_de_la_premiere_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(17);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Date_de_la_premiere_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Date_de_la_premiere_relance = tmpContent_tDBInput_8;
                }
            } else {
                row21.Date_de_la_premiere_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 18) {
								row21.Date_de_la_deuxieme_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(18);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Date_de_la_deuxieme_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Date_de_la_deuxieme_relance = tmpContent_tDBInput_8;
                }
            } else {
                row21.Date_de_la_deuxieme_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 19) {
								row21.Penalite_et_majoration_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(19);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Penalite_et_majoration_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Penalite_et_majoration_de_paiement = tmpContent_tDBInput_8;
                }
            } else {
                row21.Penalite_et_majoration_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 20) {
								row21.Commentaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(20);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row21.Commentaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row21.Commentaire = tmpContent_tDBInput_8;
                }
            } else {
                row21.Commentaire = null;
            }
		                    }
					





 



/**
 * [tDBInput_8 begin ] stop
 */
	
	/**
	 * [tDBInput_8 main ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

 


	tos_count_tDBInput_8++;

/**
 * [tDBInput_8 main ] stop
 */
	
	/**
	 * [tDBInput_8 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

 



/**
 * [tDBInput_8 process_data_begin ] stop
 */

	
	/**
	 * [tMap_8 main ] start
	 */

	

	
	
	currentComponent="tMap_8";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row21"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_8 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_8 = false;
		  boolean mainRowRejected_tMap_8 = false;
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_8__Struct Var = Var__tMap_8;// ###############################
        // ###############################
        // # Output tables

copyOfcopyOfout_Kalys = null;


// # Output table : 'copyOfcopyOfout_Kalys'
copyOfcopyOfout_Kalys_tmp.Produit_PK = 0;
copyOfcopyOfout_Kalys_tmp.UGS = SKUGenerator.generateSKU(row21.Commentaire) ;
copyOfcopyOfout_Kalys_tmp.Nom_produit = row21.Commentaire == null || row21.Commentaire.length() == 0
    ? "Unknown"
    : row21.Commentaire.substring(0,1).toUpperCase() 
      + row21.Commentaire.substring(1).toLowerCase() ;
copyOfcopyOfout_Kalys_tmp.Description = "Unknown";
copyOfcopyOfout_Kalys_tmp.Url = "Unknown";
copyOfcopyOfout_Kalys_tmp.Visibilite = false;
copyOfcopyOfout_Kalys_tmp.Prix = 0.0;
copyOfcopyOfout_Kalys_tmp.Prix_en_promo = 0.0;
copyOfcopyOfout_Kalys_tmp.En_Promo = false;
copyOfcopyOfout_Kalys_tmp.Canal = "Achat";
copyOfcopyOfout_Kalys_tmp.Source = row21.Nom_du_Fournisseur ;
copyOfcopyOfout_Kalys = copyOfcopyOfout_Kalys_tmp;
// ###############################

} // end of Var scope

rejectedInnerJoin_tMap_8 = false;










 


	tos_count_tMap_8++;

/**
 * [tMap_8 main ] stop
 */
	
	/**
	 * [tMap_8 process_data_begin ] start
	 */

	

	
	
	currentComponent="tMap_8";

	

 



/**
 * [tMap_8 process_data_begin ] stop
 */
// Start of branch "copyOfcopyOfout_Kalys"
if(copyOfcopyOfout_Kalys != null) { 



	
	/**
	 * [tUniqRow_9 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_9";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"copyOfcopyOfout_Kalys"
						
						);
					}
					
row22 = null;			
if(copyOfcopyOfout_Kalys.Nom_produit == null){
	finder_tUniqRow_9.Nom_produit = null;
}else{
	finder_tUniqRow_9.Nom_produit = copyOfcopyOfout_Kalys.Nom_produit.toLowerCase();
}	
finder_tUniqRow_9.hashCodeDirty = true;
if (!keystUniqRow_9.contains(finder_tUniqRow_9)) {
		KeyStruct_tUniqRow_9 new_tUniqRow_9 = new KeyStruct_tUniqRow_9();

		
if(copyOfcopyOfout_Kalys.Nom_produit == null){
	new_tUniqRow_9.Nom_produit = null;
}else{
	new_tUniqRow_9.Nom_produit = copyOfcopyOfout_Kalys.Nom_produit.toLowerCase();
}
		
		keystUniqRow_9.add(new_tUniqRow_9);if(row22 == null){ 
	
	row22 = new row22Struct();
}row22.Produit_PK = copyOfcopyOfout_Kalys.Produit_PK;			row22.UGS = copyOfcopyOfout_Kalys.UGS;			row22.Nom_produit = copyOfcopyOfout_Kalys.Nom_produit;			row22.Description = copyOfcopyOfout_Kalys.Description;			row22.Url = copyOfcopyOfout_Kalys.Url;			row22.Visibilite = copyOfcopyOfout_Kalys.Visibilite;			row22.Prix = copyOfcopyOfout_Kalys.Prix;			row22.Prix_en_promo = copyOfcopyOfout_Kalys.Prix_en_promo;			row22.En_Promo = copyOfcopyOfout_Kalys.En_Promo;			row22.Canal = copyOfcopyOfout_Kalys.Canal;			row22.Source = copyOfcopyOfout_Kalys.Source;					
		nb_uniques_tUniqRow_9++;
	} else {
	  nb_duplicates_tUniqRow_9++;
	}

 


	tos_count_tUniqRow_9++;

/**
 * [tUniqRow_9 main ] stop
 */
	
	/**
	 * [tUniqRow_9 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_9";

	

 



/**
 * [tUniqRow_9 process_data_begin ] stop
 */
// Start of branch "row22"
if(row22 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row22"
									
							);
						}
						
//////////
 

// for output
			row9 = new row9Struct();
								
			row9.Produit_PK = row22.Produit_PK;								
			row9.UGS = row22.UGS;								
			row9.Nom_produit = row22.Nom_produit;								
			row9.Description = row22.Description;								
			row9.Url = row22.Url;								
			row9.Visibilite = row22.Visibilite;								
			row9.Prix = row22.Prix;								
			row9.Prix_en_promo = row22.Prix_en_promo;								
			row9.En_Promo = row22.En_Promo;								
			row9.Canal = row22.Canal;								
			row9.Source = row22.Source;			

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
	 * [tUniqRow_4 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					
row10 = null;			
if(row9.Nom_produit == null){
	finder_tUniqRow_4.Nom_produit = null;
}else{
	finder_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}	
finder_tUniqRow_4.hashCodeDirty = true;
if (!keystUniqRow_4.contains(finder_tUniqRow_4)) {
		KeyStruct_tUniqRow_4 new_tUniqRow_4 = new KeyStruct_tUniqRow_4();

		
if(row9.Nom_produit == null){
	new_tUniqRow_4.Nom_produit = null;
}else{
	new_tUniqRow_4.Nom_produit = row9.Nom_produit.toLowerCase();
}
		
		keystUniqRow_4.add(new_tUniqRow_4);if(row10 == null){ 
	
	row10 = new row10Struct();
}row10.Produit_PK = row9.Produit_PK;			row10.UGS = row9.UGS;			row10.Nom_produit = row9.Nom_produit;			row10.Description = row9.Description;			row10.Url = row9.Url;			row10.Visibilite = row9.Visibilite;			row10.Prix = row9.Prix;			row10.Prix_en_promo = row9.Prix_en_promo;			row10.En_Promo = row9.En_Promo;			row10.Canal = row9.Canal;			row10.Source = row9.Source;					
		nb_uniques_tUniqRow_4++;
	} else {
	  nb_duplicates_tUniqRow_4++;
	}

 


	tos_count_tUniqRow_4++;

/**
 * [tUniqRow_4 main ] stop
 */
	
	/**
	 * [tUniqRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_begin ] stop
 */
// Start of branch "row10"
if(row10 != null) { 



	
	/**
	 * [tLogRow_4 main ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_4 = new String[11];
              
                 row_tLogRow_4[0]=    						    
				                String.valueOf(row10.Produit_PK)			
					          ;	
										
    			   				
	    		if(row10.UGS != null) { //              
                 row_tLogRow_4[1]=    						    
				                String.valueOf(row10.UGS)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Nom_produit != null) { //              
                 row_tLogRow_4[2]=    						    
				                String.valueOf(row10.Nom_produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Description != null) { //              
                 row_tLogRow_4[3]=    						    
				                String.valueOf(row10.Description)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Url != null) { //              
                 row_tLogRow_4[4]=    						    
				                String.valueOf(row10.Url)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Visibilite != null) { //              
                 row_tLogRow_4[5]=    						    
				                String.valueOf(row10.Visibilite)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix != null) { //              
                 row_tLogRow_4[6]=    						
								FormatterUtils.formatUnwithE(row10.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Prix_en_promo != null) { //              
                 row_tLogRow_4[7]=    						
								FormatterUtils.formatUnwithE(row10.Prix_en_promo)
					          ;	
							
	    		} //			
    			   				
	    		if(row10.En_Promo != null) { //              
                 row_tLogRow_4[8]=    						    
				                String.valueOf(row10.En_Promo)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Canal != null) { //              
                 row_tLogRow_4[9]=    						    
				                String.valueOf(row10.Canal)			
					          ;	
							
	    		} //			
    			   				
	    		if(row10.Source != null) { //              
                 row_tLogRow_4[10]=    						    
				                String.valueOf(row10.Source)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_4.addRow(row_tLogRow_4);	
				nb_line_tLogRow_4++;
//////

//////                    
                    
///////////////////////    			

 
     row11 = row10;


	tos_count_tLogRow_4++;

/**
 * [tLogRow_4 main ] stop
 */
	
	/**
	 * [tLogRow_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_begin ] stop
 */

	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_produit = row11.Nom_produit;
            lookUpKey_tDBSCD_1.UGS = row11.UGS;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                    if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                    if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                    if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                    if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                    if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                    if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                    if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                    if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                    if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Description == null && row11.Description!= null) || (lookUpValue_tDBSCD_1.Description != null && !lookUpValue_tDBSCD_1.Description.equals(row11.Description)) || (lookUpValue_tDBSCD_1.En_Promo == null && row11.En_Promo!= null) || (lookUpValue_tDBSCD_1.En_Promo != null && !lookUpValue_tDBSCD_1.En_Promo.equals(row11.En_Promo)) || (lookUpValue_tDBSCD_1.Url == null && row11.Url!= null) || (lookUpValue_tDBSCD_1.Url != null && !lookUpValue_tDBSCD_1.Url.equals(row11.Url)) || (lookUpValue_tDBSCD_1.Visibilite == null && row11.Visibilite!= null) || (lookUpValue_tDBSCD_1.Visibilite != null && !lookUpValue_tDBSCD_1.Visibilite.equals(row11.Visibilite))) {
                    if(row11.Description == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row11.Description);
}

                    if(row11.En_Promo == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(2, row11.En_Promo);
}

                    if(row11.Url == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row11.Url);
}

                    if(row11.Visibilite == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
updateForType1_tDBSCD_1.setBoolean(4, row11.Visibilite);
}

                    if(row11.Nom_produit == null) {
updateForType1_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(5, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType1_tDBSCD_1.setNull(6, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(6, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
            if((lookUpValue_tDBSCD_1.Canal == null && row11.Canal!= null) || (lookUpValue_tDBSCD_1.Canal != null && !lookUpValue_tDBSCD_1.Canal.equals(row11.Canal)) || (lookUpValue_tDBSCD_1.Prix == null && row11.Prix!= null) || (lookUpValue_tDBSCD_1.Prix != null && !lookUpValue_tDBSCD_1.Prix.equals(row11.Prix)) || (lookUpValue_tDBSCD_1.Prix_en_promo == null && row11.Prix_en_promo!= null) || (lookUpValue_tDBSCD_1.Prix_en_promo != null && !lookUpValue_tDBSCD_1.Prix_en_promo.equals(row11.Prix_en_promo)) || (lookUpValue_tDBSCD_1.Source == null && row11.Source!= null) || (lookUpValue_tDBSCD_1.Source != null && !lookUpValue_tDBSCD_1.Source.equals(row11.Source))) {
                    if(row11.Nom_produit == null) {
updateForType2_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(2, row11.Nom_produit);
}

                    if(row11.UGS == null) {
updateForType2_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType2_tDBSCD_1.setString(3, row11.UGS);
}

                nb_line_update_tDBSCD_1 += updateForType2_tDBSCD_1.executeUpdate();
                            if(row11.Nom_produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row11.Nom_produit);
}

                            if(row11.UGS == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row11.UGS);
}

                            if(row11.Description == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row11.Description);
}

                            if(row11.En_Promo == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(4, row11.En_Promo);
}

                            if(row11.Url == null) {
insertionStatement_tDBSCD_1.setNull(5, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(5, row11.Url);
}

                            if(row11.Visibilite == null) {
insertionStatement_tDBSCD_1.setNull(6, java.sql.Types.BOOLEAN);
} else {
insertionStatement_tDBSCD_1.setBoolean(6, row11.Visibilite);
}

                            if(row11.Canal == null) {
insertionStatement_tDBSCD_1.setNull(7, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(7, row11.Canal);
}

                            if(row11.Prix == null) {
insertionStatement_tDBSCD_1.setNull(8, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(8, row11.Prix);
}

                            if(row11.Prix_en_promo == null) {
insertionStatement_tDBSCD_1.setNull(9, java.sql.Types.DOUBLE);
} else {
insertionStatement_tDBSCD_1.setDouble(9, row11.Prix_en_promo);
}

                            if(row11.Source == null) {
insertionStatement_tDBSCD_1.setNull(10, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(10, row11.Source);
}

                nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Description = row11.Description;
                lookUpValue_tDBSCD_1.En_Promo = row11.En_Promo;
                lookUpValue_tDBSCD_1.Url = row11.Url;
                lookUpValue_tDBSCD_1.Visibilite = row11.Visibilite;
                lookUpValue_tDBSCD_1.Canal = row11.Canal;
                lookUpValue_tDBSCD_1.Prix = row11.Prix;
                lookUpValue_tDBSCD_1.Prix_en_promo = row11.Prix_en_promo;
                lookUpValue_tDBSCD_1.Source = row11.Source;
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



	
	/**
	 * [tLogRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 process_data_end ] stop
 */

} // End of branch "row10"




	
	/**
	 * [tUniqRow_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row22"




	
	/**
	 * [tUniqRow_9 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_9";

	

 



/**
 * [tUniqRow_9 process_data_end ] stop
 */

} // End of branch "copyOfcopyOfout_Kalys"




	
	/**
	 * [tMap_8 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_8";

	

 



/**
 * [tMap_8 process_data_end ] stop
 */



	
	/**
	 * [tDBInput_8 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

 



/**
 * [tDBInput_8 process_data_end ] stop
 */
	
	/**
	 * [tDBInput_8 end ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

	}
}finally{
	if (rs_tDBInput_8 != null) {
		rs_tDBInput_8.close();
	}
	if (stmt_tDBInput_8 != null) {
		stmt_tDBInput_8.close();
	}
		if(conn_tDBInput_8 != null && !conn_tDBInput_8.isClosed()) {
			
			conn_tDBInput_8.close();
			
			if("com.mysql.cj.jdbc.Driver".equals((String)globalMap.get("driverClass_"))
			    && routines.system.BundleUtils.inOSGi()) {
			        Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread").
			            getMethod("checkedShutdown").invoke(null, (Object[]) null);
			}
			
		}
}
globalMap.put("tDBInput_8_NB_LINE",nb_line_tDBInput_8);

 

ok_Hash.put("tDBInput_8", true);
end_Hash.put("tDBInput_8", System.currentTimeMillis());




/**
 * [tDBInput_8 end ] stop
 */

	
	/**
	 * [tMap_8 end ] start
	 */

	

	
	
	currentComponent="tMap_8";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row21");
			  	}
			  	
 

ok_Hash.put("tMap_8", true);
end_Hash.put("tMap_8", System.currentTimeMillis());




/**
 * [tMap_8 end ] stop
 */

	
	/**
	 * [tUniqRow_9 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_9";

	

globalMap.put("tUniqRow_9_NB_UNIQUES",nb_uniques_tUniqRow_9);
globalMap.put("tUniqRow_9_NB_DUPLICATES",nb_duplicates_tUniqRow_9);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"copyOfcopyOfout_Kalys");
			  	}
			  	
 

ok_Hash.put("tUniqRow_9", true);
end_Hash.put("tUniqRow_9", System.currentTimeMillis());




/**
 * [tUniqRow_9 end ] stop
 */






	
	/**
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row8","row18","row16","row20","row6","row15","row7","row22");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
 */

	
	/**
	 * [tUniqRow_4 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

globalMap.put("tUniqRow_4_NB_UNIQUES",nb_uniques_tUniqRow_4);
globalMap.put("tUniqRow_4_NB_DUPLICATES",nb_duplicates_tUniqRow_4);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row9");
			  	}
			  	
 

ok_Hash.put("tUniqRow_4", true);
end_Hash.put("tUniqRow_4", System.currentTimeMillis());




/**
 * [tUniqRow_4 end ] stop
 */

	
	/**
	 * [tLogRow_4 end ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	


//////

                    
                    java.io.PrintStream consoleOut_tLogRow_4 = null;
                    if (globalMap.get("tLogRow_CONSOLE")!=null)
                    {
                    	consoleOut_tLogRow_4 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
                    }
                    else
                    {
                    	consoleOut_tLogRow_4 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                    	globalMap.put("tLogRow_CONSOLE",consoleOut_tLogRow_4);
                    }
                    
                    consoleOut_tLogRow_4.println(util_tLogRow_4.format().toString());
                    consoleOut_tLogRow_4.flush();
//////
globalMap.put("tLogRow_4_NB_LINE",nb_line_tLogRow_4);

///////////////////////    			

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row10");
			  	}
			  	
 

ok_Hash.put("tLogRow_4", true);
end_Hash.put("tLogRow_4", System.currentTimeMillis());




/**
 * [tLogRow_4 end ] stop
 */

	
	/**
	 * [tDBSCD_1 end ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

    insertionStatement_tDBSCD_1.close();
        updateForType1_tDBSCD_1.close();
        updateForType2_tDBSCD_1.close();
    
    if(conn_tDBSCD_1 != null && !conn_tDBSCD_1.isClosed()) {
        conn_tDBSCD_1.close();
    }    
    
    globalMap.put("tDBSCD_1_NB_LINE_UPDATED", nb_line_update_tDBSCD_1);
    globalMap.put("tDBSCD_1_NB_LINE_INSERTED", nb_line_inserted_tDBSCD_1);
    globalMap.put("tDBSCD_1_NB_LINE_REJECTED",nb_line_rejected_tDBSCD_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row11");
			  	}
			  	
 

ok_Hash.put("tDBSCD_1", true);
end_Hash.put("tDBSCD_1", System.currentTimeMillis());




/**
 * [tDBSCD_1 end ] stop
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
	 * [tUniqRow_1 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 finally ] stop
 */

	
	/**
	 * [tLogRow_1 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 finally ] stop
 */









	
	/**
	 * [tDBInput_2 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 



/**
 * [tDBInput_2 finally ] stop
 */

	
	/**
	 * [tFilterRow_1 finally ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 finally ] stop
 */

	
	/**
	 * [tMap_2 finally ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 finally ] stop
 */

	
	/**
	 * [tUniqRow_2 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

 



/**
 * [tUniqRow_2 finally ] stop
 */









	
	/**
	 * [tDBInput_3 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 



/**
 * [tDBInput_3 finally ] stop
 */

	
	/**
	 * [tMap_3 finally ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 finally ] stop
 */

	
	/**
	 * [tUniqRow_3 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 finally ] stop
 */

	
	/**
	 * [tLogRow_2 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 finally ] stop
 */









	
	/**
	 * [tDBInput_5 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 



/**
 * [tDBInput_5 finally ] stop
 */

	
	/**
	 * [tMap_5 finally ] start
	 */

	

	
	
	currentComponent="tMap_5";

	

 



/**
 * [tMap_5 finally ] stop
 */

	
	/**
	 * [tUniqRow_5 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_5";

	

 



/**
 * [tUniqRow_5 finally ] stop
 */






	
	/**
	 * [tDBInput_4 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 finally ] stop
 */

	
	/**
	 * [tMap_6 finally ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 finally ] stop
 */

	
	/**
	 * [tUniqRow_6 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_6";

	

 



/**
 * [tUniqRow_6 finally ] stop
 */






	
	/**
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
 */

	
	/**
	 * [tMap_4 finally ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 finally ] stop
 */

	
	/**
	 * [tUniqRow_8 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_8";

	

 



/**
 * [tUniqRow_8 finally ] stop
 */






	
	/**
	 * [tDBInput_7 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 finally ] stop
 */

	
	/**
	 * [tMap_7 finally ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 finally ] stop
 */

	
	/**
	 * [tUniqRow_7 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_7";

	

 



/**
 * [tUniqRow_7 finally ] stop
 */






	
	/**
	 * [tDBInput_8 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

 



/**
 * [tDBInput_8 finally ] stop
 */

	
	/**
	 * [tMap_8 finally ] start
	 */

	

	
	
	currentComponent="tMap_8";

	

 



/**
 * [tMap_8 finally ] stop
 */

	
	/**
	 * [tUniqRow_9 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_9";

	

 



/**
 * [tUniqRow_9 finally ] stop
 */






	
	/**
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tUniqRow_4 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_4";

	

 



/**
 * [tUniqRow_4 finally ] stop
 */

	
	/**
	 * [tLogRow_4 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_4";

	

 



/**
 * [tLogRow_4 finally ] stop
 */

	
	/**
	 * [tDBSCD_1 finally ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	

 



/**
 * [tDBSCD_1 finally ] stop
 */









				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_1_SUBPROCESS_STATE", 1);
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
        final ETL_Dim_Produits ETL_Dim_ProduitsClass = new ETL_Dim_Produits();

        int exitCode = ETL_Dim_ProduitsClass.runJobInTOS(args);

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
            java.io.InputStream inContext = ETL_Dim_Produits.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/etl_dim_produits_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ETL_Dim_Produits.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
errorCode = null;tDBInput_1Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tDBInput_1) {
globalMap.put("tDBInput_1_SUBPROCESS_STATE", -1);

e_tDBInput_1.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ETL_Dim_Produits");
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
 *     767366 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:45 CET
 ************************************************************************************************/