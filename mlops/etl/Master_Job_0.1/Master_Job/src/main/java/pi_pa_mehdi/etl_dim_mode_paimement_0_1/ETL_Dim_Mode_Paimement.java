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


package pi_pa_mehdi.etl_dim_mode_paimement_0_1;

import routines.Numeric;
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.StringHandling;
import routines.Relational;
import routines.TalendDate;
import routines.Mathematical;
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
 




	//the import part of tJava_2
	//import java.util.List;

	//the import part of tJava_1
	//import java.util.List;


@SuppressWarnings("unused")

/**
 * Job: ETL_Dim_Mode_Paimement Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ETL_Dim_Mode_Paimement implements TalendJob {

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
	private final String jobName = "ETL_Dim_Mode_Paimement";
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
				ETL_Dim_Mode_Paimement.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ETL_Dim_Mode_Paimement.this, new Object[] { e , currentComponent, globalMap});
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

			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tChronometerStop_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tChronometerStop_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tPostjob_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tPostjob_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tChronometerStart_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tChronometerStart_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBConnection_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBConnection_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBConnection_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBConnection_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBConnection_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBConnection_4_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBConnection_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBConnection_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_6_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_6_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tChronometerStop_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tPostjob_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tChronometerStart_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBConnection_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBConnection_3_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBConnection_4_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBConnection_5_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class row6Struct implements routines.system.IPersistableRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
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
	
							result = prime * result + (int) this.Mode_Paiement_PK;
						
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
		
						if (this.Mode_Paiement_PK != other.Mode_Paiement_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row6Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(row6Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
		
						returnValue = checkNullsAndCompare(this.Mode_Paiement_PK, other.Mode_Paiement_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
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
	
							result = prime * result + (int) this.Mode_Paiement_PK;
						
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
		
						if (this.Mode_Paiement_PK != other.Mode_Paiement_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row5Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(row5Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(row5Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Mode_Paiement_PK, other.Mode_Paiement_PK);
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

public static class out411Struct implements routines.system.IPersistableRow<out411Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
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
	
							result = prime * result + (int) this.Mode_Paiement_PK;
						
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
		final out411Struct other = (out411Struct) obj;
		
						if (this.Mode_Paiement_PK != other.Mode_Paiement_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out411Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(out411Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
			        this.Mode_Paiement_PK = dis.readInt();
					
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Mode_Paiement_PK);
					
					// String
				
						writeString(this.Type_paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(out411Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Mode_Paiement_PK, other.Mode_Paiement_PK);
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

public static class row3Struct implements routines.system.IPersistableRow<row3Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Mode_Paimement;

				public String getMode_Paimement () {
					return this.Mode_Paimement;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paimement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paimement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paimement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paimement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Mode_Paimement="+Mode_Paimement);
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

public static class out1Struct implements routines.system.IPersistableRow<out1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Mode_Paimement;

				public String getMode_Paimement () {
					return this.Mode_Paimement;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paimement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paimement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paimement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paimement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Mode_Paimement="+Mode_Paimement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out1Struct other) {

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

public static class row1Struct implements routines.system.IPersistableRow<row1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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

public static class after_tDBInput_6Struct implements routines.system.IPersistableRow<after_tDBInput_6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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
    public int compareTo(after_tDBInput_6Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
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
		sb.append("Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out2Struct other) {

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

public static class row2Struct implements routines.system.IPersistableRow<row2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Date;

				public String getDate () {
					return this.Date;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public Integer Prix_Unitaire__DT;

				public Integer getPrix_Unitaire__DT () {
					return this.Prix_Unitaire__DT;
				}
				
			    public String Remise;

				public String getRemise () {
					return this.Remise;
				}
				
			    public Integer Montant_Total__DT;

				public Integer getMontant_Total__DT () {
					return this.Montant_Total__DT;
				}
				
			    public Integer Montant_Remise__DT;

				public Integer getMontant_Remise__DT () {
					return this.Montant_Remise__DT;
				}
				
			    public String Type_de_Paiement__Especes___Carte;

				public String getType_de_Paiement__Especes___Carte () {
					return this.Type_de_Paiement__Especes___Carte;
				}
				
			    public String Client__optionnel;

				public String getClient__optionnel () {
					return this.Client__optionnel;
				}
				
			    public String Remarques;

				public String getRemarques () {
					return this.Remarques;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Date = readString(dis);
					
					this.Produit = readString(dis);
					
						this.Quantite = readInteger(dis);
					
						this.Prix_Unitaire__DT = readInteger(dis);
					
					this.Remise = readString(dis);
					
						this.Montant_Total__DT = readInteger(dis);
					
						this.Montant_Remise__DT = readInteger(dis);
					
					this.Type_de_Paiement__Especes___Carte = readString(dis);
					
					this.Client__optionnel = readString(dis);
					
					this.Remarques = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Date = readString(dis);
					
					this.Produit = readString(dis);
					
						this.Quantite = readInteger(dis);
					
						this.Prix_Unitaire__DT = readInteger(dis);
					
					this.Remise = readString(dis);
					
						this.Montant_Total__DT = readInteger(dis);
					
						this.Montant_Remise__DT = readInteger(dis);
					
					this.Type_de_Paiement__Especes___Carte = readString(dis);
					
					this.Client__optionnel = readString(dis);
					
					this.Remarques = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Integer
				
						writeInteger(this.Prix_Unitaire__DT,dos);
					
					// String
				
						writeString(this.Remise,dos);
					
					// Integer
				
						writeInteger(this.Montant_Total__DT,dos);
					
					// Integer
				
						writeInteger(this.Montant_Remise__DT,dos);
					
					// String
				
						writeString(this.Type_de_Paiement__Especes___Carte,dos);
					
					// String
				
						writeString(this.Client__optionnel,dos);
					
					// String
				
						writeString(this.Remarques,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// Integer
				
						writeInteger(this.Prix_Unitaire__DT,dos);
					
					// String
				
						writeString(this.Remise,dos);
					
					// Integer
				
						writeInteger(this.Montant_Total__DT,dos);
					
					// Integer
				
						writeInteger(this.Montant_Remise__DT,dos);
					
					// String
				
						writeString(this.Type_de_Paiement__Especes___Carte,dos);
					
					// String
				
						writeString(this.Client__optionnel,dos);
					
					// String
				
						writeString(this.Remarques,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Date="+Date);
		sb.append(",Produit="+Produit);
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Prix_Unitaire__DT="+String.valueOf(Prix_Unitaire__DT));
		sb.append(",Remise="+Remise);
		sb.append(",Montant_Total__DT="+String.valueOf(Montant_Total__DT));
		sb.append(",Montant_Remise__DT="+String.valueOf(Montant_Remise__DT));
		sb.append(",Type_de_Paiement__Especes___Carte="+Type_de_Paiement__Especes___Carte);
		sb.append(",Client__optionnel="+Client__optionnel);
		sb.append(",Remarques="+Remarques);
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

public static class etatAchatVenteStruct implements routines.system.IPersistableRow<etatAchatVenteStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
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
		sb.append("Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(etatAchatVenteStruct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Societe;

				public String getSociete () {
					return this.Societe;
				}
				
			    public String Nom_du_client;

				public String getNom_du_client () {
					return this.Nom_du_client;
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
				
			    public String Montant_TTC;

				public String getMontant_TTC () {
					return this.Montant_TTC;
				}
				
			    public String Retenue_a_la_source;

				public String getRetenue_a_la_source () {
					return this.Retenue_a_la_source;
				}
				
			    public String Versements_effectues_par_le_client;

				public String getVersements_effectues_par_le_client () {
					return this.Versements_effectues_par_le_client;
				}
				
			    public String Reste_du;

				public String getReste_du () {
					return this.Reste_du;
				}
				
			    public String Action_a_effectuer;

				public String getAction_a_effectuer () {
					return this.Action_a_effectuer;
				}
				
			    public String Delai_de_paiement_accorde_au_client;

				public String getDelai_de_paiement_accorde_au_client () {
					return this.Delai_de_paiement_accorde_au_client;
				}
				
			    public String Date_d_echeance;

				public String getDate_d_echeance () {
					return this.Date_d_echeance;
				}
				
			    public String Date_de_paiement_total_de_la_facture;

				public String getDate_de_paiement_total_de_la_facture () {
					return this.Date_de_paiement_total_de_la_facture;
				}
				
			    public String Retard_de_paiement__en_jours;

				public String getRetard_de_paiement__en_jours () {
					return this.Retard_de_paiement__en_jours;
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Societe = readString(dis);
					
					this.Nom_du_client = readString(dis);
					
					this.N_Facture = readString(dis);
					
					this.Date_de_facture = readString(dis);
					
					this.Montant_HT = readString(dis);
					
					this.Taux_de_TVA = readString(dis);
					
					this.Timbre = readString(dis);
					
					this.Montant_de_la_TVA = readString(dis);
					
					this.Montant_TTC = readString(dis);
					
					this.Retenue_a_la_source = readString(dis);
					
					this.Versements_effectues_par_le_client = readString(dis);
					
					this.Reste_du = readString(dis);
					
					this.Action_a_effectuer = readString(dis);
					
					this.Delai_de_paiement_accorde_au_client = readString(dis);
					
					this.Date_d_echeance = readString(dis);
					
					this.Date_de_paiement_total_de_la_facture = readString(dis);
					
					this.Retard_de_paiement__en_jours = readString(dis);
					
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Societe = readString(dis);
					
					this.Nom_du_client = readString(dis);
					
					this.N_Facture = readString(dis);
					
					this.Date_de_facture = readString(dis);
					
					this.Montant_HT = readString(dis);
					
					this.Taux_de_TVA = readString(dis);
					
					this.Timbre = readString(dis);
					
					this.Montant_de_la_TVA = readString(dis);
					
					this.Montant_TTC = readString(dis);
					
					this.Retenue_a_la_source = readString(dis);
					
					this.Versements_effectues_par_le_client = readString(dis);
					
					this.Reste_du = readString(dis);
					
					this.Action_a_effectuer = readString(dis);
					
					this.Delai_de_paiement_accorde_au_client = readString(dis);
					
					this.Date_d_echeance = readString(dis);
					
					this.Date_de_paiement_total_de_la_facture = readString(dis);
					
					this.Retard_de_paiement__en_jours = readString(dis);
					
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
				
						writeString(this.Nom_du_client,dos);
					
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
				
						writeString(this.Montant_TTC,dos);
					
					// String
				
						writeString(this.Retenue_a_la_source,dos);
					
					// String
				
						writeString(this.Versements_effectues_par_le_client,dos);
					
					// String
				
						writeString(this.Reste_du,dos);
					
					// String
				
						writeString(this.Action_a_effectuer,dos);
					
					// String
				
						writeString(this.Delai_de_paiement_accorde_au_client,dos);
					
					// String
				
						writeString(this.Date_d_echeance,dos);
					
					// String
				
						writeString(this.Date_de_paiement_total_de_la_facture,dos);
					
					// String
				
						writeString(this.Retard_de_paiement__en_jours,dos);
					
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
				
						writeString(this.Nom_du_client,dos);
					
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
				
						writeString(this.Montant_TTC,dos);
					
					// String
				
						writeString(this.Retenue_a_la_source,dos);
					
					// String
				
						writeString(this.Versements_effectues_par_le_client,dos);
					
					// String
				
						writeString(this.Reste_du,dos);
					
					// String
				
						writeString(this.Action_a_effectuer,dos);
					
					// String
				
						writeString(this.Delai_de_paiement_accorde_au_client,dos);
					
					// String
				
						writeString(this.Date_d_echeance,dos);
					
					// String
				
						writeString(this.Date_de_paiement_total_de_la_facture,dos);
					
					// String
				
						writeString(this.Retard_de_paiement__en_jours,dos);
					
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
		sb.append(",Nom_du_client="+Nom_du_client);
		sb.append(",N_Facture="+N_Facture);
		sb.append(",Date_de_facture="+Date_de_facture);
		sb.append(",Montant_HT="+Montant_HT);
		sb.append(",Taux_de_TVA="+Taux_de_TVA);
		sb.append(",Timbre="+Timbre);
		sb.append(",Montant_de_la_TVA="+Montant_de_la_TVA);
		sb.append(",Montant_TTC="+Montant_TTC);
		sb.append(",Retenue_a_la_source="+Retenue_a_la_source);
		sb.append(",Versements_effectues_par_le_client="+Versements_effectues_par_le_client);
		sb.append(",Reste_du="+Reste_du);
		sb.append(",Action_a_effectuer="+Action_a_effectuer);
		sb.append(",Delai_de_paiement_accorde_au_client="+Delai_de_paiement_accorde_au_client);
		sb.append(",Date_d_echeance="+Date_d_echeance);
		sb.append(",Date_de_paiement_total_de_la_facture="+Date_de_paiement_total_de_la_facture);
		sb.append(",Retard_de_paiement__en_jours="+Retard_de_paiement__en_jours);
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
    public int compareTo(row7Struct other) {

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

public static class EtatAchatVente2Struct implements routines.system.IPersistableRow<EtatAchatVente2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
			    public String Mode_Paiement;

				public String getMode_Paiement () {
					return this.Mode_Paiement;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Mode_Paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Mode_Paiement,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
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
		sb.append("Mode_Paiement="+Mode_Paiement);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(EtatAchatVente2Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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
    public int compareTo(row8Struct other) {

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


		tDBInput_3Process(globalMap);

		row1Struct row1 = new row1Struct();
out1Struct out1 = new out1Struct();

		row2Struct row2 = new row2Struct();
out2Struct out2 = new out2Struct();

		row7Struct row7 = new row7Struct();
etatAchatVenteStruct etatAchatVente = new etatAchatVenteStruct();

		row8Struct row8 = new row8Struct();
EtatAchatVente2Struct EtatAchatVente2 = new EtatAchatVente2Struct();

			row3Struct row3 = new row3Struct();
out411Struct out411 = new out411Struct();
row5Struct row5 = new row5Struct();
row5Struct row6 = row5;







	
	/**
	 * [tDBOutput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBOutput_1", false);
		start_Hash.put("tDBOutput_1", System.currentTimeMillis());
		
	
	currentComponent="tDBOutput_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row6");
					}
				
		int tos_count_tDBOutput_1 = 0;
		



int nb_line_tDBOutput_1 = 0;
int nb_line_update_tDBOutput_1 = 0;
int nb_line_inserted_tDBOutput_1 = 0;
int nb_line_deleted_tDBOutput_1 = 0;
int nb_line_rejected_tDBOutput_1 = 0;

int deletedCount_tDBOutput_1=0;
int updatedCount_tDBOutput_1=0;
int insertedCount_tDBOutput_1=0;
int rowsToCommitCount_tDBOutput_1=0;
int rejectedCount_tDBOutput_1=0;
String dbschema_tDBOutput_1 = null;
String tableName_tDBOutput_1 = null;
boolean whetherReject_tDBOutput_1 = false;

java.util.Calendar calendar_tDBOutput_1 = java.util.Calendar.getInstance();
long year1_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd","0001-01-01").getTime();
long year2_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd","1753-01-01").getTime();
long year10000_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd HH:mm:ss","9999-12-31 24:00:00").getTime();
long date_tDBOutput_1;

java.util.Calendar calendar_datetimeoffset_tDBOutput_1 = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));



	
java.sql.Connection conn_tDBOutput_1 = null;
String dbUser_tDBOutput_1 = null;
    dbschema_tDBOutput_1 = "";
    String driverClass_tDBOutput_1 = "net.sourceforge.jtds.jdbc.Driver";
	
    java.lang.Class.forName(driverClass_tDBOutput_1);
    String port_tDBOutput_1 = "1433";
    String dbname_tDBOutput_1 = "DW_E-Commerce" ;
    String url_tDBOutput_1 = "jdbc:jtds:sqlserver://" + "localhost" ; 
    if (!"".equals(port_tDBOutput_1)) {
    	url_tDBOutput_1 += ":" + "1433";
    }
    if (!"".equals(dbname_tDBOutput_1)) {
				url_tDBOutput_1 += "//" + "DW_E-Commerce"; 
	
    }
    url_tDBOutput_1 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
    dbUser_tDBOutput_1 = "sa";

 
	final String decryptedPassword_tDBOutput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:NLAJbln+QYZhJd3CDw7nzyxHukYAbGRiLdFiTWYIDb0kbrWdTbY=");

    String dbPwd_tDBOutput_1 = decryptedPassword_tDBOutput_1;	
    conn_tDBOutput_1 = java.sql.DriverManager.getConnection(url_tDBOutput_1,dbUser_tDBOutput_1,dbPwd_tDBOutput_1);
	
		resourceMap.put("conn_tDBOutput_1", conn_tDBOutput_1);
	
        conn_tDBOutput_1.setAutoCommit(false);
        int commitEvery_tDBOutput_1 = 10000;
        int commitCounter_tDBOutput_1 = 0;

   int batchSize_tDBOutput_1 = 10000;
   int batchSizeCounter_tDBOutput_1=0;

if(dbschema_tDBOutput_1 == null || dbschema_tDBOutput_1.trim().length() == 0) {
    tableName_tDBOutput_1 = "Dim_Mode_Paiement";
} else {
    tableName_tDBOutput_1 = dbschema_tDBOutput_1 + "].[" + "Dim_Mode_Paiement";
}
	int count_tDBOutput_1=0;

        String insert_tDBOutput_1 = "INSERT INTO [" + tableName_tDBOutput_1 + "] ([Type_paiement]) VALUES (?)";
        java.sql.PreparedStatement pstmt_tDBOutput_1 = conn_tDBOutput_1.prepareStatement(insert_tDBOutput_1);
        resourceMap.put("pstmt_tDBOutput_1", pstmt_tDBOutput_1);


 



/**
 * [tDBOutput_1 begin ] stop
 */



	
	/**
	 * [tLogRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_1", false);
		start_Hash.put("tLogRow_1", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
					}
				
		int tos_count_tLogRow_1 = 0;
		

	///////////////////////
	
         class Util_tLogRow_1 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[2];

        public void addRow(String[] row) {

            for (int i = 0; i < 2; i++) {
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
                    for (k = 0; k < (totals + 1 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 1 - name.length() - k; i++) {
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

                
                    //last column
                    for (int i = 0; i < colLengths[1] - fillChars[1].length() + 1; i++) {
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
        util_tLogRow_1.addRow(new String[]{"Mode_Paiement_PK","Type_paiement",});        
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out411");
					}
				
		int tos_count_tUniqRow_1 = 0;
		

int nb_uniques_tUniqRow_1 = 0;
int nb_duplicates_tUniqRow_1 = 0; 

 



/**
 * [tUniqRow_1 begin ] stop
 */



	
	/**
	 * [tMap_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_3", false);
		start_Hash.put("tMap_3", System.currentTimeMillis());
		
	
	currentComponent="tMap_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row3");
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
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_3__Struct  {
}
Var__tMap_3__Struct Var__tMap_3 = new Var__tMap_3__Struct();
// ###############################

// ###############################
// # Outputs initialization
out411Struct out411_tmp = new out411Struct();
// ###############################

        
        



        









 



/**
 * [tMap_3 begin ] stop
 */



	
	/**
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"EtatAchatVente2","etatAchatVente","out2","out1");
					}
				
		int tos_count_tUnite_1 = 0;
		

int nb_line_tUnite_1 = 0;

 



/**
 * [tUnite_1 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:1B1w6WtM78SdFRfejBgo8XcCyWXdn5vJaZyy/Y82WMAJCaPibSM=");
				
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

		    String dbquery_tDBInput_6 = "SELECT \"Commandes(produit_par_ligne)\".\"Numéro de commande\",\n		\"Commandes(produit_par_ligne)\".\"État de la command"
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
								row1.Numero_de_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(1);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Numero_de_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Numero_de_commande = tmpContent_tDBInput_6;
                }
            } else {
                row1.Numero_de_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row1.Etat_de_la_commande = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Etat_de_la_commande = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Etat_de_la_commande = tmpContent_tDBInput_6;
                }
            } else {
                row1.Etat_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 3) {
								row1.Date_de_commande = null;
							} else {
										
			row1.Date_de_commande = mssqlGTU_tDBInput_6.getDate(rsmd_tDBInput_6, rs_tDBInput_6, 3);
			
		                    }
							if(colQtyInRs_tDBInput_6 < 4) {
								row1.Note_du_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(4);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Note_du_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Note_du_client = tmpContent_tDBInput_6;
                }
            } else {
                row1.Note_du_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 5) {
								row1.Prenom__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(5);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Prenom__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Prenom__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Prenom__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 6) {
								row1.NOM_DE_FAMILLE__FACTURATION = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(6);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.NOM_DE_FAMILLE__FACTURATION = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.NOM_DE_FAMILLE__FACTURATION = tmpContent_tDBInput_6;
                }
            } else {
                row1.NOM_DE_FAMILLE__FACTURATION = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 7) {
								row1.Societe__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(7);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Societe__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Societe__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Societe__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 8) {
								row1.Adresse_1___2__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(8);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse_1___2__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Adresse_1___2__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Adresse_1___2__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 9) {
								row1.Ville__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(9);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Ville__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Ville__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Ville__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 10) {
								row1.Code_de_l_etat__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(10);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_de_l_etat__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_de_l_etat__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_de_l_etat__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 11) {
								row1.Code_postal__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(11);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_postal__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_postal__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_postal__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 12) {
								row1.Code_du_pays__Facturation = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(12);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_du_pays__Facturation = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_du_pays__Facturation = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_du_pays__Facturation = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 13) {
								row1.Prenom__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(13);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Prenom__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Prenom__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Prenom__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 14) {
								row1.NOM_DE_FAMILLE__LIVRAISON = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(14);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.NOM_DE_FAMILLE__LIVRAISON = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.NOM_DE_FAMILLE__LIVRAISON = tmpContent_tDBInput_6;
                }
            } else {
                row1.NOM_DE_FAMILLE__LIVRAISON = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 15) {
								row1.Adresse_1___2__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(15);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse_1___2__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Adresse_1___2__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Adresse_1___2__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 16) {
								row1.Ville__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(16);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Ville__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Ville__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Ville__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 17) {
								row1.Code_de_l_etat__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(17);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_de_l_etat__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_de_l_etat__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_de_l_etat__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 18) {
								row1.Code_postal__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(18);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_postal__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_postal__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_postal__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 19) {
								row1.Code_du_pays__Livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(19);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_du_pays__Livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_du_pays__Livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_du_pays__Livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 20) {
								row1.Titre_de_la_methode_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(20);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Titre_de_la_methode_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Titre_de_la_methode_de_paiement = tmpContent_tDBInput_6;
                }
            } else {
                row1.Titre_de_la_methode_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 21) {
								row1.Montant_de_la_remise_panier = null;
							} else {
		                          
            row1.Montant_de_la_remise_panier = rs_tDBInput_6.getBigDecimal(21);
            if(rs_tDBInput_6.wasNull()){
                    row1.Montant_de_la_remise_panier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 22) {
								row1.Cart_Discount_Amount_inc__tax = null;
							} else {
		                          
            row1.Cart_Discount_Amount_inc__tax = rs_tDBInput_6.getBigDecimal(22);
            if(rs_tDBInput_6.wasNull()){
                    row1.Cart_Discount_Amount_inc__tax = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 23) {
								row1.Sous_total_de_la_commande = null;
							} else {
		                          
            row1.Sous_total_de_la_commande = rs_tDBInput_6.getBigDecimal(23);
            if(rs_tDBInput_6.wasNull()){
                    row1.Sous_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 24) {
								row1.Titre_de_la_methode_de_livraison = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(24);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(24).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Titre_de_la_methode_de_livraison = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Titre_de_la_methode_de_livraison = tmpContent_tDBInput_6;
                }
            } else {
                row1.Titre_de_la_methode_de_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 25) {
								row1.Montant_de_la_livraison = null;
							} else {
		                          
            row1.Montant_de_la_livraison = rs_tDBInput_6.getBigDecimal(25);
            if(rs_tDBInput_6.wasNull()){
                    row1.Montant_de_la_livraison = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 26) {
								row1.Montant_du_remboursement_de_la_commande = null;
							} else {
		                          
            row1.Montant_du_remboursement_de_la_commande = rs_tDBInput_6.getBigDecimal(26);
            if(rs_tDBInput_6.wasNull()){
                    row1.Montant_du_remboursement_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 27) {
								row1.Montant_total_de_la_commande = null;
							} else {
		                          
            row1.Montant_total_de_la_commande = rs_tDBInput_6.getBigDecimal(27);
            if(rs_tDBInput_6.wasNull()){
                    row1.Montant_total_de_la_commande = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 28) {
								row1.Montant_total_de_la_taxe = null;
							} else {
		                          
            row1.Montant_total_de_la_taxe = rs_tDBInput_6.getBigDecimal(28);
            if(rs_tDBInput_6.wasNull()){
                    row1.Montant_total_de_la_taxe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 29) {
								row1.UGS = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(29);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(29).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.UGS = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.UGS = tmpContent_tDBInput_6;
                }
            } else {
                row1.UGS = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 30) {
								row1.Article = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(30);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(30).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Article = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Article = tmpContent_tDBInput_6;
                }
            } else {
                row1.Article = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 31) {
								row1.Nom_de_l_element = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(31);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(31).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Nom_de_l_element = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Nom_de_l_element = tmpContent_tDBInput_6;
                }
            } else {
                row1.Nom_de_l_element = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 32) {
								row1.Quantite____Remboursement = null;
							} else {
		                          
            row1.Quantite____Remboursement = rs_tDBInput_6.getInt(32);
            if(rs_tDBInput_6.wasNull()){
                    row1.Quantite____Remboursement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 33) {
								row1.Prix_du_produit = null;
							} else {
		                          
            row1.Prix_du_produit = rs_tDBInput_6.getBigDecimal(33);
            if(rs_tDBInput_6.wasNull()){
                    row1.Prix_du_produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 34) {
								row1.Code_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(34);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(34).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Code_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row1.Code_promo = tmpContent_tDBInput_6;
                }
            } else {
                row1.Code_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 35) {
								row1.Reduction = null;
							} else {
		                          
            row1.Reduction = rs_tDBInput_6.getBigDecimal(35);
            if(rs_tDBInput_6.wasNull()){
                    row1.Reduction = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 36) {
								row1.Taxe_de_la_reduction = null;
							} else {
		                          
            row1.Taxe_de_la_reduction = rs_tDBInput_6.getBigDecimal(36);
            if(rs_tDBInput_6.wasNull()){
                    row1.Taxe_de_la_reduction = null;
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
out1_tmp.Mode_Paimement = row1.Titre_de_la_methode_de_paiement == null 
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
								
			row3.Mode_Paimement = out1.Mode_Paimement;			

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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
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
								
                        		    		    row4HashKey.Type_paiement = row3.Mode_Paimement;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row4.hasNext()) { // G_TM_M_090

  								
		  				
	  								
						
									
	
		  								forceLooprow4 = true;
	  					
  									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
								
								else { // G 20 - G 21
   									forceLooprow4 = true;
			           		  	} // G 21
                    		  	
                    		

							row4Struct row4 = null;
                    		  	 
							

								while ((tHash_Lookup_row4 != null && tHash_Lookup_row4.hasNext()) || forceLooprow4) { // G_TM_M_043

								
									 // CALL close loop of lookup 'row4'
									
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								
								if(!forceLooprow4) { // G 46
								
							
								 
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	                    	
	                    		} // G 46
	                    		  	
								forceLooprow4 = false;
									 	
							
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

out411 = null;


// # Output table : 'out411'
// # Filter conditions 
if( 

row4.Type_paiement ==null

 ) {
out411_tmp.Mode_Paiement_PK = 0;
out411_tmp.Type_paiement = row3.Mode_Paimement ;
out411 = out411_tmp;
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
// Start of branch "out411"
if(out411 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out411"
						
						);
					}
					
row5.Mode_Paiement_PK = out411.Mode_Paiement_PK;			row5.Type_paiement = out411.Type_paiement;			

 


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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[2];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(row5.Mode_Paiement_PK)			
					          ;	
										
    			   				
	    		if(row5.Type_paiement != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(row5.Type_paiement)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 
     row6 = row5;


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
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(row6.Type_paiement == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.VARCHAR);
} else {pstmt_tDBOutput_1.setString(1, row6.Type_paiement);
}


        		pstmt_tDBOutput_1.addBatch();
        		nb_line_tDBOutput_1++;
        		
    		 
    		  batchSizeCounter_tDBOutput_1++;
    		
            if(!whetherReject_tDBOutput_1) {
            }
            	//////////batch execute by batch size///////
            	class LimitBytesHelper_tDBOutput_1{
            		public int limitBytePart1(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
						
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	int countSum_tDBOutput_1 = 0;
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
				
            	    	
                		System.err.println(e.getMessage());
                	
               			 }
    				return counter;
            	}
            	
            	public int limitBytePart2(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
                		
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
					
            	    	
                        System.err.println(e.getMessage());
                	
                		}	
                	return counter;	
            	}
            }
    		if ((batchSize_tDBOutput_1 > 0) && (batchSize_tDBOutput_1 <= batchSizeCounter_tDBOutput_1)) {
    		
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    		rowsToCommitCount_tDBOutput_1 = insertedCount_tDBOutput_1;
            	    	
    			
			    batchSizeCounter_tDBOutput_1 = 0;
			}
    		

    	////////////commit every////////////
    			
    		    commitCounter_tDBOutput_1++;
                if(commitEvery_tDBOutput_1 <= commitCounter_tDBOutput_1) {
                if ((batchSize_tDBOutput_1 > 0) && (batchSizeCounter_tDBOutput_1 > 0)) {
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    	
            	batchSizeCounter_tDBOutput_1 = 0;
            	}
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                }
                conn_tDBOutput_1.commit();
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                	rowsToCommitCount_tDBOutput_1 = 0;	
                }
                commitCounter_tDBOutput_1=0;
                }

 


	tos_count_tDBOutput_1++;

/**
 * [tDBOutput_1 main ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "out411"



	
		} // close loop of lookup 'row4' // G_TM_M_043
	
	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
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
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:8aYhd9VtQcdNlWD/uwHmvSSea+rQqqJSnSL6opeJzUM0uMf6Yp4=");
				
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

		    String dbquery_tDBInput_2 = "SELECT Suivi_Ventes_Popup_store.Date,\n		Suivi_Ventes_Popup_store.Produit,\n		Suivi_Ventes_Popup_store.Quantite,\n		Suivi_"
+"Ventes_Popup_store.Prix_Unitaire__DT,\n		Suivi_Ventes_Popup_store.Remise,\n		Suivi_Ventes_Popup_store.Montant_Total__DT,\n	"
+"	Suivi_Ventes_Popup_store.Montant_Remise__DT,\n		Suivi_Ventes_Popup_store.Type_de_Paiement__Especes___Carte,\n		Suivi_Vent"
+"es_Popup_store.Client__optionnel,\n		Suivi_Ventes_Popup_store.Remarques\nFROM	Suivi_Ventes_Popup_store";
		    

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
								row2.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Date = tmpContent_tDBInput_2;
                }
            } else {
                row2.Date = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row2.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Produit = tmpContent_tDBInput_2;
                }
            } else {
                row2.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row2.Quantite = null;
							} else {
		                          
            row2.Quantite = rs_tDBInput_2.getInt(3);
            if(rs_tDBInput_2.wasNull()){
                    row2.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row2.Prix_Unitaire__DT = null;
							} else {
		                          
            row2.Prix_Unitaire__DT = rs_tDBInput_2.getInt(4);
            if(rs_tDBInput_2.wasNull()){
                    row2.Prix_Unitaire__DT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row2.Remise = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Remise = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Remise = tmpContent_tDBInput_2;
                }
            } else {
                row2.Remise = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row2.Montant_Total__DT = null;
							} else {
		                          
            row2.Montant_Total__DT = rs_tDBInput_2.getInt(6);
            if(rs_tDBInput_2.wasNull()){
                    row2.Montant_Total__DT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row2.Montant_Remise__DT = null;
							} else {
		                          
            row2.Montant_Remise__DT = rs_tDBInput_2.getInt(7);
            if(rs_tDBInput_2.wasNull()){
                    row2.Montant_Remise__DT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row2.Type_de_Paiement__Especes___Carte = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Type_de_Paiement__Especes___Carte = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Type_de_Paiement__Especes___Carte = tmpContent_tDBInput_2;
                }
            } else {
                row2.Type_de_Paiement__Especes___Carte = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row2.Client__optionnel = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Client__optionnel = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Client__optionnel = tmpContent_tDBInput_2;
                }
            } else {
                row2.Client__optionnel = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row2.Remarques = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(10);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Remarques = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Remarques = tmpContent_tDBInput_2;
                }
            } else {
                row2.Remarques = null;
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
out2_tmp.Mode_Paiement = row2.Type_de_Paiement__Especes___Carte== null 
? "AUTRE" 
: StringHandling.TRIM(row2.Type_de_Paiement__Especes___Carte).toUpperCase() ;
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
								
			row3.Mode_Paimement = out2.Mode_Paiement;			

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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
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
								
                        		    		    row4HashKey.Type_paiement = row3.Mode_Paimement;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row4.hasNext()) { // G_TM_M_090

  								
		  				
	  								
						
									
	
		  								forceLooprow4 = true;
	  					
  									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
								
								else { // G 20 - G 21
   									forceLooprow4 = true;
			           		  	} // G 21
                    		  	
                    		

							row4Struct row4 = null;
                    		  	 
							

								while ((tHash_Lookup_row4 != null && tHash_Lookup_row4.hasNext()) || forceLooprow4) { // G_TM_M_043

								
									 // CALL close loop of lookup 'row4'
									
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								
								if(!forceLooprow4) { // G 46
								
							
								 
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	                    	
	                    		} // G 46
	                    		  	
								forceLooprow4 = false;
									 	
							
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

out411 = null;


// # Output table : 'out411'
// # Filter conditions 
if( 

row4.Type_paiement ==null

 ) {
out411_tmp.Mode_Paiement_PK = 0;
out411_tmp.Type_paiement = row3.Mode_Paimement ;
out411 = out411_tmp;
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
// Start of branch "out411"
if(out411 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out411"
						
						);
					}
					
row5.Mode_Paiement_PK = out411.Mode_Paiement_PK;			row5.Type_paiement = out411.Type_paiement;			

 


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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[2];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(row5.Mode_Paiement_PK)			
					          ;	
										
    			   				
	    		if(row5.Type_paiement != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(row5.Type_paiement)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 
     row6 = row5;


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
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(row6.Type_paiement == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.VARCHAR);
} else {pstmt_tDBOutput_1.setString(1, row6.Type_paiement);
}


        		pstmt_tDBOutput_1.addBatch();
        		nb_line_tDBOutput_1++;
        		
    		 
    		  batchSizeCounter_tDBOutput_1++;
    		
            if(!whetherReject_tDBOutput_1) {
            }
            	//////////batch execute by batch size///////
            	class LimitBytesHelper_tDBOutput_1{
            		public int limitBytePart1(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
						
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	int countSum_tDBOutput_1 = 0;
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
				
            	    	
                		System.err.println(e.getMessage());
                	
               			 }
    				return counter;
            	}
            	
            	public int limitBytePart2(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
                		
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
					
            	    	
                        System.err.println(e.getMessage());
                	
                		}	
                	return counter;	
            	}
            }
    		if ((batchSize_tDBOutput_1 > 0) && (batchSize_tDBOutput_1 <= batchSizeCounter_tDBOutput_1)) {
    		
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    		rowsToCommitCount_tDBOutput_1 = insertedCount_tDBOutput_1;
            	    	
    			
			    batchSizeCounter_tDBOutput_1 = 0;
			}
    		

    	////////////commit every////////////
    			
    		    commitCounter_tDBOutput_1++;
                if(commitEvery_tDBOutput_1 <= commitCounter_tDBOutput_1) {
                if ((batchSize_tDBOutput_1 > 0) && (batchSizeCounter_tDBOutput_1 > 0)) {
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    	
            	batchSizeCounter_tDBOutput_1 = 0;
            	}
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                }
                conn_tDBOutput_1.commit();
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                	rowsToCommitCount_tDBOutput_1 = 0;	
                }
                commitCounter_tDBOutput_1=0;
                }

 


	tos_count_tDBOutput_1++;

/**
 * [tDBOutput_1 main ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "out411"



	
		} // close loop of lookup 'row4' // G_TM_M_043
	
	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
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
	 * [tMap_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_4", false);
		start_Hash.put("tMap_4", System.currentTimeMillis());
		
	
	currentComponent="tMap_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row7");
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
etatAchatVenteStruct etatAchatVente_tmp = new etatAchatVenteStruct();
// ###############################

        
        



        









 



/**
 * [tMap_4 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:KXO3diXEZawIQWq0feydojM+AP6u2IzliX0lkMNybk0XsvIUERc=");
				
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

		    String dbquery_tDBInput_4 = "SELECT etat_achatVentes_SuiviVentes.Societe,\n		etat_achatVentes_SuiviVentes.Nom_du_client,\n		etat_achatVentes_SuiviVent"
+"es.N_Facture,\n		etat_achatVentes_SuiviVentes.Date_de_facture,\n		etat_achatVentes_SuiviVentes.Montant_HT,\n		etat_achatVen"
+"tes_SuiviVentes.Taux_de_TVA,\n		etat_achatVentes_SuiviVentes.Timbre,\n		etat_achatVentes_SuiviVentes.Montant_de_la_TVA,\n		"
+"etat_achatVentes_SuiviVentes.Montant_TTC,\n		etat_achatVentes_SuiviVentes.Retenue_a_la_source,\n		etat_achatVentes_SuiviVe"
+"ntes.Versements_effectues_par_le_client,\n		etat_achatVentes_SuiviVentes.Reste_du,\n		etat_achatVentes_SuiviVentes.Action_"
+"a_effectuer,\n		etat_achatVentes_SuiviVentes.Delai_de_paiement_accorde_au_client,\n		etat_achatVentes_SuiviVentes.Date_d_e"
+"cheance,\n		etat_achatVentes_SuiviVentes.Date_de_paiement_total_de_la_facture,\n		etat_achatVentes_SuiviVentes.Retard_de_p"
+"aiement__en_jours,\n		etat_achatVentes_SuiviVentes.mode_de_reglement,\n		etat_achatVentes_SuiviVentes.Date_de_la_premiere_"
+"relance,\n		etat_achatVentes_SuiviVentes.Date_de_la_deuxieme_relance,\n		etat_achatVentes_SuiviVentes.Penalite_et_majorati"
+"on_de_paiement,\n		etat_achatVentes_SuiviVentes.Commentaire\nFROM	etat_achatVentes_SuiviVentes";
		    

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
								row7.Societe = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(1);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Societe = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Societe = tmpContent_tDBInput_4;
                }
            } else {
                row7.Societe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row7.Nom_du_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Nom_du_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Nom_du_client = tmpContent_tDBInput_4;
                }
            } else {
                row7.Nom_du_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 3) {
								row7.N_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(3);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.N_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.N_Facture = tmpContent_tDBInput_4;
                }
            } else {
                row7.N_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 4) {
								row7.Date_de_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(4);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date_de_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Date_de_facture = tmpContent_tDBInput_4;
                }
            } else {
                row7.Date_de_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 5) {
								row7.Montant_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(5);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Montant_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Montant_HT = tmpContent_tDBInput_4;
                }
            } else {
                row7.Montant_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 6) {
								row7.Taux_de_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(6);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Taux_de_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Taux_de_TVA = tmpContent_tDBInput_4;
                }
            } else {
                row7.Taux_de_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 7) {
								row7.Timbre = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(7);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Timbre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Timbre = tmpContent_tDBInput_4;
                }
            } else {
                row7.Timbre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 8) {
								row7.Montant_de_la_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(8);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Montant_de_la_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Montant_de_la_TVA = tmpContent_tDBInput_4;
                }
            } else {
                row7.Montant_de_la_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 9) {
								row7.Montant_TTC = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(9);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Montant_TTC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Montant_TTC = tmpContent_tDBInput_4;
                }
            } else {
                row7.Montant_TTC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 10) {
								row7.Retenue_a_la_source = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(10);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Retenue_a_la_source = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Retenue_a_la_source = tmpContent_tDBInput_4;
                }
            } else {
                row7.Retenue_a_la_source = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 11) {
								row7.Versements_effectues_par_le_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(11);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Versements_effectues_par_le_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Versements_effectues_par_le_client = tmpContent_tDBInput_4;
                }
            } else {
                row7.Versements_effectues_par_le_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 12) {
								row7.Reste_du = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(12);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Reste_du = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Reste_du = tmpContent_tDBInput_4;
                }
            } else {
                row7.Reste_du = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 13) {
								row7.Action_a_effectuer = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(13);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Action_a_effectuer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Action_a_effectuer = tmpContent_tDBInput_4;
                }
            } else {
                row7.Action_a_effectuer = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 14) {
								row7.Delai_de_paiement_accorde_au_client = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(14);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Delai_de_paiement_accorde_au_client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Delai_de_paiement_accorde_au_client = tmpContent_tDBInput_4;
                }
            } else {
                row7.Delai_de_paiement_accorde_au_client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 15) {
								row7.Date_d_echeance = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(15);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date_d_echeance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Date_d_echeance = tmpContent_tDBInput_4;
                }
            } else {
                row7.Date_d_echeance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 16) {
								row7.Date_de_paiement_total_de_la_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(16);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date_de_paiement_total_de_la_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Date_de_paiement_total_de_la_facture = tmpContent_tDBInput_4;
                }
            } else {
                row7.Date_de_paiement_total_de_la_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 17) {
								row7.Retard_de_paiement__en_jours = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(17);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Retard_de_paiement__en_jours = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Retard_de_paiement__en_jours = tmpContent_tDBInput_4;
                }
            } else {
                row7.Retard_de_paiement__en_jours = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 18) {
								row7.mode_de_reglement = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(18);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.mode_de_reglement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.mode_de_reglement = tmpContent_tDBInput_4;
                }
            } else {
                row7.mode_de_reglement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 19) {
								row7.Date_de_la_premiere_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(19);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date_de_la_premiere_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Date_de_la_premiere_relance = tmpContent_tDBInput_4;
                }
            } else {
                row7.Date_de_la_premiere_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 20) {
								row7.Date_de_la_deuxieme_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(20);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date_de_la_deuxieme_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Date_de_la_deuxieme_relance = tmpContent_tDBInput_4;
                }
            } else {
                row7.Date_de_la_deuxieme_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 21) {
								row7.Penalite_et_majoration_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(21);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(21).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Penalite_et_majoration_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Penalite_et_majoration_de_paiement = tmpContent_tDBInput_4;
                }
            } else {
                row7.Penalite_et_majoration_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 22) {
								row7.Commentaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(22);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(22).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Commentaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row7.Commentaire = tmpContent_tDBInput_4;
                }
            } else {
                row7.Commentaire = null;
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
	 * [tMap_4 main ] start
	 */

	

	
	
	currentComponent="tMap_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
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

etatAchatVente = null;


// # Output table : 'etatAchatVente'
etatAchatVente_tmp.Mode_Paiement = row7.mode_de_reglement== null 
? "AUTRE" 
: StringHandling.TRIM(row7.mode_de_reglement).toUpperCase() ;
etatAchatVente = etatAchatVente_tmp;
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
// Start of branch "etatAchatVente"
if(etatAchatVente != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"etatAchatVente"
									
							);
						}
						
//////////
 

// for output
			row3 = new row3Struct();
								
			row3.Mode_Paimement = etatAchatVente.Mode_Paiement;			

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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
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
								
                        		    		    row4HashKey.Type_paiement = row3.Mode_Paimement;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row4.hasNext()) { // G_TM_M_090

  								
		  				
	  								
						
									
	
		  								forceLooprow4 = true;
	  					
  									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
								
								else { // G 20 - G 21
   									forceLooprow4 = true;
			           		  	} // G 21
                    		  	
                    		

							row4Struct row4 = null;
                    		  	 
							

								while ((tHash_Lookup_row4 != null && tHash_Lookup_row4.hasNext()) || forceLooprow4) { // G_TM_M_043

								
									 // CALL close loop of lookup 'row4'
									
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								
								if(!forceLooprow4) { // G 46
								
							
								 
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	                    	
	                    		} // G 46
	                    		  	
								forceLooprow4 = false;
									 	
							
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

out411 = null;


// # Output table : 'out411'
// # Filter conditions 
if( 

row4.Type_paiement ==null

 ) {
out411_tmp.Mode_Paiement_PK = 0;
out411_tmp.Type_paiement = row3.Mode_Paimement ;
out411 = out411_tmp;
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
// Start of branch "out411"
if(out411 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out411"
						
						);
					}
					
row5.Mode_Paiement_PK = out411.Mode_Paiement_PK;			row5.Type_paiement = out411.Type_paiement;			

 


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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[2];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(row5.Mode_Paiement_PK)			
					          ;	
										
    			   				
	    		if(row5.Type_paiement != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(row5.Type_paiement)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 
     row6 = row5;


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
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(row6.Type_paiement == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.VARCHAR);
} else {pstmt_tDBOutput_1.setString(1, row6.Type_paiement);
}


        		pstmt_tDBOutput_1.addBatch();
        		nb_line_tDBOutput_1++;
        		
    		 
    		  batchSizeCounter_tDBOutput_1++;
    		
            if(!whetherReject_tDBOutput_1) {
            }
            	//////////batch execute by batch size///////
            	class LimitBytesHelper_tDBOutput_1{
            		public int limitBytePart1(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
						
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	int countSum_tDBOutput_1 = 0;
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
				
            	    	
                		System.err.println(e.getMessage());
                	
               			 }
    				return counter;
            	}
            	
            	public int limitBytePart2(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
                		
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
					
            	    	
                        System.err.println(e.getMessage());
                	
                		}	
                	return counter;	
            	}
            }
    		if ((batchSize_tDBOutput_1 > 0) && (batchSize_tDBOutput_1 <= batchSizeCounter_tDBOutput_1)) {
    		
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    		rowsToCommitCount_tDBOutput_1 = insertedCount_tDBOutput_1;
            	    	
    			
			    batchSizeCounter_tDBOutput_1 = 0;
			}
    		

    	////////////commit every////////////
    			
    		    commitCounter_tDBOutput_1++;
                if(commitEvery_tDBOutput_1 <= commitCounter_tDBOutput_1) {
                if ((batchSize_tDBOutput_1 > 0) && (batchSizeCounter_tDBOutput_1 > 0)) {
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    	
            	batchSizeCounter_tDBOutput_1 = 0;
            	}
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                }
                conn_tDBOutput_1.commit();
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                	rowsToCommitCount_tDBOutput_1 = 0;	
                }
                commitCounter_tDBOutput_1=0;
                }

 


	tos_count_tDBOutput_1++;

/**
 * [tDBOutput_1 main ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "out411"



	
		} // close loop of lookup 'row4' // G_TM_M_043
	
	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "etatAchatVente"




	
	/**
	 * [tMap_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 process_data_end ] stop
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
	 * [tMap_4 end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row7");
			  	}
			  	
 

ok_Hash.put("tMap_4", true);
end_Hash.put("tMap_4", System.currentTimeMillis());




/**
 * [tMap_4 end ] stop
 */




	
	/**
	 * [tMap_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_5", false);
		start_Hash.put("tMap_5", System.currentTimeMillis());
		
	
	currentComponent="tMap_5";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row8");
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
EtatAchatVente2Struct EtatAchatVente2_tmp = new EtatAchatVente2Struct();
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
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:1iO1rAPGlk9LLwYWM2KWY7V7jTobWSjhzw2WKUukMuO242UuREk=");
				
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

		    String dbquery_tDBInput_5 = "SELECT etats_achats_ventes_Suivi_achats.Societe,\n		etats_achats_ventes_Suivi_achats.Nom_du_Fournisseur,\n		etats_achats_"
+"ventes_Suivi_achats.N_Facture,\n		etats_achats_ventes_Suivi_achats.Date_de_facture,\n		etats_achats_ventes_Suivi_achats.Mo"
+"ntant_HT,\n		etats_achats_ventes_Suivi_achats.Taux_de_TVA,\n		etats_achats_ventes_Suivi_achats.Timbre,\n		etats_achats_vent"
+"es_Suivi_achats.Montant_de_la_TVA,\n		etats_achats_ventes_Suivi_achats.FODEC,\n		etats_achats_ventes_Suivi_achats.Montant_"
+"TTC,\n		etats_achats_ventes_Suivi_achats.Retenue,\n		etats_achats_ventes_Suivi_achats.Versements_effectues_par_SOUGUI,\n		e"
+"tats_achats_ventes_Suivi_achats.Reste_du,\n		etats_achats_ventes_Suivi_achats.Action_a_effectuer,\n		etats_achats_ventes_S"
+"uivi_achats.Date_de_paiement_total_de_la_facture,\n		etats_achats_ventes_Suivi_achats.mode_de_reglement,\n		etats_achats_v"
+"entes_Suivi_achats.Date_de_la_premiere_relance,\n		etats_achats_ventes_Suivi_achats.Date_de_la_deuxieme_relance,\n		etats_"
+"achats_ventes_Suivi_achats.Penalite_et_majoration_de_paiement,\n		etats_achats_ventes_Suivi_achats.Commentaire\nFROM	etats"
+"_achats_ventes_Suivi_achats";
		    

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
								row8.Societe = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(1);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Societe = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Societe = tmpContent_tDBInput_5;
                }
            } else {
                row8.Societe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 2) {
								row8.Nom_du_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(2);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Nom_du_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Nom_du_Fournisseur = tmpContent_tDBInput_5;
                }
            } else {
                row8.Nom_du_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 3) {
								row8.N_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(3);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.N_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.N_Facture = tmpContent_tDBInput_5;
                }
            } else {
                row8.N_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 4) {
								row8.Date_de_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(4);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Date_de_facture = tmpContent_tDBInput_5;
                }
            } else {
                row8.Date_de_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 5) {
								row8.Montant_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(5);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Montant_HT = tmpContent_tDBInput_5;
                }
            } else {
                row8.Montant_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 6) {
								row8.Taux_de_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(6);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Taux_de_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Taux_de_TVA = tmpContent_tDBInput_5;
                }
            } else {
                row8.Taux_de_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 7) {
								row8.Timbre = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(7);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Timbre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Timbre = tmpContent_tDBInput_5;
                }
            } else {
                row8.Timbre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 8) {
								row8.Montant_de_la_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(8);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_de_la_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Montant_de_la_TVA = tmpContent_tDBInput_5;
                }
            } else {
                row8.Montant_de_la_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 9) {
								row8.FODEC = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(9);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.FODEC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.FODEC = tmpContent_tDBInput_5;
                }
            } else {
                row8.FODEC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 10) {
								row8.Montant_TTC = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(10);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_TTC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Montant_TTC = tmpContent_tDBInput_5;
                }
            } else {
                row8.Montant_TTC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 11) {
								row8.Retenue = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(11);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Retenue = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Retenue = tmpContent_tDBInput_5;
                }
            } else {
                row8.Retenue = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 12) {
								row8.Versements_effectues_par_SOUGUI = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(12);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Versements_effectues_par_SOUGUI = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Versements_effectues_par_SOUGUI = tmpContent_tDBInput_5;
                }
            } else {
                row8.Versements_effectues_par_SOUGUI = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 13) {
								row8.Reste_du = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(13);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Reste_du = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Reste_du = tmpContent_tDBInput_5;
                }
            } else {
                row8.Reste_du = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 14) {
								row8.Action_a_effectuer = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(14);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Action_a_effectuer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Action_a_effectuer = tmpContent_tDBInput_5;
                }
            } else {
                row8.Action_a_effectuer = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 15) {
								row8.Date_de_paiement_total_de_la_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(15);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_paiement_total_de_la_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Date_de_paiement_total_de_la_facture = tmpContent_tDBInput_5;
                }
            } else {
                row8.Date_de_paiement_total_de_la_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 16) {
								row8.mode_de_reglement = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(16);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.mode_de_reglement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.mode_de_reglement = tmpContent_tDBInput_5;
                }
            } else {
                row8.mode_de_reglement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 17) {
								row8.Date_de_la_premiere_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(17);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_la_premiere_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Date_de_la_premiere_relance = tmpContent_tDBInput_5;
                }
            } else {
                row8.Date_de_la_premiere_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 18) {
								row8.Date_de_la_deuxieme_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(18);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_la_deuxieme_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Date_de_la_deuxieme_relance = tmpContent_tDBInput_5;
                }
            } else {
                row8.Date_de_la_deuxieme_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 19) {
								row8.Penalite_et_majoration_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(19);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Penalite_et_majoration_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Penalite_et_majoration_de_paiement = tmpContent_tDBInput_5;
                }
            } else {
                row8.Penalite_et_majoration_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 20) {
								row8.Commentaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(20);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Commentaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row8.Commentaire = tmpContent_tDBInput_5;
                }
            } else {
                row8.Commentaire = null;
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
						
							,"row8"
						
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

EtatAchatVente2 = null;


// # Output table : 'EtatAchatVente2'
EtatAchatVente2_tmp.Mode_Paiement = row8.mode_de_reglement== null 
? "AUTRE" 
: StringHandling.TRIM(row8.mode_de_reglement).toUpperCase() ;
EtatAchatVente2 = EtatAchatVente2_tmp;
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
// Start of branch "EtatAchatVente2"
if(EtatAchatVente2 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"EtatAchatVente2"
									
							);
						}
						
//////////
 

// for output
			row3 = new row3Struct();
								
			row3.Mode_Paimement = EtatAchatVente2.Mode_Paiement;			

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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
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
								
                        		    		    row4HashKey.Type_paiement = row3.Mode_Paimement;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row4.hasNext()) { // G_TM_M_090

  								
		  				
	  								
						
									
	
		  								forceLooprow4 = true;
	  					
  									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
								
								else { // G 20 - G 21
   									forceLooprow4 = true;
			           		  	} // G 21
                    		  	
                    		

							row4Struct row4 = null;
                    		  	 
							

								while ((tHash_Lookup_row4 != null && tHash_Lookup_row4.hasNext()) || forceLooprow4) { // G_TM_M_043

								
									 // CALL close loop of lookup 'row4'
									
                    		  	 
							   
                    		  	 
	       		  	    	row4Struct fromLookup_row4 = null;
							row4 = row4Default;
										 
							
								
								if(!forceLooprow4) { // G 46
								
							
								 
							
								
								fromLookup_row4 = tHash_Lookup_row4.next();

							

							if(fromLookup_row4 != null) {
								row4 = fromLookup_row4;
							}
							
							
							
			  							
								
	                    		  	
		                    
	                    	
	                    		} // G 46
	                    		  	
								forceLooprow4 = false;
									 	
							
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

out411 = null;


// # Output table : 'out411'
// # Filter conditions 
if( 

row4.Type_paiement ==null

 ) {
out411_tmp.Mode_Paiement_PK = 0;
out411_tmp.Type_paiement = row3.Mode_Paimement ;
out411 = out411_tmp;
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
// Start of branch "out411"
if(out411 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out411"
						
						);
					}
					
row5.Mode_Paiement_PK = out411.Mode_Paiement_PK;			row5.Type_paiement = out411.Type_paiement;			

 


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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[2];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(row5.Mode_Paiement_PK)			
					          ;	
										
    			   				
	    		if(row5.Type_paiement != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(row5.Type_paiement)			
					          ;	
							
	    		} //			
    			 

				util_tLogRow_1.addRow(row_tLogRow_1);	
				nb_line_tLogRow_1++;
//////

//////                    
                    
///////////////////////    			

 
     row6 = row5;


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
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(row6.Type_paiement == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.VARCHAR);
} else {pstmt_tDBOutput_1.setString(1, row6.Type_paiement);
}


        		pstmt_tDBOutput_1.addBatch();
        		nb_line_tDBOutput_1++;
        		
    		 
    		  batchSizeCounter_tDBOutput_1++;
    		
            if(!whetherReject_tDBOutput_1) {
            }
            	//////////batch execute by batch size///////
            	class LimitBytesHelper_tDBOutput_1{
            		public int limitBytePart1(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
						
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	int countSum_tDBOutput_1 = 0;
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
				
            	    	
                		System.err.println(e.getMessage());
                	
               			 }
    				return counter;
            	}
            	
            	public int limitBytePart2(int counter,java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                try {
                		
						for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
							if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
								break;
							}
							counter += countEach_tDBOutput_1;
						}
						
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
					
            	    	
                        System.err.println(e.getMessage());
                	
                		}	
                	return counter;	
            	}
            }
    		if ((batchSize_tDBOutput_1 > 0) && (batchSize_tDBOutput_1 <= batchSizeCounter_tDBOutput_1)) {
    		
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    		rowsToCommitCount_tDBOutput_1 = insertedCount_tDBOutput_1;
            	    	
    			
			    batchSizeCounter_tDBOutput_1 = 0;
			}
    		

    	////////////commit every////////////
    			
    		    commitCounter_tDBOutput_1++;
                if(commitEvery_tDBOutput_1 <= commitCounter_tDBOutput_1) {
                if ((batchSize_tDBOutput_1 > 0) && (batchSizeCounter_tDBOutput_1 > 0)) {
    		            
            	    		insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1,pstmt_tDBOutput_1);
            	    	
            	batchSizeCounter_tDBOutput_1 = 0;
            	}
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                }
                conn_tDBOutput_1.commit();
                if(rowsToCommitCount_tDBOutput_1 != 0){
                	
                	rowsToCommitCount_tDBOutput_1 = 0;	
                }
                commitCounter_tDBOutput_1=0;
                }

 


	tos_count_tDBOutput_1++;

/**
 * [tDBOutput_1 main ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	

 



/**
 * [tDBOutput_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "out411"



	
		} // close loop of lookup 'row4' // G_TM_M_043
	
	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "EtatAchatVente2"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row8");
			  	}
			  	
 

ok_Hash.put("tMap_5", true);
end_Hash.put("tMap_5", System.currentTimeMillis());




/**
 * [tMap_5 end ] stop
 */



	
	/**
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"EtatAchatVente2","etatAchatVente","out2","out1");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
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

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row3");
			  	}
			  	
 

ok_Hash.put("tMap_3", true);
end_Hash.put("tMap_3", System.currentTimeMillis());




/**
 * [tMap_3 end ] stop
 */

	
	/**
	 * [tUniqRow_1 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

globalMap.put("tUniqRow_1_NB_UNIQUES",nb_uniques_tUniqRow_1);
globalMap.put("tUniqRow_1_NB_DUPLICATES",nb_duplicates_tUniqRow_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out411");
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
			  	}
			  	
 

ok_Hash.put("tLogRow_1", true);
end_Hash.put("tLogRow_1", System.currentTimeMillis());




/**
 * [tLogRow_1 end ] stop
 */

	
	/**
	 * [tDBOutput_1 end ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	



                try {
						int countSum_tDBOutput_1 = 0;
						if (pstmt_tDBOutput_1 != null && batchSizeCounter_tDBOutput_1 > 0) {
							
							for(int countEach_tDBOutput_1: pstmt_tDBOutput_1.executeBatch()) {
								if(countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
									break;
								}
								countSum_tDBOutput_1 += countEach_tDBOutput_1;
							}
							rowsToCommitCount_tDBOutput_1 += countSum_tDBOutput_1;
							
						}
            	    	
            	    		insertedCount_tDBOutput_1 += countSum_tDBOutput_1;
            	    	
                }catch (java.sql.BatchUpdateException e){
globalMap.put("tDBOutput_1_ERROR_MESSAGE",e.getMessage());
                	
                	int countSum_tDBOutput_1 = 0;
					for(int countEach_tDBOutput_1: e.getUpdateCounts()) {
						countSum_tDBOutput_1 += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
					}
					rowsToCommitCount_tDBOutput_1 += countSum_tDBOutput_1;
					
            	    		insertedCount_tDBOutput_1 += countSum_tDBOutput_1;
            	    	
                		System.err.println(e.getMessage());
                	
            	}
        if(pstmt_tDBOutput_1 != null) {
			
				pstmt_tDBOutput_1.close();
				resourceMap.remove("pstmt_tDBOutput_1");
			
        }
    resourceMap.put("statementClosed_tDBOutput_1", true);
            if(rowsToCommitCount_tDBOutput_1 != 0){
            	
            }
            conn_tDBOutput_1.commit();
            if(rowsToCommitCount_tDBOutput_1 != 0){
            	
            	rowsToCommitCount_tDBOutput_1 = 0;
            }
            commitCounter_tDBOutput_1 = 0;
        conn_tDBOutput_1 .close();
        resourceMap.put("finish_tDBOutput_1", true);

	nb_line_deleted_tDBOutput_1=nb_line_deleted_tDBOutput_1+ deletedCount_tDBOutput_1;
	nb_line_update_tDBOutput_1=nb_line_update_tDBOutput_1 + updatedCount_tDBOutput_1;
	nb_line_inserted_tDBOutput_1=nb_line_inserted_tDBOutput_1 + insertedCount_tDBOutput_1;
	nb_line_rejected_tDBOutput_1=nb_line_rejected_tDBOutput_1 + rejectedCount_tDBOutput_1;
	
        globalMap.put("tDBOutput_1_NB_LINE",nb_line_tDBOutput_1);
        globalMap.put("tDBOutput_1_NB_LINE_UPDATED",nb_line_update_tDBOutput_1);
        globalMap.put("tDBOutput_1_NB_LINE_INSERTED",nb_line_inserted_tDBOutput_1);
        globalMap.put("tDBOutput_1_NB_LINE_DELETED",nb_line_deleted_tDBOutput_1);
        globalMap.put("tDBOutput_1_NB_LINE_REJECTED", nb_line_rejected_tDBOutput_1);
    

	

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row6");
			  	}
			  	
 

ok_Hash.put("tDBOutput_1", true);
end_Hash.put("tDBOutput_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk2", 0, "ok");
				}
				tChronometerStop_1Process(globalMap);



/**
 * [tDBOutput_1 end ] stop
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
				     			
				try{
					
	
	/**
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
 */

	
	/**
	 * [tMap_1 finally ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 finally ] stop
 */



	
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
	 * [tDBInput_4 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 finally ] stop
 */

	
	/**
	 * [tMap_4 finally ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 finally ] stop
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
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tMap_3 finally ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 finally ] stop
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
	 * [tDBOutput_1 finally ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	



    try {
    if (resourceMap.get("statementClosed_tDBOutput_1") == null) {
                java.sql.PreparedStatement pstmtToClose_tDBOutput_1 = null;
                if ((pstmtToClose_tDBOutput_1 = (java.sql.PreparedStatement) resourceMap.remove("pstmt_tDBOutput_1")) != null) {
                    pstmtToClose_tDBOutput_1.close();
                }
    }
    } finally {
        if(resourceMap.get("finish_tDBOutput_1") == null){
            java.sql.Connection ctn_tDBOutput_1 = null;
            if((ctn_tDBOutput_1 = (java.sql.Connection)resourceMap.get("conn_tDBOutput_1")) != null){
                try {
                    ctn_tDBOutput_1.close();
                } catch (java.sql.SQLException sqlEx_tDBOutput_1) {
                    String errorMessage_tDBOutput_1 = "failed to close the connection in tDBOutput_1 :" + sqlEx_tDBOutput_1.getMessage();
                    System.err.println(errorMessage_tDBOutput_1);
                }
            }
        }
    }
 



/**
 * [tDBOutput_1 finally ] stop
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
	

public void tChronometerStop_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tChronometerStop_1_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tChronometerStop_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tChronometerStop_1", false);
		start_Hash.put("tChronometerStop_1", System.currentTimeMillis());
		
	
	currentComponent="tChronometerStop_1";

	
		int tos_count_tChronometerStop_1 = 0;
		
	
	long timetChronometerStop_1;
	
		timetChronometerStop_1 = System.currentTimeMillis() - startTime;
		
	   		System.out.print("[ tChronometerStop_1 ]  ");
		
	  	System.out.println("" + "  " + timetChronometerStop_1 + " milliseconds");
	  	 
	
	Long currentTimetChronometerStop_1 = System.currentTimeMillis();
	globalMap.put("tChronometerStop_1", currentTimetChronometerStop_1);
	
	globalMap.put("tChronometerStop_1_STOPTIME", currentTimetChronometerStop_1);
	globalMap.put("tChronometerStop_1_DURATION", timetChronometerStop_1);
 



/**
 * [tChronometerStop_1 begin ] stop
 */
	
	/**
	 * [tChronometerStop_1 main ] start
	 */

	

	
	
	currentComponent="tChronometerStop_1";

	

 


	tos_count_tChronometerStop_1++;

/**
 * [tChronometerStop_1 main ] stop
 */
	
	/**
	 * [tChronometerStop_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tChronometerStop_1";

	

 



/**
 * [tChronometerStop_1 process_data_begin ] stop
 */
	
	/**
	 * [tChronometerStop_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tChronometerStop_1";

	

 



/**
 * [tChronometerStop_1 process_data_end ] stop
 */
	
	/**
	 * [tChronometerStop_1 end ] start
	 */

	

	
	
	currentComponent="tChronometerStop_1";

	

 

ok_Hash.put("tChronometerStop_1", true);
end_Hash.put("tChronometerStop_1", System.currentTimeMillis());




/**
 * [tChronometerStop_1 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tChronometerStop_1:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk7", 0, "ok");
								} 
							
							tJava_2Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tChronometerStop_1 finally ] start
	 */

	

	
	
	currentComponent="tChronometerStop_1";

	

 



/**
 * [tChronometerStop_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tChronometerStop_1_SUBPROCESS_STATE", 1);
	}
	

public void tJava_2Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_2_SUBPROCESS_STATE", 0);

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





	
	/**
	 * [tJava_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_2", false);
		start_Hash.put("tJava_2", System.currentTimeMillis());
		
	
	currentComponent="tJava_2";

	
		int tos_count_tJava_2 = 0;
		


System.out.println("===== FIN EXECUTION JOB =====");

System.out.println(
    "Temps total (ms) : " 
    + ((Long)globalMap.get("tChronometerStop_1_DURATION"))
);
 



/**
 * [tJava_2 begin ] stop
 */
	
	/**
	 * [tJava_2 main ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 


	tos_count_tJava_2++;

/**
 * [tJava_2 main ] stop
 */
	
	/**
	 * [tJava_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 process_data_begin ] stop
 */
	
	/**
	 * [tJava_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 process_data_end ] stop
 */
	
	/**
	 * [tJava_2 end ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 

ok_Hash.put("tJava_2", true);
end_Hash.put("tJava_2", System.currentTimeMillis());




/**
 * [tJava_2 end ] stop
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
	 * [tJava_2 finally ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_2_SUBPROCESS_STATE", 1);
	}
	


public static class row4Struct implements routines.system.IPersistableComparableLookupRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[0];
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
		final row4Struct other = (row4Struct) obj;
		
						if (this.Type_paiement == null) {
							if (other.Type_paiement != null)
								return false;
						
						} else if (!this.Type_paiement.equals(other.Type_paiement))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row4Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(row4Struct other) {

		other.Type_paiement = this.Type_paiement;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Mode_Paimement, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

        	try {

        		int length = 0;
		
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Mode_Paimement) {

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
    public int compareTo(row4Struct other) {

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
			   		// source node:tDBInput_3 - inputs:(after_tDBInput_6) outputs:(row4,row4) | target node:tAdvancedHash_row4 - inputs:(row4) outputs:()
			   		// linked node: tMap_3 - inputs:(row3,row4) outputs:(out411)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row4 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.ALL_MATCHES;
			   			
			   
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
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:WnwrWwMK5lzQms1PoWk7OR7uOb62ljHM2DcLin33vkVTjPxWMkY=");
				
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

		    String dbquery_tDBInput_3 = "SELECT Dim_Mode_Paiement.Mode_Paiement_PK,\n		Dim_Mode_Paiement.Type_paiement\nFROM	Dim_Mode_Paiement";
		    

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
								row4.Mode_Paiement_PK = 0;
							} else {
		                          
            row4.Mode_Paiement_PK = rs_tDBInput_3.getInt(1);
            if(rs_tDBInput_3.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row4.Type_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Type_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Type_paiement = tmpContent_tDBInput_3;
                }
            } else {
                row4.Type_paiement = null;
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
		   	   	   
				
				row4_HashRow.Mode_Paiement_PK = row4.Mode_Paiement_PK;
				
				row4_HashRow.Type_paiement = row4.Type_paiement;
				
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
	

public void tPostjob_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tPostjob_1_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tPostjob_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tPostjob_1", false);
		start_Hash.put("tPostjob_1", System.currentTimeMillis());
		
	
	currentComponent="tPostjob_1";

	
		int tos_count_tPostjob_1 = 0;
		

 



/**
 * [tPostjob_1 begin ] stop
 */
	
	/**
	 * [tPostjob_1 main ] start
	 */

	

	
	
	currentComponent="tPostjob_1";

	

 


	tos_count_tPostjob_1++;

/**
 * [tPostjob_1 main ] stop
 */
	
	/**
	 * [tPostjob_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tPostjob_1";

	

 



/**
 * [tPostjob_1 process_data_begin ] stop
 */
	
	/**
	 * [tPostjob_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tPostjob_1";

	

 



/**
 * [tPostjob_1 process_data_end ] stop
 */
	
	/**
	 * [tPostjob_1 end ] start
	 */

	

	
	
	currentComponent="tPostjob_1";

	

 

ok_Hash.put("tPostjob_1", true);
end_Hash.put("tPostjob_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk1", 0, "ok");
				}
				tChronometerStart_1Process(globalMap);



/**
 * [tPostjob_1 end ] stop
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
	 * [tPostjob_1 finally ] start
	 */

	

	
	
	currentComponent="tPostjob_1";

	

 



/**
 * [tPostjob_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tPostjob_1_SUBPROCESS_STATE", 1);
	}
	

public void tChronometerStart_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tChronometerStart_1_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tChronometerStart_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tChronometerStart_1", false);
		start_Hash.put("tChronometerStart_1", System.currentTimeMillis());
		
	
	currentComponent="tChronometerStart_1";

	
		int tos_count_tChronometerStart_1 = 0;
		

	
	
	Long currentTimetChronometerStart_1 = System.currentTimeMillis();
	
	globalMap.put("tChronometerStart_1",currentTimetChronometerStart_1);
	globalMap.put("tChronometerStart_1_STARTTIME", currentTimetChronometerStart_1);
 



/**
 * [tChronometerStart_1 begin ] stop
 */
	
	/**
	 * [tChronometerStart_1 main ] start
	 */

	

	
	
	currentComponent="tChronometerStart_1";

	

 


	tos_count_tChronometerStart_1++;

/**
 * [tChronometerStart_1 main ] stop
 */
	
	/**
	 * [tChronometerStart_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tChronometerStart_1";

	

 



/**
 * [tChronometerStart_1 process_data_begin ] stop
 */
	
	/**
	 * [tChronometerStart_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tChronometerStart_1";

	

 



/**
 * [tChronometerStart_1 process_data_end ] stop
 */
	
	/**
	 * [tChronometerStart_1 end ] start
	 */

	

	
	
	currentComponent="tChronometerStart_1";

	

 

ok_Hash.put("tChronometerStart_1", true);
end_Hash.put("tChronometerStart_1", System.currentTimeMillis());




/**
 * [tChronometerStart_1 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tChronometerStart_1:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk5", 0, "ok");
								} 
							
							tDBConnection_1Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tChronometerStart_1 finally ] start
	 */

	

	
	
	currentComponent="tChronometerStart_1";

	

 



/**
 * [tChronometerStart_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tChronometerStart_1_SUBPROCESS_STATE", 1);
	}
	

public void tDBConnection_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBConnection_1_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tDBConnection_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBConnection_1", false);
		start_Hash.put("tDBConnection_1", System.currentTimeMillis());
		
	
	currentComponent="tDBConnection_1";

	
		int tos_count_tDBConnection_1 = 0;
		
	

	
			String url_tDBConnection_1 = "jdbc:jtds:sqlserver://" + "localhost" ;
		String port_tDBConnection_1 = "1433";
		String dbname_tDBConnection_1 = "PA_BI_SA" ;
    	if (!"".equals(port_tDBConnection_1)) {
    		url_tDBConnection_1 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_1)) {
    		
				url_tDBConnection_1 += "//" + "PA_BI_SA"; 
    	}

		url_tDBConnection_1 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_1 = "sa1";
	
	
		 
	final String decryptedPassword_tDBConnection_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:M7RwHJOgAVIJ7nmogSFwp+kXXq8E2cvyBAzp126MoTKT95zLxlE=");
		String dbPwd_tDBConnection_1 = decryptedPassword_tDBConnection_1;
	
	
	java.sql.Connection conn_tDBConnection_1 = null;
	
		
			String driverClass_tDBConnection_1 = "net.sourceforge.jtds.jdbc.Driver";
			java.lang.Class jdbcclazz_tDBConnection_1 = java.lang.Class.forName(driverClass_tDBConnection_1);
			globalMap.put("driverClass_tDBConnection_1", driverClass_tDBConnection_1);
		
			conn_tDBConnection_1 = java.sql.DriverManager.getConnection(url_tDBConnection_1,dbUser_tDBConnection_1,dbPwd_tDBConnection_1);

		globalMap.put("conn_tDBConnection_1", conn_tDBConnection_1);
	if (null != conn_tDBConnection_1) {
		
			conn_tDBConnection_1.setAutoCommit(false);
	}

	globalMap.put("dbschema_tDBConnection_1", "");

	globalMap.put("db_tDBConnection_1",  "PA_BI_SA");
	
	globalMap.put("shareIdentitySetting_tDBConnection_1",  false);

	globalMap.put("driver_tDBConnection_1", "JTDS");

 



/**
 * [tDBConnection_1 begin ] stop
 */
	
	/**
	 * [tDBConnection_1 main ] start
	 */

	

	
	
	currentComponent="tDBConnection_1";

	

 


	tos_count_tDBConnection_1++;

/**
 * [tDBConnection_1 main ] stop
 */
	
	/**
	 * [tDBConnection_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBConnection_1";

	

 



/**
 * [tDBConnection_1 process_data_begin ] stop
 */
	
	/**
	 * [tDBConnection_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBConnection_1";

	

 



/**
 * [tDBConnection_1 process_data_end ] stop
 */
	
	/**
	 * [tDBConnection_1 end ] start
	 */

	

	
	
	currentComponent="tDBConnection_1";

	

 

ok_Hash.put("tDBConnection_1", true);
end_Hash.put("tDBConnection_1", System.currentTimeMillis());




/**
 * [tDBConnection_1 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tDBConnection_1:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk1", 0, "ok");
								} 
							
							tDBConnection_2Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBConnection_1 finally ] start
	 */

	

	
	
	currentComponent="tDBConnection_1";

	

 



/**
 * [tDBConnection_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBConnection_1_SUBPROCESS_STATE", 1);
	}
	

public void tDBConnection_2Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBConnection_2_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tDBConnection_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBConnection_2", false);
		start_Hash.put("tDBConnection_2", System.currentTimeMillis());
		
	
	currentComponent="tDBConnection_2";

	
		int tos_count_tDBConnection_2 = 0;
		
	

	
			String url_tDBConnection_2 = "jdbc:jtds:sqlserver://" + "localhost" ;
		String port_tDBConnection_2 = "1433";
		String dbname_tDBConnection_2 = "PA_BI_SA" ;
    	if (!"".equals(port_tDBConnection_2)) {
    		url_tDBConnection_2 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_2)) {
    		
				url_tDBConnection_2 += "//" + "PA_BI_SA"; 
    	}

		url_tDBConnection_2 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_2 = "sa1";
	
	
		 
	final String decryptedPassword_tDBConnection_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:DKLyEI5qNl4/FTmRtojLEpygvX7F67bqwpvx8Q/IUCZ1gfRJiTw=");
		String dbPwd_tDBConnection_2 = decryptedPassword_tDBConnection_2;
	
	
	java.sql.Connection conn_tDBConnection_2 = null;
	
		
			String driverClass_tDBConnection_2 = "net.sourceforge.jtds.jdbc.Driver";
			java.lang.Class jdbcclazz_tDBConnection_2 = java.lang.Class.forName(driverClass_tDBConnection_2);
			globalMap.put("driverClass_tDBConnection_2", driverClass_tDBConnection_2);
		
			conn_tDBConnection_2 = java.sql.DriverManager.getConnection(url_tDBConnection_2,dbUser_tDBConnection_2,dbPwd_tDBConnection_2);

		globalMap.put("conn_tDBConnection_2", conn_tDBConnection_2);
	if (null != conn_tDBConnection_2) {
		
			conn_tDBConnection_2.setAutoCommit(false);
	}

	globalMap.put("dbschema_tDBConnection_2", "");

	globalMap.put("db_tDBConnection_2",  "PA_BI_SA");
	
	globalMap.put("shareIdentitySetting_tDBConnection_2",  false);

	globalMap.put("driver_tDBConnection_2", "JTDS");

 



/**
 * [tDBConnection_2 begin ] stop
 */
	
	/**
	 * [tDBConnection_2 main ] start
	 */

	

	
	
	currentComponent="tDBConnection_2";

	

 


	tos_count_tDBConnection_2++;

/**
 * [tDBConnection_2 main ] stop
 */
	
	/**
	 * [tDBConnection_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBConnection_2";

	

 



/**
 * [tDBConnection_2 process_data_begin ] stop
 */
	
	/**
	 * [tDBConnection_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBConnection_2";

	

 



/**
 * [tDBConnection_2 process_data_end ] stop
 */
	
	/**
	 * [tDBConnection_2 end ] start
	 */

	

	
	
	currentComponent="tDBConnection_2";

	

 

ok_Hash.put("tDBConnection_2", true);
end_Hash.put("tDBConnection_2", System.currentTimeMillis());




/**
 * [tDBConnection_2 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tDBConnection_2:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk2", 0, "ok");
								} 
							
							tDBConnection_3Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBConnection_2 finally ] start
	 */

	

	
	
	currentComponent="tDBConnection_2";

	

 



/**
 * [tDBConnection_2 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBConnection_2_SUBPROCESS_STATE", 1);
	}
	

public void tDBConnection_3Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBConnection_3_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tDBConnection_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBConnection_3", false);
		start_Hash.put("tDBConnection_3", System.currentTimeMillis());
		
	
	currentComponent="tDBConnection_3";

	
		int tos_count_tDBConnection_3 = 0;
		
	

	
			String url_tDBConnection_3 = "jdbc:jtds:sqlserver://" + "localhost" ;
		String port_tDBConnection_3 = "1433";
		String dbname_tDBConnection_3 = "PA_BI_SA" ;
    	if (!"".equals(port_tDBConnection_3)) {
    		url_tDBConnection_3 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_3)) {
    		
				url_tDBConnection_3 += "//" + "PA_BI_SA"; 
    	}

		url_tDBConnection_3 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_3 = "sa1";
	
	
		 
	final String decryptedPassword_tDBConnection_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:jyA0zdRwtjxiM6P77ZQRcWHqNrEDJf/tPCP1OSu83GZ+YfKCTlU=");
		String dbPwd_tDBConnection_3 = decryptedPassword_tDBConnection_3;
	
	
	java.sql.Connection conn_tDBConnection_3 = null;
	
		
			String driverClass_tDBConnection_3 = "net.sourceforge.jtds.jdbc.Driver";
			java.lang.Class jdbcclazz_tDBConnection_3 = java.lang.Class.forName(driverClass_tDBConnection_3);
			globalMap.put("driverClass_tDBConnection_3", driverClass_tDBConnection_3);
		
			conn_tDBConnection_3 = java.sql.DriverManager.getConnection(url_tDBConnection_3,dbUser_tDBConnection_3,dbPwd_tDBConnection_3);

		globalMap.put("conn_tDBConnection_3", conn_tDBConnection_3);
	if (null != conn_tDBConnection_3) {
		
			conn_tDBConnection_3.setAutoCommit(false);
	}

	globalMap.put("dbschema_tDBConnection_3", "");

	globalMap.put("db_tDBConnection_3",  "PA_BI_SA");
	
	globalMap.put("shareIdentitySetting_tDBConnection_3",  false);

	globalMap.put("driver_tDBConnection_3", "JTDS");

 



/**
 * [tDBConnection_3 begin ] stop
 */
	
	/**
	 * [tDBConnection_3 main ] start
	 */

	

	
	
	currentComponent="tDBConnection_3";

	

 


	tos_count_tDBConnection_3++;

/**
 * [tDBConnection_3 main ] stop
 */
	
	/**
	 * [tDBConnection_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBConnection_3";

	

 



/**
 * [tDBConnection_3 process_data_begin ] stop
 */
	
	/**
	 * [tDBConnection_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBConnection_3";

	

 



/**
 * [tDBConnection_3 process_data_end ] stop
 */
	
	/**
	 * [tDBConnection_3 end ] start
	 */

	

	
	
	currentComponent="tDBConnection_3";

	

 

ok_Hash.put("tDBConnection_3", true);
end_Hash.put("tDBConnection_3", System.currentTimeMillis());




/**
 * [tDBConnection_3 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tDBConnection_3:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk3", 0, "ok");
								} 
							
							tDBConnection_4Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBConnection_3 finally ] start
	 */

	

	
	
	currentComponent="tDBConnection_3";

	

 



/**
 * [tDBConnection_3 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBConnection_3_SUBPROCESS_STATE", 1);
	}
	

public void tDBConnection_4Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBConnection_4_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tDBConnection_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBConnection_4", false);
		start_Hash.put("tDBConnection_4", System.currentTimeMillis());
		
	
	currentComponent="tDBConnection_4";

	
		int tos_count_tDBConnection_4 = 0;
		
	

	
			String url_tDBConnection_4 = "jdbc:jtds:sqlserver://" + "localhost" ;
		String port_tDBConnection_4 = "1433";
		String dbname_tDBConnection_4 = "PA_BI_SA" ;
    	if (!"".equals(port_tDBConnection_4)) {
    		url_tDBConnection_4 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_4)) {
    		
				url_tDBConnection_4 += "//" + "PA_BI_SA"; 
    	}

		url_tDBConnection_4 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_4 = "sa1";
	
	
		 
	final String decryptedPassword_tDBConnection_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:nXqSlnSNrsj29fBTKuBIdLqMAQaWpkjOVLLEfX1DdiNxy5mfmCI=");
		String dbPwd_tDBConnection_4 = decryptedPassword_tDBConnection_4;
	
	
	java.sql.Connection conn_tDBConnection_4 = null;
	
		
			String driverClass_tDBConnection_4 = "net.sourceforge.jtds.jdbc.Driver";
			java.lang.Class jdbcclazz_tDBConnection_4 = java.lang.Class.forName(driverClass_tDBConnection_4);
			globalMap.put("driverClass_tDBConnection_4", driverClass_tDBConnection_4);
		
			conn_tDBConnection_4 = java.sql.DriverManager.getConnection(url_tDBConnection_4,dbUser_tDBConnection_4,dbPwd_tDBConnection_4);

		globalMap.put("conn_tDBConnection_4", conn_tDBConnection_4);
	if (null != conn_tDBConnection_4) {
		
			conn_tDBConnection_4.setAutoCommit(false);
	}

	globalMap.put("dbschema_tDBConnection_4", "");

	globalMap.put("db_tDBConnection_4",  "PA_BI_SA");
	
	globalMap.put("shareIdentitySetting_tDBConnection_4",  false);

	globalMap.put("driver_tDBConnection_4", "JTDS");

 



/**
 * [tDBConnection_4 begin ] stop
 */
	
	/**
	 * [tDBConnection_4 main ] start
	 */

	

	
	
	currentComponent="tDBConnection_4";

	

 


	tos_count_tDBConnection_4++;

/**
 * [tDBConnection_4 main ] stop
 */
	
	/**
	 * [tDBConnection_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBConnection_4";

	

 



/**
 * [tDBConnection_4 process_data_begin ] stop
 */
	
	/**
	 * [tDBConnection_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBConnection_4";

	

 



/**
 * [tDBConnection_4 process_data_end ] stop
 */
	
	/**
	 * [tDBConnection_4 end ] start
	 */

	

	
	
	currentComponent="tDBConnection_4";

	

 

ok_Hash.put("tDBConnection_4", true);
end_Hash.put("tDBConnection_4", System.currentTimeMillis());




/**
 * [tDBConnection_4 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tDBConnection_4:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk4", 0, "ok");
								} 
							
							tDBConnection_5Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBConnection_4 finally ] start
	 */

	

	
	
	currentComponent="tDBConnection_4";

	

 



/**
 * [tDBConnection_4 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBConnection_4_SUBPROCESS_STATE", 1);
	}
	

public void tDBConnection_5Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBConnection_5_SUBPROCESS_STATE", 0);

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



		


	
	/**
	 * [tDBConnection_5 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBConnection_5", false);
		start_Hash.put("tDBConnection_5", System.currentTimeMillis());
		
	
	currentComponent="tDBConnection_5";

	
		int tos_count_tDBConnection_5 = 0;
		
	

	
			String url_tDBConnection_5 = "jdbc:jtds:sqlserver://" + "localhost" ;
		String port_tDBConnection_5 = "1433";
		String dbname_tDBConnection_5 = "DW_E-Commerce" ;
    	if (!"".equals(port_tDBConnection_5)) {
    		url_tDBConnection_5 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_5)) {
    		
				url_tDBConnection_5 += "//" + "DW_E-Commerce"; 
    	}

		url_tDBConnection_5 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_5 = "sa";
	
	
		 
	final String decryptedPassword_tDBConnection_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:cAZg73jv5wzh+KqVD5yVfnAkL+SVzbBb+XasyDiRuoJFIIhAqiA=");
		String dbPwd_tDBConnection_5 = decryptedPassword_tDBConnection_5;
	
	
	java.sql.Connection conn_tDBConnection_5 = null;
	
		
			String driverClass_tDBConnection_5 = "net.sourceforge.jtds.jdbc.Driver";
			java.lang.Class jdbcclazz_tDBConnection_5 = java.lang.Class.forName(driverClass_tDBConnection_5);
			globalMap.put("driverClass_tDBConnection_5", driverClass_tDBConnection_5);
		
			conn_tDBConnection_5 = java.sql.DriverManager.getConnection(url_tDBConnection_5,dbUser_tDBConnection_5,dbPwd_tDBConnection_5);

		globalMap.put("conn_tDBConnection_5", conn_tDBConnection_5);
	if (null != conn_tDBConnection_5) {
		
			conn_tDBConnection_5.setAutoCommit(false);
	}

	globalMap.put("dbschema_tDBConnection_5", "");

	globalMap.put("db_tDBConnection_5",  "DW_E-Commerce");
	
	globalMap.put("shareIdentitySetting_tDBConnection_5",  false);

	globalMap.put("driver_tDBConnection_5", "JTDS");

 



/**
 * [tDBConnection_5 begin ] stop
 */
	
	/**
	 * [tDBConnection_5 main ] start
	 */

	

	
	
	currentComponent="tDBConnection_5";

	

 


	tos_count_tDBConnection_5++;

/**
 * [tDBConnection_5 main ] stop
 */
	
	/**
	 * [tDBConnection_5 process_data_begin ] start
	 */

	

	
	
	currentComponent="tDBConnection_5";

	

 



/**
 * [tDBConnection_5 process_data_begin ] stop
 */
	
	/**
	 * [tDBConnection_5 process_data_end ] start
	 */

	

	
	
	currentComponent="tDBConnection_5";

	

 



/**
 * [tDBConnection_5 process_data_end ] stop
 */
	
	/**
	 * [tDBConnection_5 end ] start
	 */

	

	
	
	currentComponent="tDBConnection_5";

	

 

ok_Hash.put("tDBConnection_5", true);
end_Hash.put("tDBConnection_5", System.currentTimeMillis());




/**
 * [tDBConnection_5 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tDBConnection_5:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk6", 0, "ok");
								} 
							
							tJava_1Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tDBConnection_5 finally ] start
	 */

	

	
	
	currentComponent="tDBConnection_5";

	

 



/**
 * [tDBConnection_5 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBConnection_5_SUBPROCESS_STATE", 1);
	}
	

public void tJava_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_1_SUBPROCESS_STATE", 0);

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





	
	/**
	 * [tJava_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_1", false);
		start_Hash.put("tJava_1", System.currentTimeMillis());
		
	
	currentComponent="tJava_1";

	
		int tos_count_tJava_1 = 0;
		


System.out.println("===== DEBUT EXECUTION JOB =====");

//System.out.println("Nom du Job : " + JobName);

System.out.println("Date debut : " + TalendDate.getCurrentDate());

System.out.println("Chronometre demarre...");
 



/**
 * [tJava_1 begin ] stop
 */
	
	/**
	 * [tJava_1 main ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 


	tos_count_tJava_1++;

/**
 * [tJava_1 main ] stop
 */
	
	/**
	 * [tJava_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 process_data_begin ] stop
 */
	
	/**
	 * [tJava_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 process_data_end ] stop
 */
	
	/**
	 * [tJava_1 end ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 

ok_Hash.put("tJava_1", true);
end_Hash.put("tJava_1", System.currentTimeMillis());




/**
 * [tJava_1 end ] stop
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
	 * [tJava_1 finally ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_1_SUBPROCESS_STATE", 1);
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
        final ETL_Dim_Mode_Paimement ETL_Dim_Mode_PaimementClass = new ETL_Dim_Mode_Paimement();

        int exitCode = ETL_Dim_Mode_PaimementClass.runJobInTOS(args);

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
            java.io.InputStream inContext = ETL_Dim_Mode_Paimement.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/etl_dim_mode_paimement_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ETL_Dim_Mode_Paimement.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
errorCode = null;tDBInput_6Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tDBInput_6) {
globalMap.put("tDBInput_6_SUBPROCESS_STATE", -1);

e_tDBInput_6.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob

try {
errorCode = null;tPostjob_1Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tPostjob_1) {
globalMap.put("tPostjob_1_SUBPROCESS_STATE", -1);

e_tPostjob_1.printStackTrace();

}



        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ETL_Dim_Mode_Paimement");
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
    closeSqlDbConnections();


    }



    private void closeSqlDbConnections() {
        try {
            Object obj_conn;
            obj_conn = globalMap.remove("conn_tDBConnection_1");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
            obj_conn = globalMap.remove("conn_tDBConnection_2");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
            obj_conn = globalMap.remove("conn_tDBConnection_3");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
            obj_conn = globalMap.remove("conn_tDBConnection_4");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
            obj_conn = globalMap.remove("conn_tDBConnection_5");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
        } catch (java.lang.Exception e) {
        }
    }











    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();
            connections.put("conn_tDBConnection_1", globalMap.get("conn_tDBConnection_1"));
            connections.put("conn_tDBConnection_2", globalMap.get("conn_tDBConnection_2"));
            connections.put("conn_tDBConnection_3", globalMap.get("conn_tDBConnection_3"));
            connections.put("conn_tDBConnection_4", globalMap.get("conn_tDBConnection_4"));
            connections.put("conn_tDBConnection_5", globalMap.get("conn_tDBConnection_5"));






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
 *     384445 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:45 CET
 ************************************************************************************************/