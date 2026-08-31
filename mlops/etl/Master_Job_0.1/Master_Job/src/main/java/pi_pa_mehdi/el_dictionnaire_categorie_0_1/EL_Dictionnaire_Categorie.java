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


package pi_pa_mehdi.el_dictionnaire_categorie_0_1;

import routines.Numeric;
import routines.DataOperation;
import routines.MyStringUtils;
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
 





@SuppressWarnings("unused")

/**
 * Job: EL_Dictionnaire_Categorie Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class EL_Dictionnaire_Categorie implements TalendJob {

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
	private final String jobName = "EL_Dictionnaire_Categorie";
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
				EL_Dictionnaire_Categorie.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(EL_Dictionnaire_Categorie.this, new Object[] { e , currentComponent, globalMap});
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
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFilterRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBSCD_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_8_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row9_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row12_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row13_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tSortRow_1_SortOut_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
							tSortRow_1_SortIn_error(exception, errorComponent, globalMap);
						
						}
					
			public void tSortRow_1_SortIn_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAggregateRow_1_AGGOUT_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
							tAggregateRow_1_AGGIN_error(exception, errorComponent, globalMap);
						
						}
					
			public void tAggregateRow_1_AGGIN_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_5_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBInput_5_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class row6Struct implements routines.system.IPersistableRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row6Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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

public static class row5Struct implements routines.system.IPersistableRow<row5Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row5Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

public static class copyOfout1Struct implements routines.system.IPersistableRow<copyOfout1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfout1Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

public static class copyOfout1_0Struct implements routines.system.IPersistableRow<copyOfout1_0Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfout1_0Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

public static class copyOfout1_1Struct implements routines.system.IPersistableRow<copyOfout1_1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfout1_1Struct other) {

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

public static class row4Struct implements routines.system.IPersistableRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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



		row1Struct row1 = new row1Struct();
out1Struct out1 = new out1Struct();

		row2Struct row2 = new row2Struct();
copyOfout1Struct copyOfout1 = new copyOfout1Struct();

		row3Struct row3 = new row3Struct();
copyOfout1_0Struct copyOfout1_0 = new copyOfout1_0Struct();

		row4Struct row4 = new row4Struct();
copyOfout1_1Struct copyOfout1_1 = new copyOfout1_1Struct();

			row5Struct row5 = new row5Struct();
row7Struct row7 = new row7Struct();
row6Struct row6 = new row6Struct();






	
	/**
	 * [tDBSCD_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBSCD_1", false);
		start_Hash.put("tDBSCD_1", System.currentTimeMillis());
		
	
	currentComponent="tDBSCD_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row6");
					}
				
		int tos_count_tDBSCD_1 = 0;
		



        class SCDSK_tDBSCD_1 {
private int hashCode;
public boolean hashCodeDirty = true;
String Produit;
public boolean equals(Object obj) {
if (this == obj) return true;
if (obj == null) return false;
if (getClass() != obj.getClass()) return false;
final SCDSK_tDBSCD_1 other = (SCDSK_tDBSCD_1) obj;
if (this.Produit == null) {
if (other.Produit!= null)
return false;
} else if (!this.Produit.equals(other.Produit))
return false;

return true;
}
public int hashCode() {
if(hashCodeDirty) {
int prime = 31;hashCode = prime * hashCode + (Produit == null ? 0 : Produit.hashCode());
hashCodeDirty = false;
}
return hashCode;
}
}

    class SCDStruct_tDBSCD_1 {
private String Categorie;
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

 
	final String decryptedPassword_tDBSCD_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:+a7+KVa9D6oaipWia/3czPxvnhB4QrbsoN/rYSlC9J+zexvXw7c=");

    String dbPwd_tDBSCD_1 = decryptedPassword_tDBSCD_1;	
    conn_tDBSCD_1 = java.sql.DriverManager.getConnection(url_tDBSCD_1,dbUser_tDBSCD_1,dbPwd_tDBSCD_1);
	

    if(dbschema_tDBSCD_1 == null || dbschema_tDBSCD_1.trim().length() == 0) {
        tableName_tDBSCD_1 = "Dictionnaire_Categorie";
    } else {
        tableName_tDBSCD_1 = dbschema_tDBSCD_1 + "].[" + "Dictionnaire_Categorie";
    }
	org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBSCD_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();	
    String tmpValue_tDBSCD_1 = null;    
        String search_tDBSCD_1 = "SELECT [Produit], [Categorie] FROM [" + tableName_tDBSCD_1 + "]";
        java.sql.Statement statement_tDBSCD_1 = conn_tDBSCD_1.createStatement();
        java.sql.ResultSet resultSet_tDBSCD_1 = statement_tDBSCD_1.executeQuery(search_tDBSCD_1);
        java.util.Map<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1> cache_tDBSCD_1 = new java.util.HashMap<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1>();
        while(resultSet_tDBSCD_1.next()) {
            SCDSK_tDBSCD_1 sk_tDBSCD_1 = new SCDSK_tDBSCD_1();
            SCDStruct_tDBSCD_1 row_tDBSCD_1 = new SCDStruct_tDBSCD_1();
                    if(resultSet_tDBSCD_1.getObject(1) != null) {
                        sk_tDBSCD_1.Produit = resultSet_tDBSCD_1.getString(1);
                    }
                    if(resultSet_tDBSCD_1.getObject(2) != null) {
                        row_tDBSCD_1.Categorie = resultSet_tDBSCD_1.getString(2);
                    }
            cache_tDBSCD_1.put(sk_tDBSCD_1, row_tDBSCD_1);
        }
        resultSet_tDBSCD_1.close();
        statement_tDBSCD_1.close();
    String insertionSQL_tDBSCD_1 = "INSERT INTO [" + tableName_tDBSCD_1 + "]([Produit], [Categorie]) VALUES(?, ?)";
    java.sql.PreparedStatement insertionStatement_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(insertionSQL_tDBSCD_1);
        String updateSQLForType1_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [Categorie] = ? WHERE [Produit] = ?";
        java.sql.PreparedStatement updateForType1_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType1_tDBSCD_1);        
    
        SCDSK_tDBSCD_1 lookUpKey_tDBSCD_1 = null;        
    SCDStruct_tDBSCD_1 lookUpValue_tDBSCD_1 = null;

 



/**
 * [tDBSCD_1 begin ] stop
 */



	
	/**
	 * [tUniqRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_1", false);
		start_Hash.put("tUniqRow_1", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row7");
					}
				
		int tos_count_tUniqRow_1 = 0;
		

	
		class KeyStruct_tUniqRow_1 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Produit;        
	        
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
				final KeyStruct_tUniqRow_1 other = (KeyStruct_tUniqRow_1) obj;
				
									if (this.Produit == null) {
										if (other.Produit != null) 
											return false;
								
									} else if (!this.Produit.equals(other.Produit))
								 
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
	 * [tFilterRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tFilterRow_1", false);
		start_Hash.put("tFilterRow_1", System.currentTimeMillis());
		
	
	currentComponent="tFilterRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
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
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out1","copyOfout1_1","copyOfout1_0","copyOfout1");
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
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:t767v+RJr8uMa6BmeDU0MLHg4xajhxmy2A92dOIGs6+vVnhyZZM=");
				
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

		    String dbquery_tDBInput_2 = "SELECT produits_sougui_webscraping.titre,\n		produits_sougui_webscraping.url,\n		produits_sougui_webscraping.pack,\n		prod"
+"uits_sougui_webscraping.prix_avant_promo,\n		produits_sougui_webscraping.prix_apres_promo,\n		produits_sougui_webscraping."
+"categories,\n		produits_sougui_webscraping.image,\n		produits_sougui_webscraping.en_promotion,\n		produits_sougui_webscrapi"
+"ng.sku\nFROM	produits_sougui_webscraping";
		    

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
								row1.titre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.titre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.titre = tmpContent_tDBInput_2;
                }
            } else {
                row1.titre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row1.url = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.url = tmpContent_tDBInput_2;
                }
            } else {
                row1.url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row1.pack = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(3);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.pack = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.pack = tmpContent_tDBInput_2;
                }
            } else {
                row1.pack = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row1.prix_avant_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(4);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.prix_avant_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.prix_avant_promo = tmpContent_tDBInput_2;
                }
            } else {
                row1.prix_avant_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row1.prix_apres_promo = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.prix_apres_promo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.prix_apres_promo = tmpContent_tDBInput_2;
                }
            } else {
                row1.prix_apres_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row1.categories = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(6);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.categories = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.categories = tmpContent_tDBInput_2;
                }
            } else {
                row1.categories = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row1.image = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.image = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.image = tmpContent_tDBInput_2;
                }
            } else {
                row1.image = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row1.en_promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.en_promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.en_promotion = tmpContent_tDBInput_2;
                }
            } else {
                row1.en_promotion = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row1.sku = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.sku = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row1.sku = tmpContent_tDBInput_2;
                }
            } else {
                row1.sku = null;
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
out1_tmp.Produit = row1.pack.replace(" ","" ).equals("N/A") ? 
    (row1.titre != null ? row1.titre.replace("?", "") : "") : 
    (row1.titre != null ? row1.titre.replace("?", "") + "-" + row1.pack.replace(" " ,"" ) : "-" + row1.pack) ;
out1_tmp.Categorie = row1.categories==null?"AUTRE":
StringHandling.TRIM(row1.categories).toUpperCase() ;
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
			row5 = new row5Struct();
								
			row5.Produit = out1.Produit;								
			row5.Categorie = out1.Categorie;			

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
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

          row7 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("&&");
	        ope_tFilterRow_1.matches((row5.Produit != null)
	                       , "Produit!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row7 == null){ 
                row7 = new row7Struct();
              }
               row7.Produit = row5.Produit;
               row7.Categorie = row5.Categorie;    
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
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					
row6 = null;			
if(row7.Produit == null){
	finder_tUniqRow_1.Produit = null;
}else{
	finder_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row7.Produit == null){
	new_tUniqRow_1.Produit = null;
}else{
	new_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row6 == null){ 
	
	row6 = new row6Struct();
}row6.Produit = row7.Produit;			row6.Categorie = row7.Categorie;					
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
// Start of branch "row6"
if(row6 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Produit = row6.Produit;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row6.Produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row6.Produit);
}

                    if(row6.Categorie == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row6.Categorie);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Categorie == null && row6.Categorie!= null) || (lookUpValue_tDBSCD_1.Categorie != null && !lookUpValue_tDBSCD_1.Categorie.equals(row6.Categorie))) {
                    if(row6.Categorie == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row6.Categorie);
}

                    if(row6.Produit == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row6.Produit);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Categorie = row6.Categorie;
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

} // End of branch "row6"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
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
copyOfout1Struct copyOfout1_tmp = new copyOfout1Struct();
// ###############################

        
        



        









 



/**
 * [tMap_2 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:+ZMwnU3bNcFsPxTrcxIQMpPTO+kIxiVl/ouw1AAbXtr2kLrl81w=");
				
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

		    String dbquery_tDBInput_1 = "SELECT ERP_Inventaire.Reference_Produit,\n		ERP_Inventaire.Nom_Produit,\n		ERP_Inventaire.Stock_Initial,\n		ERP_Inventaire"
+".Entrees,\n		ERP_Inventaire.Sorties,\n		ERP_Inventaire.Stock_Actuel,\n		ERP_Inventaire.Categorie,\n		ERP_Inventaire.Fourniss"
+"eur,\n		ERP_Inventaire.Prix_achat,\n		ERP_Inventaire.Prix_unitaire\nFROM	ERP_Inventaire";
		    

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
								row2.Reference_Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(1);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Reference_Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Reference_Produit = tmpContent_tDBInput_1;
                }
            } else {
                row2.Reference_Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 2) {
								row2.Nom_Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(2);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Nom_Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Nom_Produit = tmpContent_tDBInput_1;
                }
            } else {
                row2.Nom_Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 3) {
								row2.Stock_Initial = null;
							} else {
		                          
            row2.Stock_Initial = rs_tDBInput_1.getInt(3);
            if(rs_tDBInput_1.wasNull()){
                    row2.Stock_Initial = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 4) {
								row2.Entrees = null;
							} else {
		                          
            row2.Entrees = rs_tDBInput_1.getInt(4);
            if(rs_tDBInput_1.wasNull()){
                    row2.Entrees = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 5) {
								row2.Sorties = null;
							} else {
		                          
            row2.Sorties = rs_tDBInput_1.getInt(5);
            if(rs_tDBInput_1.wasNull()){
                    row2.Sorties = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 6) {
								row2.Stock_Actuel = null;
							} else {
		                          
            row2.Stock_Actuel = rs_tDBInput_1.getInt(6);
            if(rs_tDBInput_1.wasNull()){
                    row2.Stock_Actuel = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 7) {
								row2.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(7);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Categorie = tmpContent_tDBInput_1;
                }
            } else {
                row2.Categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 8) {
								row2.Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(8);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Fournisseur = tmpContent_tDBInput_1;
                }
            } else {
                row2.Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 9) {
								row2.Prix_achat = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(9);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Prix_achat = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Prix_achat = tmpContent_tDBInput_1;
                }
            } else {
                row2.Prix_achat = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 10) {
								row2.Prix_unitaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(10);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Prix_unitaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row2.Prix_unitaire = tmpContent_tDBInput_1;
                }
            } else {
                row2.Prix_unitaire = null;
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

copyOfout1 = null;


// # Output table : 'copyOfout1'
copyOfout1_tmp.Produit = row2.Nom_Produit ;
copyOfout1_tmp.Categorie = row2.Categorie==null?"AUTRE":
StringHandling.TRIM(row2.Categorie).toUpperCase() ;
copyOfout1 = copyOfout1_tmp;
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
// Start of branch "copyOfout1"
if(copyOfout1 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"copyOfout1"
									
							);
						}
						
//////////
 

// for output
			row5 = new row5Struct();
								
			row5.Produit = copyOfout1.Produit;								
			row5.Categorie = copyOfout1.Categorie;			

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
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

          row7 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("&&");
	        ope_tFilterRow_1.matches((row5.Produit != null)
	                       , "Produit!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row7 == null){ 
                row7 = new row7Struct();
              }
               row7.Produit = row5.Produit;
               row7.Categorie = row5.Categorie;    
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
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					
row6 = null;			
if(row7.Produit == null){
	finder_tUniqRow_1.Produit = null;
}else{
	finder_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row7.Produit == null){
	new_tUniqRow_1.Produit = null;
}else{
	new_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row6 == null){ 
	
	row6 = new row6Struct();
}row6.Produit = row7.Produit;			row6.Categorie = row7.Categorie;					
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
// Start of branch "row6"
if(row6 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Produit = row6.Produit;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row6.Produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row6.Produit);
}

                    if(row6.Categorie == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row6.Categorie);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Categorie == null && row6.Categorie!= null) || (lookUpValue_tDBSCD_1.Categorie != null && !lookUpValue_tDBSCD_1.Categorie.equals(row6.Categorie))) {
                    if(row6.Categorie == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row6.Categorie);
}

                    if(row6.Produit == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row6.Produit);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Categorie = row6.Categorie;
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

} // End of branch "row6"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "copyOfout1"




	
	/**
	 * [tMap_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 process_data_end ] stop
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
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_3__Struct  {
}
Var__tMap_3__Struct Var__tMap_3 = new Var__tMap_3__Struct();
// ###############################

// ###############################
// # Outputs initialization
copyOfout1_0Struct copyOfout1_0_tmp = new copyOfout1_0Struct();
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
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:2ZNJ4Mr8UCnpKZkoxGGRek5XP9F96SEtcminDsxH7y5RnqmN7cs=");
				
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

		    String dbquery_tDBInput_3 = "SELECT produits_edayetna_webscraping.titre,\n		produits_edayetna_webscraping.categorie,\n		produits_edayetna_webscraping."
+"prix_avant,\n		produits_edayetna_webscraping.prix_apres,\n		produits_edayetna_webscraping.promotion,\n		produits_edayetna_w"
+"ebscraping.url,\n		produits_edayetna_webscraping.image\nFROM	produits_edayetna_webscraping";
		    

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
								row3.titre = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(1);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.titre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.titre = tmpContent_tDBInput_3;
                }
            } else {
                row3.titre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row3.categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.categorie = tmpContent_tDBInput_3;
                }
            } else {
                row3.categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row3.prix_avant = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(3);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.prix_avant = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.prix_avant = tmpContent_tDBInput_3;
                }
            } else {
                row3.prix_avant = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row3.prix_apres = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.prix_apres = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.prix_apres = tmpContent_tDBInput_3;
                }
            } else {
                row3.prix_apres = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 5) {
								row3.promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(5);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.promotion = tmpContent_tDBInput_3;
                }
            } else {
                row3.promotion = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 6) {
								row3.url = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(6);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.url = tmpContent_tDBInput_3;
                }
            } else {
                row3.url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 7) {
								row3.image = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(7);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row3.image = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row3.image = tmpContent_tDBInput_3;
                }
            } else {
                row3.image = null;
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
						
							,"row3"
						
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

copyOfout1_0 = null;


// # Output table : 'copyOfout1_0'
copyOfout1_0_tmp.Produit = row3.titre ;
copyOfout1_0_tmp.Categorie = row3.categorie==null?"AUTRE":
StringHandling.TRIM(row3.categorie).toUpperCase() ;
copyOfout1_0 = copyOfout1_0_tmp;
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
// Start of branch "copyOfout1_0"
if(copyOfout1_0 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"copyOfout1_0"
									
							);
						}
						
//////////
 

// for output
			row5 = new row5Struct();
								
			row5.Produit = copyOfout1_0.Produit;								
			row5.Categorie = copyOfout1_0.Categorie;			

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
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

          row7 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("&&");
	        ope_tFilterRow_1.matches((row5.Produit != null)
	                       , "Produit!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row7 == null){ 
                row7 = new row7Struct();
              }
               row7.Produit = row5.Produit;
               row7.Categorie = row5.Categorie;    
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
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					
row6 = null;			
if(row7.Produit == null){
	finder_tUniqRow_1.Produit = null;
}else{
	finder_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row7.Produit == null){
	new_tUniqRow_1.Produit = null;
}else{
	new_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row6 == null){ 
	
	row6 = new row6Struct();
}row6.Produit = row7.Produit;			row6.Categorie = row7.Categorie;					
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
// Start of branch "row6"
if(row6 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Produit = row6.Produit;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row6.Produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row6.Produit);
}

                    if(row6.Categorie == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row6.Categorie);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Categorie == null && row6.Categorie!= null) || (lookUpValue_tDBSCD_1.Categorie != null && !lookUpValue_tDBSCD_1.Categorie.equals(row6.Categorie))) {
                    if(row6.Categorie == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row6.Categorie);
}

                    if(row6.Produit == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row6.Produit);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Categorie = row6.Categorie;
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

} // End of branch "row6"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "copyOfout1_0"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row3");
			  	}
			  	
 

ok_Hash.put("tMap_3", true);
end_Hash.put("tMap_3", System.currentTimeMillis());




/**
 * [tMap_3 end ] stop
 */




	
	/**
	 * [tMap_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_4", false);
		start_Hash.put("tMap_4", System.currentTimeMillis());
		
	
	currentComponent="tMap_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row4");
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
copyOfout1_1Struct copyOfout1_1_tmp = new copyOfout1_1Struct();
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
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:uzrDSK4gfQqPZT7XSe9J0PFyh2Zef0d7xWlUfv9Di7HnBSHRsS8=");
				
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

		    String dbquery_tDBInput_4 = "SELECT produits_kalys_webscraping.Produit,\n		produits_kalys_webscraping.Categorie,\n		produits_kalys_webscraping.Sous_ca"
+"tegorie,\n		produits_kalys_webscraping.Categories_supplementaires,\n		produits_kalys_webscraping.Prix_avant_promo__TND,\n		"
+"produits_kalys_webscraping.Prix_apres_promo__TND,\n		produits_kalys_webscraping.En_promotion\nFROM	produits_kalys_webscrap"
+"ing";
		    

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
								row4.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(1);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Produit = tmpContent_tDBInput_4;
                }
            } else {
                row4.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row4.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Categorie = tmpContent_tDBInput_4;
                }
            } else {
                row4.Categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 3) {
								row4.Sous_categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(3);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Sous_categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Sous_categorie = tmpContent_tDBInput_4;
                }
            } else {
                row4.Sous_categorie = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 4) {
								row4.Categories_supplementaires = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(4);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Categories_supplementaires = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Categories_supplementaires = tmpContent_tDBInput_4;
                }
            } else {
                row4.Categories_supplementaires = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 5) {
								row4.Prix_avant_promo__TND = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(5);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Prix_avant_promo__TND = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Prix_avant_promo__TND = tmpContent_tDBInput_4;
                }
            } else {
                row4.Prix_avant_promo__TND = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 6) {
								row4.Prix_apres_promo__TND = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(6);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Prix_apres_promo__TND = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.Prix_apres_promo__TND = tmpContent_tDBInput_4;
                }
            } else {
                row4.Prix_apres_promo__TND = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 7) {
								row4.En_promotion = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(7);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.En_promotion = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row4.En_promotion = tmpContent_tDBInput_4;
                }
            } else {
                row4.En_promotion = null;
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
						
							,"row4"
						
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

copyOfout1_1 = null;


// # Output table : 'copyOfout1_1'
copyOfout1_1_tmp.Produit = row4.Produit ;
copyOfout1_1_tmp.Categorie = row4.Categorie==null?"AUTRE":
StringHandling.TRIM(row4.Categorie).toUpperCase() ;
copyOfout1_1 = copyOfout1_1_tmp;
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
// Start of branch "copyOfout1_1"
if(copyOfout1_1 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"copyOfout1_1"
									
							);
						}
						
//////////
 

// for output
			row5 = new row5Struct();
								
			row5.Produit = copyOfout1_1.Produit;								
			row5.Categorie = copyOfout1_1.Categorie;			

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
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

          row7 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("&&");
	        ope_tFilterRow_1.matches((row5.Produit != null)
	                       , "Produit!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row7 == null){ 
                row7 = new row7Struct();
              }
               row7.Produit = row5.Produit;
               row7.Categorie = row5.Categorie;    
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
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					
row6 = null;			
if(row7.Produit == null){
	finder_tUniqRow_1.Produit = null;
}else{
	finder_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row7.Produit == null){
	new_tUniqRow_1.Produit = null;
}else{
	new_tUniqRow_1.Produit = row7.Produit.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row6 == null){ 
	
	row6 = new row6Struct();
}row6.Produit = row7.Produit;			row6.Categorie = row7.Categorie;					
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
// Start of branch "row6"
if(row6 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Produit = row6.Produit;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row6.Produit == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row6.Produit);
}

                    if(row6.Categorie == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row6.Categorie);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Categorie == null && row6.Categorie!= null) || (lookUpValue_tDBSCD_1.Categorie != null && !lookUpValue_tDBSCD_1.Categorie.equals(row6.Categorie))) {
                    if(row6.Categorie == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row6.Categorie);
}

                    if(row6.Produit == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row6.Produit);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Categorie = row6.Categorie;
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

} // End of branch "row6"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "copyOfout1_1"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row4");
			  	}
			  	
 

ok_Hash.put("tMap_4", true);
end_Hash.put("tMap_4", System.currentTimeMillis());




/**
 * [tMap_4 end ] stop
 */



	
	/**
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out1","copyOfout1_1","copyOfout1_0","copyOfout1");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
 */

	
	/**
	 * [tFilterRow_1 end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
    globalMap.put("tFilterRow_1_NB_LINE", nb_line_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_OK", nb_line_ok_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_REJECT", nb_line_reject_tFilterRow_1);
    

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
			  	}
			  	
 

ok_Hash.put("tFilterRow_1", true);
end_Hash.put("tFilterRow_1", System.currentTimeMillis());




/**
 * [tFilterRow_1 end ] stop
 */

	
	/**
	 * [tUniqRow_1 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

globalMap.put("tUniqRow_1_NB_UNIQUES",nb_uniques_tUniqRow_1);
globalMap.put("tUniqRow_1_NB_DUPLICATES",nb_duplicates_tUniqRow_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row7");
			  	}
			  	
 

ok_Hash.put("tUniqRow_1", true);
end_Hash.put("tUniqRow_1", System.currentTimeMillis());




/**
 * [tUniqRow_1 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row6");
			  	}
			  	
 

ok_Hash.put("tDBSCD_1", true);
end_Hash.put("tDBSCD_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk1", 0, "ok");
				}
				tDBInput_5Process(globalMap);



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
	 * [tDBInput_2 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_2";

	

 



/**
 * [tDBInput_2 finally ] stop
 */

	
	/**
	 * [tMap_1 finally ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 finally ] stop
 */



	
	/**
	 * [tDBInput_1 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_1";

	

 



/**
 * [tDBInput_1 finally ] stop
 */

	
	/**
	 * [tMap_2 finally ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 finally ] stop
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
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tFilterRow_1 finally ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 finally ] stop
 */

	
	/**
	 * [tUniqRow_1 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 finally ] stop
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
		

		globalMap.put("tDBInput_2_SUBPROCESS_STATE", 1);
	}
	


public static class out_finaallStruct implements routines.system.IPersistableRow<out_finaallStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out_finaallStruct other) {

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

public static class out_finallStruct implements routines.system.IPersistableRow<out_finallStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out_finallStruct other) {

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

public static class row11Struct implements routines.system.IPersistableRow<row11Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Produit_Ref;

				public String getProduit_Ref () {
					return this.Produit_Ref;
				}
				
			    public Integer Distance;

				public Integer getDistance () {
					return this.Distance;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
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
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Produit_Ref="+Produit_Ref);
		sb.append(",Distance="+String.valueOf(Distance));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row11Struct other) {

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

public static class OnRowsEndStructtAggregateRow_1 implements routines.system.IPersistableRow<OnRowsEndStructtAggregateRow_1> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Produit_Ref;

				public String getProduit_Ref () {
					return this.Produit_Ref;
				}
				
			    public Integer Distance;

				public Integer getDistance () {
					return this.Distance;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
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
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Produit_Ref="+Produit_Ref);
		sb.append(",Distance="+String.valueOf(Distance));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(OnRowsEndStructtAggregateRow_1 other) {

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

public static class row10Struct implements routines.system.IPersistableRow<row10Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Produit_Ref;

				public String getProduit_Ref () {
					return this.Produit_Ref;
				}
				
			    public Integer Distance;

				public Integer getDistance () {
					return this.Distance;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
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
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Produit_Ref="+Produit_Ref);
		sb.append(",Distance="+String.valueOf(Distance));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row10Struct other) {

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

public static class OnRowsEndStructtSortRow_1 implements routines.system.IPersistableRow<OnRowsEndStructtSortRow_1> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Produit_Ref;

				public String getProduit_Ref () {
					return this.Produit_Ref;
				}
				
			    public Integer Distance;

				public Integer getDistance () {
					return this.Distance;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
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
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Produit_Ref="+Produit_Ref);
		sb.append(",Distance="+String.valueOf(Distance));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(OnRowsEndStructtSortRow_1 other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Produit_Ref;

				public String getProduit_Ref () {
					return this.Produit_Ref;
				}
				
			    public Integer Distance;

				public Integer getDistance () {
					return this.Distance;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Produit_Ref = readString(dis);
					
						this.Distance = readInteger(dis);
					
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
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Produit,dos);
					
					// String
				
						writeString(this.Produit_Ref,dos);
					
					// Integer
				
						writeInteger(this.Distance,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Produit="+Produit);
		sb.append(",Produit_Ref="+Produit_Ref);
		sb.append(",Distance="+String.valueOf(Distance));
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

public static class row8Struct implements routines.system.IPersistableRow<row8Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
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
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
					
					this.date_debut = readDate(dis);
					
					this.date_fin = readDate(dis);
					
						this.actif = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
					
					this.date_debut = readDate(dis);
					
					this.date_fin = readDate(dis);
					
						this.actif = readInteger(dis);
					
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
					
					// java.util.Date
				
						writeDate(this.date_debut,dos);
					
					// java.util.Date
				
						writeDate(this.date_fin,dos);
					
					// Integer
				
						writeInteger(this.actif,dos);
					
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
					
					// java.util.Date
				
						writeDate(this.date_debut,dos);
					
					// java.util.Date
				
						writeDate(this.date_fin,dos);
					
					// Integer
				
						writeInteger(this.actif,dos);
					
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

public static class after_tDBInput_5Struct implements routines.system.IPersistableRow<after_tDBInput_5Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
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
		final after_tDBInput_5Struct other = (after_tDBInput_5Struct) obj;
		
						if (this.Produit_PK != other.Produit_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(after_tDBInput_5Struct other) {

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

	public void copyKeysDataTo(after_tDBInput_5Struct other) {

		other.Produit_PK = this.Produit_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
					
					this.date_debut = readDate(dis);
					
					this.date_fin = readDate(dis);
					
						this.actif = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
					
					this.date_debut = readDate(dis);
					
					this.date_fin = readDate(dis);
					
						this.actif = readInteger(dis);
					
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
					
					// java.util.Date
				
						writeDate(this.date_debut,dos);
					
					// java.util.Date
				
						writeDate(this.date_fin,dos);
					
					// Integer
				
						writeInteger(this.actif,dos);
					
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
					
					// java.util.Date
				
						writeDate(this.date_debut,dos);
					
					// java.util.Date
				
						writeDate(this.date_fin,dos);
					
					// Integer
				
						writeInteger(this.actif,dos);
					
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
		sb.append(",date_debut="+String.valueOf(date_debut));
		sb.append(",date_fin="+String.valueOf(date_fin));
		sb.append(",actif="+String.valueOf(actif));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(after_tDBInput_5Struct other) {

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
public void tDBInput_5Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_5_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
		String currentVirtualComponent = null;
	
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


		tDBInput_6Process(globalMap);
		tDBInput_7Process(globalMap);
		tDBInput_8Process(globalMap);

		row8Struct row8 = new row8Struct();
out2Struct out2 = new out2Struct();
row10Struct row10 = new row10Struct();
row11Struct row11 = new row11Struct();
out_finallStruct out_finall = new out_finallStruct();
out_finaallStruct out_finaall = new out_finaallStruct();





	
	/**
	 * [tSortRow_1_SortOut begin ] start
	 */

	

	
		
		ok_Hash.put("tSortRow_1_SortOut", false);
		start_Hash.put("tSortRow_1_SortOut", System.currentTimeMillis());
		
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out2");
					}
				
		int tos_count_tSortRow_1_SortOut = 0;
		


class Comparableout2Struct extends out2Struct implements Comparable<Comparableout2Struct> {
	
	public int compareTo(Comparableout2Struct other) {

		if(this.Produit == null && other.Produit != null){
			return -1;
						
		}else if(this.Produit != null && other.Produit == null){
			return 1;
						
		}else if(this.Produit != null && other.Produit != null){
			if(!this.Produit.equals(other.Produit)){
				return this.Produit.compareTo(other.Produit);
			}
		}
		if(this.Distance == null && other.Distance != null){
			return -1;
						
		}else if(this.Distance != null && other.Distance == null){
			return 1;
						
		}else if(this.Distance != null && other.Distance != null){
			if(!this.Distance.equals(other.Distance)){
				return this.Distance.compareTo(other.Distance);
			}
		}
		return 0;
	}
}

java.util.List<Comparableout2Struct> list_tSortRow_1_SortOut = new java.util.ArrayList<Comparableout2Struct>();


 



/**
 * [tSortRow_1_SortOut begin ] stop
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
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct> tHash_Lookup_row9 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct>) 
					globalMap.get( "tHash_Lookup_row9" ))
					;					
					
	
		tHash_Lookup_row9.initGet();
	

row9Struct row9HashKey = new row9Struct();
row9Struct row9Default = new row9Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_5__Struct  {
}
Var__tMap_5__Struct Var__tMap_5 = new Var__tMap_5__Struct();
// ###############################

// ###############################
// # Outputs initialization
out2Struct out2_tmp = new out2Struct();
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
				String dbUser_tDBInput_5 = "sa";
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:X+rkGUSYqv2Lh6d9Il0gYXoQIp7uHQM/JiGJwcc53Qzvw3hYTGU=");
				
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
+"rl,\n		Dim_Produit.Visibilite,\n		Dim_Produit.Prix,\n		Dim_Produit.Prix_en_promo,\n		Dim_Produit.En_Promo,\n		Dim_Produit.Can"
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
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row9" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow9 = false;
       		  	    	
       		  	    	
 							row9Struct row9ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_5) { // G_TM_M_020

								

								
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row9.lookup( row9HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row9.hasNext()) { // G_TM_M_090

  								
		  				
	  								
						
									
	
		  								forceLooprow9 = true;
	  					
  									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
								
								else { // G 20 - G 21
   									forceLooprow9 = true;
			           		  	} // G 21
                    		  	
                    		

							row9Struct row9 = null;
                    		  	 
							

								while ((tHash_Lookup_row9 != null && tHash_Lookup_row9.hasNext()) || forceLooprow9) { // G_TM_M_043

								
									 // CALL close loop of lookup 'row9'
									
                    		  	 
							   
                    		  	 
	       		  	    	row9Struct fromLookup_row9 = null;
							row9 = row9Default;
										 
							
								
								if(!forceLooprow9) { // G 46
								
							
								 
							
								
								fromLookup_row9 = tHash_Lookup_row9.next();

							

							if(fromLookup_row9 != null) {
								row9 = fromLookup_row9;
							}
							
							
							
			  							
								
	                    		  	
		                    
	                    	
	                    		} // G 46
	                    		  	
								forceLooprow9 = false;
									 	
							
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_5__Struct Var = Var__tMap_5;// ###############################
        // ###############################
        // # Output tables

out2 = null;


// # Output table : 'out2'
out2_tmp.Produit = row8.Nom_produit ;
out2_tmp.Produit_Ref = row9.Produit ;
out2_tmp.Distance = routines.MyStringUtils.levenshtein(row8.Nom_produit, row9.Produit) ;
out2 = out2_tmp;
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
// Start of branch "out2"
if(out2 != null) { 



	
	/**
	 * [tSortRow_1_SortOut main ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out2"
						
						);
					}
					



	Comparableout2Struct arrayRowtSortRow_1_SortOut = new Comparableout2Struct();

	arrayRowtSortRow_1_SortOut.Produit = out2.Produit;
	arrayRowtSortRow_1_SortOut.Produit_Ref = out2.Produit_Ref;
	arrayRowtSortRow_1_SortOut.Distance = out2.Distance;	
	list_tSortRow_1_SortOut.add(arrayRowtSortRow_1_SortOut);

 


	tos_count_tSortRow_1_SortOut++;

/**
 * [tSortRow_1_SortOut main ] stop
 */
	
	/**
	 * [tSortRow_1_SortOut process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	

 



/**
 * [tSortRow_1_SortOut process_data_begin ] stop
 */
	
	/**
	 * [tSortRow_1_SortOut process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	

 



/**
 * [tSortRow_1_SortOut process_data_end ] stop
 */

} // End of branch "out2"



	
		} // close loop of lookup 'row9' // G_TM_M_043
	
	
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
					if(tHash_Lookup_row9 != null) {
						tHash_Lookup_row9.endGet();
					}
					globalMap.remove( "tHash_Lookup_row9" );

					
					
				
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
	 * [tSortRow_1_SortOut end ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	

out2Struct[] array_tSortRow_1_SortOut = list_tSortRow_1_SortOut.toArray(new Comparableout2Struct[0]);

java.util.Arrays.sort(array_tSortRow_1_SortOut);

globalMap.put("tSortRow_1",array_tSortRow_1_SortOut);


				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out2");
			  	}
			  	
 

ok_Hash.put("tSortRow_1_SortOut", true);
end_Hash.put("tSortRow_1_SortOut", System.currentTimeMillis());




/**
 * [tSortRow_1_SortOut end ] stop
 */


	
	/**
	 * [tAggregateRow_1_AGGOUT begin ] start
	 */

	

	
		
		ok_Hash.put("tAggregateRow_1_AGGOUT", false);
		start_Hash.put("tAggregateRow_1_AGGOUT", System.currentTimeMillis());
		
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row10");
					}
				
		int tos_count_tAggregateRow_1_AGGOUT = 0;
		

// ------------ Seems it is not used

java.util.Map hashAggreg_tAggregateRow_1 = new java.util.HashMap(); 

// ------------

	class UtilClass_tAggregateRow_1 { // G_OutBegin_AggR_144

		public double sd(Double[] data) {
	        final int n = data.length;
        	if (n < 2) {
	            return Double.NaN;
        	}
        	double d1 = 0d;
        	double d2 =0d;
	        
	        for (int i = 0; i < data.length; i++) {
            	d1 += (data[i]*data[i]);
            	d2 += data[i];
        	}
        
	        return Math.sqrt((n*d1 - d2*d2)/n/(n-1));
	    }
	    
		public void checkedIADD(byte a, byte b, boolean checkTypeOverFlow, boolean checkUlp) {
		    byte r = (byte) (a + b);
		    if (checkTypeOverFlow && ((a ^ r) & (b ^ r)) < 0) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'short/Short'", "'byte/Byte'"));
		    }
		}
		
		public void checkedIADD(short a, short b, boolean checkTypeOverFlow, boolean checkUlp) {
		    short r = (short) (a + b);
		    if (checkTypeOverFlow && ((a ^ r) & (b ^ r)) < 0) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'int/Integer'", "'short/Short'"));
		    }
		}
		
		public void checkedIADD(int a, int b, boolean checkTypeOverFlow, boolean checkUlp) {
		    int r = a + b;
		    if (checkTypeOverFlow && ((a ^ r) & (b ^ r)) < 0) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'long/Long'", "'int/Integer'"));
		    }
		}
		
		public void checkedIADD(long a, long b, boolean checkTypeOverFlow, boolean checkUlp) {
		    long r = a + b;
		    if (checkTypeOverFlow && ((a ^ r) & (b ^ r)) < 0) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'long/Long'"));
		    }
		}
		
		public void checkedIADD(float a, float b, boolean checkTypeOverFlow, boolean checkUlp) {
		
			if(checkUlp) {
			    float minAddedValue = Math.ulp(a);
			    if (minAddedValue > Math.abs(b)) {
			        throw new RuntimeException(buildPrecisionMessage(String.valueOf(a), String.valueOf(b), "'double' or 'BigDecimal'", "'float/Float'"));
			    }
			}
			
		    if (checkTypeOverFlow && ((double) a + (double) b > (double) Float.MAX_VALUE) || ((double) a + (double) b < (double) -Float.MAX_VALUE)) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'double' or 'BigDecimal'", "'float/Float'"));
		    }
		}
		
		public void checkedIADD(double a, double b, boolean checkTypeOverFlow, boolean checkUlp) {
		
			if(checkUlp) {
			    double minAddedValue = Math.ulp(a);
			    if (minAddedValue > Math.abs(b)) {
			        throw new RuntimeException(buildPrecisionMessage(String.valueOf(a), String.valueOf(a), "'BigDecimal'", "'double/Double'"));
			    }
			}
		
		    if (checkTypeOverFlow && (a + b > (double) Double.MAX_VALUE) || (a + b < -Double.MAX_VALUE )) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'double/Double'"));
		    }
		}
		
		public void checkedIADD(double a, byte b, boolean checkTypeOverFlow, boolean checkUlp) {
		
		    if (checkTypeOverFlow && (a + b > (double) Double.MAX_VALUE) || (a + b < -Double.MAX_VALUE )) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'double/Double'"));
		    }
		}
		
		public void checkedIADD(double a, short b, boolean checkTypeOverFlow, boolean checkUlp) {
		
		    if (checkTypeOverFlow && (a + b > (double) Double.MAX_VALUE) || (a + b < -Double.MAX_VALUE )) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'double/Double'"));
		    }
		}
		
		public void checkedIADD(double a, int b, boolean checkTypeOverFlow, boolean checkUlp) {
		
		    if (checkTypeOverFlow && (a + b > (double) Double.MAX_VALUE) || (a + b < -Double.MAX_VALUE )) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'double/Double'"));
		    }
		}
		
		public void checkedIADD(double a, float b, boolean checkTypeOverFlow, boolean checkUlp) {
		
			if(checkUlp) {
			    double minAddedValue = Math.ulp(a);
			    if (minAddedValue > Math.abs(b)) {
			        throw new RuntimeException(buildPrecisionMessage(String.valueOf(a), String.valueOf(a), "'BigDecimal'", "'double/Double'"));
			    }
			}
		
		    if (checkTypeOverFlow && (a + b > (double) Double.MAX_VALUE) || (a + b < -Double.MAX_VALUE )) {
		        throw new RuntimeException(buildOverflowMessage(String.valueOf(a), String.valueOf(b), "'BigDecimal'", "'double/Double'"));
		    }
		}
		
		private String buildOverflowMessage(String a, String b, String advicedTypes, String originalType) {
		    return "Type overflow when adding " + b + " to " + a
		    + ", to resolve this problem, increase the precision by using "+ advicedTypes +" type in place of "+ originalType +".";
		}
		
		private String buildPrecisionMessage(String a, String b, String advicedTypes, String originalType) {
		    return "The double precision is unsufficient to add the value " + b + " to " + a
		    + ", to resolve this problem, increase the precision by using "+ advicedTypes +" type in place of "+ originalType +".";
		}

	} // G_OutBegin_AggR_144

	UtilClass_tAggregateRow_1 utilClass_tAggregateRow_1 = new UtilClass_tAggregateRow_1();

	

	class AggOperationStruct_tAggregateRow_1 { // G_OutBegin_AggR_100

		private static final int DEFAULT_HASHCODE = 1;
	    private static final int PRIME = 31;
	    private int hashCode = DEFAULT_HASHCODE;
	    public boolean hashCodeDirty = true;

    				String Produit;
         			String Produit_Ref_first;
        
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
			final AggOperationStruct_tAggregateRow_1 other = (AggOperationStruct_tAggregateRow_1) obj;
			
							if (this.Produit == null) {
								if (other.Produit != null) 
									return false;
							} else if (!this.Produit.equals(other.Produit)) 
								return false;
						
			
			return true;
		}
  
        
	} // G_OutBegin_AggR_100

	AggOperationStruct_tAggregateRow_1 operation_result_tAggregateRow_1 = null;
	AggOperationStruct_tAggregateRow_1 operation_finder_tAggregateRow_1 = new AggOperationStruct_tAggregateRow_1();
	java.util.Map<AggOperationStruct_tAggregateRow_1,AggOperationStruct_tAggregateRow_1> hash_tAggregateRow_1 = new java.util.HashMap<AggOperationStruct_tAggregateRow_1,AggOperationStruct_tAggregateRow_1>();
	

 



/**
 * [tAggregateRow_1_AGGOUT begin ] stop
 */



	
	/**
	 * [tSortRow_1_SortIn begin ] start
	 */

	

	
		
		ok_Hash.put("tSortRow_1_SortIn", false);
		start_Hash.put("tSortRow_1_SortIn", System.currentTimeMillis());
		
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	
		int tos_count_tSortRow_1_SortIn = 0;
		


out2Struct[] array_tSortRow_1_SortIn = (out2Struct[]) globalMap.remove("tSortRow_1");

int nb_line_tSortRow_1_SortIn = 0;

out2Struct current_tSortRow_1_SortIn = null;

for(int i_tSortRow_1_SortIn = 0; i_tSortRow_1_SortIn < array_tSortRow_1_SortIn.length; i_tSortRow_1_SortIn++){
	current_tSortRow_1_SortIn = array_tSortRow_1_SortIn[i_tSortRow_1_SortIn];
	row10.Produit = current_tSortRow_1_SortIn.Produit;
	row10.Produit_Ref = current_tSortRow_1_SortIn.Produit_Ref;
	row10.Distance = current_tSortRow_1_SortIn.Distance;
	// increase number of line sorted
	nb_line_tSortRow_1_SortIn++;

 



/**
 * [tSortRow_1_SortIn begin ] stop
 */
	
	/**
	 * [tSortRow_1_SortIn main ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	

 


	tos_count_tSortRow_1_SortIn++;

/**
 * [tSortRow_1_SortIn main ] stop
 */
	
	/**
	 * [tSortRow_1_SortIn process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	

 



/**
 * [tSortRow_1_SortIn process_data_begin ] stop
 */

	
	/**
	 * [tAggregateRow_1_AGGOUT main ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					
	
operation_finder_tAggregateRow_1.Produit = row10.Produit;
			

	operation_finder_tAggregateRow_1.hashCodeDirty = true;
	
	operation_result_tAggregateRow_1 = hash_tAggregateRow_1.get(operation_finder_tAggregateRow_1);

	
		boolean isFirstAdd_tAggregateRow_1 = false;
	

	if(operation_result_tAggregateRow_1 == null) { // G_OutMain_AggR_001

		operation_result_tAggregateRow_1 = new AggOperationStruct_tAggregateRow_1();

		operation_result_tAggregateRow_1.Produit = operation_finder_tAggregateRow_1.Produit;
				
		
		
			isFirstAdd_tAggregateRow_1 = true;
		

		hash_tAggregateRow_1.put(operation_result_tAggregateRow_1, operation_result_tAggregateRow_1);
	
	} // G_OutMain_AggR_001


	
				if(isFirstAdd_tAggregateRow_1 ) {
					operation_result_tAggregateRow_1.Produit_Ref_first = row10.Produit_Ref;
				}
				


 


	tos_count_tAggregateRow_1_AGGOUT++;

/**
 * [tAggregateRow_1_AGGOUT main ] stop
 */
	
	/**
	 * [tAggregateRow_1_AGGOUT process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	

 



/**
 * [tAggregateRow_1_AGGOUT process_data_begin ] stop
 */
	
	/**
	 * [tAggregateRow_1_AGGOUT process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	

 



/**
 * [tAggregateRow_1_AGGOUT process_data_end ] stop
 */



	
	/**
	 * [tSortRow_1_SortIn process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	

 



/**
 * [tSortRow_1_SortIn process_data_end ] stop
 */
	
	/**
	 * [tSortRow_1_SortIn end ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	


}

globalMap.put("tSortRow_1_SortIn_NB_LINE",nb_line_tSortRow_1_SortIn);

 

ok_Hash.put("tSortRow_1_SortIn", true);
end_Hash.put("tSortRow_1_SortIn", System.currentTimeMillis());




/**
 * [tSortRow_1_SortIn end ] stop
 */

	
	/**
	 * [tAggregateRow_1_AGGOUT end ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row10");
			  	}
			  	
 

ok_Hash.put("tAggregateRow_1_AGGOUT", true);
end_Hash.put("tAggregateRow_1_AGGOUT", System.currentTimeMillis());




/**
 * [tAggregateRow_1_AGGOUT end ] stop
 */




	
	/**
	 * [tDBOutput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBOutput_1", false);
		start_Hash.put("tDBOutput_1", System.currentTimeMillis());
		
	
	currentComponent="tDBOutput_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out_finaall");
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

 
	final String decryptedPassword_tDBOutput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:yCegMAyjj3Rj94JLiTalsQZhOQTxaFEjWlRjFWwHKpi7IN0F4yM=");

    String dbPwd_tDBOutput_1 = decryptedPassword_tDBOutput_1;	
    conn_tDBOutput_1 = java.sql.DriverManager.getConnection(url_tDBOutput_1,dbUser_tDBOutput_1,dbPwd_tDBOutput_1);
	
		resourceMap.put("conn_tDBOutput_1", conn_tDBOutput_1);
	
        conn_tDBOutput_1.setAutoCommit(false);
        int commitEvery_tDBOutput_1 = 10000;
        int commitCounter_tDBOutput_1 = 0;

   int batchSize_tDBOutput_1 = 10000;
   int batchSizeCounter_tDBOutput_1=0;

if(dbschema_tDBOutput_1 == null || dbschema_tDBOutput_1.trim().length() == 0) {
    tableName_tDBOutput_1 = "Dictionnaire_Categorie";
} else {
    tableName_tDBOutput_1 = dbschema_tDBOutput_1 + "].[" + "Dictionnaire_Categorie";
}
	int count_tDBOutput_1=0;

        String insert_tDBOutput_1 = "INSERT INTO [" + tableName_tDBOutput_1 + "] ([Produit],[Categorie]) VALUES (?,?)";
        java.sql.PreparedStatement pstmt_tDBOutput_1 = conn_tDBOutput_1.prepareStatement(insert_tDBOutput_1);
        resourceMap.put("pstmt_tDBOutput_1", pstmt_tDBOutput_1);


 



/**
 * [tDBOutput_1 begin ] stop
 */



	
	/**
	 * [tMap_7 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_7", false);
		start_Hash.put("tMap_7", System.currentTimeMillis());
		
	
	currentComponent="tMap_7";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out_finall");
					}
				
		int tos_count_tMap_7 = 0;
		




// ###############################
// # Lookup's keys initialization
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct> tHash_Lookup_row13 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct>) 
					globalMap.get( "tHash_Lookup_row13" ))
					;					
					
	

row13Struct row13HashKey = new row13Struct();
row13Struct row13Default = new row13Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_7__Struct  {
}
Var__tMap_7__Struct Var__tMap_7 = new Var__tMap_7__Struct();
// ###############################

// ###############################
// # Outputs initialization
out_finaallStruct out_finaall_tmp = new out_finaallStruct();
// ###############################

        
        



        









 



/**
 * [tMap_7 begin ] stop
 */



	
	/**
	 * [tMap_6 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_6", false);
		start_Hash.put("tMap_6", System.currentTimeMillis());
		
	
	currentComponent="tMap_6";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row11");
					}
				
		int tos_count_tMap_6 = 0;
		




// ###############################
// # Lookup's keys initialization
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct> tHash_Lookup_row12 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct>) 
					globalMap.get( "tHash_Lookup_row12" ))
					;					
					
	

row12Struct row12HashKey = new row12Struct();
row12Struct row12Default = new row12Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_6__Struct  {
}
Var__tMap_6__Struct Var__tMap_6 = new Var__tMap_6__Struct();
// ###############################

// ###############################
// # Outputs initialization
out_finallStruct out_finall_tmp = new out_finallStruct();
// ###############################

        
        



        









 



/**
 * [tMap_6 begin ] stop
 */



	
	/**
	 * [tAggregateRow_1_AGGIN begin ] start
	 */

	

	
		
		ok_Hash.put("tAggregateRow_1_AGGIN", false);
		start_Hash.put("tAggregateRow_1_AGGIN", System.currentTimeMillis());
		
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	
		int tos_count_tAggregateRow_1_AGGIN = 0;
		

java.util.Collection<AggOperationStruct_tAggregateRow_1> values_tAggregateRow_1 = hash_tAggregateRow_1.values();

globalMap.put("tAggregateRow_1_NB_LINE", values_tAggregateRow_1.size());

for(AggOperationStruct_tAggregateRow_1 aggregated_row_tAggregateRow_1 : values_tAggregateRow_1) { // G_AggR_600



 



/**
 * [tAggregateRow_1_AGGIN begin ] stop
 */
	
	/**
	 * [tAggregateRow_1_AGGIN main ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	

            				    row11.Produit = aggregated_row_tAggregateRow_1.Produit;
            				    
                                row11.Produit_Ref = aggregated_row_tAggregateRow_1.Produit_Ref_first;

 


	tos_count_tAggregateRow_1_AGGIN++;

/**
 * [tAggregateRow_1_AGGIN main ] stop
 */
	
	/**
	 * [tAggregateRow_1_AGGIN process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	

 



/**
 * [tAggregateRow_1_AGGIN process_data_begin ] stop
 */

	
	/**
	 * [tMap_6 main ] start
	 */

	

	
	
	currentComponent="tMap_6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row11"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_6 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_6 = false;
		  boolean mainRowRejected_tMap_6 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row12" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow12 = false;
       		  	    	
       		  	    	
 							row12Struct row12ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_6) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_6 = false;
								
                        		    		    row12HashKey.Produit = row11.Produit_Ref ;
                        		    		

								
		                        	row12HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row12.lookup( row12HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row12.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_6 = true;
	  								
						
									
  									  		
 								
								  
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
        
Var__tMap_6__Struct Var = Var__tMap_6;// ###############################
        // ###############################
        // # Output tables

out_finall = null;

if(!rejectedInnerJoin_tMap_6 ) {

// # Output table : 'out_finall'
out_finall_tmp.Produit = row11.Produit ;
out_finall_tmp.Categorie = row12.Categorie ;
out_finall = out_finall_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out_finall"
if(out_finall != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_finall"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row13" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow13 = false;
       		  	    	
       		  	    	
 							row13Struct row13ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row13HashKey.Produit = out_finall.Produit ;
                        		    		

								
		                        	row13HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row13.lookup( row13HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row13.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row13 != null && tHash_Lookup_row13.getCount(row13HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row13' and it contains more one result from keys :  row13.Produit = '" + row13HashKey.Produit + "'");
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
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out_finaall = null;

if(!rejectedInnerJoin_tMap_7 ) {
} // closing inner join bracket (1)
// ###### START REJECTS ##### 

// # Output reject table : 'out_finaall'
// # Filter conditions 
if( rejectedInnerJoin_tMap_7 ) {
out_finaall_tmp.Produit = out_finall.Produit ;
out_finaall_tmp.Categorie = out_finall.Categorie ;
out_finaall = out_finaall_tmp;
} // closing filter/reject
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
// Start of branch "out_finaall"
if(out_finaall != null) { 



	
	/**
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out_finaall"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(out_finaall.Produit == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.VARCHAR);
} else {String driver = null;
driver = "JTDS";
if ("MSSQL_PROP".equals(driver)) {
pstmt_tDBOutput_1.setNString(1, out_finaall.Produit);
} else {
pstmt_tDBOutput_1.setString(1, out_finaall.Produit);
}
}

                    if(out_finaall.Categorie == null) {
pstmt_tDBOutput_1.setNull(2, java.sql.Types.VARCHAR);
} else {String driver = null;
driver = "JTDS";
if ("MSSQL_PROP".equals(driver)) {
pstmt_tDBOutput_1.setNString(2, out_finaall.Categorie);
} else {
pstmt_tDBOutput_1.setString(2, out_finaall.Categorie);
}
}


        		pstmt_tDBOutput_1.addBatch();
        		nb_line_tDBOutput_1++;
        		
    		 
    		  batchSizeCounter_tDBOutput_1++;
    		
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

} // End of branch "out_finaall"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "out_finall"




	
	/**
	 * [tMap_6 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 process_data_end ] stop
 */



	
	/**
	 * [tAggregateRow_1_AGGIN process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	

 



/**
 * [tAggregateRow_1_AGGIN process_data_end ] stop
 */
	
	/**
	 * [tAggregateRow_1_AGGIN end ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	

} // G_AggR_600

 

ok_Hash.put("tAggregateRow_1_AGGIN", true);
end_Hash.put("tAggregateRow_1_AGGIN", System.currentTimeMillis());




/**
 * [tAggregateRow_1_AGGIN end ] stop
 */

	
	/**
	 * [tMap_6 end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row12 != null) {
						tHash_Lookup_row12.endGet();
					}
					globalMap.remove( "tHash_Lookup_row12" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row11");
			  	}
			  	
 

ok_Hash.put("tMap_6", true);
end_Hash.put("tMap_6", System.currentTimeMillis());




/**
 * [tMap_6 end ] stop
 */

	
	/**
	 * [tMap_7 end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row13 != null) {
						tHash_Lookup_row13.endGet();
					}
					globalMap.remove( "tHash_Lookup_row13" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out_finall");
			  	}
			  	
 

ok_Hash.put("tMap_7", true);
end_Hash.put("tMap_7", System.currentTimeMillis());




/**
 * [tMap_7 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out_finaall");
			  	}
			  	
 

ok_Hash.put("tDBOutput_1", true);
end_Hash.put("tDBOutput_1", System.currentTimeMillis());




/**
 * [tDBOutput_1 end ] stop
 */
























				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
					te.setVirtualComponentName(currentVirtualComponent);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
					     			//free memory for "tMap_7"
					     			globalMap.remove("tHash_Lookup_row13"); 
				     			
					     			//free memory for "tMap_6"
					     			globalMap.remove("tHash_Lookup_row12"); 
				     			
							//free memory for "tAggregateRow_1_AGGIN"
							globalMap.remove("tAggregateRow_1");
						
							//free memory for "tSortRow_1_SortIn"
							globalMap.remove("tSortRow_1");
						
					     			//free memory for "tMap_5"
					     			globalMap.remove("tHash_Lookup_row9"); 
				     			
				try{
					
	
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
	 * [tSortRow_1_SortOut finally ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortOut";

	

 



/**
 * [tSortRow_1_SortOut finally ] stop
 */

	
	/**
	 * [tSortRow_1_SortIn finally ] start
	 */

	

	
	
		currentVirtualComponent = "tSortRow_1";
	
	currentComponent="tSortRow_1_SortIn";

	

 



/**
 * [tSortRow_1_SortIn finally ] stop
 */

	
	/**
	 * [tAggregateRow_1_AGGOUT finally ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGOUT";

	

 



/**
 * [tAggregateRow_1_AGGOUT finally ] stop
 */

	
	/**
	 * [tAggregateRow_1_AGGIN finally ] start
	 */

	

	
	
		currentVirtualComponent = "tAggregateRow_1";
	
	currentComponent="tAggregateRow_1_AGGIN";

	

 



/**
 * [tAggregateRow_1_AGGIN finally ] stop
 */

	
	/**
	 * [tMap_6 finally ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 finally ] stop
 */

	
	/**
	 * [tMap_7 finally ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 finally ] stop
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
		

		globalMap.put("tDBInput_5_SUBPROCESS_STATE", 1);
	}
	


public static class row9Struct implements routines.system.IPersistableRow<row9Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];

	
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public String Categorie;

				public String getCategorie () {
					return this.Categorie;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
					this.Categorie = readString(dis);
					
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
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row9Struct other) {

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
			   		// source node:tDBInput_6 - inputs:(after_tDBInput_5) outputs:(row9,row9) | target node:tAdvancedHash_row9 - inputs:(row9) outputs:()
			   		// linked node: tMap_5 - inputs:(row8,row9) outputs:(out2)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row9 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.ALL_ROWS;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row9Struct> tHash_Lookup_row9 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row9Struct>getLookup(matchingModeEnum_row9);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row9", tHash_Lookup_row9);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row9 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:3/KjbA7G7yeviZk6qbBtPvu8rSgRb1mJKHD+SZ+MVDfzLuX3n7g=");
				
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

		    String dbquery_tDBInput_6 = "SELECT Dictionnaire_Categorie.Produit,\n		Dictionnaire_Categorie.Categorie\nFROM	Dictionnaire_Categorie";
		    

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
								row9.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(1);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row9.Produit = tmpContent_tDBInput_6;
                }
            } else {
                row9.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row9.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row9.Categorie = tmpContent_tDBInput_6;
                }
            } else {
                row9.Categorie = null;
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
	 * [tAdvancedHash_row9 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row9";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row9"
						
						);
					}
					


			   
			   

					row9Struct row9_HashRow = new row9Struct();
		   	   	   
				
				row9_HashRow.Produit = row9.Produit;
				
				row9_HashRow.Categorie = row9.Categorie;
				
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
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
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
		

		globalMap.put("tDBInput_6_SUBPROCESS_STATE", 1);
	}
	


public static class row12Struct implements routines.system.IPersistableComparableLookupRow<row12Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
			   		// source node:tDBInput_7 - inputs:(after_tDBInput_5) outputs:(row12,row12) | target node:tAdvancedHash_row12 - inputs:(row12) outputs:()
			   		// linked node: tMap_6 - inputs:(row11,row12) outputs:(out_finall)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row12 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row12Struct> tHash_Lookup_row12 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row12Struct>getLookup(matchingModeEnum_row12);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row12", tHash_Lookup_row12);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row12 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_7 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:8LcxMPzLFRbERzpeQRXZrqTR9AeG6XQED8OVnXGzkroxfyMTQPY=");
				
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

		    String dbquery_tDBInput_7 = "SELECT Dictionnaire_Categorie.Produit,\n		Dictionnaire_Categorie.Categorie\nFROM	Dictionnaire_Categorie";
		    

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
								row12.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(1);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row12.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row12.Produit = tmpContent_tDBInput_7;
                }
            } else {
                row12.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 2) {
								row12.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(2);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row12.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row12.Categorie = tmpContent_tDBInput_7;
                }
            } else {
                row12.Categorie = null;
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
	 * [tDBInput_7 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 finally ] stop
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
		

		globalMap.put("tDBInput_7_SUBPROCESS_STATE", 1);
	}
	


public static class row13Struct implements routines.system.IPersistableComparableLookupRow<row13Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[0];
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
		final row13Struct other = (row13Struct) obj;
		
						if (this.Produit == null) {
							if (other.Produit != null)
								return false;
						
						} else if (!this.Produit.equals(other.Produit))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row13Struct other) {

		other.Produit = this.Produit;
	            other.Categorie = this.Categorie;
	            
	}

	public void copyKeysDataTo(row13Struct other) {

		other.Produit = this.Produit;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_EL_Dictionnaire_Categorie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

        	try {

        		int length = 0;
		
					this.Produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_EL_Dictionnaire_Categorie) {

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
    public int compareTo(row13Struct other) {

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
public void tDBInput_8Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tDBInput_8_SUBPROCESS_STATE", 0);

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
			   		// source node:tDBInput_8 - inputs:(after_tDBInput_5) outputs:(row13,row13) | target node:tAdvancedHash_row13 - inputs:(row13) outputs:()
			   		// linked node: tMap_7 - inputs:(out_finall,row13) outputs:(out_finaall)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row13 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row13Struct> tHash_Lookup_row13 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row13Struct>getLookup(matchingModeEnum_row13);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row13", tHash_Lookup_row13);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row13 begin ] stop
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
				String dbUser_tDBInput_8 = "sa";
				
				 
	final String decryptedPassword_tDBInput_8 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:mP+TDv2r7rOeIyl6VCf/319PYZBtkH1xwXxRBYtA9/oPsBSB1Zo=");
				
				String dbPwd_tDBInput_8 = decryptedPassword_tDBInput_8;
				
		    String port_tDBInput_8 = "1433";
		    String dbname_tDBInput_8 = "DW_E-Commerce" ;
			String url_tDBInput_8 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_8)) {
		    	url_tDBInput_8 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_8)) {
				url_tDBInput_8 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_8 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_8 = "";
				
				conn_tDBInput_8 = java.sql.DriverManager.getConnection(url_tDBInput_8,dbUser_tDBInput_8,dbPwd_tDBInput_8);
		        
		    
			java.sql.Statement stmt_tDBInput_8 = conn_tDBInput_8.createStatement();

		    String dbquery_tDBInput_8 = "SELECT Dictionnaire_Categorie.Produit,\n		Dictionnaire_Categorie.Categorie\nFROM	Dictionnaire_Categorie";
		    

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
								row13.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(1);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row13.Produit = tmpContent_tDBInput_8;
                }
            } else {
                row13.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_8 < 2) {
								row13.Categorie = null;
							} else {
	                         		
           		tmpContent_tDBInput_8 = rs_tDBInput_8.getString(2);
            if(tmpContent_tDBInput_8 != null) {
            	if (talendToDBList_tDBInput_8 .contains(rsmd_tDBInput_8.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row13.Categorie = FormatterUtils.formatUnwithE(tmpContent_tDBInput_8);
            	} else {
                	row13.Categorie = tmpContent_tDBInput_8;
                }
            } else {
                row13.Categorie = null;
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
	 * [tAdvancedHash_row13 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row13";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row13"
						
						);
					}
					


			   
			   

					row13Struct row13_HashRow = new row13Struct();
		   	   	   
				
				row13_HashRow.Produit = row13.Produit;
				
				row13_HashRow.Categorie = row13.Categorie;
				
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
	 * [tDBInput_8 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_8";

	

 



/**
 * [tDBInput_8 finally ] stop
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
		

		globalMap.put("tDBInput_8_SUBPROCESS_STATE", 1);
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
        final EL_Dictionnaire_Categorie EL_Dictionnaire_CategorieClass = new EL_Dictionnaire_Categorie();

        int exitCode = EL_Dictionnaire_CategorieClass.runJobInTOS(args);

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
            java.io.InputStream inContext = EL_Dictionnaire_Categorie.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/el_dictionnaire_categorie_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = EL_Dictionnaire_Categorie.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : EL_Dictionnaire_Categorie");
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
 *     386074 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:46 CET
 ************************************************************************************************/