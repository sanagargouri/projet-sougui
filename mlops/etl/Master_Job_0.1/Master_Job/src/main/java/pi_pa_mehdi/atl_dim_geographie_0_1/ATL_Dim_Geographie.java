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


package pi_pa_mehdi.atl_dim_geographie_0_1;

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
 





@SuppressWarnings("unused")

/**
 * Job: ATL_Dim_Geographie Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ATL_Dim_Geographie implements TalendJob {

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
	private final String jobName = "ATL_Dim_Geographie";
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
				ATL_Dim_Geographie.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ATL_Dim_Geographie.this, new Object[] { e , currentComponent, globalMap});
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
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBSCD_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
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
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
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
			
			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class row12Struct implements routines.system.IPersistableRow<row12Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
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
	
							result = prime * result + (int) this.Geographie_PK;
						
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
		
						if (this.Geographie_PK != other.Geographie_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row12Struct other) {

		other.Geographie_PK = this.Geographie_PK;
	            other.Ville = this.Ville;
	            other.Code_postal = this.Code_postal;
	            other.Pays = this.Pays;
	            other.Code_pays = this.Code_pays;
	            
	}

	public void copyKeysDataTo(row12Struct other) {

		other.Geographie_PK = this.Geographie_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
			        this.Geographie_PK = dis.readInt();
					
					this.Ville = readString(dis);
					
					this.Code_postal = readString(dis);
					
					this.Pays = readString(dis);
					
					this.Code_pays = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
			        this.Geographie_PK = dis.readInt();
					
					this.Ville = readString(dis);
					
					this.Code_postal = readString(dis);
					
					this.Pays = readString(dis);
					
					this.Code_pays = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Geographie_PK);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.Code_postal,dos);
					
					// String
				
						writeString(this.Pays,dos);
					
					// String
				
						writeString(this.Code_pays,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Geographie_PK);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.Code_postal,dos);
					
					// String
				
						writeString(this.Pays,dos);
					
					// String
				
						writeString(this.Code_pays,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(row12Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Geographie_PK, other.Geographie_PK);
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

public static class out4Struct implements routines.system.IPersistableRow<out4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
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
	
							result = prime * result + (int) this.Geographie_PK;
						
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
		final out4Struct other = (out4Struct) obj;
		
						if (this.Geographie_PK != other.Geographie_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out4Struct other) {

		other.Geographie_PK = this.Geographie_PK;
	            other.Ville = this.Ville;
	            other.Code_postal = this.Code_postal;
	            other.Pays = this.Pays;
	            other.Code_pays = this.Code_pays;
	            
	}

	public void copyKeysDataTo(out4Struct other) {

		other.Geographie_PK = this.Geographie_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
			        this.Geographie_PK = dis.readInt();
					
					this.Ville = readString(dis);
					
					this.Code_postal = readString(dis);
					
					this.Pays = readString(dis);
					
					this.Code_pays = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
			        this.Geographie_PK = dis.readInt();
					
					this.Ville = readString(dis);
					
					this.Code_postal = readString(dis);
					
					this.Pays = readString(dis);
					
					this.Code_pays = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Geographie_PK);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.Code_postal,dos);
					
					// String
				
						writeString(this.Pays,dos);
					
					// String
				
						writeString(this.Code_pays,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Geographie_PK);
					
					// String
				
						writeString(this.Ville,dos);
					
					// String
				
						writeString(this.Code_postal,dos);
					
					// String
				
						writeString(this.Pays,dos);
					
					// String
				
						writeString(this.Code_pays,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(out4Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Geographie_PK, other.Geographie_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
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

public static class row4Struct implements routines.system.IPersistableRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
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

public static class out1Struct implements routines.system.IPersistableRow<out1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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

public static class after_tDBInput_1Struct implements routines.system.IPersistableRow<after_tDBInput_1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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
    public int compareTo(after_tDBInput_1Struct other) {

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

public static class out3Struct implements routines.system.IPersistableRow<out3Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out3Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Numero_Devis;

				public String getNumero_Devis () {
					return this.Numero_Devis;
				}
				
			    public String Matricule;

				public String getMatricule () {
					return this.Matricule;
				}
				
			    public String Date;

				public String getDate () {
					return this.Date;
				}
				
			    public String Nom_Client;

				public String getNom_Client () {
					return this.Nom_Client;
				}
				
			    public String Adresse_Client;

				public String getAdresse_Client () {
					return this.Adresse_Client;
				}
				
			    public String Reference;

				public String getReference () {
					return this.Reference;
				}
				
			    public String Description;

				public String getDescription () {
					return this.Description;
				}
				
			    public Integer Quantite;

				public Integer getQuantite () {
					return this.Quantite;
				}
				
			    public String PU_HT;

				public String getPU_HT () {
					return this.PU_HT;
				}
				
			    public Double Total_HT;

				public Double getTotal_HT () {
					return this.Total_HT;
				}
				
			    public String Fichier;

				public String getFichier () {
					return this.Fichier;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Numero_Devis = readString(dis);
					
					this.Matricule = readString(dis);
					
					this.Date = readString(dis);
					
					this.Nom_Client = readString(dis);
					
					this.Adresse_Client = readString(dis);
					
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
						this.Quantite = readInteger(dis);
					
					this.PU_HT = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Total_HT = null;
           				} else {
           			    	this.Total_HT = dis.readDouble();
           				}
					
					this.Fichier = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Numero_Devis = readString(dis);
					
					this.Matricule = readString(dis);
					
					this.Date = readString(dis);
					
					this.Nom_Client = readString(dis);
					
					this.Adresse_Client = readString(dis);
					
					this.Reference = readString(dis);
					
					this.Description = readString(dis);
					
						this.Quantite = readInteger(dis);
					
					this.PU_HT = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Total_HT = null;
           				} else {
           			    	this.Total_HT = dis.readDouble();
           				}
					
					this.Fichier = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Numero_Devis,dos);
					
					// String
				
						writeString(this.Matricule,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse_Client,dos);
					
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// Double
				
						if(this.Total_HT == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Total_HT);
		            	}
					
					// String
				
						writeString(this.Fichier,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Numero_Devis,dos);
					
					// String
				
						writeString(this.Matricule,dos);
					
					// String
				
						writeString(this.Date,dos);
					
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse_Client,dos);
					
					// String
				
						writeString(this.Reference,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// Integer
				
						writeInteger(this.Quantite,dos);
					
					// String
				
						writeString(this.PU_HT,dos);
					
					// Double
				
						if(this.Total_HT == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Total_HT);
		            	}
					
					// String
				
						writeString(this.Fichier,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Numero_Devis="+Numero_Devis);
		sb.append(",Matricule="+Matricule);
		sb.append(",Date="+Date);
		sb.append(",Nom_Client="+Nom_Client);
		sb.append(",Adresse_Client="+Adresse_Client);
		sb.append(",Reference="+Reference);
		sb.append(",Description="+Description);
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",PU_HT="+PU_HT);
		sb.append(",Total_HT="+String.valueOf(Total_HT));
		sb.append(",Fichier="+Fichier);
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

public static class out5Struct implements routines.system.IPersistableRow<out5Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out5Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public Integer ID_Client;

				public Integer getID_Client () {
					return this.ID_Client;
				}
				
			    public String Nom_Client;

				public String getNom_Client () {
					return this.Nom_Client;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String Type_Client;

				public String getType_Client () {
					return this.Type_Client;
				}
				
			    public java.util.Date Date_Inscription;

				public java.util.Date getDate_Inscription () {
					return this.Date_Inscription;
				}
				
			    public String Notes;

				public String getNotes () {
					return this.Notes;
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
						this.ID_Client = readInteger(dis);
					
					this.Nom_Client = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Type_Client = readString(dis);
					
					this.Date_Inscription = readDate(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
						this.ID_Client = readInteger(dis);
					
					this.Nom_Client = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Type_Client = readString(dis);
					
					this.Date_Inscription = readDate(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// Integer
				
						writeInteger(this.ID_Client,dos);
					
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Type_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_Inscription,dos);
					
					// String
				
						writeString(this.Notes,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// Integer
				
						writeInteger(this.ID_Client,dos);
					
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Type_Client,dos);
					
					// java.util.Date
				
						writeDate(this.Date_Inscription,dos);
					
					// String
				
						writeString(this.Notes,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("ID_Client="+String.valueOf(ID_Client));
		sb.append(",Nom_Client="+Nom_Client);
		sb.append(",Adresse="+Adresse);
		sb.append(",Type_Client="+Type_Client);
		sb.append(",Date_Inscription="+String.valueOf(Date_Inscription));
		sb.append(",Notes="+Notes);
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

public static class out6Struct implements routines.system.IPersistableRow<out6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out6Struct other) {

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

public static class row9Struct implements routines.system.IPersistableRow<row9Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Titre;

				public String getTitre () {
					return this.Titre;
				}
				
			    public String Description;

				public String getDescription () {
					return this.Description;
				}
				
			    public String Periodicite;

				public String getPeriodicite () {
					return this.Periodicite;
				}
				
			    public String Lieu;

				public String getLieu () {
					return this.Lieu;
				}
				
			    public String Date;

				public String getDate () {
					return this.Date;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Titre = readString(dis);
					
					this.Description = readString(dis);
					
					this.Periodicite = readString(dis);
					
					this.Lieu = readString(dis);
					
					this.Date = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Titre = readString(dis);
					
					this.Description = readString(dis);
					
					this.Periodicite = readString(dis);
					
					this.Lieu = readString(dis);
					
					this.Date = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Titre,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Periodicite,dos);
					
					// String
				
						writeString(this.Lieu,dos);
					
					// String
				
						writeString(this.Date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Titre,dos);
					
					// String
				
						writeString(this.Description,dos);
					
					// String
				
						writeString(this.Periodicite,dos);
					
					// String
				
						writeString(this.Lieu,dos);
					
					// String
				
						writeString(this.Date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Titre="+Titre);
		sb.append(",Description="+Description);
		sb.append(",Periodicite="+Periodicite);
		sb.append(",Lieu="+Lieu);
		sb.append(",Date="+Date);
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

public static class out7Struct implements routines.system.IPersistableRow<out7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out7Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Nom_Client;

				public String getNom_Client () {
					return this.Nom_Client;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String MF;

				public String getMF () {
					return this.MF;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Nom_Client = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.MF = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Nom_Client = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.MF = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.MF,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Nom_Client,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.MF,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Nom_Client="+Nom_Client);
		sb.append(",Adresse="+Adresse);
		sb.append(",MF="+MF);
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

public static class out8Struct implements routines.system.IPersistableRow<out8Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
			    public String Ville;

				public String getVille () {
					return this.Ville;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Ville = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Ville,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Ville="+Ville);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out8Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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


		tDBInput_7Process(globalMap);

		row1Struct row1 = new row1Struct();
out1Struct out1 = new out1Struct();

		row7Struct row7 = new row7Struct();
out3Struct out3 = new out3Struct();

		row8Struct row8 = new row8Struct();
out5Struct out5 = new out5Struct();

		row9Struct row9 = new row9Struct();
out6Struct out6 = new out6Struct();

		row10Struct row10 = new row10Struct();
out7Struct out7 = new out7Struct();

		row11Struct row11 = new row11Struct();
out8Struct out8 = new out8Struct();

			row4Struct row4 = new row4Struct();
row5Struct row5 = new row5Struct();
out4Struct out4 = new out4Struct();
row12Struct row12 = new row12Struct();







	
	/**
	 * [tDBSCD_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBSCD_1", false);
		start_Hash.put("tDBSCD_1", System.currentTimeMillis());
		
	
	currentComponent="tDBSCD_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row12");
					}
				
		int tos_count_tDBSCD_1 = 0;
		



        class SCDSK_tDBSCD_1 {
private int hashCode;
public boolean hashCodeDirty = true;
String Ville;
public boolean equals(Object obj) {
if (this == obj) return true;
if (obj == null) return false;
if (getClass() != obj.getClass()) return false;
final SCDSK_tDBSCD_1 other = (SCDSK_tDBSCD_1) obj;
if (this.Ville == null) {
if (other.Ville!= null)
return false;
} else if (!this.Ville.equals(other.Ville))
return false;

return true;
}
public int hashCode() {
if(hashCodeDirty) {
int prime = 31;hashCode = prime * hashCode + (Ville == null ? 0 : Ville.hashCode());
hashCodeDirty = false;
}
return hashCode;
}
}

    class SCDStruct_tDBSCD_1 {
private String Code_pays;
private String Code_postal;
private String Pays;
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

 
	final String decryptedPassword_tDBSCD_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:WaT+APYlWLd9fkaEukD4n5HrpXa1lHUWJlBXzs5au4PDZQxCWas=");

    String dbPwd_tDBSCD_1 = decryptedPassword_tDBSCD_1;	
    conn_tDBSCD_1 = java.sql.DriverManager.getConnection(url_tDBSCD_1,dbUser_tDBSCD_1,dbPwd_tDBSCD_1);
	

    if(dbschema_tDBSCD_1 == null || dbschema_tDBSCD_1.trim().length() == 0) {
        tableName_tDBSCD_1 = "Dim_Geographie";
    } else {
        tableName_tDBSCD_1 = dbschema_tDBSCD_1 + "].[" + "Dim_Geographie";
    }
	org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBSCD_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();	
    String tmpValue_tDBSCD_1 = null;    
        String search_tDBSCD_1 = "SELECT [Ville], [Code_pays], [Code_postal], [Pays] FROM [" + tableName_tDBSCD_1 + "]";
        java.sql.Statement statement_tDBSCD_1 = conn_tDBSCD_1.createStatement();
        java.sql.ResultSet resultSet_tDBSCD_1 = statement_tDBSCD_1.executeQuery(search_tDBSCD_1);
        java.util.Map<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1> cache_tDBSCD_1 = new java.util.HashMap<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1>();
        while(resultSet_tDBSCD_1.next()) {
            SCDSK_tDBSCD_1 sk_tDBSCD_1 = new SCDSK_tDBSCD_1();
            SCDStruct_tDBSCD_1 row_tDBSCD_1 = new SCDStruct_tDBSCD_1();
                    if(resultSet_tDBSCD_1.getObject(1) != null) {
                        sk_tDBSCD_1.Ville = resultSet_tDBSCD_1.getString(1);
                    }
                    if(resultSet_tDBSCD_1.getObject(2) != null) {
                        row_tDBSCD_1.Code_pays = resultSet_tDBSCD_1.getString(2);
                    }
                    if(resultSet_tDBSCD_1.getObject(3) != null) {
                        row_tDBSCD_1.Code_postal = resultSet_tDBSCD_1.getString(3);
                    }
                    if(resultSet_tDBSCD_1.getObject(4) != null) {
                        row_tDBSCD_1.Pays = resultSet_tDBSCD_1.getString(4);
                    }
            cache_tDBSCD_1.put(sk_tDBSCD_1, row_tDBSCD_1);
        }
        resultSet_tDBSCD_1.close();
        statement_tDBSCD_1.close();
    String insertionSQL_tDBSCD_1 = "INSERT INTO [" + tableName_tDBSCD_1 + "]([Ville], [Code_pays], [Code_postal], [Pays]) VALUES(?, ?, ?, ?)";
    java.sql.PreparedStatement insertionStatement_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(insertionSQL_tDBSCD_1);
        String updateSQLForType1_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [Code_pays] = ?, [Code_postal] = ?, [Pays] = ? WHERE [Ville] = ?";
        java.sql.PreparedStatement updateForType1_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType1_tDBSCD_1);        
    
        SCDSK_tDBSCD_1 lookUpKey_tDBSCD_1 = null;        
    SCDStruct_tDBSCD_1 lookUpValue_tDBSCD_1 = null;

 



/**
 * [tDBSCD_1 begin ] stop
 */



	
	/**
	 * [tUniqRow_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_3", false);
		start_Hash.put("tUniqRow_3", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out4");
					}
				
		int tos_count_tUniqRow_3 = 0;
		

	
		class KeyStruct_tUniqRow_3 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Ville;        
	        
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
				final KeyStruct_tUniqRow_3 other = (KeyStruct_tUniqRow_3) obj;
				
									if (this.Ville == null) {
										if (other.Ville != null) 
											return false;
								
									} else if (!this.Ville.equals(other.Ville))
								 
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
	 * [tMap_7 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_7", false);
		start_Hash.put("tMap_7", System.currentTimeMillis());
		
	
	currentComponent="tMap_7";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
					}
				
		int tos_count_tMap_7 = 0;
		




// ###############################
// # Lookup's keys initialization
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct> tHash_Lookup_row6 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct>) 
					globalMap.get( "tHash_Lookup_row6" ))
					;					
					
	

row6Struct row6HashKey = new row6Struct();
row6Struct row6Default = new row6Struct();
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_7__Struct  {
}
Var__tMap_7__Struct Var__tMap_7 = new Var__tMap_7__Struct();
// ###############################

// ###############################
// # Outputs initialization
out4Struct out4_tmp = new out4Struct();
// ###############################

        
        



        









 



/**
 * [tMap_7 begin ] stop
 */



	
	/**
	 * [tUniqRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_1", false);
		start_Hash.put("tUniqRow_1", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row4");
					}
				
		int tos_count_tUniqRow_1 = 0;
		

	
		class KeyStruct_tUniqRow_1 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Ville;        
	        
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
				final KeyStruct_tUniqRow_1 other = (KeyStruct_tUniqRow_1) obj;
				
									if (this.Ville == null) {
										if (other.Ville != null) 
											return false;
								
									} else if (!this.Ville.equals(other.Ville))
								 
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
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out7","out5","out8","out1","out6","out3");
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
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:1Bj3btslAAujl6Ugd+PjKrkCVD+ljTPTt3IX5rXMsbDmflARhO8=");
				
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
out1_tmp.Ville = row1.Adresse_1___2__Livraison==null?row1.Adresse_1___2__Facturation==null?"Tunis":row1.Adresse_1___2__Facturation:row1.Adresse_1___2__Livraison ;
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
			row4 = new row4Struct();
								
			row4.Ville = out1.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
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
	 * [tMap_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_2", false);
		start_Hash.put("tMap_2", System.currentTimeMillis());
		
	
	currentComponent="tMap_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row7");
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
out3Struct out3_tmp = new out3Struct();
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
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:ww4MxFU1usyYZJlzFGhnJIAKxTp2pEHb+aPThHEI4sfwBJAEn4M=");
				
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

		    String dbquery_tDBInput_2 = "SELECT Devis.Numero_Devis,\n		Devis.Matricule,\n		Devis.Date,\n		Devis.Nom_Client,\n		Devis.Adresse_Client,\n		Devis.Referen"
+"ce,\n		Devis.Description,\n		Devis.Quantite,\n		Devis.PU_HT,\n		Devis.Total_HT,\n		Devis.Fichier\nFROM	Devis";
		    

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
								row7.Numero_Devis = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Numero_Devis = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Numero_Devis = tmpContent_tDBInput_2;
                }
            } else {
                row7.Numero_Devis = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row7.Matricule = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Matricule = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Matricule = tmpContent_tDBInput_2;
                }
            } else {
                row7.Matricule = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row7.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(3);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Date = tmpContent_tDBInput_2;
                }
            } else {
                row7.Date = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row7.Nom_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(4);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Nom_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Nom_Client = tmpContent_tDBInput_2;
                }
            } else {
                row7.Nom_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row7.Adresse_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Adresse_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Adresse_Client = tmpContent_tDBInput_2;
                }
            } else {
                row7.Adresse_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row7.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(6);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Reference = tmpContent_tDBInput_2;
                }
            } else {
                row7.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row7.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Description = tmpContent_tDBInput_2;
                }
            } else {
                row7.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row7.Quantite = null;
							} else {
		                          
            row7.Quantite = rs_tDBInput_2.getInt(8);
            if(rs_tDBInput_2.wasNull()){
                    row7.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row7.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.PU_HT = tmpContent_tDBInput_2;
                }
            } else {
                row7.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row7.Total_HT = null;
							} else {
	                         		
            row7.Total_HT = rs_tDBInput_2.getDouble(10);
            if(rs_tDBInput_2.wasNull()){
                    row7.Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 11) {
								row7.Fichier = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(11);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Fichier = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row7.Fichier = tmpContent_tDBInput_2;
                }
            } else {
                row7.Fichier = null;
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
						
							,"row7"
						
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

out3 = null;


// # Output table : 'out3'
out3_tmp.Ville = row7.Adresse_Client ;
out3 = out3_tmp;
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
// Start of branch "out3"
if(out3 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out3"
									
							);
						}
						
//////////
 

// for output
			row4 = new row4Struct();
								
			row4.Ville = out3.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out3"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row7");
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row8");
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
out5Struct out5_tmp = new out5Struct();
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
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:fR2rmKK2NymI1V1sMbuN7ZsbMW/OhtPsOKL15UVRqggzVmSlgF0=");
				
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

		    String dbquery_tDBInput_3 = "SELECT ERP_Clients.ID_Client,\n		ERP_Clients.Nom_Client,\n		ERP_Clients.Adresse,\n		ERP_Clients.Type_Client,\n		ERP_Clients"
+".Date_Inscription,\n		ERP_Clients.Notes\nFROM	ERP_Clients";
		    

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
								row8.ID_Client = null;
							} else {
		                          
            row8.ID_Client = rs_tDBInput_3.getInt(1);
            if(rs_tDBInput_3.wasNull()){
                    row8.ID_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row8.Nom_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Nom_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row8.Nom_Client = tmpContent_tDBInput_3;
                }
            } else {
                row8.Nom_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row8.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(3);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row8.Adresse = tmpContent_tDBInput_3;
                }
            } else {
                row8.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row8.Type_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Type_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row8.Type_Client = tmpContent_tDBInput_3;
                }
            } else {
                row8.Type_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 5) {
								row8.Date_Inscription = null;
							} else {
										
			row8.Date_Inscription = mssqlGTU_tDBInput_3.getDate(rsmd_tDBInput_3, rs_tDBInput_3, 5);
			
		                    }
							if(colQtyInRs_tDBInput_3 < 6) {
								row8.Notes = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(6);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Notes = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row8.Notes = tmpContent_tDBInput_3;
                }
            } else {
                row8.Notes = null;
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
						
							,"row8"
						
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

out5 = null;


// # Output table : 'out5'
out5_tmp.Ville = row8.Adresse ;
out5 = out5_tmp;
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
// Start of branch "out5"
if(out5 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out5"
									
							);
						}
						
//////////
 

// for output
			row4 = new row4Struct();
								
			row4.Ville = out5.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out5"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row8");
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row9");
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
out6Struct out6_tmp = new out6Struct();
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
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:e7x53o8Rc9HFlMJne2dckeEsj/FIOckpmH2Ur+Uqq7K5s/31u5g=");
				
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

		    String dbquery_tDBInput_4 = "SELECT EventEye.Titre,\n		EventEye.Description,\n		EventEye.Periodicite,\n		EventEye.Lieu,\n		EventEye.Date\nFROM	EventEye";
		    

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
								row9.Titre = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(1);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Titre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row9.Titre = tmpContent_tDBInput_4;
                }
            } else {
                row9.Titre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row9.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row9.Description = tmpContent_tDBInput_4;
                }
            } else {
                row9.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 3) {
								row9.Periodicite = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(3);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Periodicite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row9.Periodicite = tmpContent_tDBInput_4;
                }
            } else {
                row9.Periodicite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 4) {
								row9.Lieu = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(4);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Lieu = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row9.Lieu = tmpContent_tDBInput_4;
                }
            } else {
                row9.Lieu = null;
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 5) {
								row9.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(5);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row9.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row9.Date = tmpContent_tDBInput_4;
                }
            } else {
                row9.Date = null;
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
						
							,"row9"
						
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

out6 = null;


// # Output table : 'out6'
out6_tmp.Ville = row9.Lieu ;
out6 = out6_tmp;
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
// Start of branch "out6"
if(out6 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out6"
									
							);
						}
						
//////////
 

// for output
			row4 = new row4Struct();
								
			row4.Ville = out6.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out6"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row9");
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row10");
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
out7Struct out7_tmp = new out7Struct();
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
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:Cibvy8D+nkDQbnkdyj3QPtGZOxeHomW6h/u6JIqP5kQQ4WQsBN4=");
				
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

		    String dbquery_tDBInput_5 = "SELECT Factures_Sougui_Adresses.Nom_Client,\n		Factures_Sougui_Adresses.Adresse,\n		Factures_Sougui_Adresses.MF\nFROM	Fact"
+"ures_Sougui_Adresses";
		    

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
								row10.Nom_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(1);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row10.Nom_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row10.Nom_Client = tmpContent_tDBInput_5;
                }
            } else {
                row10.Nom_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 2) {
								row10.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(2);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row10.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row10.Adresse = tmpContent_tDBInput_5;
                }
            } else {
                row10.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 3) {
								row10.MF = null;
							} else {
	                         		
           		tmpContent_tDBInput_5 = rs_tDBInput_5.getString(3);
            if(tmpContent_tDBInput_5 != null) {
            	if (talendToDBList_tDBInput_5 .contains(rsmd_tDBInput_5.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row10.MF = FormatterUtils.formatUnwithE(tmpContent_tDBInput_5);
            	} else {
                	row10.MF = tmpContent_tDBInput_5;
                }
            } else {
                row10.MF = null;
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
						
							,"row10"
						
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

out7 = null;


// # Output table : 'out7'
out7_tmp.Ville = row10.Adresse ;
out7 = out7_tmp;
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
// Start of branch "out7"
if(out7 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out7"
									
							);
						}
						
//////////
 

// for output
			row4 = new row4Struct();
								
			row4.Ville = out7.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out7"




	
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row10");
			  	}
			  	
 

ok_Hash.put("tMap_5", true);
end_Hash.put("tMap_5", System.currentTimeMillis());




/**
 * [tMap_5 end ] stop
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
// ###############################        

// ###############################
// # Vars initialization
class  Var__tMap_6__Struct  {
}
Var__tMap_6__Struct Var__tMap_6 = new Var__tMap_6__Struct();
// ###############################

// ###############################
// # Outputs initialization
out8Struct out8_tmp = new out8Struct();
// ###############################

        
        



        









 



/**
 * [tMap_6 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:VfZD6SjVsUuG88vPRFQgaGFCnH8yHm5oWURacev1uq6yXUbaXiU=");
				
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

		    String dbquery_tDBInput_6 = "SELECT Factures_Ventes.Reference,\n		Factures_Ventes.Description,\n		Factures_Ventes.PU_HT,\n		Factures_Ventes.Quantite,\n	"
+"	Factures_Ventes.Prix_Total_HT,\n		Factures_Ventes.Numero_Facture,\n		Factures_Ventes.id_Client,\n		Factures_Ventes.Date,\n	"
+"	Factures_Ventes.Client_Nom,\n		Factures_Ventes.Matricule_Fiscal,\n		Factures_Ventes.Adresse,\n		Factures_Ventes.TVA\nFROM	F"
+"actures_Ventes";
		    

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
								row11.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(1);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Reference = tmpContent_tDBInput_6;
                }
            } else {
                row11.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row11.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Description = tmpContent_tDBInput_6;
                }
            } else {
                row11.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 3) {
								row11.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(3);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.PU_HT = tmpContent_tDBInput_6;
                }
            } else {
                row11.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 4) {
								row11.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(4);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Quantite = tmpContent_tDBInput_6;
                }
            } else {
                row11.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 5) {
								row11.Prix_Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(5);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Prix_Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Prix_Total_HT = tmpContent_tDBInput_6;
                }
            } else {
                row11.Prix_Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 6) {
								row11.Numero_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(6);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Numero_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Numero_Facture = tmpContent_tDBInput_6;
                }
            } else {
                row11.Numero_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 7) {
								row11.id_Client = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(7);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.id_Client = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.id_Client = tmpContent_tDBInput_6;
                }
            } else {
                row11.id_Client = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 8) {
								row11.Date = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(8);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Date = tmpContent_tDBInput_6;
                }
            } else {
                row11.Date = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 9) {
								row11.Client_Nom = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(9);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Client_Nom = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Client_Nom = tmpContent_tDBInput_6;
                }
            } else {
                row11.Client_Nom = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 10) {
								row11.Matricule_Fiscal = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(10);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Matricule_Fiscal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Matricule_Fiscal = tmpContent_tDBInput_6;
                }
            } else {
                row11.Matricule_Fiscal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 11) {
								row11.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(11);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.Adresse = tmpContent_tDBInput_6;
                }
            } else {
                row11.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 12) {
								row11.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(12);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row11.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row11.TVA = tmpContent_tDBInput_6;
                }
            } else {
                row11.TVA = null;
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
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_6__Struct Var = Var__tMap_6;// ###############################
        // ###############################
        // # Output tables

out8 = null;


// # Output table : 'out8'
out8_tmp.Ville = row11.Adresse ;
out8 = out8_tmp;
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
// Start of branch "out8"
if(out8 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"out8"
									
							);
						}
						
//////////
 

// for output
			row4 = new row4Struct();
								
			row4.Ville = out8.Ville;			

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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					
row5 = null;			
if(row4.Ville == null){
	finder_tUniqRow_1.Ville = null;
}else{
	finder_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row4.Ville == null){
	new_tUniqRow_1.Ville = null;
}else{
	new_tUniqRow_1.Ville = row4.Ville.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row5 == null){ 
	
	row5 = new row5Struct();
}row5.Ville = row4.Ville;					
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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_7 main ] start
	 */

	

	
	
	currentComponent="tMap_7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_7 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_7 = false;
		  boolean mainRowRejected_tMap_7 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_7) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_7 = false;
								
                        		    		    row6HashKey.Input_Geo = row5.Ville ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_7 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Input_Geo = '" + row6HashKey.Input_Geo + "'");
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
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_7__Struct Var = Var__tMap_7;// ###############################
        // ###############################
        // # Output tables

out4 = null;

if(!rejectedInnerJoin_tMap_7 ) {

// # Output table : 'out4'
out4_tmp.Geographie_PK = 0;
out4_tmp.Ville = row6.Ville != null ?
row6.Ville
    .replaceAll("(?i)Governorate", "")
    .replaceAll("[éèêë]", "e")
    .replaceAll("[àâä]", "a")
    .replaceAll("[îï]", "i")
    .replaceAll("[ôö]", "o")
    .replaceAll("[ùûü]", "u")
    .replaceAll("[^a-zA-Z\\s]", "")
    .replaceAll("\\s+", " ")
    .trim()
: null ;
out4_tmp.Code_postal = null;
out4_tmp.Pays = row6.Pays ;
out4_tmp.Code_pays = "TN";
out4 = out4_tmp;
}  // closing inner join bracket (2)
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
// Start of branch "out4"
if(out4 != null) { 



	
	/**
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out4"
						
						);
					}
					
row12 = null;			
if(out4.Ville == null){
	finder_tUniqRow_3.Ville = null;
}else{
	finder_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(out4.Ville == null){
	new_tUniqRow_3.Ville = null;
}else{
	new_tUniqRow_3.Ville = out4.Ville.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row12 == null){ 
	
	row12 = new row12Struct();
}row12.Geographie_PK = out4.Geographie_PK;			row12.Ville = out4.Ville;			row12.Code_postal = out4.Code_postal;			row12.Pays = out4.Pays;			row12.Code_pays = out4.Code_pays;					
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
// Start of branch "row12"
if(row12 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row12"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Ville = row12.Ville;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(row12.Ville == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, row12.Ville);
}

                    if(row12.Code_pays == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, row12.Code_postal);
}

                    if(row12.Pays == null) {
insertionStatement_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(4, row12.Pays);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Code_pays == null && row12.Code_pays!= null) || (lookUpValue_tDBSCD_1.Code_pays != null && !lookUpValue_tDBSCD_1.Code_pays.equals(row12.Code_pays)) || (lookUpValue_tDBSCD_1.Code_postal == null && row12.Code_postal!= null) || (lookUpValue_tDBSCD_1.Code_postal != null && !lookUpValue_tDBSCD_1.Code_postal.equals(row12.Code_postal)) || (lookUpValue_tDBSCD_1.Pays == null && row12.Pays!= null) || (lookUpValue_tDBSCD_1.Pays != null && !lookUpValue_tDBSCD_1.Pays.equals(row12.Pays))) {
                    if(row12.Code_pays == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, row12.Code_pays);
}

                    if(row12.Code_postal == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, row12.Code_postal);
}

                    if(row12.Pays == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, row12.Pays);
}

                    if(row12.Ville == null) {
updateForType1_tDBSCD_1.setNull(4, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(4, row12.Ville);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Code_pays = row12.Code_pays;
                lookUpValue_tDBSCD_1.Code_postal = row12.Code_postal;
                lookUpValue_tDBSCD_1.Pays = row12.Pays;
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

} // End of branch "row12"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
 */

} // End of branch "out4"




	
	/**
	 * [tMap_7 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "out8"




	
	/**
	 * [tMap_6 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 process_data_end ] stop
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
	 * [tMap_6 end ] start
	 */

	

	
	
	currentComponent="tMap_6";

	


// ###############################
// # Lookup hashes releasing
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
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out7","out5","out8","out1","out6","out3");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
 */

	
	/**
	 * [tUniqRow_1 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

globalMap.put("tUniqRow_1_NB_UNIQUES",nb_uniques_tUniqRow_1);
globalMap.put("tUniqRow_1_NB_DUPLICATES",nb_duplicates_tUniqRow_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row4");
			  	}
			  	
 

ok_Hash.put("tUniqRow_1", true);
end_Hash.put("tUniqRow_1", System.currentTimeMillis());




/**
 * [tUniqRow_1 end ] stop
 */

	
	/**
	 * [tMap_7 end ] start
	 */

	

	
	
	currentComponent="tMap_7";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row6 != null) {
						tHash_Lookup_row6.endGet();
					}
					globalMap.remove( "tHash_Lookup_row6" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
			  	}
			  	
 

ok_Hash.put("tMap_7", true);
end_Hash.put("tMap_7", System.currentTimeMillis());




/**
 * [tMap_7 end ] stop
 */

	
	/**
	 * [tUniqRow_3 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

globalMap.put("tUniqRow_3_NB_UNIQUES",nb_uniques_tUniqRow_3);
globalMap.put("tUniqRow_3_NB_DUPLICATES",nb_duplicates_tUniqRow_3);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out4");
			  	}
			  	
 

ok_Hash.put("tUniqRow_3", true);
end_Hash.put("tUniqRow_3", System.currentTimeMillis());




/**
 * [tUniqRow_3 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row12");
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
				
					     			//free memory for "tMap_7"
					     			globalMap.remove("tHash_Lookup_row6"); 
				     			
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
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
 */

	
	/**
	 * [tMap_6 finally ] start
	 */

	

	
	
	currentComponent="tMap_6";

	

 



/**
 * [tMap_6 finally ] stop
 */



	
	/**
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tUniqRow_1 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 finally ] stop
 */

	
	/**
	 * [tMap_7 finally ] start
	 */

	

	
	
	currentComponent="tMap_7";

	

 



/**
 * [tMap_7 finally ] stop
 */

	
	/**
	 * [tUniqRow_3 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 finally ] stop
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
	


public static class row6Struct implements routines.system.IPersistableComparableLookupRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[0];
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
		final row6Struct other = (row6Struct) obj;
		
						if (this.Input_Geo == null) {
							if (other.Input_Geo != null)
								return false;
						
						} else if (!this.Input_Geo.equals(other.Input_Geo))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row6Struct other) {

		other.Input_Geo = this.Input_Geo;
	            other.Ville = this.Ville;
	            other.Pays = this.Pays;
	            other.Code_Postale = this.Code_Postale;
	            
	}

	public void copyKeysDataTo(row6Struct other) {

		other.Input_Geo = this.Input_Geo;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ATL_Dim_Geographie, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

        	try {

        		int length = 0;
		
					this.Input_Geo = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ATL_Dim_Geographie) {

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
    public int compareTo(row6Struct other) {

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
			   		// source node:tDBInput_7 - inputs:(after_tDBInput_1) outputs:(row6,row6) | target node:tAdvancedHash_row6 - inputs:(row6) outputs:()
			   		// linked node: tMap_7 - inputs:(row5,row6) outputs:(out4)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row6 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct> tHash_Lookup_row6 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row6Struct>getLookup(matchingModeEnum_row6);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row6", tHash_Lookup_row6);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row6 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_7 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:uyCA9oVIH9O18Ev8IR30R/GPVxZylVHsrTiwU1ykyUzzL3SWSAg=");
				
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

		    String dbquery_tDBInput_7 = "SELECT Dictionnaire_Geographie.Input_Geo,\n		Dictionnaire_Geographie.Ville,\n		Dictionnaire_Geographie.Pays,\n		Dictionnai"
+"re_Geographie.Code_Postale\nFROM	Dictionnaire_Geographie";
		    

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
								row6.Input_Geo = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(1);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Input_Geo = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row6.Input_Geo = tmpContent_tDBInput_7;
                }
            } else {
                row6.Input_Geo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 2) {
								row6.Ville = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(2);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Ville = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row6.Ville = tmpContent_tDBInput_7;
                }
            } else {
                row6.Ville = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 3) {
								row6.Pays = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(3);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Pays = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row6.Pays = tmpContent_tDBInput_7;
                }
            } else {
                row6.Pays = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 4) {
								row6.Code_Postale = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(4);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Code_Postale = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row6.Code_Postale = tmpContent_tDBInput_7;
                }
            } else {
                row6.Code_Postale = null;
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
	 * [tAdvancedHash_row6 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					


			   
			   

					row6Struct row6_HashRow = new row6Struct();
		   	   	   
				
				row6_HashRow.Input_Geo = row6.Input_Geo;
				
				row6_HashRow.Ville = row6.Ville;
				
				row6_HashRow.Pays = row6.Pays;
				
				row6_HashRow.Code_Postale = row6.Code_Postale;
				
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
	 * [tDBInput_7 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 finally ] stop
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
		

		globalMap.put("tDBInput_7_SUBPROCESS_STATE", 1);
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
        final ATL_Dim_Geographie ATL_Dim_GeographieClass = new ATL_Dim_Geographie();

        int exitCode = ATL_Dim_GeographieClass.runJobInTOS(args);

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
            java.io.InputStream inContext = ATL_Dim_Geographie.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/atl_dim_geographie_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ATL_Dim_Geographie.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ATL_Dim_Geographie");
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
 *     374233 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:44 CET
 ************************************************************************************************/