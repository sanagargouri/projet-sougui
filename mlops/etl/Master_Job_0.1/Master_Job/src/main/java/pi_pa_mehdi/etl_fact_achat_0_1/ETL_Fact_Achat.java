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


package pi_pa_mehdi.etl_fact_achat_0_1;

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
 * Job: ETL_Fact_Achat Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ETL_Fact_Achat implements TalendJob {

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
	private final String jobName = "ETL_Fact_Achat";
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
				ETL_Fact_Achat.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ETL_Fact_Achat.this, new Object[] { e , currentComponent, globalMap});
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

			public void tDBInput_7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFilterRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tLogRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row5_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row6_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row7_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tAdvancedHash_row2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_7_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_7_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






public static class ffStruct implements routines.system.IPersistableRow<ffStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Fournisseur_FK;

				public Integer getFournisseur_FK () {
					return this.Fournisseur_FK;
				}
				
			    public Integer Tye_Paiement_FK;

				public Integer getTye_Paiement_FK () {
					return this.Tye_Paiement_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		final ffStruct other = (ffStruct) obj;
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(ffStruct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date_FK = this.Date_FK;
	            other.Fournisseur_FK = this.Fournisseur_FK;
	            other.Tye_Paiement_FK = this.Tye_Paiement_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            
	}

	public void copyKeysDataTo(ffStruct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Fournisseur_FK="+String.valueOf(Fournisseur_FK));
		sb.append(",Tye_Paiement_FK="+String.valueOf(Tye_Paiement_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(ffStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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

public static class out1Struct implements routines.system.IPersistableRow<out1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Fournisseur_FK;

				public Integer getFournisseur_FK () {
					return this.Fournisseur_FK;
				}
				
			    public Integer Tye_Paiement_FK;

				public Integer getTye_Paiement_FK () {
					return this.Tye_Paiement_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Fournisseur_FK="+String.valueOf(Fournisseur_FK));
		sb.append(",Tye_Paiement_FK="+String.valueOf(Tye_Paiement_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
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

public static class reject1Struct implements routines.system.IPersistableRow<reject1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public String Fournisseur_FK;

				public String getFournisseur_FK () {
					return this.Fournisseur_FK;
				}
				
			    public String Tye_Paiement_FK;

				public String getTye_Paiement_FK () {
					return this.Tye_Paiement_FK;
				}
				
			    public String Produit_FK;

				public String getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		final reject1Struct other = (reject1Struct) obj;
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(reject1Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date_FK = this.Date_FK;
	            other.Fournisseur_FK = this.Fournisseur_FK;
	            other.Tye_Paiement_FK = this.Tye_Paiement_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            
	}

	public void copyKeysDataTo(reject1Struct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
					this.Fournisseur_FK = readString(dis);
					
					this.Tye_Paiement_FK = readString(dis);
					
					this.Produit_FK = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
						this.Date_FK = readInteger(dis);
					
					this.Fournisseur_FK = readString(dis);
					
					this.Tye_Paiement_FK = readString(dis);
					
					this.Produit_FK = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// String
				
						writeString(this.Fournisseur_FK,dos);
					
					// String
				
						writeString(this.Tye_Paiement_FK,dos);
					
					// String
				
						writeString(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// String
				
						writeString(this.Fournisseur_FK,dos);
					
					// String
				
						writeString(this.Tye_Paiement_FK,dos);
					
					// String
				
						writeString(this.Produit_FK,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Fournisseur_FK="+Fournisseur_FK);
		sb.append(",Tye_Paiement_FK="+Tye_Paiement_FK);
		sb.append(",Produit_FK="+Produit_FK);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(reject1Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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

public static class rejected_dateStruct implements routines.system.IPersistableRow<rejected_dateStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
			    public java.util.Date date;

				public java.util.Date getDate () {
					return this.date;
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.date = readDate(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.date = readDate(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// java.util.Date
				
						writeDate(this.date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// java.util.Date
				
						writeDate(this.date,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("date="+String.valueOf(date));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(rejected_dateStruct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Tye_Paiement;

				public String getTye_Paiement () {
					return this.Tye_Paiement;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				
			    public Double TVA;

				public Double getTVA () {
					return this.TVA;
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

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Tye_Paiement="+Tye_Paiement);
		sb.append(",Produit="+Produit);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
		sb.append(",TVA="+String.valueOf(TVA));
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

public static class row9Struct implements routines.system.IPersistableRow<row9Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Tye_Paiement;

				public String getTye_Paiement () {
					return this.Tye_Paiement;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				
			    public Double TVA;

				public Double getTVA () {
					return this.TVA;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row9Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date = this.Date;
	            other.Fournisseur = this.Fournisseur;
	            other.Tye_Paiement = this.Tye_Paiement;
	            other.Produit = this.Produit;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            other.TVA = this.TVA;
	            
	}

	public void copyKeysDataTo(row9Struct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Tye_Paiement="+Tye_Paiement);
		sb.append(",Produit="+Produit);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
		sb.append(",TVA="+String.valueOf(TVA));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row9Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Tye_Paiement;

				public String getTye_Paiement () {
					return this.Tye_Paiement;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				
			    public Double TVA;

				public Double getTVA () {
					return this.TVA;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(row10Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date = this.Date;
	            other.Fournisseur = this.Fournisseur;
	            other.Tye_Paiement = this.Tye_Paiement;
	            other.Produit = this.Produit;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            other.TVA = this.TVA;
	            
	}

	public void copyKeysDataTo(row10Struct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Tye_Paiement="+Tye_Paiement);
		sb.append(",Produit="+Produit);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
		sb.append(",TVA="+String.valueOf(TVA));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row10Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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

public static class copyOfout2Struct implements routines.system.IPersistableRow<copyOfout2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Tye_Paiement;

				public String getTye_Paiement () {
					return this.Tye_Paiement;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				
			    public Double TVA;

				public Double getTVA () {
					return this.TVA;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		final copyOfout2Struct other = (copyOfout2Struct) obj;
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(copyOfout2Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date = this.Date;
	            other.Fournisseur = this.Fournisseur;
	            other.Tye_Paiement = this.Tye_Paiement;
	            other.Produit = this.Produit;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            other.TVA = this.TVA;
	            
	}

	public void copyKeysDataTo(copyOfout2Struct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Tye_Paiement="+Tye_Paiement);
		sb.append(",Produit="+Produit);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
		sb.append(",TVA="+String.valueOf(TVA));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(copyOfout2Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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

public static class row8Struct implements routines.system.IPersistableRow<row8Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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

public static class after_tDBInput_7Struct implements routines.system.IPersistableRow<after_tDBInput_7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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
    public int compareTo(after_tDBInput_7Struct other) {

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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public java.util.Date Date;

				public java.util.Date getDate () {
					return this.Date;
				}
				
			    public String Fournisseur;

				public String getFournisseur () {
					return this.Fournisseur;
				}
				
			    public String Tye_Paiement;

				public String getTye_Paiement () {
					return this.Tye_Paiement;
				}
				
			    public String Produit;

				public String getProduit () {
					return this.Produit;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				
			    public Double TVA;

				public Double getTVA () {
					return this.TVA;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
							result = prime * result + (int) this.Achat_PK;
						
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
		
						if (this.Achat_PK != other.Achat_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out2Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date = this.Date;
	            other.Fournisseur = this.Fournisseur;
	            other.Tye_Paiement = this.Tye_Paiement;
	            other.Produit = this.Produit;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            other.TVA = this.TVA;
	            
	}

	public void copyKeysDataTo(out2Struct other) {

		other.Achat_PK = this.Achat_PK;
	            	
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

	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
			        this.Achat_PK = dis.readInt();
					
					this.Date = readDate(dis);
					
					this.Fournisseur = readString(dis);
					
					this.Tye_Paiement = readString(dis);
					
					this.Produit = readString(dis);
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.TVA = null;
           				} else {
           			    	this.TVA = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Achat_PK);
					
					// java.util.Date
				
						writeDate(this.Date,dos);
					
					// String
				
						writeString(this.Fournisseur,dos);
					
					// String
				
						writeString(this.Tye_Paiement,dos);
					
					// String
				
						writeString(this.Produit,dos);
					
					// Double
				
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
					// Double
				
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
					// Double
				
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
					// Double
				
						if(this.TVA == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.TVA);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date="+String.valueOf(Date));
		sb.append(",Fournisseur="+Fournisseur);
		sb.append(",Tye_Paiement="+Tye_Paiement);
		sb.append(",Produit="+Produit);
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
		sb.append(",TVA="+String.valueOf(TVA));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(out2Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Achat_PK, other.Achat_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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


		tDBInput_3Process(globalMap);
		tDBInput_4Process(globalMap);
		tDBInput_5Process(globalMap);
		tDBInput_6Process(globalMap);
		tDBInput_2Process(globalMap);

		row8Struct row8 = new row8Struct();
copyOfout2Struct copyOfout2 = new copyOfout2Struct();
copyOfout2Struct row10 = copyOfout2;
row9Struct row9 = new row9Struct();

		row1Struct row1 = new row1Struct();
out2Struct out2 = new out2Struct();

			row3Struct row3 = new row3Struct();
out1Struct out1 = new out1Struct();
ffStruct ff = new ffStruct();
reject1Struct reject1 = new reject1Struct();
rejected_dateStruct rejected_date = new rejected_dateStruct();






	
	/**
	 * [tDBOutput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBOutput_1", false);
		start_Hash.put("tDBOutput_1", System.currentTimeMillis());
		
	
	currentComponent="tDBOutput_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"ff");
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

 
	final String decryptedPassword_tDBOutput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:LnzY8a1z6XlvHsiHbKy6HnLzCiLq8Q05yHVokkLBDS/L6cDJRV4=");

    String dbPwd_tDBOutput_1 = decryptedPassword_tDBOutput_1;	
    conn_tDBOutput_1 = java.sql.DriverManager.getConnection(url_tDBOutput_1,dbUser_tDBOutput_1,dbPwd_tDBOutput_1);
	
		resourceMap.put("conn_tDBOutput_1", conn_tDBOutput_1);
	
        conn_tDBOutput_1.setAutoCommit(false);
        int commitEvery_tDBOutput_1 = 10000;
        int commitCounter_tDBOutput_1 = 0;

   int batchSize_tDBOutput_1 = 10000;
   int batchSizeCounter_tDBOutput_1=0;

if(dbschema_tDBOutput_1 == null || dbschema_tDBOutput_1.trim().length() == 0) {
    tableName_tDBOutput_1 = "Fact_Achats";
} else {
    tableName_tDBOutput_1 = dbschema_tDBOutput_1 + "].[" + "Fact_Achats";
}
	int count_tDBOutput_1=0;

        String insert_tDBOutput_1 = "INSERT INTO [" + tableName_tDBOutput_1 + "] ([Date_FK],[Fournisseur_FK],[Tye_Paiement_FK],[Produit_FK],[Prix],[Quantite],[Montant_Total]) VALUES (?,?,?,?,?,?,?)";
        java.sql.PreparedStatement pstmt_tDBOutput_1 = conn_tDBOutput_1.prepareStatement(insert_tDBOutput_1);
        resourceMap.put("pstmt_tDBOutput_1", pstmt_tDBOutput_1);


 



/**
 * [tDBOutput_1 begin ] stop
 */



	
	/**
	 * [tMap_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_3", false);
		start_Hash.put("tMap_3", System.currentTimeMillis());
		
	
	currentComponent="tMap_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"out1");
					}
				
		int tos_count_tMap_3 = 0;
		




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
class  Var__tMap_3__Struct  {
}
Var__tMap_3__Struct Var__tMap_3 = new Var__tMap_3__Struct();
// ###############################

// ###############################
// # Outputs initialization
ffStruct ff_tmp = new ffStruct();
// ###############################

        
        



        









 



/**
 * [tMap_3 begin ] stop
 */




	
	/**
	 * [tLogRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_1", false);
		start_Hash.put("tLogRow_1", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"reject1");
					}
				
		int tos_count_tLogRow_1 = 0;
		

	///////////////////////
	
         class Util_tLogRow_1 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[8];

        public void addRow(String[] row) {

            for (int i = 0; i < 8; i++) {
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
                    for (k = 0; k < (totals + 7 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 7 - name.length() - k; i++) {
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
                
                    //last column
                    for (int i = 0; i < colLengths[7] - fillChars[1].length() + 1; i++) {
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
        util_tLogRow_1.addRow(new String[]{"Achat_PK","Date_FK","Fournisseur_FK","Tye_Paiement_FK","Produit_FK","Prix","Quantite","Montant_Total",});        
 		StringBuilder strBuffer_tLogRow_1 = null;
		int nb_line_tLogRow_1 = 0;
///////////////////////    			



 



/**
 * [tLogRow_1 begin ] stop
 */




	
	/**
	 * [tLogRow_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_2", false);
		start_Hash.put("tLogRow_2", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"rejected_date");
					}
				
		int tos_count_tLogRow_2 = 0;
		

	///////////////////////
	
         class Util_tLogRow_2 {

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
        Util_tLogRow_2 util_tLogRow_2 = new Util_tLogRow_2();
        util_tLogRow_2.setTableName("tLogRow_2");
        util_tLogRow_2.addRow(new String[]{"date",});        
 		StringBuilder strBuffer_tLogRow_2 = null;
		int nb_line_tLogRow_2 = 0;
///////////////////////    			



 



/**
 * [tLogRow_2 begin ] stop
 */



	
	/**
	 * [tMap_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_1", false);
		start_Hash.put("tMap_1", System.currentTimeMillis());
		
	
	currentComponent="tMap_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row3");
					}
				
		int tos_count_tMap_1 = 0;
		




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
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct> tHash_Lookup_row7 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct>) 
					globalMap.get( "tHash_Lookup_row7" ))
					;					
					
	

row7Struct row7HashKey = new row7Struct();
row7Struct row7Default = new row7Struct();
	
		org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row2Struct> tHash_Lookup_row2 = (org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row2Struct>) 
				((org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row2Struct>) 
					globalMap.get( "tHash_Lookup_row2" ))
					;					
					
	

row2Struct row2HashKey = new row2Struct();
row2Struct row2Default = new row2Struct();
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
reject1Struct reject1_tmp = new reject1Struct();
rejected_dateStruct rejected_date_tmp = new rejected_dateStruct();
// ###############################

        
        



        









 



/**
 * [tMap_1 begin ] stop
 */



	
	/**
	 * [tUnite_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUnite_1", false);
		start_Hash.put("tUnite_1", System.currentTimeMillis());
		
	
	currentComponent="tUnite_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row9","out2");
					}
				
		int tos_count_tUnite_1 = 0;
		

int nb_line_tUnite_1 = 0;

 



/**
 * [tUnite_1 begin ] stop
 */



	
	/**
	 * [tFilterRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tFilterRow_1", false);
		start_Hash.put("tFilterRow_1", System.currentTimeMillis());
		
	
	currentComponent="tFilterRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row10");
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
	 * [tLogRow_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tLogRow_3", false);
		start_Hash.put("tLogRow_3", System.currentTimeMillis());
		
	
	currentComponent="tLogRow_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"copyOfout2");
					}
				
		int tos_count_tLogRow_3 = 0;
		

	///////////////////////
	
         class Util_tLogRow_3 {

        String[] des_top = { ".", ".", "-", "+" };

        String[] des_head = { "|=", "=|", "-", "+" };

        String[] des_bottom = { "'", "'", "-", "+" };

        String name="";

        java.util.List<String[]> list = new java.util.ArrayList<String[]>();

        int[] colLengths = new int[9];

        public void addRow(String[] row) {

            for (int i = 0; i < 9; i++) {
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
                    for (k = 0; k < (totals + 8 - name.length()) / 2; k++) {
                        sb.append(' ');
                    }
                    sb.append(name);
                    for (int i = 0; i < totals + 8 - name.length() - k; i++) {
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
                
                    //last column
                    for (int i = 0; i < colLengths[8] - fillChars[1].length() + 1; i++) {
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
        Util_tLogRow_3 util_tLogRow_3 = new Util_tLogRow_3();
        util_tLogRow_3.setTableName("tLogRow_3");
        util_tLogRow_3.addRow(new String[]{"Achat_PK","Date","Fournisseur","Tye_Paiement","Produit","Prix","Quantite","Montant_Total","TVA",});        
 		StringBuilder strBuffer_tLogRow_3 = null;
		int nb_line_tLogRow_3 = 0;
///////////////////////    			



 



/**
 * [tLogRow_3 begin ] stop
 */



	
	/**
	 * [tMap_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_4", false);
		start_Hash.put("tMap_4", System.currentTimeMillis());
		
	
	currentComponent="tMap_4";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row8");
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
copyOfout2Struct copyOfout2_tmp = new copyOfout2Struct();
// ###############################

        
        



        









 



/**
 * [tMap_4 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_7 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:Xs+Oq6/k2A/Gu6wqHZV6EfUiKhAEazVJ5tAxJDQPrr+GIwHMIGQ=");
				
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

		    String dbquery_tDBInput_7 = "SELECT etats_achats_ventes_Suivi_achats.Societe,\n		etats_achats_ventes_Suivi_achats.Nom_du_Fournisseur,\n		etats_achats_"
+"ventes_Suivi_achats.N_Facture,\n		etats_achats_ventes_Suivi_achats.Date_de_facture,\n		etats_achats_ventes_Suivi_achats.Mo"
+"ntant_HT,\n		etats_achats_ventes_Suivi_achats.Taux_de_TVA,\n		etats_achats_ventes_Suivi_achats.Timbre,\n		etats_achats_vent"
+"es_Suivi_achats.Montant_de_la_TVA,\n		etats_achats_ventes_Suivi_achats.FODEC,\n		etats_achats_ventes_Suivi_achats.Montant_"
+"TTC,\n		etats_achats_ventes_Suivi_achats.Retenue,\n		etats_achats_ventes_Suivi_achats.Versements_effectues_par_SOUGUI,\n		e"
+"tats_achats_ventes_Suivi_achats.Reste_du,\n		etats_achats_ventes_Suivi_achats.Action_a_effectuer,\n		etats_achats_ventes_S"
+"uivi_achats.Date_de_paiement_total_de_la_facture,\n		etats_achats_ventes_Suivi_achats.mode_de_reglement,\n		etats_achats_v"
+"entes_Suivi_achats.Date_de_la_premiere_relance,\n		etats_achats_ventes_Suivi_achats.Date_de_la_deuxieme_relance,\n		etats_"
+"achats_ventes_Suivi_achats.Penalite_et_majoration_de_paiement,\n		etats_achats_ventes_Suivi_achats.Commentaire\nFROM	etats"
+"_achats_ventes_Suivi_achats";
		    

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
								row8.Societe = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(1);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Societe = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Societe = tmpContent_tDBInput_7;
                }
            } else {
                row8.Societe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 2) {
								row8.Nom_du_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(2);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Nom_du_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Nom_du_Fournisseur = tmpContent_tDBInput_7;
                }
            } else {
                row8.Nom_du_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 3) {
								row8.N_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(3);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.N_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.N_Facture = tmpContent_tDBInput_7;
                }
            } else {
                row8.N_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 4) {
								row8.Date_de_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(4);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Date_de_facture = tmpContent_tDBInput_7;
                }
            } else {
                row8.Date_de_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 5) {
								row8.Montant_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(5);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Montant_HT = tmpContent_tDBInput_7;
                }
            } else {
                row8.Montant_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 6) {
								row8.Taux_de_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(6);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Taux_de_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Taux_de_TVA = tmpContent_tDBInput_7;
                }
            } else {
                row8.Taux_de_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 7) {
								row8.Timbre = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(7);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Timbre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Timbre = tmpContent_tDBInput_7;
                }
            } else {
                row8.Timbre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 8) {
								row8.Montant_de_la_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(8);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_de_la_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Montant_de_la_TVA = tmpContent_tDBInput_7;
                }
            } else {
                row8.Montant_de_la_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 9) {
								row8.FODEC = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(9);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.FODEC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.FODEC = tmpContent_tDBInput_7;
                }
            } else {
                row8.FODEC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 10) {
								row8.Montant_TTC = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(10);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Montant_TTC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Montant_TTC = tmpContent_tDBInput_7;
                }
            } else {
                row8.Montant_TTC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 11) {
								row8.Retenue = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(11);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Retenue = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Retenue = tmpContent_tDBInput_7;
                }
            } else {
                row8.Retenue = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 12) {
								row8.Versements_effectues_par_SOUGUI = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(12);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Versements_effectues_par_SOUGUI = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Versements_effectues_par_SOUGUI = tmpContent_tDBInput_7;
                }
            } else {
                row8.Versements_effectues_par_SOUGUI = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 13) {
								row8.Reste_du = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(13);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Reste_du = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Reste_du = tmpContent_tDBInput_7;
                }
            } else {
                row8.Reste_du = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 14) {
								row8.Action_a_effectuer = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(14);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Action_a_effectuer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Action_a_effectuer = tmpContent_tDBInput_7;
                }
            } else {
                row8.Action_a_effectuer = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 15) {
								row8.Date_de_paiement_total_de_la_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(15);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_paiement_total_de_la_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Date_de_paiement_total_de_la_facture = tmpContent_tDBInput_7;
                }
            } else {
                row8.Date_de_paiement_total_de_la_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 16) {
								row8.mode_de_reglement = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(16);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.mode_de_reglement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.mode_de_reglement = tmpContent_tDBInput_7;
                }
            } else {
                row8.mode_de_reglement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 17) {
								row8.Date_de_la_premiere_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(17);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_la_premiere_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Date_de_la_premiere_relance = tmpContent_tDBInput_7;
                }
            } else {
                row8.Date_de_la_premiere_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 18) {
								row8.Date_de_la_deuxieme_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(18);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Date_de_la_deuxieme_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Date_de_la_deuxieme_relance = tmpContent_tDBInput_7;
                }
            } else {
                row8.Date_de_la_deuxieme_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 19) {
								row8.Penalite_et_majoration_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(19);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Penalite_et_majoration_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Penalite_et_majoration_de_paiement = tmpContent_tDBInput_7;
                }
            } else {
                row8.Penalite_et_majoration_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_7 < 20) {
								row8.Commentaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_7 = rs_tDBInput_7.getString(20);
            if(tmpContent_tDBInput_7 != null) {
            	if (talendToDBList_tDBInput_7 .contains(rsmd_tDBInput_7.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row8.Commentaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_7);
            	} else {
                	row8.Commentaire = tmpContent_tDBInput_7;
                }
            } else {
                row8.Commentaire = null;
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
	 * [tMap_4 main ] start
	 */

	

	
	
	currentComponent="tMap_4";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row8"
						
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

copyOfout2 = null;


// # Output table : 'copyOfout2'
copyOfout2_tmp.Achat_PK = 0;
copyOfout2_tmp.Date = (row8.Date_de_facture != null 
 && !row8.Date_de_facture.trim().isEmpty()
 && !row8.Date_de_facture.trim().equalsIgnoreCase("TOTAUX"))
    ? (row8.Date_de_facture.contains("/")
        ? TalendDate.parseDate("dd/MM/yyyy", row8.Date_de_facture.trim())
        : TalendDate.parseDateLocale("EEE MMM dd HH:mm:ss zzz yyyy", row8.Date_de_facture.trim(), "en"))
    : TalendDate.parseDate("yyyy-MM-dd", "1111-11-11") ;
copyOfout2_tmp.Fournisseur = row8.Nom_du_Fournisseur==null?"Unknown":row8.Nom_du_Fournisseur.trim().substring(0, 1).toUpperCase()+row8.Nom_du_Fournisseur.trim().toLowerCase().substring( 1) ;
copyOfout2_tmp.Tye_Paiement = row8.mode_de_reglement== null 
? "AUTRE" 
: StringHandling.TRIM(row8.mode_de_reglement).toUpperCase() ;
copyOfout2_tmp.Produit = row8.Commentaire == null || row8.Commentaire.length() == 0
    ? "Unknown"
    : row8.Commentaire.substring(0,1).toUpperCase() 
      + row8.Commentaire.substring(1).toLowerCase() ;
copyOfout2_tmp.Prix = row8.Montant_TTC!= null 
&& row8.Montant_TTC.trim().replace(",", ".").matches("-?\\d+(\\.\\d+)?")
    ? Double.parseDouble(row8.Montant_TTC.trim().replace(",", "."))
    : null ;
copyOfout2_tmp.Quantite = 1.0;
copyOfout2_tmp.Montant_Total = row8.Montant_TTC!= null 
&& row8.Montant_TTC.trim().replace(",", ".").matches("-?\\d+(\\.\\d+)?")
    ? Mathematical.ABS(Double.parseDouble(row8.Montant_TTC.trim().replace(",", ".")))
    : null ;
copyOfout2_tmp.TVA = row8.Taux_de_TVA == null ? 0.0f :
row8.Taux_de_TVA.trim().equals("0%*") ? 0.0d :
row8.Taux_de_TVA.trim().equals("0%")  ? 0.0d :
row8.Taux_de_TVA.trim().equals("0")   ? 0.0d :
row8.Taux_de_TVA.trim().equals("7")   ? 0.07d :
row8.Taux_de_TVA.trim().equals("19")  ? 0.19d :
row8.Taux_de_TVA.trim().equals("1")   ? 0.01d :
row8.Taux_de_TVA.trim().equals("0,07") ? 0.07d :
row8.Taux_de_TVA.trim().equals("0,19") ? 0.19d :
0.0d ;
copyOfout2 = copyOfout2_tmp;
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
// Start of branch "copyOfout2"
if(copyOfout2 != null) { 



	
	/**
	 * [tLogRow_3 main ] start
	 */

	

	
	
	currentComponent="tLogRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"copyOfout2"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_3 = new String[9];
              
                 row_tLogRow_3[0]=    						    
				                String.valueOf(copyOfout2.Achat_PK)			
					          ;	
										
    			   				
	    		if(copyOfout2.Date != null) { //              
                 row_tLogRow_3[1]=    						
								FormatterUtils.format_Date(copyOfout2.Date, "dd-MM-yyyy")
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Fournisseur != null) { //              
                 row_tLogRow_3[2]=    						    
				                String.valueOf(copyOfout2.Fournisseur)			
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Tye_Paiement != null) { //              
                 row_tLogRow_3[3]=    						    
				                String.valueOf(copyOfout2.Tye_Paiement)			
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Produit != null) { //              
                 row_tLogRow_3[4]=    						    
				                String.valueOf(copyOfout2.Produit)			
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Prix != null) { //              
                 row_tLogRow_3[5]=    						
								FormatterUtils.formatUnwithE(copyOfout2.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Quantite != null) { //              
                 row_tLogRow_3[6]=    						
								FormatterUtils.formatUnwithE(copyOfout2.Quantite)
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.Montant_Total != null) { //              
                 row_tLogRow_3[7]=    						
								FormatterUtils.formatUnwithE(copyOfout2.Montant_Total)
					          ;	
							
	    		} //			
    			   				
	    		if(copyOfout2.TVA != null) { //              
                 row_tLogRow_3[8]=    						
								FormatterUtils.formatUnwithE(copyOfout2.TVA)
					          ;	
							
	    		} //			
    			 

				util_tLogRow_3.addRow(row_tLogRow_3);	
				nb_line_tLogRow_3++;
//////

//////                    
                    
///////////////////////    			

 
     row10 = copyOfout2;


	tos_count_tLogRow_3++;

/**
 * [tLogRow_3 main ] stop
 */
	
	/**
	 * [tLogRow_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tLogRow_3";

	

 



/**
 * [tLogRow_3 process_data_begin ] stop
 */

	
	/**
	 * [tFilterRow_1 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row10"
						
						);
					}
					

          row9 = null;
    Operator_tFilterRow_1 ope_tFilterRow_1 = new Operator_tFilterRow_1("&&");
	        ope_tFilterRow_1.matches((row10.Montant_Total != null)
	                       , "Montant_Total!=null failed");
    
    if (ope_tFilterRow_1.getMatchFlag()) {
              if(row9 == null){ 
                row9 = new row9Struct();
              }
               row9.Achat_PK = row10.Achat_PK;
               row9.Date = row10.Date;
               row9.Fournisseur = row10.Fournisseur;
               row9.Tye_Paiement = row10.Tye_Paiement;
               row9.Produit = row10.Produit;
               row9.Prix = row10.Prix;
               row9.Quantite = row10.Quantite;
               row9.Montant_Total = row10.Montant_Total;
               row9.TVA = row10.TVA;    
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
// Start of branch "row9"
if(row9 != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"row9"
									
							);
						}
						
//////////
 

// for output
			row3 = new row3Struct();
								
			row3.Achat_PK = row9.Achat_PK;								
			row3.Date = row9.Date;								
			row3.Fournisseur = row9.Fournisseur;								
			row3.Tye_Paiement = row9.Tye_Paiement;								
			row3.Produit = row9.Produit;								
			row3.Prix = row9.Prix;								
			row3.Quantite = row9.Quantite;								
			row3.Montant_Total = row9.Montant_Total;								
			row3.TVA = row9.TVA;			

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
	 * [tMap_1 main ] start
	 */

	

	
	
	currentComponent="tMap_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_1 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_1 = false;
		  boolean mainRowRejected_tMap_1 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row4" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow4 = false;
       		  	    	
       		  	    	
 							row4Struct row4ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row4HashKey.Nom_fournisseur = row3.Fournisseur ;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row4 != null && tHash_Lookup_row4.getCount(row4HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row4' and it contains more one result from keys :  row4.Nom_fournisseur = '" + row4HashKey.Nom_fournisseur + "'");
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
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row5HashKey.Type_paiement = row3.Tye_Paiement;
                        		    		

								
		                        	row5HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row5.lookup( row5HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row5 != null && tHash_Lookup_row5.getCount(row5HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row5' and it contains more one result from keys :  row5.Type_paiement = '" + row5HashKey.Type_paiement + "'");
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
				// Starting Lookup Table "row7" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow7 = false;
       		  	    	
       		  	    	
 							row7Struct row7ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row7HashKey.Nom_produit = row3.Produit;
                        		    		

								
		                        	row7HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row7.lookup( row7HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row7 != null && tHash_Lookup_row7.getCount(row7HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row7' and it contains more one result from keys :  row7.Nom_produit = '" + row7HashKey.Nom_produit + "'");
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
				// Starting Lookup Table "row2" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow2 = false;
       		  	    	
       		  	    	
 							row2Struct row2ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row2HashKey.Date = row3.Date == null ? null : new java.util.Date(row3.Date.getTime());
                        		    		

								
		                        	row2HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row2.lookup( row2HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row2 != null && tHash_Lookup_row2.getCount(row2HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row2' and it contains more one result from keys :  row2.Date = '" + row2HashKey.Date + "'");
								} // G 071
							

							row2Struct row2 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row2Struct fromLookup_row2 = null;
							row2 = row2Default;
										 
							
								 
							
							
								if (tHash_Lookup_row2 !=null && tHash_Lookup_row2.hasNext()) { // G 099
								
							
								
								fromLookup_row2 = tHash_Lookup_row2.next();

							
							
								} // G 099
							
							

							if(fromLookup_row2 != null) {
								row2 = fromLookup_row2;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
        // ###############################
        // # Output tables

out1 = null;
reject1 = null;
rejected_date = null;


// # Output table : 'out1'
// # Filter conditions 
if( 

row7.Nom_produit != null

 ) {
out1_tmp.Achat_PK = 0;
out1_tmp.Date_FK = row2.Date_PK ;
out1_tmp.Fournisseur_FK = row4.Fournisseur_PK ;
out1_tmp.Tye_Paiement_FK = row5.Mode_Paiement_PK ;
out1_tmp.Produit_FK = row7.Produit_PK ;
out1_tmp.Prix = row3.Prix ;
out1_tmp.Quantite = row3.Quantite ;
out1_tmp.Montant_Total = row3.Montant_Total==0.0?
row3.Prix*row3.Quantite*(1+row3.TVA):row3.Montant_Total ;
out1 = out1_tmp;
} // closing filter/reject

// # Output table : 'reject1'
// # Filter conditions 
if( 

row7.Nom_produit == null

 ) {
reject1_tmp.Achat_PK = 0;
reject1_tmp.Date_FK = row2.Date_PK  ;
reject1_tmp.Fournisseur_FK = row3.Fournisseur ;
reject1_tmp.Tye_Paiement_FK = row3.Tye_Paiement ;
reject1_tmp.Produit_FK = row3.Produit ;
reject1_tmp.Prix = row3.Prix  ;
reject1_tmp.Quantite = row3.Quantite  ;
reject1_tmp.Montant_Total = row3.Montant_Total==null || row3.Montant_Total==0?
row3.Prix*row3.Montant_Total*(1+row3.TVA):row3.Montant_Total  ;
reject1 = reject1_tmp;
} // closing filter/reject

// # Output table : 'rejected_date'
// # Filter conditions 
if( 

row2.Date== null

 ) {
rejected_date_tmp.date = row3.Date ;
rejected_date = rejected_date_tmp;
} // closing filter/reject
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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out1"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_3 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_3 = false;
		  boolean mainRowRejected_tMap_3 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row6HashKey.Date_FK = out1.Date_FK ;
                        		    		
                        		    		    row6HashKey.Fournisseur_FK = out1.Fournisseur_FK ;
                        		    		
                        		    		    row6HashKey.Tye_Paiement_FK = out1.Tye_Paiement_FK ;
                        		    		
                        		    		    row6HashKey.Produit_FK = out1.Produit_FK ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_3 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Date_FK = '" + row6HashKey.Date_FK + "', row6.Fournisseur_FK = '" + row6HashKey.Fournisseur_FK + "', row6.Tye_Paiement_FK = '" + row6HashKey.Tye_Paiement_FK + "', row6.Produit_FK = '" + row6HashKey.Produit_FK + "'");
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
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

ff = null;

if(!rejectedInnerJoin_tMap_3 ) {
} // closing inner join bracket (1)
// ###### START REJECTS ##### 

// # Output reject table : 'ff'
// # Filter conditions 
if( rejectedInnerJoin_tMap_3 ) {
ff_tmp.Achat_PK = out1.Achat_PK;
ff_tmp.Date_FK = out1.Date_FK;
ff_tmp.Fournisseur_FK = out1.Fournisseur_FK;
ff_tmp.Tye_Paiement_FK = out1.Tye_Paiement_FK;
ff_tmp.Produit_FK = out1.Produit_FK;
ff_tmp.Prix = out1.Prix;
ff_tmp.Quantite = out1.Quantite;
ff_tmp.Montant_Total = out1.Montant_Total;
ff = ff_tmp;
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
// Start of branch "ff"
if(ff != null) { 



	
	/**
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"ff"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(ff.Date_FK == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(1, ff.Date_FK);
}

                    if(ff.Fournisseur_FK == null) {
pstmt_tDBOutput_1.setNull(2, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(2, ff.Fournisseur_FK);
}

                    if(ff.Tye_Paiement_FK == null) {
pstmt_tDBOutput_1.setNull(3, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(3, ff.Tye_Paiement_FK);
}

                    if(ff.Produit_FK == null) {
pstmt_tDBOutput_1.setNull(4, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(4, ff.Produit_FK);
}

                    if(ff.Prix == null) {
pstmt_tDBOutput_1.setNull(5, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(5, ff.Prix);
}

                    if(ff.Quantite == null) {
pstmt_tDBOutput_1.setNull(6, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(6, ff.Quantite);
}

                    if(ff.Montant_Total == null) {
pstmt_tDBOutput_1.setNull(7, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(7, ff.Montant_Total);
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

} // End of branch "ff"




	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */

} // End of branch "out1"




// Start of branch "reject1"
if(reject1 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"reject1"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[8];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(reject1.Achat_PK)			
					          ;	
										
    			   				
	    		if(reject1.Date_FK != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(reject1.Date_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Fournisseur_FK != null) { //              
                 row_tLogRow_1[2]=    						    
				                String.valueOf(reject1.Fournisseur_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Tye_Paiement_FK != null) { //              
                 row_tLogRow_1[3]=    						    
				                String.valueOf(reject1.Tye_Paiement_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Produit_FK != null) { //              
                 row_tLogRow_1[4]=    						    
				                String.valueOf(reject1.Produit_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Prix != null) { //              
                 row_tLogRow_1[5]=    						
								FormatterUtils.formatUnwithE(reject1.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Quantite != null) { //              
                 row_tLogRow_1[6]=    						
								FormatterUtils.formatUnwithE(reject1.Quantite)
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Montant_Total != null) { //              
                 row_tLogRow_1[7]=    						
								FormatterUtils.formatUnwithE(reject1.Montant_Total)
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

} // End of branch "reject1"




// Start of branch "rejected_date"
if(rejected_date != null) { 



	
	/**
	 * [tLogRow_2 main ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"rejected_date"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_2 = new String[1];
   				
	    		if(rejected_date.date != null) { //              
                 row_tLogRow_2[0]=    						
								FormatterUtils.format_Date(rejected_date.date, "dd-MM-yyyy")
					          ;	
							
	    		} //			
    			 

				util_tLogRow_2.addRow(row_tLogRow_2);	
				nb_line_tLogRow_2++;
//////

//////                    
                    
///////////////////////    			

 


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
	 * [tLogRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 process_data_end ] stop
 */

} // End of branch "rejected_date"




	
	/**
	 * [tMap_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 process_data_end ] stop
 */



	
	/**
	 * [tUnite_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 process_data_end ] stop
 */

} // End of branch "row9"




	
	/**
	 * [tFilterRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 process_data_end ] stop
 */



	
	/**
	 * [tLogRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_3";

	

 



/**
 * [tLogRow_3 process_data_end ] stop
 */

} // End of branch "copyOfout2"




	
	/**
	 * [tMap_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 process_data_end ] stop
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
	 * [tMap_4 end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row8");
			  	}
			  	
 

ok_Hash.put("tMap_4", true);
end_Hash.put("tMap_4", System.currentTimeMillis());




/**
 * [tMap_4 end ] stop
 */

	
	/**
	 * [tLogRow_3 end ] start
	 */

	

	
	
	currentComponent="tLogRow_3";

	


//////

                    
                    java.io.PrintStream consoleOut_tLogRow_3 = null;
                    if (globalMap.get("tLogRow_CONSOLE")!=null)
                    {
                    	consoleOut_tLogRow_3 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
                    }
                    else
                    {
                    	consoleOut_tLogRow_3 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                    	globalMap.put("tLogRow_CONSOLE",consoleOut_tLogRow_3);
                    }
                    
                    consoleOut_tLogRow_3.println(util_tLogRow_3.format().toString());
                    consoleOut_tLogRow_3.flush();
//////
globalMap.put("tLogRow_3_NB_LINE",nb_line_tLogRow_3);

///////////////////////    			

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"copyOfout2");
			  	}
			  	
 

ok_Hash.put("tLogRow_3", true);
end_Hash.put("tLogRow_3", System.currentTimeMillis());




/**
 * [tLogRow_3 end ] stop
 */

	
	/**
	 * [tFilterRow_1 end ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	
    globalMap.put("tFilterRow_1_NB_LINE", nb_line_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_OK", nb_line_ok_tFilterRow_1);
    globalMap.put("tFilterRow_1_NB_LINE_REJECT", nb_line_reject_tFilterRow_1);
    

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row10");
			  	}
			  	
 

ok_Hash.put("tFilterRow_1", true);
end_Hash.put("tFilterRow_1", System.currentTimeMillis());




/**
 * [tFilterRow_1 end ] stop
 */










	
	/**
	 * [tMap_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_2", false);
		start_Hash.put("tMap_2", System.currentTimeMillis());
		
	
	currentComponent="tMap_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row1");
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
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:FRgFuKSSD0rhyIj3C0ivA0iwwsx+1FxDw9iU7ibrTrAtwX/A2Y8=");
				
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

		    String dbquery_tDBInput_1 = "SELECT Factures_achat.Nom_du_fichier,\n		Factures_achat.N__Facture,\n		Factures_achat.Date_Facture,\n		Factures_achat.Nom_"
+"du_vendeur,\n		Factures_achat.Adresse_du_vendeur,\n		Factures_achat.Matricule_Fiscale_du_vendeur,\n		Factures_achat.Produit"
+",\n		Factures_achat.Reference,\n		Factures_achat.Quantite,\n		Factures_achat.PU_HT,\n		Factures_achat.TVA,\n		Factures_achat."
+"Total_HT,\n		Factures_achat.Montant_Total_net_a_payer\nFROM	Factures_achat";
		    

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
								row1.Nom_du_fichier = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(1);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Nom_du_fichier = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Nom_du_fichier = tmpContent_tDBInput_1;
                }
            } else {
                row1.Nom_du_fichier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 2) {
								row1.N__Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(2);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.N__Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.N__Facture = tmpContent_tDBInput_1;
                }
            } else {
                row1.N__Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 3) {
								row1.Date_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(3);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Date_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Date_Facture = tmpContent_tDBInput_1;
                }
            } else {
                row1.Date_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 4) {
								row1.Nom_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(4);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Nom_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Nom_du_vendeur = tmpContent_tDBInput_1;
                }
            } else {
                row1.Nom_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 5) {
								row1.Adresse_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(5);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Adresse_du_vendeur = tmpContent_tDBInput_1;
                }
            } else {
                row1.Adresse_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 6) {
								row1.Matricule_Fiscale_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(6);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Matricule_Fiscale_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Matricule_Fiscale_du_vendeur = tmpContent_tDBInput_1;
                }
            } else {
                row1.Matricule_Fiscale_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 7) {
								row1.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(7);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Produit = tmpContent_tDBInput_1;
                }
            } else {
                row1.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 8) {
								row1.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(8);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Reference = tmpContent_tDBInput_1;
                }
            } else {
                row1.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 9) {
								row1.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(9);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Quantite = tmpContent_tDBInput_1;
                }
            } else {
                row1.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 10) {
								row1.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(10);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.PU_HT = tmpContent_tDBInput_1;
                }
            } else {
                row1.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 11) {
								row1.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(11);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.TVA = tmpContent_tDBInput_1;
                }
            } else {
                row1.TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 12) {
								row1.Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(12);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Total_HT = tmpContent_tDBInput_1;
                }
            } else {
                row1.Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 13) {
								row1.Montant_Total_net_a_payer = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(13);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Montant_Total_net_a_payer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Montant_Total_net_a_payer = tmpContent_tDBInput_1;
                }
            } else {
                row1.Montant_Total_net_a_payer = null;
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
						
							,"row1"
						
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
out2_tmp.Achat_PK = 0;
out2_tmp.Date = row1.Date_Facture!= null && !row1.Date_Facture.trim().isEmpty()
    ? (row1.Date_Facture.contains("/")
        ? new java.text.SimpleDateFormat("dd/MM/yyyy").parse(row1.Date_Facture.trim())
        : new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", java.util.Locale.ENGLISH).parse(row1.Date_Facture.trim()))
    : TalendDate.parseDate("dd-MM-yyyy", "11-11-1111") ;
out2_tmp.Fournisseur = row1.Nom_du_vendeur==null?"Unknown":row1.Nom_du_vendeur.trim().substring(0, 1).toUpperCase()+row1.Nom_du_vendeur.trim().toLowerCase().substring( 1) ;
out2_tmp.Tye_Paiement = "CHÈQUE";
out2_tmp.Produit = row1.Produit == null || row1.Produit.length() == 0
    ? "Unknown"
    : row1.Produit.substring(0,1).toUpperCase() 
      + row1.Produit.substring(1).toLowerCase() ;
out2_tmp.Prix = row1.PU_HT != null && !row1.PU_HT.trim().isEmpty()
    ? Double.parseDouble(row1.PU_HT.trim().replace(",", "."))
    : 0.0 ;
out2_tmp.Quantite = row1.Quantite!= null && !row1.Quantite.trim().isEmpty()
    ? Double.parseDouble(row1.Quantite.trim().replace(",", "."))
    : 0.0 ;
out2_tmp.Montant_Total = row1.Montant_Total_net_a_payer== null || row1.Montant_Total_net_a_payer.trim().isEmpty()
    ? 0.0
    : Mathematical.ABS(Double.parseDouble(row1.Montant_Total_net_a_payer.trim().replace(",", "."))) ;
out2_tmp.TVA = row1.TVA == null ? 0.0f :
row1.TVA.trim().equals("0%*") ? 0.0d :
row1.TVA.trim().equals("0%")  ? 0.0d :
row1.TVA.trim().equals("0")   ? 0.0d :
row1.TVA.trim().equals("7")   ? 0.07d :
row1.TVA.trim().equals("19")  ? 0.19d :
row1.TVA.trim().equals("1")   ? 0.01d :
row1.TVA.trim().equals("0,07") ? 0.07d :
row1.TVA.trim().equals("0,19") ? 0.19d :
0.0d ;
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
								
			row3.Achat_PK = out2.Achat_PK;								
			row3.Date = out2.Date;								
			row3.Fournisseur = out2.Fournisseur;								
			row3.Tye_Paiement = out2.Tye_Paiement;								
			row3.Produit = out2.Produit;								
			row3.Prix = out2.Prix;								
			row3.Quantite = out2.Quantite;								
			row3.Montant_Total = out2.Montant_Total;								
			row3.TVA = out2.TVA;			

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
	 * [tMap_1 main ] start
	 */

	

	
	
	currentComponent="tMap_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row3"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_1 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_1 = false;
		  boolean mainRowRejected_tMap_1 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row4" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow4 = false;
       		  	    	
       		  	    	
 							row4Struct row4ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row4HashKey.Nom_fournisseur = row3.Fournisseur ;
                        		    		

								
		                        	row4HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row4.lookup( row4HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row4 != null && tHash_Lookup_row4.getCount(row4HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row4' and it contains more one result from keys :  row4.Nom_fournisseur = '" + row4HashKey.Nom_fournisseur + "'");
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
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row5HashKey.Type_paiement = row3.Tye_Paiement;
                        		    		

								
		                        	row5HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row5.lookup( row5HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row5 != null && tHash_Lookup_row5.getCount(row5HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row5' and it contains more one result from keys :  row5.Type_paiement = '" + row5HashKey.Type_paiement + "'");
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
				// Starting Lookup Table "row7" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow7 = false;
       		  	    	
       		  	    	
 							row7Struct row7ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row7HashKey.Nom_produit = row3.Produit;
                        		    		

								
		                        	row7HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row7.lookup( row7HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row7 != null && tHash_Lookup_row7.getCount(row7HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row7' and it contains more one result from keys :  row7.Nom_produit = '" + row7HashKey.Nom_produit + "'");
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
				// Starting Lookup Table "row2" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow2 = false;
       		  	    	
       		  	    	
 							row2Struct row2ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_1) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_1 = false;
								
                        		    		    row2HashKey.Date = row3.Date == null ? null : new java.util.Date(row3.Date.getTime());
                        		    		

								
		                        	row2HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row2.lookup( row2HashKey );

	  							

	  							

 								
		  				
	  								
						
									
  									  		
 								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row2 != null && tHash_Lookup_row2.getCount(row2HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row2' and it contains more one result from keys :  row2.Date = '" + row2HashKey.Date + "'");
								} // G 071
							

							row2Struct row2 = null;
                    		  	 
							   
                    		  	 
	       		  	    	row2Struct fromLookup_row2 = null;
							row2 = row2Default;
										 
							
								 
							
							
								if (tHash_Lookup_row2 !=null && tHash_Lookup_row2.hasNext()) { // G 099
								
							
								
								fromLookup_row2 = tHash_Lookup_row2.next();

							
							
								} // G 099
							
							

							if(fromLookup_row2 != null) {
								row2 = fromLookup_row2;
							}
							
							
							
			  							
								
	                    		  	
		                    
	            	
	            	
	            // ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
        // ###############################
        // # Output tables

out1 = null;
reject1 = null;
rejected_date = null;


// # Output table : 'out1'
// # Filter conditions 
if( 

row7.Nom_produit != null

 ) {
out1_tmp.Achat_PK = 0;
out1_tmp.Date_FK = row2.Date_PK ;
out1_tmp.Fournisseur_FK = row4.Fournisseur_PK ;
out1_tmp.Tye_Paiement_FK = row5.Mode_Paiement_PK ;
out1_tmp.Produit_FK = row7.Produit_PK ;
out1_tmp.Prix = row3.Prix ;
out1_tmp.Quantite = row3.Quantite ;
out1_tmp.Montant_Total = row3.Montant_Total==0.0?
row3.Prix*row3.Quantite*(1+row3.TVA):row3.Montant_Total ;
out1 = out1_tmp;
} // closing filter/reject

// # Output table : 'reject1'
// # Filter conditions 
if( 

row7.Nom_produit == null

 ) {
reject1_tmp.Achat_PK = 0;
reject1_tmp.Date_FK = row2.Date_PK  ;
reject1_tmp.Fournisseur_FK = row3.Fournisseur ;
reject1_tmp.Tye_Paiement_FK = row3.Tye_Paiement ;
reject1_tmp.Produit_FK = row3.Produit ;
reject1_tmp.Prix = row3.Prix  ;
reject1_tmp.Quantite = row3.Quantite  ;
reject1_tmp.Montant_Total = row3.Montant_Total==null || row3.Montant_Total==0?
row3.Prix*row3.Montant_Total*(1+row3.TVA):row3.Montant_Total  ;
reject1 = reject1_tmp;
} // closing filter/reject

// # Output table : 'rejected_date'
// # Filter conditions 
if( 

row2.Date== null

 ) {
rejected_date_tmp.date = row3.Date ;
rejected_date = rejected_date_tmp;
} // closing filter/reject
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
	 * [tMap_3 main ] start
	 */

	

	
	
	currentComponent="tMap_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"out1"
						
						);
					}
					

		
		
		boolean hasCasePrimitiveKeyWithNull_tMap_3 = false;
		

        // ###############################
        // # Input tables (lookups)
		  boolean rejectedInnerJoin_tMap_3 = false;
		  boolean mainRowRejected_tMap_3 = false;
            				    								  
		

				///////////////////////////////////////////////
				// Starting Lookup Table "row6" 
				///////////////////////////////////////////////


				
				
                            
 					    boolean forceLooprow6 = false;
       		  	    	
       		  	    	
 							row6Struct row6ObjectFromLookup = null;
                          
		           		  	if(!rejectedInnerJoin_tMap_3) { // G_TM_M_020

								
								hasCasePrimitiveKeyWithNull_tMap_3 = false;
								
                        		    		    row6HashKey.Date_FK = out1.Date_FK ;
                        		    		
                        		    		    row6HashKey.Fournisseur_FK = out1.Fournisseur_FK ;
                        		    		
                        		    		    row6HashKey.Tye_Paiement_FK = out1.Tye_Paiement_FK ;
                        		    		
                        		    		    row6HashKey.Produit_FK = out1.Produit_FK ;
                        		    		

								
		                        	row6HashKey.hashCodeDirty = true;
                        		
	  					
	  							
			  					
			  					
	  					
		  							tHash_Lookup_row6.lookup( row6HashKey );

	  							

	  							

 								
								  
								  if(!tHash_Lookup_row6.hasNext()) { // G_TM_M_090

  								
		  				
	  								
			  							rejectedInnerJoin_tMap_3 = true;
	  								
						
									
  									  		
 								
								  
								  } // G_TM_M_090

  								



							} // G_TM_M_020
			           		  	  
							
				           		if(tHash_Lookup_row6 != null && tHash_Lookup_row6.getCount(row6HashKey) > 1) { // G 071
			  							
			  						
									 		
									//System.out.println("WARNING: UNIQUE MATCH is configured for the lookup 'row6' and it contains more one result from keys :  row6.Date_FK = '" + row6HashKey.Date_FK + "', row6.Fournisseur_FK = '" + row6HashKey.Fournisseur_FK + "', row6.Tye_Paiement_FK = '" + row6HashKey.Tye_Paiement_FK + "', row6.Produit_FK = '" + row6HashKey.Produit_FK + "'");
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
        
Var__tMap_3__Struct Var = Var__tMap_3;// ###############################
        // ###############################
        // # Output tables

ff = null;

if(!rejectedInnerJoin_tMap_3 ) {
} // closing inner join bracket (1)
// ###### START REJECTS ##### 

// # Output reject table : 'ff'
// # Filter conditions 
if( rejectedInnerJoin_tMap_3 ) {
ff_tmp.Achat_PK = out1.Achat_PK;
ff_tmp.Date_FK = out1.Date_FK;
ff_tmp.Fournisseur_FK = out1.Fournisseur_FK;
ff_tmp.Tye_Paiement_FK = out1.Tye_Paiement_FK;
ff_tmp.Produit_FK = out1.Produit_FK;
ff_tmp.Prix = out1.Prix;
ff_tmp.Quantite = out1.Quantite;
ff_tmp.Montant_Total = out1.Montant_Total;
ff = ff_tmp;
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
// Start of branch "ff"
if(ff != null) { 



	
	/**
	 * [tDBOutput_1 main ] start
	 */

	

	
	
	currentComponent="tDBOutput_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"ff"
						
						);
					}
					



        whetherReject_tDBOutput_1 = false;
                    if(ff.Date_FK == null) {
pstmt_tDBOutput_1.setNull(1, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(1, ff.Date_FK);
}

                    if(ff.Fournisseur_FK == null) {
pstmt_tDBOutput_1.setNull(2, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(2, ff.Fournisseur_FK);
}

                    if(ff.Tye_Paiement_FK == null) {
pstmt_tDBOutput_1.setNull(3, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(3, ff.Tye_Paiement_FK);
}

                    if(ff.Produit_FK == null) {
pstmt_tDBOutput_1.setNull(4, java.sql.Types.INTEGER);
} else {pstmt_tDBOutput_1.setInt(4, ff.Produit_FK);
}

                    if(ff.Prix == null) {
pstmt_tDBOutput_1.setNull(5, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(5, ff.Prix);
}

                    if(ff.Quantite == null) {
pstmt_tDBOutput_1.setNull(6, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(6, ff.Quantite);
}

                    if(ff.Montant_Total == null) {
pstmt_tDBOutput_1.setNull(7, java.sql.Types.DOUBLE);
} else {pstmt_tDBOutput_1.setDouble(7, ff.Montant_Total);
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

} // End of branch "ff"




	
	/**
	 * [tMap_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 process_data_end ] stop
 */

} // End of branch "out1"




// Start of branch "reject1"
if(reject1 != null) { 



	
	/**
	 * [tLogRow_1 main ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"reject1"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_1 = new String[8];
              
                 row_tLogRow_1[0]=    						    
				                String.valueOf(reject1.Achat_PK)			
					          ;	
										
    			   				
	    		if(reject1.Date_FK != null) { //              
                 row_tLogRow_1[1]=    						    
				                String.valueOf(reject1.Date_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Fournisseur_FK != null) { //              
                 row_tLogRow_1[2]=    						    
				                String.valueOf(reject1.Fournisseur_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Tye_Paiement_FK != null) { //              
                 row_tLogRow_1[3]=    						    
				                String.valueOf(reject1.Tye_Paiement_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Produit_FK != null) { //              
                 row_tLogRow_1[4]=    						    
				                String.valueOf(reject1.Produit_FK)			
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Prix != null) { //              
                 row_tLogRow_1[5]=    						
								FormatterUtils.formatUnwithE(reject1.Prix)
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Quantite != null) { //              
                 row_tLogRow_1[6]=    						
								FormatterUtils.formatUnwithE(reject1.Quantite)
					          ;	
							
	    		} //			
    			   				
	    		if(reject1.Montant_Total != null) { //              
                 row_tLogRow_1[7]=    						
								FormatterUtils.formatUnwithE(reject1.Montant_Total)
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

} // End of branch "reject1"




// Start of branch "rejected_date"
if(rejected_date != null) { 



	
	/**
	 * [tLogRow_2 main ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"rejected_date"
						
						);
					}
					
///////////////////////		
						

				
				String[] row_tLogRow_2 = new String[1];
   				
	    		if(rejected_date.date != null) { //              
                 row_tLogRow_2[0]=    						
								FormatterUtils.format_Date(rejected_date.date, "dd-MM-yyyy")
					          ;	
							
	    		} //			
    			 

				util_tLogRow_2.addRow(row_tLogRow_2);	
				nb_line_tLogRow_2++;
//////

//////                    
                    
///////////////////////    			

 


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
	 * [tLogRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 process_data_end ] stop
 */

} // End of branch "rejected_date"




	
	/**
	 * [tMap_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 process_data_end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row1");
			  	}
			  	
 

ok_Hash.put("tMap_2", true);
end_Hash.put("tMap_2", System.currentTimeMillis());




/**
 * [tMap_2 end ] stop
 */



	
	/**
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row9","out2");
			  	}
			  	
 

ok_Hash.put("tUnite_1", true);
end_Hash.put("tUnite_1", System.currentTimeMillis());




/**
 * [tUnite_1 end ] stop
 */

	
	/**
	 * [tMap_1 end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	


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

					
					
				
					if(tHash_Lookup_row7 != null) {
						tHash_Lookup_row7.endGet();
					}
					globalMap.remove( "tHash_Lookup_row7" );

					
					
				
					if(tHash_Lookup_row2 != null) {
						tHash_Lookup_row2.endGet();
					}
					globalMap.remove( "tHash_Lookup_row2" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row3");
			  	}
			  	
 

ok_Hash.put("tMap_1", true);
end_Hash.put("tMap_1", System.currentTimeMillis());




/**
 * [tMap_1 end ] stop
 */

	
	/**
	 * [tMap_3 end ] start
	 */

	

	
	
	currentComponent="tMap_3";

	


// ###############################
// # Lookup hashes releasing
					if(tHash_Lookup_row6 != null) {
						tHash_Lookup_row6.endGet();
					}
					globalMap.remove( "tHash_Lookup_row6" );

					
					
				
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"out1");
			  	}
			  	
 

ok_Hash.put("tMap_3", true);
end_Hash.put("tMap_3", System.currentTimeMillis());




/**
 * [tMap_3 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"ff");
			  	}
			  	
 

ok_Hash.put("tDBOutput_1", true);
end_Hash.put("tDBOutput_1", System.currentTimeMillis());




/**
 * [tDBOutput_1 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"reject1");
			  	}
			  	
 

ok_Hash.put("tLogRow_1", true);
end_Hash.put("tLogRow_1", System.currentTimeMillis());




/**
 * [tLogRow_1 end ] stop
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"rejected_date");
			  	}
			  	
 

ok_Hash.put("tLogRow_2", true);
end_Hash.put("tLogRow_2", System.currentTimeMillis());




/**
 * [tLogRow_2 end ] stop
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
					     			globalMap.remove("tHash_Lookup_row6"); 
				     			
					     			//free memory for "tMap_1"
					     			globalMap.remove("tHash_Lookup_row4"); 
				     			
					     			//free memory for "tMap_1"
					     			globalMap.remove("tHash_Lookup_row5"); 
				     			
					     			//free memory for "tMap_1"
					     			globalMap.remove("tHash_Lookup_row7"); 
				     			
					     			//free memory for "tMap_1"
					     			globalMap.remove("tHash_Lookup_row2"); 
				     			
				try{
					
	
	/**
	 * [tDBInput_7 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_7";

	

 



/**
 * [tDBInput_7 finally ] stop
 */

	
	/**
	 * [tMap_4 finally ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 finally ] stop
 */

	
	/**
	 * [tLogRow_3 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_3";

	

 



/**
 * [tLogRow_3 finally ] stop
 */

	
	/**
	 * [tFilterRow_1 finally ] start
	 */

	

	
	
	currentComponent="tFilterRow_1";

	

 



/**
 * [tFilterRow_1 finally ] stop
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
	 * [tUnite_1 finally ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

 



/**
 * [tUnite_1 finally ] stop
 */

	
	/**
	 * [tMap_1 finally ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 finally ] stop
 */

	
	/**
	 * [tMap_3 finally ] start
	 */

	

	
	
	currentComponent="tMap_3";

	

 



/**
 * [tMap_3 finally ] stop
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







	
	/**
	 * [tLogRow_1 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_1";

	

 



/**
 * [tLogRow_1 finally ] stop
 */




	
	/**
	 * [tLogRow_2 finally ] start
	 */

	

	
	
	currentComponent="tLogRow_2";

	

 



/**
 * [tLogRow_2 finally ] stop
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
	


public static class row4Struct implements routines.system.IPersistableComparableLookupRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Fournisseur_PK;

				public int getFournisseur_PK () {
					return this.Fournisseur_PK;
				}
				
			    public String Matricule_fiscale;

				public String getMatricule_fiscale () {
					return this.Matricule_fiscale;
				}
				
			    public String Nom_fournisseur;

				public String getNom_fournisseur () {
					return this.Nom_fournisseur;
				}
				
			    public String Produit_ou_Service;

				public String getProduit_ou_Service () {
					return this.Produit_ou_Service;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Nom_fournisseur == null) ? 0 : this.Nom_fournisseur.hashCode());
					
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
		
						if (this.Nom_fournisseur == null) {
							if (other.Nom_fournisseur != null)
								return false;
						
						} else if (!this.Nom_fournisseur.equals(other.Nom_fournisseur))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row4Struct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            other.Matricule_fiscale = this.Matricule_fiscale;
	            other.Nom_fournisseur = this.Nom_fournisseur;
	            other.Produit_ou_Service = this.Produit_ou_Service;
	            
	}

	public void copyKeysDataTo(row4Struct other) {

		other.Nom_fournisseur = this.Nom_fournisseur;
	            	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.Nom_fournisseur = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.Nom_fournisseur = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
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
		
			            this.Fournisseur_PK = dis.readInt();
					
						this.Matricule_fiscale = readString(dis,ois);
					
						this.Produit_ou_Service = readString(dis,ois);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Fournisseur_PK = objectIn.readInt();
					
						this.Matricule_fiscale = readString(dis,objectIn);
					
						this.Produit_ou_Service = readString(dis,objectIn);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Fournisseur_PK);
					
						writeString(this.Matricule_fiscale, dos, oos);
					
						writeString(this.Produit_ou_Service, dos, oos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Fournisseur_PK);
					
						writeString(this.Matricule_fiscale, dos, objectOut);
					
						writeString(this.Produit_ou_Service, dos, objectOut);
					
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
		sb.append("Fournisseur_PK="+String.valueOf(Fournisseur_PK));
		sb.append(",Matricule_fiscale="+Matricule_fiscale);
		sb.append(",Nom_fournisseur="+Nom_fournisseur);
		sb.append(",Produit_ou_Service="+Produit_ou_Service);
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row4Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Nom_fournisseur, other.Nom_fournisseur);
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
			   		// source node:tDBInput_3 - inputs:(after_tDBInput_7) outputs:(row4,row4) | target node:tAdvancedHash_row4 - inputs:(row4) outputs:()
			   		// linked node: tMap_1 - inputs:(row3,row4,row5,row7,row2) outputs:(out1,reject1,rejected_date)
			   
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
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:Ry0pPhIgCE7se7B7Iw919CRnAV+BH6dSlQckTlCeGDwJLNddxjY=");
				
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

		    String dbquery_tDBInput_3 = "SELECT Dim_Fournisseur.Fournisseur_PK,\n		Dim_Fournisseur.Matricule_fiscale,\n		Dim_Fournisseur.Nom_fournisseur,\n		Dim_Fo"
+"urnisseur.Produit_ou_Service\nFROM	Dim_Fournisseur";
		    

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
								row4.Fournisseur_PK = 0;
							} else {
		                          
            row4.Fournisseur_PK = rs_tDBInput_3.getInt(1);
            if(rs_tDBInput_3.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row4.Matricule_fiscale = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Matricule_fiscale = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Matricule_fiscale = tmpContent_tDBInput_3;
                }
            } else {
                row4.Matricule_fiscale = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row4.Nom_fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(3);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Nom_fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Nom_fournisseur = tmpContent_tDBInput_3;
                }
            } else {
                row4.Nom_fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row4.Produit_ou_Service = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Produit_ou_Service = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Produit_ou_Service = tmpContent_tDBInput_3;
                }
            } else {
                row4.Produit_ou_Service = null;
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
		   	   	   
				
				row4_HashRow.Fournisseur_PK = row4.Fournisseur_PK;
				
				row4_HashRow.Matricule_fiscale = row4.Matricule_fiscale;
				
				row4_HashRow.Nom_fournisseur = row4.Nom_fournisseur;
				
				row4_HashRow.Produit_ou_Service = row4.Produit_ou_Service;
				
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
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
		final row5Struct other = (row5Struct) obj;
		
						if (this.Type_paiement == null) {
							if (other.Type_paiement != null)
								return false;
						
						} else if (!this.Type_paiement.equals(other.Type_paiement))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row5Struct other) {

		other.Mode_Paiement_PK = this.Mode_Paiement_PK;
	            other.Type_paiement = this.Type_paiement;
	            
	}

	public void copyKeysDataTo(row5Struct other) {

		other.Type_paiement = this.Type_paiement;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.Type_paiement = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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
    public int compareTo(row5Struct other) {

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
			   		// source node:tDBInput_4 - inputs:(after_tDBInput_7) outputs:(row5,row5) | target node:tAdvancedHash_row5 - inputs:(row5) outputs:()
			   		// linked node: tMap_1 - inputs:(row3,row4,row5,row7,row2) outputs:(out1,reject1,rejected_date)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row5 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row5Struct> tHash_Lookup_row5 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row5Struct>getLookup(matchingModeEnum_row5);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row5", tHash_Lookup_row5);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row5 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_4 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:4DohhydGjx3mProCS2ttXhfwv4o0Srs6DVPk/XXZUdBZDU80hVQ=");
				
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
								row5.Mode_Paiement_PK = 0;
							} else {
		                          
            row5.Mode_Paiement_PK = rs_tDBInput_4.getInt(1);
            if(rs_tDBInput_4.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_4 < 2) {
								row5.Type_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_4 = rs_tDBInput_4.getString(2);
            if(tmpContent_tDBInput_4 != null) {
            	if (talendToDBList_tDBInput_4 .contains(rsmd_tDBInput_4.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row5.Type_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_4);
            	} else {
                	row5.Type_paiement = tmpContent_tDBInput_4;
                }
            } else {
                row5.Type_paiement = null;
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
	 * [tAdvancedHash_row5 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row5";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
						);
					}
					


			   
			   

					row5Struct row5_HashRow = new row5Struct();
		   	   	   
				
				row5_HashRow.Mode_Paiement_PK = row5.Mode_Paiement_PK;
				
				row5_HashRow.Type_paiement = row5.Type_paiement;
				
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
	 * [tDBInput_4 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_4";

	

 



/**
 * [tDBInput_4 finally ] stop
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
		

		globalMap.put("tDBInput_4_SUBPROCESS_STATE", 1);
	}
	


public static class row6Struct implements routines.system.IPersistableComparableLookupRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
	protected static final int DEFAULT_HASHCODE = 1;
    protected static final int PRIME = 31;
    protected int hashCode = DEFAULT_HASHCODE;
    public boolean hashCodeDirty = true;

    public String loopKey;



	
			    public int Achat_PK;

				public int getAchat_PK () {
					return this.Achat_PK;
				}
				
			    public Integer Date_FK;

				public Integer getDate_FK () {
					return this.Date_FK;
				}
				
			    public Integer Fournisseur_FK;

				public Integer getFournisseur_FK () {
					return this.Fournisseur_FK;
				}
				
			    public Integer Tye_Paiement_FK;

				public Integer getTye_Paiement_FK () {
					return this.Tye_Paiement_FK;
				}
				
			    public Integer Produit_FK;

				public Integer getProduit_FK () {
					return this.Produit_FK;
				}
				
			    public Double Prix;

				public Double getPrix () {
					return this.Prix;
				}
				
			    public Double Quantite;

				public Double getQuantite () {
					return this.Quantite;
				}
				
			    public Double Montant_Total;

				public Double getMontant_Total () {
					return this.Montant_Total;
				}
				


	@Override
	public int hashCode() {
		if (this.hashCodeDirty) {
			final int prime = PRIME;
			int result = DEFAULT_HASHCODE;
	
						result = prime * result + ((this.Date_FK == null) ? 0 : this.Date_FK.hashCode());
					
						result = prime * result + ((this.Fournisseur_FK == null) ? 0 : this.Fournisseur_FK.hashCode());
					
						result = prime * result + ((this.Tye_Paiement_FK == null) ? 0 : this.Tye_Paiement_FK.hashCode());
					
						result = prime * result + ((this.Produit_FK == null) ? 0 : this.Produit_FK.hashCode());
					
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
		
						if (this.Date_FK == null) {
							if (other.Date_FK != null)
								return false;
						
						} else if (!this.Date_FK.equals(other.Date_FK))
						
							return false;
					
						if (this.Fournisseur_FK == null) {
							if (other.Fournisseur_FK != null)
								return false;
						
						} else if (!this.Fournisseur_FK.equals(other.Fournisseur_FK))
						
							return false;
					
						if (this.Tye_Paiement_FK == null) {
							if (other.Tye_Paiement_FK != null)
								return false;
						
						} else if (!this.Tye_Paiement_FK.equals(other.Tye_Paiement_FK))
						
							return false;
					
						if (this.Produit_FK == null) {
							if (other.Produit_FK != null)
								return false;
						
						} else if (!this.Produit_FK.equals(other.Produit_FK))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row6Struct other) {

		other.Achat_PK = this.Achat_PK;
	            other.Date_FK = this.Date_FK;
	            other.Fournisseur_FK = this.Fournisseur_FK;
	            other.Tye_Paiement_FK = this.Tye_Paiement_FK;
	            other.Produit_FK = this.Produit_FK;
	            other.Prix = this.Prix;
	            other.Quantite = this.Quantite;
	            other.Montant_Total = this.Montant_Total;
	            
	}

	public void copyKeysDataTo(row6Struct other) {

		other.Date_FK = this.Date_FK;
	            	other.Fournisseur_FK = this.Fournisseur_FK;
	            	other.Tye_Paiement_FK = this.Tye_Paiement_FK;
	            	other.Produit_FK = this.Produit_FK;
	            	
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

    public void readKeysData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
						this.Date_FK = readInteger(dis);
					
						this.Fournisseur_FK = readInteger(dis);
					
						this.Tye_Paiement_FK = readInteger(dis);
					
						this.Produit_FK = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeKeysData(ObjectOutputStream dos) {
        try {

		
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeKeysData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// Integer
				
						writeInteger(this.Date_FK,dos);
					
					// Integer
				
						writeInteger(this.Fournisseur_FK,dos);
					
					// Integer
				
						writeInteger(this.Tye_Paiement_FK,dos);
					
					// Integer
				
						writeInteger(this.Produit_FK,dos);
					
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
		
			            this.Achat_PK = dis.readInt();
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = dis.readDouble();
           				}
					
			            length = dis.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = dis.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }
    
    public void readValuesData(DataInputStream dis, org.jboss.marshalling.Unmarshaller objectIn) {
        try {
			int length = 0;
		
			            this.Achat_PK = objectIn.readInt();
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Prix = null;
           				} else {
           			    	this.Prix = objectIn.readDouble();
           				}
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Quantite = null;
           				} else {
           			    	this.Quantite = objectIn.readDouble();
           				}
					
			            length = objectIn.readByte();
           				if (length == -1) {
           	    			this.Montant_Total = null;
           				} else {
           			    	this.Montant_Total = objectIn.readDouble();
           				}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

    }

    /**
     * Return a byte array which represents Values data.
     */
    public void writeValuesData(DataOutputStream dos, ObjectOutputStream oos) {
        try {

		
		            	dos.writeInt(this.Achat_PK);
					
						if(this.Prix == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Prix);
		            	}
					
						if(this.Quantite == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Quantite);
		            	}
					
						if(this.Montant_Total == null) {
			                dos.writeByte(-1);
						} else {
               				dos.writeByte(0);
           			    	dos.writeDouble(this.Montant_Total);
		            	}
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        	}

    }
    
    public void writeValuesData(DataOutputStream dos, org.jboss.marshalling.Marshaller objectOut){
                try {

		
					objectOut.writeInt(this.Achat_PK);
					
						if(this.Prix == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeDouble(this.Prix);
		            	}
					
						if(this.Quantite == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeDouble(this.Quantite);
		            	}
					
						if(this.Montant_Total == null) {
							objectOut.writeByte(-1);
						} else {
							objectOut.writeByte(0);
							objectOut.writeDouble(this.Montant_Total);
		            	}
					
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
		sb.append("Achat_PK="+String.valueOf(Achat_PK));
		sb.append(",Date_FK="+String.valueOf(Date_FK));
		sb.append(",Fournisseur_FK="+String.valueOf(Fournisseur_FK));
		sb.append(",Tye_Paiement_FK="+String.valueOf(Tye_Paiement_FK));
		sb.append(",Produit_FK="+String.valueOf(Produit_FK));
		sb.append(",Prix="+String.valueOf(Prix));
		sb.append(",Quantite="+String.valueOf(Quantite));
		sb.append(",Montant_Total="+String.valueOf(Montant_Total));
	    sb.append("]");

	    return sb.toString();
    }

    /**
     * Compare keys
     */
    public int compareTo(row6Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Date_FK, other.Date_FK);
						if(returnValue != 0) {
							return returnValue;
						}

					
						returnValue = checkNullsAndCompare(this.Fournisseur_FK, other.Fournisseur_FK);
						if(returnValue != 0) {
							return returnValue;
						}

					
						returnValue = checkNullsAndCompare(this.Tye_Paiement_FK, other.Tye_Paiement_FK);
						if(returnValue != 0) {
							return returnValue;
						}

					
						returnValue = checkNullsAndCompare(this.Produit_FK, other.Produit_FK);
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
			   		// source node:tDBInput_5 - inputs:(after_tDBInput_7) outputs:(row6,row6) | target node:tAdvancedHash_row6 - inputs:(row6) outputs:()
			   		// linked node: tMap_3 - inputs:(out1,row6) outputs:(ff)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row6 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row6Struct> tHash_Lookup_row6 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row6Struct>getLookup(matchingModeEnum_row6);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row6", tHash_Lookup_row6);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row6 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_5 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:tACEZWV9dAonNcuAY4We/4hOdFzUp2KtcDDl2o7AOIYw4jS8zqw=");
				
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

		    String dbquery_tDBInput_5 = "SELECT Fact_Achats.Achat_PK,\n		Fact_Achats.Date_FK,\n		Fact_Achats.Fournisseur_FK,\n		Fact_Achats.Tye_Paiement_FK,\n		Fact"
+"_Achats.Produit_FK,\n		Fact_Achats.Prix,\n		Fact_Achats.Quantite,\n		Fact_Achats.Montant_Total\nFROM	Fact_Achats";
		    

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
								row6.Achat_PK = 0;
							} else {
		                          
            row6.Achat_PK = rs_tDBInput_5.getInt(1);
            if(rs_tDBInput_5.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 2) {
								row6.Date_FK = null;
							} else {
		                          
            row6.Date_FK = rs_tDBInput_5.getInt(2);
            if(rs_tDBInput_5.wasNull()){
                    row6.Date_FK = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 3) {
								row6.Fournisseur_FK = null;
							} else {
		                          
            row6.Fournisseur_FK = rs_tDBInput_5.getInt(3);
            if(rs_tDBInput_5.wasNull()){
                    row6.Fournisseur_FK = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 4) {
								row6.Tye_Paiement_FK = null;
							} else {
		                          
            row6.Tye_Paiement_FK = rs_tDBInput_5.getInt(4);
            if(rs_tDBInput_5.wasNull()){
                    row6.Tye_Paiement_FK = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 5) {
								row6.Produit_FK = null;
							} else {
		                          
            row6.Produit_FK = rs_tDBInput_5.getInt(5);
            if(rs_tDBInput_5.wasNull()){
                    row6.Produit_FK = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 6) {
								row6.Prix = null;
							} else {
	                         		
            row6.Prix = rs_tDBInput_5.getDouble(6);
            if(rs_tDBInput_5.wasNull()){
                    row6.Prix = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 7) {
								row6.Quantite = null;
							} else {
	                         		
            row6.Quantite = rs_tDBInput_5.getDouble(7);
            if(rs_tDBInput_5.wasNull()){
                    row6.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_5 < 8) {
								row6.Montant_Total = null;
							} else {
	                         		
            row6.Montant_Total = rs_tDBInput_5.getDouble(8);
            if(rs_tDBInput_5.wasNull()){
                    row6.Montant_Total = null;
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
	 * [tAdvancedHash_row6 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row6";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					


			   
			   

					row6Struct row6_HashRow = new row6Struct();
		   	   	   
				
				row6_HashRow.Achat_PK = row6.Achat_PK;
				
				row6_HashRow.Date_FK = row6.Date_FK;
				
				row6_HashRow.Fournisseur_FK = row6.Fournisseur_FK;
				
				row6_HashRow.Tye_Paiement_FK = row6.Tye_Paiement_FK;
				
				row6_HashRow.Produit_FK = row6.Produit_FK;
				
				row6_HashRow.Prix = row6.Prix;
				
				row6_HashRow.Quantite = row6.Quantite;
				
				row6_HashRow.Montant_Total = row6.Montant_Total;
				
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
	 * [tDBInput_5 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_5";

	

 



/**
 * [tDBInput_5 finally ] stop
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
		

		globalMap.put("tDBInput_5_SUBPROCESS_STATE", 1);
	}
	


public static class row7Struct implements routines.system.IPersistableComparableLookupRow<row7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
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
		final row7Struct other = (row7Struct) obj;
		
						if (this.Nom_produit == null) {
							if (other.Nom_produit != null)
								return false;
						
						} else if (!this.Nom_produit.equals(other.Nom_produit))
						
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
	            other.date_debut = this.date_debut;
	            other.date_fin = this.date_fin;
	            other.actif = this.actif;
	            
	}

	public void copyKeysDataTo(row7Struct other) {

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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.Nom_produit = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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
    public int compareTo(row7Struct other) {

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
			   		// source node:tDBInput_6 - inputs:(after_tDBInput_7) outputs:(row7,row7) | target node:tAdvancedHash_row7 - inputs:(row7) outputs:()
			   		// linked node: tMap_1 - inputs:(row3,row4,row5,row7,row2) outputs:(out1,reject1,rejected_date)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row7 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row7Struct> tHash_Lookup_row7 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row7Struct>getLookup(matchingModeEnum_row7);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row7", tHash_Lookup_row7);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row7 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_6 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:FV4K9fBFdR8ql7qwP0DlAWMrQ8UIDW/9zER0PORxikxqzBduAjQ=");
				
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

		    String dbquery_tDBInput_6 = "SELECT Dim_Produit.Produit_PK,\n		Dim_Produit.UGS,\n		Dim_Produit.Nom_produit,\n		Dim_Produit.Description,\n		Dim_Produit.U"
+"rl,\n		Dim_Produit.Visibilite,\n		Dim_Produit.Prix,\n		Dim_Produit.Prix_en_promo,\n		Dim_Produit.En_Promo,\n		Dim_Produit.Can"
+"al,\n		Dim_Produit.Source,\n		Dim_Produit.date_debut,\n		Dim_Produit.date_fin,\n		Dim_Produit.actif\nFROM	Dim_Produit";
		    

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
								row7.Produit_PK = 0;
							} else {
		                          
            row7.Produit_PK = rs_tDBInput_6.getInt(1);
            if(rs_tDBInput_6.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 2) {
								row7.UGS = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(2);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.UGS = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.UGS = tmpContent_tDBInput_6;
                }
            } else {
                row7.UGS = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 3) {
								row7.Nom_produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(3);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Nom_produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.Nom_produit = tmpContent_tDBInput_6;
                }
            } else {
                row7.Nom_produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 4) {
								row7.Description = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(4);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Description = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.Description = tmpContent_tDBInput_6;
                }
            } else {
                row7.Description = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 5) {
								row7.Url = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(5);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Url = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.Url = tmpContent_tDBInput_6;
                }
            } else {
                row7.Url = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 6) {
								row7.Visibilite = null;
							} else {
	                         		
            row7.Visibilite = rs_tDBInput_6.getBoolean(6);
            if(rs_tDBInput_6.wasNull()){
                    row7.Visibilite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 7) {
								row7.Prix = null;
							} else {
	                         		
            row7.Prix = rs_tDBInput_6.getDouble(7);
            if(rs_tDBInput_6.wasNull()){
                    row7.Prix = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 8) {
								row7.Prix_en_promo = null;
							} else {
	                         		
            row7.Prix_en_promo = rs_tDBInput_6.getDouble(8);
            if(rs_tDBInput_6.wasNull()){
                    row7.Prix_en_promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 9) {
								row7.En_Promo = null;
							} else {
	                         		
            row7.En_Promo = rs_tDBInput_6.getBoolean(9);
            if(rs_tDBInput_6.wasNull()){
                    row7.En_Promo = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 10) {
								row7.Canal = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(10);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Canal = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.Canal = tmpContent_tDBInput_6;
                }
            } else {
                row7.Canal = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 11) {
								row7.Source = null;
							} else {
	                         		
           		tmpContent_tDBInput_6 = rs_tDBInput_6.getString(11);
            if(tmpContent_tDBInput_6 != null) {
            	if (talendToDBList_tDBInput_6 .contains(rsmd_tDBInput_6.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row7.Source = FormatterUtils.formatUnwithE(tmpContent_tDBInput_6);
            	} else {
                	row7.Source = tmpContent_tDBInput_6;
                }
            } else {
                row7.Source = null;
            }
		                    }
							if(colQtyInRs_tDBInput_6 < 12) {
								row7.date_debut = null;
							} else {
										
			row7.date_debut = mssqlGTU_tDBInput_6.getDate(rsmd_tDBInput_6, rs_tDBInput_6, 12);
			
		                    }
							if(colQtyInRs_tDBInput_6 < 13) {
								row7.date_fin = null;
							} else {
										
			row7.date_fin = mssqlGTU_tDBInput_6.getDate(rsmd_tDBInput_6, rs_tDBInput_6, 13);
			
		                    }
							if(colQtyInRs_tDBInput_6 < 14) {
								row7.actif = null;
							} else {
		                          
            row7.actif = rs_tDBInput_6.getInt(14);
            if(rs_tDBInput_6.wasNull()){
                    row7.actif = null;
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
	 * [tAdvancedHash_row7 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row7";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					


			   
			   

					row7Struct row7_HashRow = new row7Struct();
		   	   	   
				
				row7_HashRow.Produit_PK = row7.Produit_PK;
				
				row7_HashRow.UGS = row7.UGS;
				
				row7_HashRow.Nom_produit = row7.Nom_produit;
				
				row7_HashRow.Description = row7.Description;
				
				row7_HashRow.Url = row7.Url;
				
				row7_HashRow.Visibilite = row7.Visibilite;
				
				row7_HashRow.Prix = row7.Prix;
				
				row7_HashRow.Prix_en_promo = row7.Prix_en_promo;
				
				row7_HashRow.En_Promo = row7.En_Promo;
				
				row7_HashRow.Canal = row7.Canal;
				
				row7_HashRow.Source = row7.Source;
				
				row7_HashRow.date_debut = row7.date_debut;
				
				row7_HashRow.date_fin = row7.date_fin;
				
				row7_HashRow.actif = row7.actif;
				
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
	 * [tDBInput_6 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_6";

	

 



/**
 * [tDBInput_6 finally ] stop
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
		

		globalMap.put("tDBInput_6_SUBPROCESS_STATE", 1);
	}
	


public static class row2Struct implements routines.system.IPersistableComparableLookupRow<row2Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Fact_Achat = new byte[0];
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
		final row2Struct other = (row2Struct) obj;
		
						if (this.Date == null) {
							if (other.Date != null)
								return false;
						
						} else if (!this.Date.equals(other.Date))
						
							return false;
					

		return true;
    }

	public void copyDataTo(row2Struct other) {

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

	public void copyKeysDataTo(row2Struct other) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

        	try {

        		int length = 0;
		
					this.Date = readDate(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readKeysData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Fact_Achat) {

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
    public int compareTo(row2Struct other) {

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



		row2Struct row2 = new row2Struct();




	
	/**
	 * [tAdvancedHash_row2 begin ] start
	 */

	

	
		
		ok_Hash.put("tAdvancedHash_row2", false);
		start_Hash.put("tAdvancedHash_row2", System.currentTimeMillis());
		
	
	currentComponent="tAdvancedHash_row2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row2");
					}
				
		int tos_count_tAdvancedHash_row2 = 0;
		

			   		// connection name:row2
			   		// source node:tDBInput_2 - inputs:(after_tDBInput_7) outputs:(row2,row2) | target node:tAdvancedHash_row2 - inputs:(row2) outputs:()
			   		// linked node: tMap_1 - inputs:(row3,row4,row5,row7,row2) outputs:(out1,reject1,rejected_date)
			   
			   		org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE matchingModeEnum_row2 = 
			   			org.talend.designer.components.lookup.common.ICommonLookup.MATCHING_MODE.UNIQUE_MATCH;
			   			
			   
	   			org.talend.designer.components.lookup.memory.AdvancedMemoryLookup<row2Struct> tHash_Lookup_row2 =org.talend.designer.components.lookup.memory.AdvancedMemoryLookup.
	   						<row2Struct>getLookup(matchingModeEnum_row2);
	   						   
		   	   	   globalMap.put("tHash_Lookup_row2", tHash_Lookup_row2);
		   	   	   
				
           

 



/**
 * [tAdvancedHash_row2 begin ] stop
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
				String dbUser_tDBInput_2 = "sa";
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:g6L9pr1Qg5RojcpIkdwA85Wm0UCTV+htgKnfxWVEC7kAvRMX1b0=");
				
				String dbPwd_tDBInput_2 = decryptedPassword_tDBInput_2;
				
		    String port_tDBInput_2 = "1433";
		    String dbname_tDBInput_2 = "DW_E-Commerce" ;
			String url_tDBInput_2 = "jdbc:jtds:sqlserver://" + "localhost" ;
		    if (!"".equals(port_tDBInput_2)) {
		    	url_tDBInput_2 += ":" + "1433";
		    }
		    if (!"".equals(dbname_tDBInput_2)) {
				url_tDBInput_2 += "//" + "DW_E-Commerce"; 
		    }
		    url_tDBInput_2 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";
		    String dbschema_tDBInput_2 = "";
				
				conn_tDBInput_2 = java.sql.DriverManager.getConnection(url_tDBInput_2,dbUser_tDBInput_2,dbPwd_tDBInput_2);
		        
		    
			java.sql.Statement stmt_tDBInput_2 = conn_tDBInput_2.createStatement();

		    String dbquery_tDBInput_2 = "SELECT DimDates.Date_PK,\n		DimDates.Date,\n		DimDates.Jour_Moi_Annee,\n		DimDates.Annee,\n		DimDates.ID_Semestre,\n		DimDat"
+"es.Semestre,\n		DimDates.ID_Trimestre,\n		DimDates.Trimestre,\n		DimDates.ID_Mois,\n		DimDates.Mois,\n		DimDates.Lib_Mois,\n		"
+"DimDates.Jour,\n		DimDates.Id_Lib_Jour,\n		DimDates.Lib_Jour,\n		DimDates.Semaine,\n		DimDates.JourDeAnnee,\n		DimDates.Jour_"
+"mois_lettre\nFROM	DimDates";
		    

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
								row2.Date_PK = 0;
							} else {
		                          
            row2.Date_PK = rs_tDBInput_2.getInt(1);
            if(rs_tDBInput_2.wasNull()){
                    throw new RuntimeException("Null value in non-Nullable column");
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row2.Date = null;
							} else {
										
			row2.Date = mssqlGTU_tDBInput_2.getDate(rsmd_tDBInput_2, rs_tDBInput_2, 2);
			
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row2.Jour_Moi_Annee = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(3);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Jour_Moi_Annee = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Jour_Moi_Annee = tmpContent_tDBInput_2;
                }
            } else {
                row2.Jour_Moi_Annee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row2.Annee = null;
							} else {
		                          
            row2.Annee = rs_tDBInput_2.getInt(4);
            if(rs_tDBInput_2.wasNull()){
                    row2.Annee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row2.ID_Semestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.ID_Semestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.ID_Semestre = tmpContent_tDBInput_2;
                }
            } else {
                row2.ID_Semestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row2.Semestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(6);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Semestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Semestre = tmpContent_tDBInput_2;
                }
            } else {
                row2.Semestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row2.ID_Trimestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.ID_Trimestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.ID_Trimestre = tmpContent_tDBInput_2;
                }
            } else {
                row2.ID_Trimestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row2.Trimestre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Trimestre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Trimestre = tmpContent_tDBInput_2;
                }
            } else {
                row2.Trimestre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row2.ID_Mois = null;
							} else {
		                          
            row2.ID_Mois = rs_tDBInput_2.getInt(9);
            if(rs_tDBInput_2.wasNull()){
                    row2.ID_Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row2.Mois = null;
							} else {
		                          
            row2.Mois = rs_tDBInput_2.getInt(10);
            if(rs_tDBInput_2.wasNull()){
                    row2.Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 11) {
								row2.Lib_Mois = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(11);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Lib_Mois = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Lib_Mois = tmpContent_tDBInput_2;
                }
            } else {
                row2.Lib_Mois = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 12) {
								row2.Jour = null;
							} else {
		                          
            row2.Jour = rs_tDBInput_2.getInt(12);
            if(rs_tDBInput_2.wasNull()){
                    row2.Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 13) {
								row2.Id_Lib_Jour = null;
							} else {
		                          
            row2.Id_Lib_Jour = rs_tDBInput_2.getInt(13);
            if(rs_tDBInput_2.wasNull()){
                    row2.Id_Lib_Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 14) {
								row2.Lib_Jour = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(14);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Lib_Jour = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Lib_Jour = tmpContent_tDBInput_2;
                }
            } else {
                row2.Lib_Jour = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 15) {
								row2.Semaine = null;
							} else {
		                          
            row2.Semaine = rs_tDBInput_2.getInt(15);
            if(rs_tDBInput_2.wasNull()){
                    row2.Semaine = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 16) {
								row2.JourDeAnnee = null;
							} else {
		                          
            row2.JourDeAnnee = rs_tDBInput_2.getInt(16);
            if(rs_tDBInput_2.wasNull()){
                    row2.JourDeAnnee = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 17) {
								row2.Jour_mois_lettre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(17);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row2.Jour_mois_lettre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row2.Jour_mois_lettre = tmpContent_tDBInput_2;
                }
            } else {
                row2.Jour_mois_lettre = null;
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
	 * [tAdvancedHash_row2 main ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row2"
						
						);
					}
					


			   
			   

					row2Struct row2_HashRow = new row2Struct();
		   	   	   
				
				row2_HashRow.Date_PK = row2.Date_PK;
				
				row2_HashRow.Date = row2.Date;
				
				row2_HashRow.Jour_Moi_Annee = row2.Jour_Moi_Annee;
				
				row2_HashRow.Annee = row2.Annee;
				
				row2_HashRow.ID_Semestre = row2.ID_Semestre;
				
				row2_HashRow.Semestre = row2.Semestre;
				
				row2_HashRow.ID_Trimestre = row2.ID_Trimestre;
				
				row2_HashRow.Trimestre = row2.Trimestre;
				
				row2_HashRow.ID_Mois = row2.ID_Mois;
				
				row2_HashRow.Mois = row2.Mois;
				
				row2_HashRow.Lib_Mois = row2.Lib_Mois;
				
				row2_HashRow.Jour = row2.Jour;
				
				row2_HashRow.Id_Lib_Jour = row2.Id_Lib_Jour;
				
				row2_HashRow.Lib_Jour = row2.Lib_Jour;
				
				row2_HashRow.Semaine = row2.Semaine;
				
				row2_HashRow.JourDeAnnee = row2.JourDeAnnee;
				
				row2_HashRow.Jour_mois_lettre = row2.Jour_mois_lettre;
				
			tHash_Lookup_row2.put(row2_HashRow);
			
            




 


	tos_count_tAdvancedHash_row2++;

/**
 * [tAdvancedHash_row2 main ] stop
 */
	
	/**
	 * [tAdvancedHash_row2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row2";

	

 



/**
 * [tAdvancedHash_row2 process_data_begin ] stop
 */
	
	/**
	 * [tAdvancedHash_row2 process_data_end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row2";

	

 



/**
 * [tAdvancedHash_row2 process_data_end ] stop
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
	 * [tAdvancedHash_row2 end ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row2";

	

tHash_Lookup_row2.endPut();

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row2");
			  	}
			  	
 

ok_Hash.put("tAdvancedHash_row2", true);
end_Hash.put("tAdvancedHash_row2", System.currentTimeMillis());




/**
 * [tAdvancedHash_row2 end ] stop
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
	 * [tAdvancedHash_row2 finally ] start
	 */

	

	
	
	currentComponent="tAdvancedHash_row2";

	

 



/**
 * [tAdvancedHash_row2 finally ] stop
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
        final ETL_Fact_Achat ETL_Fact_AchatClass = new ETL_Fact_Achat();

        int exitCode = ETL_Fact_AchatClass.runJobInTOS(args);

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
            java.io.InputStream inContext = ETL_Fact_Achat.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/etl_fact_achat_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ETL_Fact_Achat.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
errorCode = null;tDBInput_7Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tDBInput_7) {
globalMap.put("tDBInput_7_SUBPROCESS_STATE", -1);

e_tDBInput_7.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ETL_Fact_Achat");
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
 *     441473 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:46 CET
 ************************************************************************************************/