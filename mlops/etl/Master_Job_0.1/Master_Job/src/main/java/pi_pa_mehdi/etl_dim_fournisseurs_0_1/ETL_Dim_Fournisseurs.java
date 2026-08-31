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


package pi_pa_mehdi.etl_dim_fournisseurs_0_1;

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
 




	//the import part of tJava_1
	//import java.util.List;

	//the import part of tJava_2
	//import java.util.List;


@SuppressWarnings("unused")

/**
 * Job: ETL_Dim_Fournisseurs Purpose: <br>
 * Description:  <br>
 * @author user@talend.com
 * @version 8.0.1.20211109_1610
 * @status 
 */
public class ETL_Dim_Fournisseurs implements TalendJob {

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
			
			if(Server != null){
				
					this.setProperty("Server", Server.toString());
				
			}
			
			if(Database != null){
				
					this.setProperty("Database", Database.toString());
				
			}
			
			if(Port != null){
				
					this.setProperty("Port", Port.toString());
				
			}
			
			if(User != null){
				
					this.setProperty("User", User.toString());
				
			}
			
			if(Password != null){
				
					this.setProperty("Password", Password.toString());
				
			}
			
			if(Schema != null){
				
					this.setProperty("Schema", Schema.toString());
				
			}
			
			if(AddParms != null){
				
					this.setProperty("AddParms", AddParms.toString());
				
			}
			
			if(ServerName != null){
				
					this.setProperty("ServerName", ServerName.toString());
				
			}
			
			if(AdditinalParms != null){
				
					this.setProperty("AdditinalParms", AdditinalParms.toString());
				
			}
			
		}
		
		//if the stored or passed value is "<TALEND_NULL>" string, it mean null
		public String getStringValue(String key) {
			String origin_value = this.getProperty(key);
			if(NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY.equals(origin_value)) {
				return null;
			}
			return origin_value;
		}

public String Server;
public String getServer(){
	return this.Server;
}
public String Database;
public String getDatabase(){
	return this.Database;
}
public String Port;
public String getPort(){
	return this.Port;
}
public String User;
public String getUser(){
	return this.User;
}
public java.lang.String Password;
public java.lang.String getPassword(){
	return this.Password;
}
public String Schema;
public String getSchema(){
	return this.Schema;
}
public String AddParms;
public String getAddParms(){
	return this.AddParms;
}
public String ServerName;
public String getServerName(){
	return this.ServerName;
}
public String AdditinalParms;
public String getAdditinalParms(){
	return this.AdditinalParms;
}
	}
	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.
	public ContextProperties getContext() {
		return this.context;
	}
	private final String jobVersion = "0.1";
	private final String jobName = "ETL_Dim_Fournisseurs";
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
				ETL_Dim_Fournisseurs.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(ETL_Dim_Fournisseurs.this, new Object[] { e , currentComponent, globalMap});
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

			public void tPrejob_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tPrejob_1_onSubJobError(exception, errorComponent, globalMap);
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
			
			public void tJava_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_1_onSubJobError(exception, errorComponent, globalMap);
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
			
			public void tDBInput_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFilterRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUnite_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBSCD_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tDBInput_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tUniqRow_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tMap_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tDBInput_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tPrejob_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

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
			public void tJava_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tChronometerStop_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tDBInput_3_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	





public void tPrejob_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tPrejob_1_SUBPROCESS_STATE", 0);

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
	 * [tPrejob_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tPrejob_1", false);
		start_Hash.put("tPrejob_1", System.currentTimeMillis());
		
	
	currentComponent="tPrejob_1";

	
		int tos_count_tPrejob_1 = 0;
		

 



/**
 * [tPrejob_1 begin ] stop
 */
	
	/**
	 * [tPrejob_1 main ] start
	 */

	

	
	
	currentComponent="tPrejob_1";

	

 


	tos_count_tPrejob_1++;

/**
 * [tPrejob_1 main ] stop
 */
	
	/**
	 * [tPrejob_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tPrejob_1";

	

 



/**
 * [tPrejob_1 process_data_begin ] stop
 */
	
	/**
	 * [tPrejob_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tPrejob_1";

	

 



/**
 * [tPrejob_1 process_data_end ] stop
 */
	
	/**
	 * [tPrejob_1 end ] start
	 */

	

	
	
	currentComponent="tPrejob_1";

	

 

ok_Hash.put("tPrejob_1", true);
end_Hash.put("tPrejob_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk1", 0, "ok");
				}
				tChronometerStart_1Process(globalMap);



/**
 * [tPrejob_1 end ] stop
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
	 * [tPrejob_1 finally ] start
	 */

	

	
	
	currentComponent="tPrejob_1";

	

 



/**
 * [tPrejob_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tPrejob_1_SUBPROCESS_STATE", 1);
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
									runStat.updateStatOnConnection("OnSubjobOk1", 0, "ok");
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
	
	
		 
	final String decryptedPassword_tDBConnection_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:KMaHpDxb7SOh4Wd9Vve3AXSMhuR/mpgwK3xRDv2PenPW23GOiGI=");
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
									runStat.updateStatOnConnection("OnSubjobOk2", 0, "ok");
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
		String dbname_tDBConnection_2 = "DW_E-Commerce" ;
    	if (!"".equals(port_tDBConnection_2)) {
    		url_tDBConnection_2 += ":" + "1433";
    	}
    	if (!"".equals(dbname_tDBConnection_2)) {
    		
				url_tDBConnection_2 += "//" + "DW_E-Commerce"; 
    	}

		url_tDBConnection_2 += ";appName=" + projectName + ";" + ";instance=MSSQLDEVELOPER";  
	String dbUser_tDBConnection_2 = "sa";
	
	
		 
	final String decryptedPassword_tDBConnection_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:1APPrBQ5DyBLgM0Def7jriywvjz45tTNSFp080ECBGZQsJmu/10=");
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

	globalMap.put("db_tDBConnection_2",  "DW_E-Commerce");
	
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
									runStat.updateStatOnConnection("OnSubjobOk3", 0, "ok");
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
									runStat.updateStatOnConnection("OnSubjobOk4", 0, "ok");
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
	


public static class finalOt1Struct implements routines.system.IPersistableRow<finalOt1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
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
	
							result = prime * result + (int) this.Fournisseur_PK;
						
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
		final finalOt1Struct other = (finalOt1Struct) obj;
		
						if (this.Fournisseur_PK != other.Fournisseur_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(finalOt1Struct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            other.Matricule_fiscale = this.Matricule_fiscale;
	            other.Nom_fournisseur = this.Nom_fournisseur;
	            other.Produit_ou_Service = this.Produit_ou_Service;
	            
	}

	public void copyKeysDataTo(finalOt1Struct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(finalOt1Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Fournisseur_PK, other.Fournisseur_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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

public static class aaaStruct implements routines.system.IPersistableRow<aaaStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
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
	
							result = prime * result + (int) this.Fournisseur_PK;
						
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
		final aaaStruct other = (aaaStruct) obj;
		
						if (this.Fournisseur_PK != other.Fournisseur_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(aaaStruct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            other.Matricule_fiscale = this.Matricule_fiscale;
	            other.Nom_fournisseur = this.Nom_fournisseur;
	            other.Produit_ou_Service = this.Produit_ou_Service;
	            
	}

	public void copyKeysDataTo(aaaStruct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(aaaStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Fournisseur_PK, other.Fournisseur_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

public static class row7Struct implements routines.system.IPersistableRow<row7Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

public static class row4Struct implements routines.system.IPersistableRow<row4Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

public static class fournisseurStruct implements routines.system.IPersistableRow<fournisseurStruct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
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
	
							result = prime * result + (int) this.Fournisseur_PK;
						
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
		final fournisseurStruct other = (fournisseurStruct) obj;
		
						if (this.Fournisseur_PK != other.Fournisseur_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(fournisseurStruct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            other.Matricule_fiscale = this.Matricule_fiscale;
	            other.Nom_fournisseur = this.Nom_fournisseur;
	            other.Produit_ou_Service = this.Produit_ou_Service;
	            
	}

	public void copyKeysDataTo(fournisseurStruct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(fournisseurStruct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Fournisseur_PK, other.Fournisseur_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
			    public String ID_Fournisseur;

				public String getID_Fournisseur () {
					return this.ID_Fournisseur;
				}
				
			    public String Nom_Fournisseur;

				public String getNom_Fournisseur () {
					return this.Nom_Fournisseur;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String Produits_ou_Services;

				public String getProduits_ou_Services () {
					return this.Produits_ou_Services;
				}
				
			    public String Notes;

				public String getNotes () {
					return this.Notes;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
					this.ID_Fournisseur = readString(dis);
					
					this.Nom_Fournisseur = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Produits_ou_Services = readString(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
					this.ID_Fournisseur = readString(dis);
					
					this.Nom_Fournisseur = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Produits_ou_Services = readString(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.ID_Fournisseur,dos);
					
					// String
				
						writeString(this.Nom_Fournisseur,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Produits_ou_Services,dos);
					
					// String
				
						writeString(this.Notes,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.ID_Fournisseur,dos);
					
					// String
				
						writeString(this.Nom_Fournisseur,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Produits_ou_Services,dos);
					
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
		sb.append("ID_Fournisseur="+ID_Fournisseur);
		sb.append(",Nom_Fournisseur="+Nom_Fournisseur);
		sb.append(",Adresse="+Adresse);
		sb.append(",Produits_ou_Services="+Produits_ou_Services);
		sb.append(",Notes="+Notes);
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

public static class row1Struct implements routines.system.IPersistableRow<row1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
			    public String ID_Fournisseur;

				public String getID_Fournisseur () {
					return this.ID_Fournisseur;
				}
				
			    public String Nom_Fournisseur;

				public String getNom_Fournisseur () {
					return this.Nom_Fournisseur;
				}
				
			    public String Adresse;

				public String getAdresse () {
					return this.Adresse;
				}
				
			    public String Produits_ou_Services;

				public String getProduits_ou_Services () {
					return this.Produits_ou_Services;
				}
				
			    public String Notes;

				public String getNotes () {
					return this.Notes;
				}
				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
					this.ID_Fournisseur = readString(dis);
					
					this.Nom_Fournisseur = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Produits_ou_Services = readString(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
					this.ID_Fournisseur = readString(dis);
					
					this.Nom_Fournisseur = readString(dis);
					
					this.Adresse = readString(dis);
					
					this.Produits_ou_Services = readString(dis);
					
					this.Notes = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.ID_Fournisseur,dos);
					
					// String
				
						writeString(this.Nom_Fournisseur,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Produits_ou_Services,dos);
					
					// String
				
						writeString(this.Notes,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.ID_Fournisseur,dos);
					
					// String
				
						writeString(this.Nom_Fournisseur,dos);
					
					// String
				
						writeString(this.Adresse,dos);
					
					// String
				
						writeString(this.Produits_ou_Services,dos);
					
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
		sb.append("ID_Fournisseur="+ID_Fournisseur);
		sb.append(",Nom_Fournisseur="+Nom_Fournisseur);
		sb.append(",Adresse="+Adresse);
		sb.append(",Produits_ou_Services="+Produits_ou_Services);
		sb.append(",Notes="+Notes);
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

public static class out1Struct implements routines.system.IPersistableRow<out1Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
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
	
							result = prime * result + (int) this.Fournisseur_PK;
						
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
		
						if (this.Fournisseur_PK != other.Fournisseur_PK)
							return false;
					

		return true;
    }

	public void copyDataTo(out1Struct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            other.Matricule_fiscale = this.Matricule_fiscale;
	            other.Nom_fournisseur = this.Nom_fournisseur;
	            other.Produit_ou_Service = this.Produit_ou_Service;
	            
	}

	public void copyKeysDataTo(out1Struct other) {

		other.Fournisseur_PK = this.Fournisseur_PK;
	            	
	}




	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

        	try {

        		int length = 0;
		
			        this.Fournisseur_PK = dis.readInt();
					
					this.Matricule_fiscale = readString(dis);
					
					this.Nom_fournisseur = readString(dis);
					
					this.Produit_ou_Service = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// int
				
		            	dos.writeInt(this.Fournisseur_PK);
					
					// String
				
						writeString(this.Matricule_fiscale,dos);
					
					// String
				
						writeString(this.Nom_fournisseur,dos);
					
					// String
				
						writeString(this.Produit_ou_Service,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


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
    public int compareTo(out1Struct other) {

		int returnValue = -1;
		
						returnValue = checkNullsAndCompare(this.Fournisseur_PK, other.Fournisseur_PK);
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
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

public static class row6Struct implements routines.system.IPersistableRow<row6Struct> {
    final static byte[] commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];
    static byte[] commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[0];

	
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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
			if(length > commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length) {
				if(length < 1024 && commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs.length == 0) {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[1024];
				} else {
   					commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length);
			strReturn = new String(commonByteArray_PI_PA_MEHDI_ETL_Dim_Fournisseurs, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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

		synchronized(commonByteArrayLock_PI_PA_MEHDI_ETL_Dim_Fournisseurs) {

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
row7Struct row7 = new row7Struct();
row5Struct row5 = new row5Struct();
aaaStruct aaa = new aaaStruct();

		row1Struct row1 = new row1Struct();
row3Struct row3 = new row3Struct();
fournisseurStruct fournisseur = new fournisseurStruct();

		row6Struct row6 = new row6Struct();
row8Struct row8 = new row8Struct();
out1Struct out1 = new out1Struct();

			row2Struct row2 = new row2Struct();
finalOt1Struct finalOt1 = new finalOt1Struct();





	
	/**
	 * [tDBSCD_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tDBSCD_1", false);
		start_Hash.put("tDBSCD_1", System.currentTimeMillis());
		
	
	currentComponent="tDBSCD_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"finalOt1");
					}
				
		int tos_count_tDBSCD_1 = 0;
		



        class SCDSK_tDBSCD_1 {
private int hashCode;
public boolean hashCodeDirty = true;
String Nom_fournisseur;
public boolean equals(Object obj) {
if (this == obj) return true;
if (obj == null) return false;
if (getClass() != obj.getClass()) return false;
final SCDSK_tDBSCD_1 other = (SCDSK_tDBSCD_1) obj;
if (this.Nom_fournisseur == null) {
if (other.Nom_fournisseur!= null)
return false;
} else if (!this.Nom_fournisseur.equals(other.Nom_fournisseur))
return false;

return true;
}
public int hashCode() {
if(hashCodeDirty) {
int prime = 31;hashCode = prime * hashCode + (Nom_fournisseur == null ? 0 : Nom_fournisseur.hashCode());
hashCodeDirty = false;
}
return hashCode;
}
}

    class SCDStruct_tDBSCD_1 {
private String Produit_ou_Service;
private String Matricule_fiscale;
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

 
	final String decryptedPassword_tDBSCD_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:voP6Xwx0jdk1U8VlfuevwPpiapNwHrgNnU5WzVRrMlAptHx8EZo=");

    String dbPwd_tDBSCD_1 = decryptedPassword_tDBSCD_1;	
    conn_tDBSCD_1 = java.sql.DriverManager.getConnection(url_tDBSCD_1,dbUser_tDBSCD_1,dbPwd_tDBSCD_1);
	

    if(dbschema_tDBSCD_1 == null || dbschema_tDBSCD_1.trim().length() == 0) {
        tableName_tDBSCD_1 = "Dim_Fournisseur";
    } else {
        tableName_tDBSCD_1 = dbschema_tDBSCD_1 + "].[" + "Dim_Fournisseur";
    }
	org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBSCD_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory.getMSSqlGenerateTimestampUtil();	
    String tmpValue_tDBSCD_1 = null;    
        String search_tDBSCD_1 = "SELECT [Nom_fournisseur], [Produit_ou_Service], [Matricule_fiscale] FROM [" + tableName_tDBSCD_1 + "]";
        java.sql.Statement statement_tDBSCD_1 = conn_tDBSCD_1.createStatement();
        java.sql.ResultSet resultSet_tDBSCD_1 = statement_tDBSCD_1.executeQuery(search_tDBSCD_1);
        java.util.Map<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1> cache_tDBSCD_1 = new java.util.HashMap<SCDSK_tDBSCD_1, SCDStruct_tDBSCD_1>();
        while(resultSet_tDBSCD_1.next()) {
            SCDSK_tDBSCD_1 sk_tDBSCD_1 = new SCDSK_tDBSCD_1();
            SCDStruct_tDBSCD_1 row_tDBSCD_1 = new SCDStruct_tDBSCD_1();
                    if(resultSet_tDBSCD_1.getObject(1) != null) {
                        sk_tDBSCD_1.Nom_fournisseur = resultSet_tDBSCD_1.getString(1);
                    }
                    if(resultSet_tDBSCD_1.getObject(2) != null) {
                        row_tDBSCD_1.Produit_ou_Service = resultSet_tDBSCD_1.getString(2);
                    }
                    if(resultSet_tDBSCD_1.getObject(3) != null) {
                        row_tDBSCD_1.Matricule_fiscale = resultSet_tDBSCD_1.getString(3);
                    }
            cache_tDBSCD_1.put(sk_tDBSCD_1, row_tDBSCD_1);
        }
        resultSet_tDBSCD_1.close();
        statement_tDBSCD_1.close();
    String insertionSQL_tDBSCD_1 = "INSERT INTO [" + tableName_tDBSCD_1 + "]([Nom_fournisseur], [Produit_ou_Service], [Matricule_fiscale]) VALUES(?, ?, ?)";
    java.sql.PreparedStatement insertionStatement_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(insertionSQL_tDBSCD_1);
        String updateSQLForType1_tDBSCD_1 = "UPDATE [" + tableName_tDBSCD_1 + "] SET [Produit_ou_Service] = ?, [Matricule_fiscale] = ? WHERE [Nom_fournisseur] = ?";
        java.sql.PreparedStatement updateForType1_tDBSCD_1 = conn_tDBSCD_1.prepareStatement(updateSQLForType1_tDBSCD_1);        
    
        SCDSK_tDBSCD_1 lookUpKey_tDBSCD_1 = null;        
    SCDStruct_tDBSCD_1 lookUpValue_tDBSCD_1 = null;

 



/**
 * [tDBSCD_1 begin ] stop
 */



	
	/**
	 * [tMap_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tMap_3", false);
		start_Hash.put("tMap_3", System.currentTimeMillis());
		
	
	currentComponent="tMap_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row2");
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
finalOt1Struct finalOt1_tmp = new finalOt1Struct();
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"aaa","fournisseur","out1");
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row5");
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
aaaStruct aaa_tmp = new aaaStruct();
// ###############################

        
        



        









 



/**
 * [tMap_2 begin ] stop
 */



	
	/**
	 * [tUniqRow_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_2", false);
		start_Hash.put("tUniqRow_2", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row7");
					}
				
		int tos_count_tUniqRow_2 = 0;
		

int nb_uniques_tUniqRow_2 = 0;
int nb_duplicates_tUniqRow_2 = 0; 

 



/**
 * [tUniqRow_2 begin ] stop
 */



	
	/**
	 * [tFilterRow_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tFilterRow_2", false);
		start_Hash.put("tFilterRow_2", System.currentTimeMillis());
		
	
	currentComponent="tFilterRow_2";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row4");
					}
				
		int tos_count_tFilterRow_2 = 0;
		
    int nb_line_tFilterRow_2 = 0;
    int nb_line_ok_tFilterRow_2 = 0;
    int nb_line_reject_tFilterRow_2 = 0;

    class Operator_tFilterRow_2 {
      private String sErrorMsg = "";
      private boolean bMatchFlag = true;
      private String sUnionFlag = "&&";

      public Operator_tFilterRow_2(String unionFlag){
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
 * [tFilterRow_2 begin ] stop
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
				
				 
	final String decryptedPassword_tDBInput_3 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:ig3tykRZNCBuGiy7tyZQr4FqVTwtW1egVjNS1IVQ1524Z5zohqw=");
				
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

		    String dbquery_tDBInput_3 = "SELECT Factures_achat.Nom_du_fichier,\n		Factures_achat.N__Facture,\n		Factures_achat.Date_Facture,\n		Factures_achat.Nom_"
+"du_vendeur,\n		Factures_achat.Adresse_du_vendeur,\n		Factures_achat.Matricule_Fiscale_du_vendeur,\n		Factures_achat.Produit"
+",\n		Factures_achat.Reference,\n		Factures_achat.Quantite,\n		Factures_achat.PU_HT,\n		Factures_achat.TVA,\n		Factures_achat."
+"Total_HT,\n		Factures_achat.Montant_Total_net_a_payer\nFROM	Factures_achat";
		    

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
								row4.Nom_du_fichier = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(1);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Nom_du_fichier = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Nom_du_fichier = tmpContent_tDBInput_3;
                }
            } else {
                row4.Nom_du_fichier = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 2) {
								row4.N__Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(2);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.N__Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.N__Facture = tmpContent_tDBInput_3;
                }
            } else {
                row4.N__Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 3) {
								row4.Date_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(3);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Date_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Date_Facture = tmpContent_tDBInput_3;
                }
            } else {
                row4.Date_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 4) {
								row4.Nom_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(4);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Nom_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Nom_du_vendeur = tmpContent_tDBInput_3;
                }
            } else {
                row4.Nom_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 5) {
								row4.Adresse_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(5);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Adresse_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Adresse_du_vendeur = tmpContent_tDBInput_3;
                }
            } else {
                row4.Adresse_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 6) {
								row4.Matricule_Fiscale_du_vendeur = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(6);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Matricule_Fiscale_du_vendeur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Matricule_Fiscale_du_vendeur = tmpContent_tDBInput_3;
                }
            } else {
                row4.Matricule_Fiscale_du_vendeur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 7) {
								row4.Produit = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(7);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Produit = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Produit = tmpContent_tDBInput_3;
                }
            } else {
                row4.Produit = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 8) {
								row4.Reference = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(8);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Reference = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Reference = tmpContent_tDBInput_3;
                }
            } else {
                row4.Reference = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 9) {
								row4.Quantite = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(9);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Quantite = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Quantite = tmpContent_tDBInput_3;
                }
            } else {
                row4.Quantite = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 10) {
								row4.PU_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(10);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.PU_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.PU_HT = tmpContent_tDBInput_3;
                }
            } else {
                row4.PU_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 11) {
								row4.TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(11);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.TVA = tmpContent_tDBInput_3;
                }
            } else {
                row4.TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 12) {
								row4.Total_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(12);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Total_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Total_HT = tmpContent_tDBInput_3;
                }
            } else {
                row4.Total_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_3 < 13) {
								row4.Montant_Total_net_a_payer = null;
							} else {
	                         		
           		tmpContent_tDBInput_3 = rs_tDBInput_3.getString(13);
            if(tmpContent_tDBInput_3 != null) {
            	if (talendToDBList_tDBInput_3 .contains(rsmd_tDBInput_3.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row4.Montant_Total_net_a_payer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_3);
            	} else {
                	row4.Montant_Total_net_a_payer = tmpContent_tDBInput_3;
                }
            } else {
                row4.Montant_Total_net_a_payer = null;
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
	 * [tFilterRow_2 main ] start
	 */

	

	
	
	currentComponent="tFilterRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row4"
						
						);
					}
					

          row7 = null;
    Operator_tFilterRow_2 ope_tFilterRow_2 = new Operator_tFilterRow_2("&&");
    
    if (ope_tFilterRow_2.getMatchFlag()) {
              if(row7 == null){ 
                row7 = new row7Struct();
              }
               row7.Nom_du_fichier = row4.Nom_du_fichier;
               row7.N__Facture = row4.N__Facture;
               row7.Date_Facture = row4.Date_Facture;
               row7.Nom_du_vendeur = row4.Nom_du_vendeur;
               row7.Adresse_du_vendeur = row4.Adresse_du_vendeur;
               row7.Matricule_Fiscale_du_vendeur = row4.Matricule_Fiscale_du_vendeur;
               row7.Produit = row4.Produit;
               row7.Reference = row4.Reference;
               row7.Quantite = row4.Quantite;
               row7.PU_HT = row4.PU_HT;
               row7.TVA = row4.TVA;
               row7.Total_HT = row4.Total_HT;
               row7.Montant_Total_net_a_payer = row4.Montant_Total_net_a_payer;    
      nb_line_ok_tFilterRow_2++;
    } else {
      nb_line_reject_tFilterRow_2++;
    }

nb_line_tFilterRow_2++;

 


	tos_count_tFilterRow_2++;

/**
 * [tFilterRow_2 main ] stop
 */
	
	/**
	 * [tFilterRow_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tFilterRow_2";

	

 



/**
 * [tFilterRow_2 process_data_begin ] stop
 */
// Start of branch "row7"
if(row7 != null) { 



	
	/**
	 * [tUniqRow_2 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row7"
						
						);
					}
					
row5.Nom_du_fichier = row7.Nom_du_fichier;			row5.N__Facture = row7.N__Facture;			row5.Date_Facture = row7.Date_Facture;			row5.Nom_du_vendeur = row7.Nom_du_vendeur;			row5.Adresse_du_vendeur = row7.Adresse_du_vendeur;			row5.Matricule_Fiscale_du_vendeur = row7.Matricule_Fiscale_du_vendeur;			row5.Produit = row7.Produit;			row5.Reference = row7.Reference;			row5.Quantite = row7.Quantite;			row5.PU_HT = row7.PU_HT;			row5.TVA = row7.TVA;			row5.Total_HT = row7.Total_HT;			row5.Montant_Total_net_a_payer = row7.Montant_Total_net_a_payer;			

 


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
// Start of branch "row5"
if(row5 != null) { 



	
	/**
	 * [tMap_2 main ] start
	 */

	

	
	
	currentComponent="tMap_2";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row5"
						
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

aaa = null;


// # Output table : 'aaa'
aaa_tmp.Fournisseur_PK = 0;
aaa_tmp.Matricule_fiscale = row5.Matricule_Fiscale_du_vendeur ;
aaa_tmp.Nom_fournisseur = row5.Nom_du_vendeur==null?"Unknown":row5.Nom_du_vendeur.trim().substring(0, 1).toUpperCase()+row5.Nom_du_vendeur.trim().toLowerCase().substring( 1) ;
aaa_tmp.Produit_ou_Service = "Unknown";
aaa = aaa_tmp;
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
// Start of branch "aaa"
if(aaa != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"aaa"
									
							);
						}
						
//////////
 

// for output
			row2 = new row2Struct();
								
			row2.Fournisseur_PK = aaa.Fournisseur_PK;								
			row2.Matricule_fiscale = aaa.Matricule_fiscale;								
			row2.Nom_fournisseur = aaa.Nom_fournisseur;								
			row2.Produit_ou_Service = aaa.Produit_ou_Service;			

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
						
							,"row2"
						
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

finalOt1 = null;


// # Output table : 'finalOt1'
finalOt1_tmp.Fournisseur_PK = 0;
finalOt1_tmp.Matricule_fiscale = row2.Matricule_fiscale ;
finalOt1_tmp.Nom_fournisseur = row2.Nom_fournisseur ;
finalOt1_tmp.Produit_ou_Service = row2.Produit_ou_Service ==null?"Unknown":row2.Produit_ou_Service ;
finalOt1 = finalOt1_tmp;
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
// Start of branch "finalOt1"
if(finalOt1 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"finalOt1"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_fournisseur = finalOt1.Nom_fournisseur;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(finalOt1.Nom_fournisseur == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, finalOt1.Nom_fournisseur);
}

                    if(finalOt1.Produit_ou_Service == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, finalOt1.Matricule_fiscale);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Produit_ou_Service == null && finalOt1.Produit_ou_Service!= null) || (lookUpValue_tDBSCD_1.Produit_ou_Service != null && !lookUpValue_tDBSCD_1.Produit_ou_Service.equals(finalOt1.Produit_ou_Service)) || (lookUpValue_tDBSCD_1.Matricule_fiscale == null && finalOt1.Matricule_fiscale!= null) || (lookUpValue_tDBSCD_1.Matricule_fiscale != null && !lookUpValue_tDBSCD_1.Matricule_fiscale.equals(finalOt1.Matricule_fiscale))) {
                    if(finalOt1.Produit_ou_Service == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, finalOt1.Matricule_fiscale);
}

                    if(finalOt1.Nom_fournisseur == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, finalOt1.Nom_fournisseur);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Produit_ou_Service = finalOt1.Produit_ou_Service;
                lookUpValue_tDBSCD_1.Matricule_fiscale = finalOt1.Matricule_fiscale;
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

} // End of branch "finalOt1"




	
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

} // End of branch "aaa"




	
	/**
	 * [tMap_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	

 



/**
 * [tMap_2 process_data_end ] stop
 */

} // End of branch "row5"




	
	/**
	 * [tUniqRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

 



/**
 * [tUniqRow_2 process_data_end ] stop
 */

} // End of branch "row7"




	
	/**
	 * [tFilterRow_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tFilterRow_2";

	

 



/**
 * [tFilterRow_2 process_data_end ] stop
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
	 * [tFilterRow_2 end ] start
	 */

	

	
	
	currentComponent="tFilterRow_2";

	
    globalMap.put("tFilterRow_2_NB_LINE", nb_line_tFilterRow_2);
    globalMap.put("tFilterRow_2_NB_LINE_OK", nb_line_ok_tFilterRow_2);
    globalMap.put("tFilterRow_2_NB_LINE_REJECT", nb_line_reject_tFilterRow_2);
    

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row4");
			  	}
			  	
 

ok_Hash.put("tFilterRow_2", true);
end_Hash.put("tFilterRow_2", System.currentTimeMillis());




/**
 * [tFilterRow_2 end ] stop
 */

	
	/**
	 * [tUniqRow_2 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

globalMap.put("tUniqRow_2_NB_UNIQUES",nb_uniques_tUniqRow_2);
globalMap.put("tUniqRow_2_NB_DUPLICATES",nb_duplicates_tUniqRow_2);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row7");
			  	}
			  	
 

ok_Hash.put("tUniqRow_2", true);
end_Hash.put("tUniqRow_2", System.currentTimeMillis());




/**
 * [tUniqRow_2 end ] stop
 */

	
	/**
	 * [tMap_2 end ] start
	 */

	

	
	
	currentComponent="tMap_2";

	


// ###############################
// # Lookup hashes releasing
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row5");
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
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row3");
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
fournisseurStruct fournisseur_tmp = new fournisseurStruct();
// ###############################

        
        



        









 



/**
 * [tMap_1 begin ] stop
 */



	
	/**
	 * [tUniqRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_1", false);
		start_Hash.put("tUniqRow_1", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_1";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row1");
					}
				
		int tos_count_tUniqRow_1 = 0;
		

	
		class KeyStruct_tUniqRow_1 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String ID_Fournisseur;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.ID_Fournisseur == null) ? 0 : this.ID_Fournisseur.hashCode());
								
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
				
									if (this.ID_Fournisseur == null) {
										if (other.ID_Fournisseur != null) 
											return false;
								
									} else if (!this.ID_Fournisseur.equals(other.ID_Fournisseur))
								 
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
				
				 
	final String decryptedPassword_tDBInput_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:3tv4FFqEWf+vX4+y3zwN/57UOmSxSfIXKXXDbv6dL5a6yPR3VUo=");
				
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

		    String dbquery_tDBInput_1 = "SELECT ERP_Fournisseurs.ID_Fournisseur,\n		ERP_Fournisseurs.Nom_Fournisseur,\n		ERP_Fournisseurs.Adresse,\n		ERP_Fournisse"
+"urs.Produits_ou_Services,\n		ERP_Fournisseurs.Notes\nFROM	ERP_Fournisseurs";
		    

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
								row1.ID_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(1);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.ID_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.ID_Fournisseur = tmpContent_tDBInput_1;
                }
            } else {
                row1.ID_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 2) {
								row1.Nom_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(2);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Nom_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Nom_Fournisseur = tmpContent_tDBInput_1;
                }
            } else {
                row1.Nom_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 3) {
								row1.Adresse = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(3);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Adresse = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Adresse = tmpContent_tDBInput_1;
                }
            } else {
                row1.Adresse = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 4) {
								row1.Produits_ou_Services = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(4);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Produits_ou_Services = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Produits_ou_Services = tmpContent_tDBInput_1;
                }
            } else {
                row1.Produits_ou_Services = null;
            }
		                    }
							if(colQtyInRs_tDBInput_1 < 5) {
								row1.Notes = null;
							} else {
	                         		
           		tmpContent_tDBInput_1 = rs_tDBInput_1.getString(5);
            if(tmpContent_tDBInput_1 != null) {
            	if (talendToDBList_tDBInput_1 .contains(rsmd_tDBInput_1.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row1.Notes = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
            	} else {
                	row1.Notes = tmpContent_tDBInput_1;
                }
            } else {
                row1.Notes = null;
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
	 * [tUniqRow_1 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row1"
						
						);
					}
					
row3 = null;			
if(row1.ID_Fournisseur == null){
	finder_tUniqRow_1.ID_Fournisseur = null;
}else{
	finder_tUniqRow_1.ID_Fournisseur = row1.ID_Fournisseur.toLowerCase();
}	
finder_tUniqRow_1.hashCodeDirty = true;
if (!keystUniqRow_1.contains(finder_tUniqRow_1)) {
		KeyStruct_tUniqRow_1 new_tUniqRow_1 = new KeyStruct_tUniqRow_1();

		
if(row1.ID_Fournisseur == null){
	new_tUniqRow_1.ID_Fournisseur = null;
}else{
	new_tUniqRow_1.ID_Fournisseur = row1.ID_Fournisseur.toLowerCase();
}
		
		keystUniqRow_1.add(new_tUniqRow_1);if(row3 == null){ 
	
	row3 = new row3Struct();
}row3.ID_Fournisseur = row1.ID_Fournisseur;			row3.Nom_Fournisseur = row1.Nom_Fournisseur;			row3.Adresse = row1.Adresse;			row3.Produits_ou_Services = row1.Produits_ou_Services;			row3.Notes = row1.Notes;					
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
// Start of branch "row3"
if(row3 != null) { 



	
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
            				    								  
		// ###############################
        { // start of Var scope
        
	        // ###############################
        	// # Vars tables
        
Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
        // ###############################
        // # Output tables

fournisseur = null;


// # Output table : 'fournisseur'
fournisseur_tmp.Fournisseur_PK = 0;
fournisseur_tmp.Matricule_fiscale = row3.ID_Fournisseur ;
fournisseur_tmp.Nom_fournisseur = row3.Nom_Fournisseur.trim().substring(0, 1).toUpperCase()+row3.Nom_Fournisseur.trim().toLowerCase().substring( 1) ;
fournisseur_tmp.Produit_ou_Service = row3.Produits_ou_Services ;
fournisseur = fournisseur_tmp;
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
// Start of branch "fournisseur"
if(fournisseur != null) { 



	
	/**
	 * [tUnite_1 main ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	
						if(execStat){
							runStat.updateStatOnConnection(iterateId,1,1
								
									,"fournisseur"
									
							);
						}
						
//////////
 

// for output
			row2 = new row2Struct();
								
			row2.Fournisseur_PK = fournisseur.Fournisseur_PK;								
			row2.Matricule_fiscale = fournisseur.Matricule_fiscale;								
			row2.Nom_fournisseur = fournisseur.Nom_fournisseur;								
			row2.Produit_ou_Service = fournisseur.Produit_ou_Service;			

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
						
							,"row2"
						
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

finalOt1 = null;


// # Output table : 'finalOt1'
finalOt1_tmp.Fournisseur_PK = 0;
finalOt1_tmp.Matricule_fiscale = row2.Matricule_fiscale ;
finalOt1_tmp.Nom_fournisseur = row2.Nom_fournisseur ;
finalOt1_tmp.Produit_ou_Service = row2.Produit_ou_Service ==null?"Unknown":row2.Produit_ou_Service ;
finalOt1 = finalOt1_tmp;
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
// Start of branch "finalOt1"
if(finalOt1 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"finalOt1"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_fournisseur = finalOt1.Nom_fournisseur;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(finalOt1.Nom_fournisseur == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, finalOt1.Nom_fournisseur);
}

                    if(finalOt1.Produit_ou_Service == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, finalOt1.Matricule_fiscale);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Produit_ou_Service == null && finalOt1.Produit_ou_Service!= null) || (lookUpValue_tDBSCD_1.Produit_ou_Service != null && !lookUpValue_tDBSCD_1.Produit_ou_Service.equals(finalOt1.Produit_ou_Service)) || (lookUpValue_tDBSCD_1.Matricule_fiscale == null && finalOt1.Matricule_fiscale!= null) || (lookUpValue_tDBSCD_1.Matricule_fiscale != null && !lookUpValue_tDBSCD_1.Matricule_fiscale.equals(finalOt1.Matricule_fiscale))) {
                    if(finalOt1.Produit_ou_Service == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, finalOt1.Matricule_fiscale);
}

                    if(finalOt1.Nom_fournisseur == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, finalOt1.Nom_fournisseur);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Produit_ou_Service = finalOt1.Produit_ou_Service;
                lookUpValue_tDBSCD_1.Matricule_fiscale = finalOt1.Matricule_fiscale;
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

} // End of branch "finalOt1"




	
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

} // End of branch "fournisseur"




	
	/**
	 * [tMap_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	

 



/**
 * [tMap_1 process_data_end ] stop
 */

} // End of branch "row3"




	
	/**
	 * [tUniqRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 process_data_end ] stop
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
	 * [tUniqRow_1 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

globalMap.put("tUniqRow_1_NB_UNIQUES",nb_uniques_tUniqRow_1);
globalMap.put("tUniqRow_1_NB_DUPLICATES",nb_duplicates_tUniqRow_1);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row1");
			  	}
			  	
 

ok_Hash.put("tUniqRow_1", true);
end_Hash.put("tUniqRow_1", System.currentTimeMillis());




/**
 * [tUniqRow_1 end ] stop
 */

	
	/**
	 * [tMap_1 end ] start
	 */

	

	
	
	currentComponent="tMap_1";

	


// ###############################
// # Lookup hashes releasing
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
out1Struct out1_tmp = new out1Struct();
// ###############################

        
        



        









 



/**
 * [tMap_4 begin ] stop
 */



	
	/**
	 * [tUniqRow_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tUniqRow_3", false);
		start_Hash.put("tUniqRow_3", System.currentTimeMillis());
		
	
	currentComponent="tUniqRow_3";

	
					if(execStat) {
						runStat.updateStatOnConnection(resourceMap,iterateId,0,0,"row6");
					}
				
		int tos_count_tUniqRow_3 = 0;
		

	
		class KeyStruct_tUniqRow_3 {
	
			private static final int DEFAULT_HASHCODE = 1;
		    private static final int PRIME = 31;
		    private int hashCode = DEFAULT_HASHCODE;
		    public boolean hashCodeDirty = true;
	
	        
					String Nom_du_Fournisseur;        
	        
		    @Override
			public int hashCode() {
				if (this.hashCodeDirty) {
					final int prime = PRIME;
					int result = DEFAULT_HASHCODE;
			
								result = prime * result + ((this.Nom_du_Fournisseur == null) ? 0 : this.Nom_du_Fournisseur.hashCode());
								
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
				
									if (this.Nom_du_Fournisseur == null) {
										if (other.Nom_du_Fournisseur != null) 
											return false;
								
									} else if (!this.Nom_du_Fournisseur.equals(other.Nom_du_Fournisseur))
								 
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
				
				 
	final String decryptedPassword_tDBInput_2 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:XCFqAD7YReckfz9TFWfGk5dm7NgdXjil+hfj03UCvAMNUo4d4OU=");
				
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

		    String dbquery_tDBInput_2 = "SELECT etats_achats_ventes_Suivi_achats.Societe,\n		etats_achats_ventes_Suivi_achats.Nom_du_Fournisseur,\n		etats_achats_"
+"ventes_Suivi_achats.N_Facture,\n		etats_achats_ventes_Suivi_achats.Date_de_facture,\n		etats_achats_ventes_Suivi_achats.Mo"
+"ntant_HT,\n		etats_achats_ventes_Suivi_achats.Taux_de_TVA,\n		etats_achats_ventes_Suivi_achats.Timbre,\n		etats_achats_vent"
+"es_Suivi_achats.Montant_de_la_TVA,\n		etats_achats_ventes_Suivi_achats.FODEC,\n		etats_achats_ventes_Suivi_achats.Montant_"
+"TTC,\n		etats_achats_ventes_Suivi_achats.Retenue,\n		etats_achats_ventes_Suivi_achats.Versements_effectues_par_SOUGUI,\n		e"
+"tats_achats_ventes_Suivi_achats.Reste_du,\n		etats_achats_ventes_Suivi_achats.Action_a_effectuer,\n		etats_achats_ventes_S"
+"uivi_achats.Date_de_paiement_total_de_la_facture,\n		etats_achats_ventes_Suivi_achats.mode_de_reglement,\n		etats_achats_v"
+"entes_Suivi_achats.Date_de_la_premiere_relance,\n		etats_achats_ventes_Suivi_achats.Date_de_la_deuxieme_relance,\n		etats_"
+"achats_ventes_Suivi_achats.Penalite_et_majoration_de_paiement,\n		etats_achats_ventes_Suivi_achats.Commentaire\nFROM	etats"
+"_achats_ventes_Suivi_achats";
		    

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
								row6.Societe = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(1);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(1).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Societe = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Societe = tmpContent_tDBInput_2;
                }
            } else {
                row6.Societe = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 2) {
								row6.Nom_du_Fournisseur = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(2);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(2).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Nom_du_Fournisseur = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Nom_du_Fournisseur = tmpContent_tDBInput_2;
                }
            } else {
                row6.Nom_du_Fournisseur = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 3) {
								row6.N_Facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(3);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(3).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.N_Facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.N_Facture = tmpContent_tDBInput_2;
                }
            } else {
                row6.N_Facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 4) {
								row6.Date_de_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(4);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(4).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Date_de_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Date_de_facture = tmpContent_tDBInput_2;
                }
            } else {
                row6.Date_de_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 5) {
								row6.Montant_HT = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(5);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(5).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Montant_HT = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Montant_HT = tmpContent_tDBInput_2;
                }
            } else {
                row6.Montant_HT = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 6) {
								row6.Taux_de_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(6);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(6).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Taux_de_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Taux_de_TVA = tmpContent_tDBInput_2;
                }
            } else {
                row6.Taux_de_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 7) {
								row6.Timbre = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(7);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Timbre = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Timbre = tmpContent_tDBInput_2;
                }
            } else {
                row6.Timbre = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 8) {
								row6.Montant_de_la_TVA = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(8);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Montant_de_la_TVA = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Montant_de_la_TVA = tmpContent_tDBInput_2;
                }
            } else {
                row6.Montant_de_la_TVA = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 9) {
								row6.FODEC = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(9);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.FODEC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.FODEC = tmpContent_tDBInput_2;
                }
            } else {
                row6.FODEC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 10) {
								row6.Montant_TTC = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(10);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Montant_TTC = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Montant_TTC = tmpContent_tDBInput_2;
                }
            } else {
                row6.Montant_TTC = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 11) {
								row6.Retenue = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(11);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(11).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Retenue = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Retenue = tmpContent_tDBInput_2;
                }
            } else {
                row6.Retenue = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 12) {
								row6.Versements_effectues_par_SOUGUI = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(12);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(12).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Versements_effectues_par_SOUGUI = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Versements_effectues_par_SOUGUI = tmpContent_tDBInput_2;
                }
            } else {
                row6.Versements_effectues_par_SOUGUI = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 13) {
								row6.Reste_du = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(13);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(13).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Reste_du = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Reste_du = tmpContent_tDBInput_2;
                }
            } else {
                row6.Reste_du = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 14) {
								row6.Action_a_effectuer = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(14);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(14).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Action_a_effectuer = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Action_a_effectuer = tmpContent_tDBInput_2;
                }
            } else {
                row6.Action_a_effectuer = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 15) {
								row6.Date_de_paiement_total_de_la_facture = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(15);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(15).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Date_de_paiement_total_de_la_facture = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Date_de_paiement_total_de_la_facture = tmpContent_tDBInput_2;
                }
            } else {
                row6.Date_de_paiement_total_de_la_facture = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 16) {
								row6.mode_de_reglement = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(16);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(16).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.mode_de_reglement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.mode_de_reglement = tmpContent_tDBInput_2;
                }
            } else {
                row6.mode_de_reglement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 17) {
								row6.Date_de_la_premiere_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(17);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(17).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Date_de_la_premiere_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Date_de_la_premiere_relance = tmpContent_tDBInput_2;
                }
            } else {
                row6.Date_de_la_premiere_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 18) {
								row6.Date_de_la_deuxieme_relance = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(18);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(18).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Date_de_la_deuxieme_relance = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Date_de_la_deuxieme_relance = tmpContent_tDBInput_2;
                }
            } else {
                row6.Date_de_la_deuxieme_relance = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 19) {
								row6.Penalite_et_majoration_de_paiement = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(19);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(19).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Penalite_et_majoration_de_paiement = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Penalite_et_majoration_de_paiement = tmpContent_tDBInput_2;
                }
            } else {
                row6.Penalite_et_majoration_de_paiement = null;
            }
		                    }
							if(colQtyInRs_tDBInput_2 < 20) {
								row6.Commentaire = null;
							} else {
	                         		
           		tmpContent_tDBInput_2 = rs_tDBInput_2.getString(20);
            if(tmpContent_tDBInput_2 != null) {
            	if (talendToDBList_tDBInput_2 .contains(rsmd_tDBInput_2.getColumnTypeName(20).toUpperCase(java.util.Locale.ENGLISH))) {
            		row6.Commentaire = FormatterUtils.formatUnwithE(tmpContent_tDBInput_2);
            	} else {
                	row6.Commentaire = tmpContent_tDBInput_2;
                }
            } else {
                row6.Commentaire = null;
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
	 * [tUniqRow_3 main ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"row6"
						
						);
					}
					
row8 = null;			
if(row6.Nom_du_Fournisseur == null){
	finder_tUniqRow_3.Nom_du_Fournisseur = null;
}else{
	finder_tUniqRow_3.Nom_du_Fournisseur = row6.Nom_du_Fournisseur.toLowerCase();
}	
finder_tUniqRow_3.hashCodeDirty = true;
if (!keystUniqRow_3.contains(finder_tUniqRow_3)) {
		KeyStruct_tUniqRow_3 new_tUniqRow_3 = new KeyStruct_tUniqRow_3();

		
if(row6.Nom_du_Fournisseur == null){
	new_tUniqRow_3.Nom_du_Fournisseur = null;
}else{
	new_tUniqRow_3.Nom_du_Fournisseur = row6.Nom_du_Fournisseur.toLowerCase();
}
		
		keystUniqRow_3.add(new_tUniqRow_3);if(row8 == null){ 
	
	row8 = new row8Struct();
}row8.Societe = row6.Societe;			row8.Nom_du_Fournisseur = row6.Nom_du_Fournisseur;			row8.N_Facture = row6.N_Facture;			row8.Date_de_facture = row6.Date_de_facture;			row8.Montant_HT = row6.Montant_HT;			row8.Taux_de_TVA = row6.Taux_de_TVA;			row8.Timbre = row6.Timbre;			row8.Montant_de_la_TVA = row6.Montant_de_la_TVA;			row8.FODEC = row6.FODEC;			row8.Montant_TTC = row6.Montant_TTC;			row8.Retenue = row6.Retenue;			row8.Versements_effectues_par_SOUGUI = row6.Versements_effectues_par_SOUGUI;			row8.Reste_du = row6.Reste_du;			row8.Action_a_effectuer = row6.Action_a_effectuer;			row8.Date_de_paiement_total_de_la_facture = row6.Date_de_paiement_total_de_la_facture;			row8.mode_de_reglement = row6.mode_de_reglement;			row8.Date_de_la_premiere_relance = row6.Date_de_la_premiere_relance;			row8.Date_de_la_deuxieme_relance = row6.Date_de_la_deuxieme_relance;			row8.Penalite_et_majoration_de_paiement = row6.Penalite_et_majoration_de_paiement;			row8.Commentaire = row6.Commentaire;					
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
// Start of branch "row8"
if(row8 != null) { 



	
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

out1 = null;


// # Output table : 'out1'
out1_tmp.Fournisseur_PK = 0;
out1_tmp.Matricule_fiscale = "Unknown";
out1_tmp.Nom_fournisseur = row8.Nom_du_Fournisseur == null || row8.Nom_du_Fournisseur.trim().isEmpty()
? "Unknown"
: row8.Nom_du_Fournisseur.trim().matches("^[0-9].*")
    ? "Unknown"
    : row8.Nom_du_Fournisseur.trim().substring(0, 1).toUpperCase()
      + row8.Nom_du_Fournisseur.trim().toLowerCase().substring(1) ;
out1_tmp.Produit_ou_Service = "Unknown";
out1 = out1_tmp;
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
			row2 = new row2Struct();
								
			row2.Fournisseur_PK = out1.Fournisseur_PK;								
			row2.Matricule_fiscale = out1.Matricule_fiscale;								
			row2.Nom_fournisseur = out1.Nom_fournisseur;								
			row2.Produit_ou_Service = out1.Produit_ou_Service;			

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
						
							,"row2"
						
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

finalOt1 = null;


// # Output table : 'finalOt1'
finalOt1_tmp.Fournisseur_PK = 0;
finalOt1_tmp.Matricule_fiscale = row2.Matricule_fiscale ;
finalOt1_tmp.Nom_fournisseur = row2.Nom_fournisseur ;
finalOt1_tmp.Produit_ou_Service = row2.Produit_ou_Service ==null?"Unknown":row2.Produit_ou_Service ;
finalOt1 = finalOt1_tmp;
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
// Start of branch "finalOt1"
if(finalOt1 != null) { 



	
	/**
	 * [tDBSCD_1 main ] start
	 */

	

	
	
	currentComponent="tDBSCD_1";

	
					if(execStat){
						runStat.updateStatOnConnection(iterateId,1,1
						
							,"finalOt1"
						
						);
					}
					

	try {
        lookUpKey_tDBSCD_1 = new SCDSK_tDBSCD_1();
            lookUpKey_tDBSCD_1.Nom_fournisseur = finalOt1.Nom_fournisseur;
        lookUpKey_tDBSCD_1.hashCodeDirty = true;
        lookUpValue_tDBSCD_1 = cache_tDBSCD_1.get(lookUpKey_tDBSCD_1);    
    if(lookUpValue_tDBSCD_1 == null) {
            lookUpValue_tDBSCD_1 = new SCDStruct_tDBSCD_1();
        
                    if(finalOt1.Nom_fournisseur == null) {
insertionStatement_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(1, finalOt1.Nom_fournisseur);
}

                    if(finalOt1.Produit_ou_Service == null) {
insertionStatement_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(2, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
insertionStatement_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
insertionStatement_tDBSCD_1.setString(3, finalOt1.Matricule_fiscale);
}

        nb_line_inserted_tDBSCD_1 += insertionStatement_tDBSCD_1.executeUpdate();
    } else {
            if((lookUpValue_tDBSCD_1.Produit_ou_Service == null && finalOt1.Produit_ou_Service!= null) || (lookUpValue_tDBSCD_1.Produit_ou_Service != null && !lookUpValue_tDBSCD_1.Produit_ou_Service.equals(finalOt1.Produit_ou_Service)) || (lookUpValue_tDBSCD_1.Matricule_fiscale == null && finalOt1.Matricule_fiscale!= null) || (lookUpValue_tDBSCD_1.Matricule_fiscale != null && !lookUpValue_tDBSCD_1.Matricule_fiscale.equals(finalOt1.Matricule_fiscale))) {
                    if(finalOt1.Produit_ou_Service == null) {
updateForType1_tDBSCD_1.setNull(1, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(1, finalOt1.Produit_ou_Service);
}

                    if(finalOt1.Matricule_fiscale == null) {
updateForType1_tDBSCD_1.setNull(2, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(2, finalOt1.Matricule_fiscale);
}

                    if(finalOt1.Nom_fournisseur == null) {
updateForType1_tDBSCD_1.setNull(3, java.sql.Types.VARCHAR);
} else {
updateForType1_tDBSCD_1.setString(3, finalOt1.Nom_fournisseur);
}

                nb_line_update_tDBSCD_1 += updateForType1_tDBSCD_1.executeUpdate();
            }
    }
    
	} catch (java.lang.Exception e) {//catch
globalMap.put("tDBSCD_1_ERROR_MESSAGE",e.getMessage());
  		
                System.err.print(e.getMessage());
	}//end catch
	
                lookUpValue_tDBSCD_1.Produit_ou_Service = finalOt1.Produit_ou_Service;
                lookUpValue_tDBSCD_1.Matricule_fiscale = finalOt1.Matricule_fiscale;
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

} // End of branch "finalOt1"




	
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
	 * [tMap_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tMap_4";

	

 



/**
 * [tMap_4 process_data_end ] stop
 */

} // End of branch "row8"




	
	/**
	 * [tUniqRow_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 process_data_end ] stop
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
	 * [tUniqRow_3 end ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

globalMap.put("tUniqRow_3_NB_UNIQUES",nb_uniques_tUniqRow_3);
globalMap.put("tUniqRow_3_NB_DUPLICATES",nb_duplicates_tUniqRow_3);

				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row6");
			  	}
			  	
 

ok_Hash.put("tUniqRow_3", true);
end_Hash.put("tUniqRow_3", System.currentTimeMillis());




/**
 * [tUniqRow_3 end ] stop
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
	 * [tUnite_1 end ] start
	 */

	

	
	
	currentComponent="tUnite_1";

	

globalMap.put("tUnite_1_NB_LINE", nb_line_tUnite_1);
				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"aaa","fournisseur","out1");
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
// ###############################      





				if(execStat){
			  		runStat.updateStat(resourceMap,iterateId,2,0,"row2");
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
			  		runStat.updateStat(resourceMap,iterateId,2,0,"finalOt1");
			  	}
			  	
 

ok_Hash.put("tDBSCD_1", true);
end_Hash.put("tDBSCD_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk2", 0, "ok");
				}
				tChronometerStop_1Process(globalMap);



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
	 * [tDBInput_3 finally ] start
	 */

	

	
	
	currentComponent="tDBInput_3";

	

 



/**
 * [tDBInput_3 finally ] stop
 */

	
	/**
	 * [tFilterRow_2 finally ] start
	 */

	

	
	
	currentComponent="tFilterRow_2";

	

 



/**
 * [tFilterRow_2 finally ] stop
 */

	
	/**
	 * [tUniqRow_2 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_2";

	

 



/**
 * [tUniqRow_2 finally ] stop
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
	 * [tUniqRow_1 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_1";

	

 



/**
 * [tUniqRow_1 finally ] stop
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
	 * [tUniqRow_3 finally ] start
	 */

	

	
	
	currentComponent="tUniqRow_3";

	

 



/**
 * [tUniqRow_3 finally ] stop
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






				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tDBInput_3_SUBPROCESS_STATE", 1);
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
        final ETL_Dim_Fournisseurs ETL_Dim_FournisseursClass = new ETL_Dim_Fournisseurs();

        int exitCode = ETL_Dim_FournisseursClass.runJobInTOS(args);

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
            java.io.InputStream inContext = ETL_Dim_Fournisseurs.class.getClassLoader().getResourceAsStream("pi_pa_mehdi/etl_dim_fournisseurs_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = ETL_Dim_Fournisseurs.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
                        context.setContextType("Server", "id_String");
                        if(context.getStringValue("Server") == null) {
                            context.Server = null;
                        } else {
                            context.Server=(String) context.getProperty("Server");
                        }
                        context.setContextType("Database", "id_String");
                        if(context.getStringValue("Database") == null) {
                            context.Database = null;
                        } else {
                            context.Database=(String) context.getProperty("Database");
                        }
                        context.setContextType("Port", "id_String");
                        if(context.getStringValue("Port") == null) {
                            context.Port = null;
                        } else {
                            context.Port=(String) context.getProperty("Port");
                        }
                        context.setContextType("User", "id_String");
                        if(context.getStringValue("User") == null) {
                            context.User = null;
                        } else {
                            context.User=(String) context.getProperty("User");
                        }
                        context.setContextType("Password", "id_Password");
                        if(context.getStringValue("Password") == null) {
                            context.Password = null;
                        } else {
                            String pwd_Password_value = context.getProperty("Password");
                            context.Password = null;
                            if(pwd_Password_value!=null) {
                                if(context_param.containsKey("Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.Password = pwd_Password_value;
                                } else if (!pwd_Password_value.isEmpty()) {
                                    try {
                                        context.Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_Password_value);
                                        context.put("Password",context.Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("Schema", "id_String");
                        if(context.getStringValue("Schema") == null) {
                            context.Schema = null;
                        } else {
                            context.Schema=(String) context.getProperty("Schema");
                        }
                        context.setContextType("AddParms", "id_String");
                        if(context.getStringValue("AddParms") == null) {
                            context.AddParms = null;
                        } else {
                            context.AddParms=(String) context.getProperty("AddParms");
                        }
                        context.setContextType("ServerName", "id_String");
                        if(context.getStringValue("ServerName") == null) {
                            context.ServerName = null;
                        } else {
                            context.ServerName=(String) context.getProperty("ServerName");
                        }
                        context.setContextType("AdditinalParms", "id_String");
                        if(context.getStringValue("AdditinalParms") == null) {
                            context.AdditinalParms = null;
                        } else {
                            context.AdditinalParms=(String) context.getProperty("AdditinalParms");
                        }
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
        if (parentContextMap != null && !parentContextMap.isEmpty()) {if (parentContextMap.containsKey("Server")) {
                context.Server = (String) parentContextMap.get("Server");
            }if (parentContextMap.containsKey("Database")) {
                context.Database = (String) parentContextMap.get("Database");
            }if (parentContextMap.containsKey("Port")) {
                context.Port = (String) parentContextMap.get("Port");
            }if (parentContextMap.containsKey("User")) {
                context.User = (String) parentContextMap.get("User");
            }if (parentContextMap.containsKey("Password")) {
                context.Password = (java.lang.String) parentContextMap.get("Password");
            }if (parentContextMap.containsKey("Schema")) {
                context.Schema = (String) parentContextMap.get("Schema");
            }if (parentContextMap.containsKey("AddParms")) {
                context.AddParms = (String) parentContextMap.get("AddParms");
            }if (parentContextMap.containsKey("ServerName")) {
                context.ServerName = (String) parentContextMap.get("ServerName");
            }if (parentContextMap.containsKey("AdditinalParms")) {
                context.AdditinalParms = (String) parentContextMap.get("AdditinalParms");
            }
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
			parametersToEncrypt.add("Password");
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

try {
errorCode = null;tPrejob_1Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tPrejob_1) {
globalMap.put("tPrejob_1_SUBPROCESS_STATE", -1);

e_tPrejob_1.printStackTrace();

}




this.globalResumeTicket = false;//to run others jobs

try {
errorCode = null;tDBInput_3Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tDBInput_3) {
globalMap.put("tDBInput_3_SUBPROCESS_STATE", -1);

e_tDBInput_3.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : ETL_Dim_Fournisseurs");
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
        } catch (java.lang.Exception e) {
        }
    }











    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();
            connections.put("conn_tDBConnection_1", globalMap.get("conn_tDBConnection_1"));
            connections.put("conn_tDBConnection_2", globalMap.get("conn_tDBConnection_2"));






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
 *     268167 characters generated by Talend Open Studio for Data Integration 
 *     on the 5 mai 2026 à 13:48:45 CET
 ************************************************************************************************/