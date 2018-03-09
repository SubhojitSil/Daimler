/*
 * The following source code ("Code") may only be used in accordance with the terms
 * and conditions of the license agreement you have with IBM Corporation. The Code 
 * is provided to you on an "AS IS" basis, without warranty of any kind.  
 * SUBJECT TO ANY STATUTORY WARRANTIES WHICH CAN NOT BE EXCLUDED, IBM MAKES NO 
 * WARRANTIES OR CONDITIONS EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
 * TO, THE IMPLIED WARRANTIES OR CONDITIONS OF MERCHANTABILITY, FITNESS FOR A 
 * PARTICULAR PURPOSE, AND NON-INFRINGEMENT, REGARDING THE CODE. IN NO EVENT WILL 
 * IBM BE LIABLE TO YOU OR ANY PARTY FOR ANY DIRECT, INDIRECT, SPECIAL OR OTHER 
 * CONSEQUENTIAL DAMAGES FOR ANY USE OF THE CODE, INCLUDING, WITHOUT LIMITATION, 
 * LOSS OF, OR DAMAGE TO, DATA, OR LOST PROFITS, BUSINESS, REVENUE, GOODWILL, OR 
 * ANTICIPATED SAVINGS, EVEN IF IBM HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGES. SOME JURISDICTIONS DO NOT ALLOW THE EXCLUSION OR LIMITATION OF 
 * INCIDENTAL OR CONSEQUENTIAL DAMAGES, SO THE ABOVE LIMITATION OR EXCLUSION MAY 
 * NOT APPLY TO YOU.
 */

/*
 * IBM-MDMWB-1.0-[1a5ab8ec0dcfe2893dafd71f6f790a73]
 */

package com.daimler.cdm.mdm.services.compositeTxn;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.dwl.base.DWLCommon;
import com.dwl.base.DWLResponse;
import com.dwl.base.IDWLErrorMessage;
import com.dwl.base.db.SQLParam;
import com.dwl.base.db.SQLQuery;
import com.dwl.base.error.DWLError;
import com.dwl.base.error.DWLErrorCode;
import com.dwl.base.error.DWLStatus;
import com.dwl.base.requestHandler.DWLTransactionPersistent;
import com.dwl.base.requestHandler.DWLTxnBP;
import com.dwl.base.util.StringUtils;
import com.dwl.base.xml.IToXml;
import com.dwl.tcrm.common.TCRMResponse;
import com.dwl.tcrm.coreParty.component.TCRMAdminContEquivBObj;
import com.dwl.tcrm.coreParty.component.TCRMPartyGroupingBObj;
import com.dwl.tcrm.utilities.TCRMClassFactory;
import com.dwl.unifi.tx.exception.BusinessProxyException;


import com.daimler.cdm.mdm.addition.component.CustomerVehicleRelationshipBObj;
import com.daimler.cdm.mdm.addition.component.DaimlerAdditionComponent;
import com.daimler.cdm.mdm.addition.component.XCONTACTVEHICLERELBObj;
import com.daimler.cdm.mdm.addition.component.XCONTACTVEHICLEROLEBObj;
import com.daimler.cdm.mdm.addition.component.XVEHICLEBObj;
import com.daimler.cdm.mdm.services.bp.DaimlerCompositeServiceBP;
import com.daimler.cdm.mdm.services.constant.DaimlerBusinessServicesComponentID;
import com.daimler.cdm.mdm.services.constant.DaimlerBusinessServicesErrorReasonCode;
import com.daimler.cdm.mdm.services.constants.DaimlerCommonSQLConstants;
import com.daimler.cdm.mdm.services.exception.DaimlerCompositeException;
import com.daimler.cdm.mdm.services.helper.DaimlerCustomerVehicleRelHelper;
import com.daimler.cdm.mdm.services.utils.DaimlerCompositeUtils;
import com.dwl.base.DWLControl;

/**
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 *
 * 
 * @generated NOT
 */
public class MaintainCustomerVehicleRelationshipCompositeTxnBP  extends DWLTxnBP {

	/**
	 * @generated
	 **/
	private IDWLErrorMessage errHandler;
	
	/**
    * <!-- begin-user-doc -->
	  * <!-- end-user-doc -->
    * @generated 
    */
	 private final static com.dwl.base.logging.IDWLLogger logger = com.dwl.base.logging.DWLLoggerManager.getLogger(MaintainCustomerVehicleRelationshipCompositeTxnBP.class);
	/**
	 * @generated not
	 **/
    public MaintainCustomerVehicleRelationshipCompositeTxnBP() {
        super();
        errHandler = TCRMClassFactory.getErrorHandler();
    }
	/**
	 * @generated not
	 **/
    @SuppressWarnings("null")
	//public synchronized Object execute(Object inputObj) throws BusinessProxyException {
    	public synchronized Object execute(Object inputObj) throws BusinessProxyException {
		logger.finest("ENTER Object execute(Object inputObj)");
		
		DWLResponse responseObj = null;
        TCRMResponse outputTxnObj = null;
        DWLTransactionPersistent inputTxnObj = (DWLTransactionPersistent) inputObj;
        DWLControl control = inputTxnObj.getTxnControl();
        
        XCONTACTVEHICLERELBObj contVechRel = null;
        TCRMAdminContEquivBObj contEquiv = null;
        String partyId = null;
        CustomerVehicleRelationshipBObj responsePartyRel = new CustomerVehicleRelationshipBObj();
        Vector<XCONTACTVEHICLERELBObj> vecDBContactVehicleRelBObj = null;
        
        CustomerVehicleRelationshipBObj partyVechRel = (CustomerVehicleRelationshipBObj)inputTxnObj.getTxnTopLevelObject();
        if(logger.isWarnEnabled()){
			try {
				logger.warn("Input Request:"+partyVechRel.toXML().replaceAll("\n", "").replaceAll("\t", ""));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        Vector<XCONTACTVEHICLERELBObj> sourceVehicleIdRelBObj = null;
        Vector<XVEHICLEBObj> vecDBActiveVehicleBObj = null;
        Vector<XVEHICLEBObj> vecDBVehicleBObj = null;
        
        Vector<XVEHICLEBObj> vecVehicleEntDatedBObj = null;
        
        XVEHICLEBObj updateParentVehicleBObj = null;
        XVEHICLEBObj updateDBVehicleBObj = null;
        XVEHICLEBObj vecUpdateVehicleBObj = null;
        Vector<XCONTACTVEHICLERELBObj> parentContactRelBObj= partyVechRel.getItemsXCONTACTVEHICLERELBObj();
        
        XVEHICLEBObj vecDBUpdateVehicleBObj = null;
        
		DaimlerAdditionComponent xContactVechRelComp =  new DaimlerAdditionComponent();
		
        try{
        	// Validate the input
        	preExecute(inputTxnObj);
        	
        	// Check UCID validation
        	partyId = validUCIDParty(partyVechRel.getTCRMAdminContEquivBObj(),control);
        	
        	if(partyId!=null || StringUtils.isNonBlank(partyId)){
        		
        		for(XCONTACTVEHICLERELBObj contVechRelObj : parentContactRelBObj){
        			
        			String commissionNo = contVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO();
        			
        			vecDBContactVehicleRelBObj = contactVehicleRelObjByPartyId(partyId, commissionNo, control);
        			      			
        		}
        	        	
        	// Fire transaction
        	
        	for(XCONTACTVEHICLERELBObj relBobj : parentContactRelBObj){
    			
    			String commissionNo = relBobj.getXVEHICLEBObj().getCOMMISSION_NO();
    			
    			vecDBVehicleBObj = vehicleObjectBySourceVehicleId(commissionNo, control);
    			      			
    		}
        	
        	if(vecDBContactVehicleRelBObj == null || vecDBContactVehicleRelBObj.isEmpty()){
        		
        		if(vecDBVehicleBObj == null || vecDBVehicleBObj.isEmpty()){

        		vecDBContactVehicleRelBObj = fireAddTransaction(parentContactRelBObj, control, partyId);
        		
        		} else {
        			
        			parentContactRelBObj.get(0).setObjectReferenceId("true");
        			       			
        			vecDBContactVehicleRelBObj = fireAddVehicleTransaction(parentContactRelBObj, vecDBVehicleBObj, control, partyId);
        			
        		}
        		
        	} else {
        		
        		resolveIdentity(parentContactRelBObj, vecDBContactVehicleRelBObj, control, partyId);
        		
        		vecDBContactVehicleRelBObj = fireAddUpdateTransaction(parentContactRelBObj, vecDBContactVehicleRelBObj, control, partyId);
        		
        		//End date active owner role for this vehicle if it is present for another customer and owner role is coming in request - starts - 21/08/2017
        		
        		XVEHICLEBObj vehicleBObj = vecDBContactVehicleRelBObj.get(0).getXVEHICLEBObj();
    			
        		for(XCONTACTVEHICLERELBObj relBobj : parentContactRelBObj){
        			
        			Vector<XCONTACTVEHICLEROLEBObj> vecXContactVehicleRoleBObj = relBobj.getItemsXCONTACTVEHICLEROLEBObj();
        			
        			for(XCONTACTVEHICLEROLEBObj xcontactvehiclerolebObj : vecXContactVehicleRoleBObj){
    					if(DaimlerCommonSQLConstants.VEHICLE_ROLE_TP_CDVALUE_OWNER.equals(xcontactvehiclerolebObj.getVEHICLE_ROLE_TP_CDValue())){
    						Vector<XCONTACTVEHICLEROLEBObj> vecDbVecRoleBObj = getVehicleRoleByVehiclePkID(vehicleBObj.getXVEHICLEpkId(),partyId, control);
    						EndDateActiveOwnerVehicleRole(vecDbVecRoleBObj,control);
    					}
    				}
        		}
        		
				
				//End date active owner role for this vehicle if it is present for another customer and owner role is coming in request - Ends 21/08/2017
				
        		
        		updateParentVehicleBObj = parentContactRelBObj.get(0).getXVEHICLEBObj();
        		
        		updateDBVehicleBObj = vecDBContactVehicleRelBObj.get(0).getXVEHICLEBObj();
        		
        		updateParentVehicleBObj.setXVEHICLEpkId(updateDBVehicleBObj.getXVEHICLEpkId());
        		
        		updateParentVehicleBObj.setXVEHICLELastUpdateDate(updateDBVehicleBObj.getXVEHICLELastUpdateDate());
        		
        		vecUpdateVehicleBObj = (XVEHICLEBObj) (xContactVechRelComp.updateXVEHICLE(updateParentVehicleBObj)).getData();
        		
        		for(XCONTACTVEHICLERELBObj responseBObj : vecDBContactVehicleRelBObj){
        			
        			responseBObj.setXVEHICLEBObj(vecUpdateVehicleBObj);
        			
        		}
        		
        		
        	}
        	
        	for(XCONTACTVEHICLERELBObj contVechRelObj1 : parentContactRelBObj){
    			
    			String commissionNo = contVechRelObj1.getXVEHICLEBObj().getCOMMISSION_NO();
    			
    			vecDBActiveVehicleBObj = activeVehicleObjBySourceVehicleId(commissionNo, control);
    			
    			vecVehicleEntDatedBObj = vehicleObjectBySourceVehicleId(commissionNo, control);
    			      			
    		}
        	
        	if(vecDBActiveVehicleBObj == null || vecDBActiveVehicleBObj.isEmpty()){       		
        		
        		long milliseconds = System.currentTimeMillis();
        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");    
        		Date resultdate = new Date(milliseconds);
        		
        		for(XVEHICLEBObj vehicleUpdateBObj : vecVehicleEntDatedBObj){
        			
        			vehicleUpdateBObj.setEND_DT(sdf.format(resultdate));
        			vecDBUpdateVehicleBObj = (XVEHICLEBObj) (xContactVechRelComp.updateXVEHICLE(vehicleUpdateBObj)).getData();
        		
        		}       		
        		
        	}
        	       	
        	responsePartyRel.setControl(control);
        	for(XCONTACTVEHICLERELBObj responseBObj : vecDBContactVehicleRelBObj){
        		if(vecDBUpdateVehicleBObj != null){
        		responseBObj.setXVEHICLEBObj(vecDBUpdateVehicleBObj);      		
        		}
        		responsePartyRel.setXCONTACTVEHICLERELBObj(responseBObj);
        	}
        	
    		responsePartyRel.setTCRMAdminContEquivBObj(partyVechRel.getTCRMAdminContEquivBObj());
    		
        	} else {
        		// throw error
        		
        		DWLError error = errHandler.getErrorMessage(DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_VEHICLE_RELATIONSHIP_BUSINESS_PROXY,
						DWLErrorCode.FIELD_VALIDATION_ERROR,
                        DaimlerBusinessServicesErrorReasonCode.MAINTAINVALIDUCID_FAILED,
                        control, new String[0]);
        		
        		throw new BusinessProxyException(error.getErrorMessage());

        	}
        
        }catch (DaimlerCompositeException ex){ 
        	return ex.getResponseObject();
        }
        catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}

        // Construct the response object.
        DWLStatus outputStatus = new DWLStatus();
        outputStatus.setStatus(DWLStatus.SUCCESS);
        
        responseObj = new TCRMResponse();
        responseObj.setStatus(outputStatus);
        responseObj.setData(responsePartyRel);
        
		logger.finest("RETURN Object execute(Object inputObj)");
        return responseObj;
        
    }
    
    private void preExecute(DWLTransactionPersistent theDWLTxnObj) throws BusinessProxyException  {
		// TODO Auto-generated method stub
		
		DWLCommon toplevelObject = (DWLCommon) theDWLTxnObj.getTxnTopLevelObject();     
        
        if (!(toplevelObject instanceof CustomerVehicleRelationshipBObj)) {
        DWLError error = errHandler.getErrorMessage(DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_VEHICLE_RELATIONSHIP_BUSINESS_PROXY,
        											DWLErrorCode.FIELD_VALIDATION_ERROR,
                                                    DaimlerBusinessServicesErrorReasonCode.MAINTAINCUSTOMERVEHICLERELATIONSHIP_FAILED,
                                                    theDWLTxnObj.getTxnControl(), new String[0]);
        throw new BusinessProxyException(error.getErrorMessage());
        }
    
        CustomerVehicleRelationshipBObj customerVehicleRelationship = (CustomerVehicleRelationshipBObj) toplevelObject;
        
        XCONTACTVEHICLERELBObj xContactVehicleRel = (XCONTACTVEHICLERELBObj)customerVehicleRelationship.getItemsXCONTACTVEHICLERELBObj().get(0);
        TCRMAdminContEquivBObj adminContEquiv = customerVehicleRelationship.getTCRMAdminContEquivBObj();
        
        if (xContactVehicleRel == null) 
        {
            DWLError error = errHandler.getErrorMessage(DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_VEHICLE_RELATIONSHIP_BUSINESS_PROXY,
                                                        DWLErrorCode.FIELD_VALIDATION_ERROR,
                                                        DaimlerBusinessServicesErrorReasonCode.MAINTAINCUSTOMERVEHICLERELATIONSHIP_FAILED,
                                                        theDWLTxnObj.getTxnControl(), new String[0]);
            throw new BusinessProxyException(error.getErrorMessage());
        }
        if (adminContEquiv == null || !StringUtils.isNonBlank(adminContEquiv.getAdminPartyId())) 
        {
            DWLError error = errHandler.getErrorMessage(DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_VEHICLE_RELATIONSHIP_BUSINESS_PROXY,
                                                        DWLErrorCode.FIELD_VALIDATION_ERROR,
                                                        DaimlerBusinessServicesErrorReasonCode.MAINTAINCUSTOMERVEHICLERELATIONSHIP_FAILED,
                                                        theDWLTxnObj.getTxnControl(), new String[0]);
            throw new BusinessProxyException(error.getErrorMessage());
        }
              
	}
    
    private String validUCIDParty(TCRMAdminContEquivBObj contEquiv,DWLControl dwlControl) throws Exception {
		// TODO Auto-generated method stub
		
		SQLQuery sqlQuery = new SQLQuery();
		ResultSet objResultSet = null;
		String getPartyIdSql = DaimlerCommonSQLConstants.SEARCH_CUSTOMER_BY_CONTEQUIV;
		String strContIDActiveParty = null;
		String adminSystemType = null;
		String adminPartyId = null;
		String adminSystemValue = null;
		List<SQLParam> params = new ArrayList<SQLParam>();
		
		adminSystemType =contEquiv.getAdminSystemType();
		adminPartyId = contEquiv.getAdminPartyId();
		adminSystemValue =contEquiv.getAdminSystemValue();
		
	if (adminSystemType == null )
			{
	   adminSystemType =DaimlerCustomerVehicleRelHelper.getCodeType(adminSystemValue,
	   DaimlerCommonSQLConstants.CODETABLE_ADMINSYSTP,dwlControl);
			}
	
	
	
		try {
			params = new ArrayList<SQLParam>();
			params.add(0, new SQLParam(adminPartyId));
			params.add(1, new SQLParam(adminSystemType));
			
			objResultSet = sqlQuery.executeQuery(getPartyIdSql, params);
			while (objResultSet.next()) {
				strContIDActiveParty = objResultSet.getString(DaimlerCommonSQLConstants.GET_PARTY_ID);
			}
			
			objResultSet.close();
		}catch(Exception ex){
			
		}
		
		return strContIDActiveParty;
	}
    
    private Vector<XVEHICLEBObj> vehicleObjectBySourceVehicleId(String commissionNo, DWLControl control) throws Exception
	{

		Vector<XVEHICLEBObj> resultVehicleBObj = new Vector<XVEHICLEBObj>();

    	
		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(commissionNo));

		resultVehicleBObj = executeQueryVehicleObject(DaimlerCommonSQLConstants.GET_VEHICLE_ID_BY_COMMSN_NO, params, control);

		return resultVehicleBObj;
	}
    
    private Vector<XVEHICLEBObj> activeVehicleObjBySourceVehicleId(String commissionNo, DWLControl control) throws Exception
   	{

   		Vector<XVEHICLEBObj> resultVehicleBObj = new Vector<XVEHICLEBObj>();

       	
   		List<SQLParam> params = new ArrayList<SQLParam>();

   		params.add(0, new SQLParam(commissionNo));

   		resultVehicleBObj = executeQueryActiveVehicle(DaimlerCommonSQLConstants.GET_ACTIVE_VEHICLE_ID, params, control);

   		return resultVehicleBObj;
   	}
    
    private Vector<XCONTACTVEHICLERELBObj> contactVehicleRelObjByPartyId(String partyId, String commissionNo, DWLControl control) throws Exception
   	{

   		Vector<XCONTACTVEHICLERELBObj> resultContVechRel = new Vector<XCONTACTVEHICLERELBObj>();
   		
   		List<SQLParam> params = new ArrayList<SQLParam>();
   		
   		params.add(0, new SQLParam(commissionNo));
   		
   		params.add(1, new SQLParam(partyId));

   		resultContVechRel = executeQueryContactVehicleRel(DaimlerCommonSQLConstants.GET_SOURCE_CONTACT_VEHICLE_REL_ID, params, control);

   		return resultContVechRel;
   	}
    
    public Vector<XCONTACTVEHICLERELBObj> executeQueryContactVehicleRel(String sqlStatement, List params, DWLControl control) throws Exception {
    	
    	Vector<XCONTACTVEHICLERELBObj> resultContVechRel = new Vector<XCONTACTVEHICLERELBObj>();
    	DaimlerAdditionComponent xContactVechRelComp =  new DaimlerAdditionComponent();
    	//DWLControl control = null;
    	String contVechId = null;
    	SQLQuery query = new SQLQuery();
    	ResultSet rs = null;
    	try {		
    		rs = query.executeQuery(sqlStatement, params);
    			if (rs.next()) {
    				do {
    					contVechId = rs.getString(DaimlerCommonSQLConstants.CONTACT_VEHICLE_REL_ID);

    					XCONTACTVEHICLERELBObj contVechRelObj = ((XCONTACTVEHICLERELBObj)
    							(xContactVechRelComp.getXCONTACTVEHICLEREL(contVechId, control)).getData());
    					if(contVechRelObj != null){
    					resultContVechRel.add(contVechRelObj);
    					}
    				}while (rs.next());
    			}
    	} catch (Exception e) {
    		throw e;
    	}
    	finally
    	{
    		if (rs != null && !rs.isClosed())
    		{
    			rs.close();
    			rs = null;
    		}
    		query.close();
    	}
    	
    	return resultContVechRel;
    	
    }
    
public Vector<XVEHICLEBObj> executeQueryVehicleObject(String sqlStatement, List params, DWLControl control) throws Exception {
    	
    	Vector<XVEHICLEBObj> resultContVechRel = new Vector<XVEHICLEBObj>();
    	DaimlerAdditionComponent xContactVechRelComp =  new DaimlerAdditionComponent();
    	//DWLControl control = null;
    	String xVehicleId = null;
    	SQLQuery query = new SQLQuery();
    	ResultSet rs = null;
    	try {		
    		rs = query.executeQuery(sqlStatement, params);
    			if (rs.next()) {
    				do {
    					xVehicleId = rs.getString(DaimlerCommonSQLConstants.VEHICLE_ID);

    					XVEHICLEBObj contVechRelObj = ((XVEHICLEBObj)
    							(xContactVechRelComp.getXVEHICLE(xVehicleId, control)).getData());
    					if(contVechRelObj != null){
    					resultContVechRel.add(contVechRelObj);
    					}
    				}while (rs.next());
    			}
    	} catch (Exception e) {
    		throw e;
    	}
    	finally
    	{
    		if (rs != null && !rs.isClosed())
    		{
    			rs.close();
    			rs = null;
    		}
    		query.close();
    	}
    	
    	return resultContVechRel;
    	
    }

public Vector<XVEHICLEBObj> executeQueryActiveVehicle(String sqlStatement, List params, DWLControl control) throws Exception {
	
	Vector<XVEHICLEBObj> resultContVechRel = new Vector<XVEHICLEBObj>();
	DaimlerAdditionComponent xContactVechRelComp =  new DaimlerAdditionComponent();
	//DWLControl control = null;
	String xVehicleId = null;
	SQLQuery query = new SQLQuery();
	ResultSet rs = null;
	try {		
		rs = query.executeQuery(sqlStatement, params);
			if (rs.next()) {
				do {
					xVehicleId = rs.getString(DaimlerCommonSQLConstants.ACTIVE_VEHICLE_ID);

					XVEHICLEBObj contVechRelObj = ((XVEHICLEBObj)
							(xContactVechRelComp.getXVEHICLE(xVehicleId, control)).getData());
					if(contVechRelObj != null){
					resultContVechRel.add(contVechRelObj);
					}
				}while (rs.next());
			}
	} catch (Exception e) {
		throw e;
	}
	finally
	{
		if (rs != null && !rs.isClosed())
		{
			rs.close();
			rs = null;
		}
		query.close();
	}
	
	return resultContVechRel;
	
}
    
    private void resolveIdentity(Vector<XCONTACTVEHICLERELBObj> itemsXContactVehicleRelBObj,
   		 Vector<XCONTACTVEHICLERELBObj> vecDBContactVehicleRelBObj,DWLControl control, String partyId) 
   		throws BusinessProxyException, DaimlerCompositeException {
		// TODO Auto-generated method stub
		
   	logger.info("MaintainCustomerVehicleRelationCompositeTxnBP:resolveIdentity- ENTER");

   	Vector<XCONTACTVEHICLERELBObj> inputRecords =  itemsXContactVehicleRelBObj;
   	Vector<XCONTACTVEHICLERELBObj> dbRecords =  vecDBContactVehicleRelBObj;
		 DWLStatus status = new DWLStatus();
		 // To handle update scenarios

		 try {
			 
			HashMap<String, XCONTACTVEHICLERELBObj> resContVechRelMap = new HashMap<String, XCONTACTVEHICLERELBObj>();
			HashMap<String, XCONTACTVEHICLERELBObj> resContVechRelMap1 = new HashMap<String, XCONTACTVEHICLERELBObj>();
			HashMap<String, XCONTACTVEHICLERELBObj> nonMatchContVechRelMap = new HashMap<String, XCONTACTVEHICLERELBObj>();
			
			boolean vehicleMatch=true;
			
			if(inputRecords.size() > 0
					|| inputRecords != null) {
				for(XCONTACTVEHICLERELBObj inputContVechRel : inputRecords){
					
					for(XCONTACTVEHICLERELBObj dbContVechRel : dbRecords){
						
						if (StringUtils.isNonBlank(inputContVechRel.getXVEHICLEBObj().getCOMMISSION_NO())
							 && (inputContVechRel.getXVEHICLEBObj().getCOMMISSION_NO()).equals(dbContVechRel.getXVEHICLEBObj().getCOMMISSION_NO())) {
						 
						 vehicleMatch = false;
						 inputContVechRel.setCONT_ID(partyId);
						 resContVechRelMap1.put(dbContVechRel.getXVEHICLEBObj().getCOMMISSION_NO(), dbContVechRel);
						 resContVechRelMap.put(inputContVechRel.getXVEHICLEBObj().getCOMMISSION_NO(), inputContVechRel);
						 
						 for(Map.Entry<String, XCONTACTVEHICLERELBObj> entrySource : resContVechRelMap.entrySet()){
							 
							 for(Map.Entry<String, XCONTACTVEHICLERELBObj> entryDB : resContVechRelMap1.entrySet()){
							 
							 XCONTACTVEHICLERELBObj xContVechSourceValue = entrySource.getValue();
							 XCONTACTVEHICLERELBObj xContVechDBValue = entryDB.getValue();
							 new DaimlerCustomerVehicleRelHelper().handleUpdateCustomerVehicleRel(xContVechSourceValue, xContVechDBValue, control, status);
							 
							//Start of change to search vehicle with commission number instead of global vin
							 if (status.getDwlErrorGroup() != null && status.getDwlErrorGroup().size() > 0) {
								 status.setStatus(DWLStatus.FATAL);
									 throw new DaimlerCompositeException( DaimlerCompositeUtils.createResponseByStatus(status));
								}
							//End of change to search vehicle with commission number instead of global vin
							 break;
							 }
							 
						 } break;
						 						 
						} else {
							
							vehicleMatch = true;							
							continue;						 
								 
							}										
						}
					
					if(vehicleMatch == true) {
						
						nonMatchContVechRelMap.put(inputContVechRel.getXVEHICLEBObj().getCOMMISSION_NO(), inputContVechRel);
						 inputContVechRel.setCONT_ID(partyId);
							for(Map.Entry<String, XCONTACTVEHICLERELBObj> entryNonMatch : nonMatchContVechRelMap.entrySet()){
							 XCONTACTVEHICLERELBObj xContVechValue = entryNonMatch.getValue();
							 new DaimlerCustomerVehicleRelHelper().handleAddPartyVehicleRel(xContVechValue, status);
							 
							}
						}
						
					}
							
				}
				
			if (status.getDwlErrorGroup() != null && status.getDwlErrorGroup().size() > 0) {
			 status.setStatus(DWLStatus.FATAL);
				 throw new DaimlerCompositeException( DaimlerCompositeUtils.createResponseByStatus(status));
			}

		 } catch (Exception exception) {
			 DaimlerCompositeUtils.handleException(exception);
		 } finally {
			 logger.info("MaintainCustomerVehicleRelationCompositeTxnBP:resolveIdentity- RETURN");
		 }
	}
    
    private Vector<XCONTACTVEHICLERELBObj> fireAddTransaction(Vector<XCONTACTVEHICLERELBObj> reqVecContVechRel, DWLControl control, String partyId) 
			throws BusinessProxyException, DaimlerCompositeException {
		DWLResponse response = null;
		
		DWLStatus status = null;
		Vector<DWLError> error = null;
		String serr = null;
		StringBuffer sBuffer = new StringBuffer();
		Vector<XCONTACTVEHICLERELBObj> respContVehicleRel = new Vector<XCONTACTVEHICLERELBObj>();
		
		try{
		
		for(XCONTACTVEHICLERELBObj contVechRelObj : reqVecContVechRel){
			DWLTransactionPersistent contVehicleRelTxnObj = new DWLTransactionPersistent();
			contVechRelObj.setControl(control);
			contVechRelObj.setCONT_ID(partyId);
				
	    	contVehicleRelTxnObj.setTxnType("addXCONTACTVEHICLEREL");
	    	contVehicleRelTxnObj.setTxnControl(control);
	    	contVehicleRelTxnObj.setTxnTopLevelObject(contVechRelObj);
	    	
	    	response = (DWLResponse) super.execute(contVehicleRelTxnObj);
	    	
	    	status = response.getStatus();
	    	
	    	error = status.getDwlErrorGroup();
	    		
	    	if (error != null && error.size() > 0) {
				 status.setStatus(DWLStatus.FATAL);
				 
				 for(DWLError dw : error){
					 
					 serr = dw.getErrorMessage();
					 
					 sBuffer.append("::" + serr);
					 
				 }
				
				 throw new DaimlerCompositeException( DaimlerCompositeUtils.createResponseByStatus(status));
				 
			}

			respContVehicleRel.add((XCONTACTVEHICLERELBObj)response.getData());

		}
		
		} 
		
		catch(DaimlerCompositeException ex){
			throw ex;
		}
		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}
		
	        return respContVehicleRel;
	}
    
    private Vector<XCONTACTVEHICLERELBObj> fireAddVehicleTransaction(Vector<XCONTACTVEHICLERELBObj> reqVecContVechRel, Vector<XVEHICLEBObj> vecDBVehicleBObj, DWLControl control, String partyId) 
			throws BusinessProxyException, DaimlerCompositeException {
		DWLResponse response = null;
		Vector<XCONTACTVEHICLERELBObj> respContVehicleRel = new Vector<XCONTACTVEHICLERELBObj>();
		
		DWLStatus status1 = null;
		Vector<DWLError> error1 = null;
		String serr1 = null;
		StringBuffer sBuffer1 = new StringBuffer();
		
		try{
		
		for(XCONTACTVEHICLERELBObj contVechRelObj : reqVecContVechRel){
			DWLTransactionPersistent contVehicleRelTxnObj = new DWLTransactionPersistent();
			contVechRelObj.setControl(control);
			contVechRelObj.setCONT_ID(partyId);
			
			for(XVEHICLEBObj vehicleObj : vecDBVehicleBObj){				
				contVechRelObj.setVEHICLE_ID(vehicleObj.getXVEHICLEpkId());	
				
				//End date active owner role for this vehicle if it is present for another customer and owner role is coming in request - starts 21/08/2017
			Vector<XCONTACTVEHICLEROLEBObj> vecXContactVehicleRoleBObj =  contVechRelObj.getItemsXCONTACTVEHICLEROLEBObj();
				
				for(XCONTACTVEHICLEROLEBObj xcontactvehiclerolebObj : vecXContactVehicleRoleBObj){
					if(DaimlerCommonSQLConstants.VEHICLE_ROLE_TP_CDVALUE_OWNER.equals(xcontactvehiclerolebObj.getVEHICLE_ROLE_TP_CDValue())){
						Vector<XCONTACTVEHICLEROLEBObj> vecDbVecRoleBObj = getVehicleRoleByVehiclePkID(vehicleObj.getXVEHICLEpkId(),null, control);
						EndDateActiveOwnerVehicleRole(vecDbVecRoleBObj,control);
					}
				}
				//End date active owner role for this vehicle if it is present for another customer and owner role is coming in request - Ends 21/08/2017
				
			}
			
	    	contVehicleRelTxnObj.setTxnType("addXCONTACTVEHICLEREL");
	    	contVehicleRelTxnObj.setTxnControl(control);
	    	contVehicleRelTxnObj.setTxnTopLevelObject(contVechRelObj);
	    	
	    	response = (DWLResponse) super.execute(contVehicleRelTxnObj);
	    	
	    	status1 = response.getStatus();
	    	
	    	error1 = status1.getDwlErrorGroup();
	    		
	    	if (error1 != null && error1.size() > 0) {
				 status1.setStatus(DWLStatus.FATAL);
				 
				 for(DWLError dw : error1){
					 
					 serr1 = dw.getErrorMessage();
					 
					 sBuffer1.append("::" + serr1);
					 
				 }
				
				 throw new BusinessProxyException(sBuffer1.toString());
				 
			}
	    	

			for(XVEHICLEBObj vehicleObj : vecDBVehicleBObj){				
				((XCONTACTVEHICLERELBObj)response.getData()).setXVEHICLEBObj(vehicleObj);		
			}

			respContVehicleRel.add((XCONTACTVEHICLERELBObj)response.getData());
			
		}
		
		} 
		
		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}
		
	        return respContVehicleRel;
	}
    
   
	private Vector<XCONTACTVEHICLERELBObj> fireAddUpdateTransaction(Vector<XCONTACTVEHICLERELBObj> reqVecContVechRel, Vector<XCONTACTVEHICLERELBObj> vecDBContactVehicleRelBObj, DWLControl control, String partyId) 
			throws BusinessProxyException, DaimlerCompositeException {
		DWLResponse response = null;
		Vector<XCONTACTVEHICLERELBObj> respContVehicleRel = new Vector<XCONTACTVEHICLERELBObj>();
		
		DWLStatus status2 = null;
		Vector<DWLError> error2 = null;
		String serr2 = null;
		StringBuffer sBuffer2 = new StringBuffer();
		
		try{
			
		HashMap<String, XCONTACTVEHICLERELBObj> resContVechRelMap = new HashMap<String, XCONTACTVEHICLERELBObj>();
		HashMap<String, XCONTACTVEHICLERELBObj> nonMatchContVechRelMap = new HashMap<String, XCONTACTVEHICLERELBObj>();
		
		boolean vehicleToMatch=true;
		
		for(XCONTACTVEHICLERELBObj contVechRelObj : reqVecContVechRel){
			
			DWLTransactionPersistent contVehicleRelTxnObj = new DWLTransactionPersistent();
			
			DaimlerAdditionComponent daimlerAddComponent = new  DaimlerAdditionComponent();
			
			for(XCONTACTVEHICLERELBObj dbContVechRelObj : vecDBContactVehicleRelBObj){
			
			if(StringUtils.isNonBlank(contVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO())
					 && (contVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO()).equals(dbContVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO())){
				
				vehicleToMatch = false;
				
				resContVechRelMap.put(contVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO(), contVechRelObj);
				
				for(Map.Entry<String, XCONTACTVEHICLERELBObj> entrySource : resContVechRelMap.entrySet()){
					 
					 XCONTACTVEHICLERELBObj xContVechSourceValue = entrySource.getValue();
					 
					 xContVechSourceValue.setControl(control);
					 xContVechSourceValue.setCONT_ID(partyId);
					 
					 response = daimlerAddComponent.updateXCONTACTVEHICLEREL(xContVechSourceValue);
					 
					 status2 = response.getStatus();
				    	
				     error2 = status2.getDwlErrorGroup();
				    		
			    	 if (error2 != null && error2.size() > 0) {
						 status2.setStatus(DWLStatus.FATAL);
							 
						 for(DWLError dw : error2){
								 
								 serr2 = dw.getErrorMessage();
								 
								 sBuffer2.append("::" + serr2);
								 
							}
							
							 throw new BusinessProxyException(sBuffer2.toString());
							 
						}
				     
				     break;
				 } break;				
				
			} else {
				
				vehicleToMatch = true;							
				continue;
			}

		}
			
		if(vehicleToMatch == true){
			
			nonMatchContVechRelMap.put(contVechRelObj.getXVEHICLEBObj().getCOMMISSION_NO(), contVechRelObj);
			
			for(Map.Entry<String, XCONTACTVEHICLERELBObj> entryNonMatch : nonMatchContVechRelMap.entrySet()){
				 XCONTACTVEHICLERELBObj xContVechValue = entryNonMatch.getValue();
				 
				 response = daimlerAddComponent.addXCONTACTVEHICLEREL(xContVechValue);
				 
				 status2 = response.getStatus();
			    	
			     error2 = status2.getDwlErrorGroup();
			    		
		    	 if (error2 != null && error2.size() > 0) {
					 status2.setStatus(DWLStatus.FATAL);
						 
					 for(DWLError dw : error2){
							 
							 serr2 = dw.getErrorMessage();
							 
							 sBuffer2.append("::" + serr2);
							 
						}
						
						 throw new BusinessProxyException(sBuffer2.toString());
						 
					}
					 
				}
			
			}

			respContVehicleRel.add((XCONTACTVEHICLERELBObj)response.getData());
			
		}
		
		} 
		
		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}
		
	        return respContVehicleRel;
	}
    
	
	//This method returns all the active owner roles for a vehicle 
    public Vector<XCONTACTVEHICLEROLEBObj> getVehicleRoleByVehiclePkID(String vehicleId, String partyId, DWLControl control) throws Exception
   	{

   		Vector<XCONTACTVEHICLEROLEBObj> resultVehicleRoleBObj = new Vector<XCONTACTVEHICLEROLEBObj>();
       	
   		List<SQLParam> params = new ArrayList<SQLParam>();

   		if(partyId == null){
   			partyId = String.valueOf(0);
   		}
   		params.add(0, new SQLParam(vehicleId));
   		params.add(1, new SQLParam(partyId));

   		resultVehicleRoleBObj = executeQueryVehicleRoleObject(DaimlerCommonSQLConstants.GET_VEHICLE_ROLE_BY_VEHICLE_ID, params, control);

   		return resultVehicleRoleBObj;
   	}
    
   
    public Vector<XCONTACTVEHICLEROLEBObj> executeQueryVehicleRoleObject(String sqlStatement, List params, DWLControl control) throws Exception {
    	
    	Vector<XCONTACTVEHICLEROLEBObj> resultContVechRole = new Vector<XCONTACTVEHICLEROLEBObj>();
    	DaimlerAdditionComponent xContactVechRoleComp =  new DaimlerAdditionComponent();
    	//DWLControl control = null;
    	String xContactVehicleRoleId = null;
    	SQLQuery query = new SQLQuery();
    	ResultSet rs = null;
    	try {		
    		rs = query.executeQuery(sqlStatement, params);
    			if (rs.next()) {
    				do {
    					xContactVehicleRoleId = rs.getString(DaimlerCommonSQLConstants.VEHICLE_ROLE_ID);

    					XCONTACTVEHICLEROLEBObj contVechRoleObj = ((XCONTACTVEHICLEROLEBObj)
    							(xContactVechRoleComp.getXCONTACTVEHICLEROLE(xContactVehicleRoleId, control)).getData());
    					if(contVechRoleObj != null){
    						resultContVechRole.add(contVechRoleObj);
    					}
    				}while (rs.next());
    			}
    	} catch (Exception e) {
    		throw e;
    	}
    	finally
    	{
    		if (rs != null && !rs.isClosed())
    		{
    			rs.close();
    			rs = null;
    		}
    		query.close();
    	}
    	
    	return resultContVechRole;
    	
    }
    
    //THis method is used to end date active owner role for a vehicle
    public void EndDateActiveOwnerVehicleRole(
			Vector<XCONTACTVEHICLEROLEBObj> vecDbVecRoleBObj, DWLControl control) {
		
    	DaimlerAdditionComponent xContactVechRelComp =  new DaimlerAdditionComponent();
    	SQLQuery query = new SQLQuery();
    	ResultSet rs = null;
    	DWLResponse response = null;
    	DWLStatus status = null;
    	Vector<DWLError> error = null;
    	String serr = null;
		StringBuffer sBuffer = new StringBuffer();
    	try{
	    	for(XCONTACTVEHICLEROLEBObj dbRoleBObj : vecDbVecRoleBObj){
	    		//XCONTACTVEHICLEROLEBObj xcontactvehiclerolebObj = new XCONTACTVEHICLEROLEBObj();
	        	
	    		DWLTransactionPersistent contVehicleRoleTxnObj = new DWLTransactionPersistent();
	    		dbRoleBObj.setControl(control);
	    		//xcontactvehiclerolebObj.setXCONTACT_VEHICLE_ROLE_PK_ID(dbRoleBObj.getXCONTACT_VEHICLE_ROLE_PK_ID());
	    		
	    		long milliseconds = System.currentTimeMillis();
	    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");    
	    		Date endDate = new Date(milliseconds);
	    		dbRoleBObj.setEND_DT(sdf.format(endDate));
	    		//xcontactvehiclerolebObj.setXCONTACTVEHICLEROLELastUpdateDate(sdf.format(endDate));
					
	    		contVehicleRoleTxnObj.setTxnType("updateXCONTACTVEHICLEROLE");
	    		contVehicleRoleTxnObj.setTxnControl(control);
	    		contVehicleRoleTxnObj.setTxnTopLevelObject(dbRoleBObj);
	    		
		    	response = (DWLResponse) super.execute(contVehicleRoleTxnObj);
		    	
		    	status = response.getStatus();
		    	
		    	error = status.getDwlErrorGroup();
		    		
		    	if (error != null && error.size() > 0) {
					 status.setStatus(DWLStatus.FATAL);
					 
					 for(DWLError dw : error){
						 
						 serr = dw.getErrorMessage();
						 
						 sBuffer.append("::" + serr);
						 
					 }
					
					 throw new DaimlerCompositeException( DaimlerCompositeUtils.createResponseByStatus(status));
					 
				}
	
	    	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
	}
}


