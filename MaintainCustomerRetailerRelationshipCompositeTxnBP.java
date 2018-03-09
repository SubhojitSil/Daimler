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
 * IBM-MDMWB-1.0-[c82e35897b1eda850e481f1a27b7f1cb]
 */

package com.daimler.cdm.mdm.services.compositeTxn;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.daimler.cdm.mdm.addition.component.CustomerRetailerRelationshipBObj;
import com.daimler.cdm.mdm.addition.component.DaimlerAdditionComponent;
import com.daimler.cdm.mdm.addition.component.XCONTACTRETAILERRELBObj;
import com.daimler.cdm.mdm.addition.component.XCONTACTRETAILERROLEBObj;
import com.daimler.cdm.mdm.addition.component.XCONTACTVEHICLERELBObj;
import com.daimler.cdm.mdm.addition.component.XRETAILERBObj;
import com.daimler.cdm.mdm.addition.component.XVEHICLEBObj;
import com.daimler.cdm.mdm.services.constants.DaimlerCommonSQLConstants;
import com.daimler.cdm.mdm.services.exception.DaimlerCompositeException;
import com.daimler.cdm.mdm.services.helper.DaimlerCustomerRetailerRelHelper;
import com.daimler.cdm.mdm.services.utils.DaimlerCompositeUtils;
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
import com.dwl.tcrm.common.TCRMResponse;
import com.dwl.tcrm.coreParty.component.TCRMAdminContEquivBObj;
import com.dwl.tcrm.utilities.TCRMClassFactory;
import com.dwl.unifi.tx.exception.BusinessProxyException;
import com.daimler.cdm.mdm.services.constant.*;
import com.dwl.base.DWLControl;

/**
 * <!-- begin-user-doc --> <!-- end-user-doc -->
 *
 * 
 * @generated NOT
 */
public class MaintainCustomerRetailerRelationshipCompositeTxnBP extends
		DWLTxnBP {

	/**
	 * @generated NOT
	 **/
	private IDWLErrorMessage errHandler;
	Vector<DWLError> vectReqDWLError = new Vector<DWLError>();
	boolean gscode = false;
	boolean gssncode = false;

	/**
	 * <!-- begin-user-doc --> <!-- end-user-doc -->
	 * 
	 * @generated
	 */
	private final static com.dwl.base.logging.IDWLLogger logger = com.dwl.base.logging.DWLLoggerManager
			.getLogger(MaintainCustomerRetailerRelationshipCompositeTxnBP.class);

	/**
	 * @generated NOT
	 **/
	public MaintainCustomerRetailerRelationshipCompositeTxnBP() {
		super();
		errHandler = TCRMClassFactory.getErrorHandler();
	}

	/**
	 * @generated NOT
	 **/
	@SuppressWarnings("unchecked")
	//public synchronized Object execute(Object inputObj) throws BusinessProxyException {
		public  Object execute(Object inputObj) throws BusinessProxyException {
		logger.finest("ENTER Object execute(Object inputObj)");
		DWLResponse responseObj = null;
		DWLTransactionPersistent inputTxnObj = (DWLTransactionPersistent) inputObj;
		DWLControl control = inputTxnObj.getTxnControl();
		String partyId = null;
		CustomerRetailerRelationshipBObj responsePartyRel = new CustomerRetailerRelationshipBObj();
		Vector<XCONTACTRETAILERRELBObj> vecDBRetailerRelBObj = null;
		Vector<XRETAILERBObj> vecRetailerEntDatedBObj = null;
		CustomerRetailerRelationshipBObj partyRetailerRel = (CustomerRetailerRelationshipBObj) inputTxnObj
				.getTxnTopLevelObject();
		Vector<XRETAILERBObj> vecDBActiveRetailerBObj = null;
		Vector<XRETAILERBObj> vecDBRetailerBObj = null;
		XRETAILERBObj updateParentRetailerBObj = null;
		XRETAILERBObj updateDBRetailerBObj = null;
		XRETAILERBObj vecUpdateRetailerBObj = null;
		XRETAILERBObj vecDBUpdateRetailerBObj = null;
		String retailer_id = null;
		long startTime;
		long stopTime;

		Vector<XCONTACTRETAILERRELBObj> parentRetailerRelBObj = partyRetailerRel
				.getItemsXCONTACTRETAILERRELBObj();
		DaimlerAdditionComponent xRetailerRelComp = new DaimlerAdditionComponent();
		try {
			// Validate the input
			preExecute(inputTxnObj, control);
			// AdminSystemType and AdminClientid validation Check
			
			long startTime1 = System.currentTimeMillis();
			partyId = validParty(partyRetailerRel.getTCRMAdminContEquivBObj(),
					control);
		
			long stopTime1 = System.currentTimeMillis();
			 long elapsedTime = stopTime1 - startTime1;
			//control.put("CRR_ValidParty",  elapsedTime);
			
			if (partyId != null || StringUtils.isNonBlank(partyId)) {
				for (XCONTACTRETAILERRELBObj retailerRelObj : parentRetailerRelBObj) {
					if (retailerRelObj != null
							&& retailerRelObj.getXRETAILERBObj() != null
							&& retailerRelObj.getXRETAILERBObj()
									.getRETAILER_GS_CODE() != null) {
						retailer_id = retailerRelObj.getXRETAILERBObj()
								.getRETAILER_GS_CODE();
						vecDBRetailerRelBObj = retailerRelObjByPartyIdandGSCODE(
								partyId, retailer_id, control);
						vecDBRetailerBObj = retailerObjectByRetailerIdandGSCode(
								retailer_id, control);
						if(vecDBRetailerRelBObj.isEmpty() && vecDBRetailerBObj.isEmpty()){
							retailer_id = retailerRelObj.getXRETAILERBObj()
									.getRETAILER_GSSN_CODE();
							vecDBRetailerRelBObj = retailerRelObjByPartyIdandGSSNCODE(
									partyId, retailer_id, control);
							vecDBRetailerBObj = retailerObjectByRetailerIdandGSSNCode(
									retailer_id, control);
						}
						gscode = true;
					} else {
						retailer_id = retailerRelObj.getXRETAILERBObj()
								.getRETAILER_GSSN_CODE();
						vecDBRetailerRelBObj = retailerRelObjByPartyIdandGSSNCODE(
								partyId, retailer_id, control);
						vecDBRetailerBObj = retailerObjectByRetailerIdandGSSNCode(
								retailer_id, control);
						gssncode = true;
					}

					if (vecDBRetailerRelBObj == null
							|| vecDBRetailerRelBObj.isEmpty()) {
						if (vecDBRetailerBObj == null
								|| vecDBRetailerBObj.isEmpty()) {
							vecDBRetailerRelBObj = fireAddTransaction(
									parentRetailerRelBObj, control, partyId);
						} else {
							parentRetailerRelBObj.get(0).setObjectReferenceId(
									"true");
							vecDBRetailerRelBObj = fireAddRetailerTransaction(
									parentRetailerRelBObj, vecDBRetailerBObj,
									control, partyId);
						}

					} else {

						resolveIdentity(parentRetailerRelBObj,
								vecDBRetailerRelBObj, control, partyId);
						vecDBRetailerRelBObj = fireAddUpdateTransaction(
								parentRetailerRelBObj, vecDBRetailerRelBObj,
								control, partyId);

						updateParentRetailerBObj = parentRetailerRelBObj.get(0)
								.getXRETAILERBObj();

						updateDBRetailerBObj = vecDBRetailerRelBObj.get(0)
								.getXRETAILERBObj();

						updateParentRetailerBObj
								.setXRETAILERpkId(updateDBRetailerBObj
										.getXRETAILERpkId());

						updateParentRetailerBObj
								.setXRETAILERLastUpdateDate(updateDBRetailerBObj
										.getXRETAILERLastUpdateDate());

						vecUpdateRetailerBObj = (XRETAILERBObj) (xRetailerRelComp
								.updateXRETAILER(updateParentRetailerBObj))
								.getData();

						for (XCONTACTRETAILERRELBObj responseBObj : vecDBRetailerRelBObj) {

							responseBObj
									.setXRETAILERBObj(vecUpdateRetailerBObj);
						}
					}

				/*	for (XCONTACTRETAILERRELBObj retailerRelObj1 : parentRetailerRelBObj) {

						if (gscode) {
							String gscode = retailerRelObj1.getXRETAILERBObj()
									.getRETAILER_GS_CODE();

							vecDBActiveRetailerBObj = activeRetailerObjByGSCODE(
									gscode, control);

							vecRetailerEntDatedBObj = retailerObjectByRetailerIdandGSCode(
									gscode, control);
						} else {
							String gssncode = retailerRelObj1
									.getXRETAILERBObj().getRETAILER_GSSN_CODE();

							vecDBActiveRetailerBObj = activeRetailerObjByGSSNCODE(
									gssncode, control);

							vecRetailerEntDatedBObj = retailerObjectByRetailerIdandGSSNCode(
									gssncode, control);
						}

					}*/

					responsePartyRel.setControl(control);
					for (XCONTACTRETAILERRELBObj responseBObj : vecDBRetailerRelBObj) {
						if (vecDBUpdateRetailerBObj != null) {
							responseBObj
									.setXRETAILERBObj(vecDBUpdateRetailerBObj);
						}

						responsePartyRel
								.setXCONTACTRETAILERRELBObj(responseBObj);
					}

					responsePartyRel.setTCRMAdminContEquivBObj(partyRetailerRel
							.getTCRMAdminContEquivBObj());
				}
			}

			else {
				// throw error

				DWLError error = errHandler
						.getErrorMessage(
								DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
								DWLErrorCode.DATA_INVALID_ERROR,
								DaimlerBusinessServicesErrorReasonCode.CUSTOMER_NOT_PRESENT,
								control, new String[0]);
				vectReqDWLError.add(error);
				throw new BusinessProxyException(error.getErrorMessage());

			}

			DWLStatus outputStatus = new DWLStatus();
			outputStatus.setStatus(DWLStatus.SUCCESS);

			responseObj = new TCRMResponse();
			responseObj.setStatus(outputStatus);
			responseObj.setData(responsePartyRel);

			logger.finest("RETURN Object execute(Object inputObj)");
			return responseObj;

		} catch (BusinessProxyException ex) {
			return fatalResponse(responseObj);
		} catch (Exception ex) {
			ex.printStackTrace();
			return fatalResponse(responseObj);
		}

	}

	/**
	 * 
	 * @param responseObj
	 * @return
	 */
	private DWLResponse fatalResponse(DWLResponse responseObj) {

		DWLStatus outputStatus = new DWLStatus();
		outputStatus.setStatus(DWLStatus.FATAL);
		/*
		 * DWLError error =new DWLError();
		 * error.setErrorMessage(ex.getMessage());
		 */

		outputStatus.setDwlErrorGroup(vectReqDWLError);
		responseObj = new TCRMResponse();
		responseObj.setStatus(outputStatus);
		return responseObj;
	}

	/**
	 * 
	 * @param sourceRetailerId
	 * @param control
	 * @return
	 */
	private Vector<XRETAILERBObj> activeRetailerObjBySourceRetailerId(
			String sourceRetailerId, DWLControl control) {
		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(sourceRetailerId));

		try {
			resultRetailerBObj = executeQueryActiveRetaler(
					DaimlerCommonSQLConstants.GET_ACTIVE_RETAILER_ID, params,
					control);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param sourceRetailerId
	 * @param control
	 * @return
	 */
	private Vector<XRETAILERBObj> activeRetailerObjByGSCODE(
			String sourceRetailerId, DWLControl control) {
		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(sourceRetailerId));

		try {
			
			long startTime = System.currentTimeMillis();
			
			resultRetailerBObj = executeQueryActiveRetaler(
					DaimlerCommonSQLConstants.GET_ACTIVE_RETAILER_ID_BY_GSCODE,
					params, control);
			
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			//control.put("CRR_GET_ACTIVE_RETAILER_ID_BY_GSCODE",  elapsedTime);
		
		

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param sourceRetailerId
	 * @param control
	 * @return
	 */
	private Vector<XRETAILERBObj> activeRetailerObjByGSSNCODE(
			String sourceRetailerId, DWLControl control) {
		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(sourceRetailerId));

		try {
			long startTime = System.currentTimeMillis();
			
			resultRetailerBObj = executeQueryActiveRetaler(
					DaimlerCommonSQLConstants.GET_ACTIVE_RETAILER_ID_BY_GSSNCODE,
					params, control);
			
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;

			//control.put("CRR_GET_ACTIVE_RETAILER_ID_BY_GSSNCODE",  elapsedTime);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param sqlStatement
	 * @param params
	 * @param control
	 * @return
	 * @throws Exception
	 */
	public Vector<XRETAILERBObj> executeQueryActiveRetaler(String sqlStatement,
			List params, DWLControl control) throws Exception {

		Vector<XRETAILERBObj> resultRetRel = new Vector<XRETAILERBObj>();
		DaimlerAdditionComponent xRetRelComp = new DaimlerAdditionComponent();
		// DWLControl control = null;
		String xRetailerId = null;
		SQLQuery query = new SQLQuery();
		ResultSet rs = null;
		try {
			rs = query.executeQuery(sqlStatement, params);
			if (rs.next()) {
				do {
					xRetailerId = rs
							.getString(DaimlerCommonSQLConstants.ACTIVE_RETAILER_ID);

					XRETAILERBObj contRetRelObj = ((XRETAILERBObj) (xRetRelComp
							.getXRETAILER(xRetailerId, control)).getData());
					if (contRetRelObj != null) {
						resultRetRel.add(contRetRelObj);
					}
				} while (rs.next());
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null && !rs.isClosed()) {
				rs.close();
				rs = null;
			}
			query.close();
		}

		return resultRetRel;

	}

	/**
	 * 
	 * @param globalVIN
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XVEHICLEBObj> vehicleObjectBySourceVehicleId(
			String globalVIN, DWLControl control) throws Exception {

		Vector<XVEHICLEBObj> resultVehicleBObj = new Vector<XVEHICLEBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(globalVIN));

		resultVehicleBObj = executeQueryVehicleObject(
				DaimlerCommonSQLConstants.GET_VEHICLE_ID, params, control);

		return resultVehicleBObj;
	}

	/**
	 * 
	 * @param theDWLTxnObj
	 * @param control
	 * @throws BusinessProxyException
	 */
	private void preExecute(DWLTransactionPersistent theDWLTxnObj,
			DWLControl control) throws BusinessProxyException {
		logger.finest("ENTER method preExecute");

		DWLCommon toplevelObject = (DWLCommon) theDWLTxnObj
				.getTxnTopLevelObject();

		if (!(toplevelObject instanceof CustomerRetailerRelationshipBObj)) {
			DWLError error = errHandler
					.getErrorMessage(
							DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
							DWLErrorCode.FIELD_VALIDATION_ERROR,
							DaimlerBusinessServicesErrorReasonCode.MAINTAINCUSTOMERRETAILERRELATIONSHIP_FAILED,
							theDWLTxnObj.getTxnControl(), new String[0]);
			vectReqDWLError.add(error);
			throw new BusinessProxyException(error.getErrorMessage());
		} else {

			CustomerRetailerRelationshipBObj customerRetailerRelationship = (CustomerRetailerRelationshipBObj) toplevelObject;

			if (customerRetailerRelationship.getItemsXCONTACTRETAILERRELBObj()
					.size() > 1) {
				DWLError error = errHandler
						.getErrorMessage(
								DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
								DWLErrorCode.FIELD_VALIDATION_ERROR,
								DaimlerBusinessServicesErrorReasonCode.MORE_THAN_1_RETAILERREL,
								theDWLTxnObj.getTxnControl(), new String[0]);
				vectReqDWLError.add(error);
				throw new BusinessProxyException(error.getErrorMessage());
			} else {
				if (customerRetailerRelationship
						.getItemsXCONTACTRETAILERRELBObj().size() == 0) {
					DWLError error = errHandler
							.getErrorMessage(
									DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
									DWLErrorCode.FIELD_VALIDATION_ERROR,
									DaimlerBusinessServicesErrorReasonCode.BLANK_XRETAILERREL,
									theDWLTxnObj.getTxnControl(), new String[0]);
					vectReqDWLError.add(error);
					throw new BusinessProxyException(error.getErrorMessage());
				}
				XCONTACTRETAILERRELBObj xRetailerRel = (XCONTACTRETAILERRELBObj) customerRetailerRelationship
						.getItemsXCONTACTRETAILERRELBObj().get(0);
				TCRMAdminContEquivBObj adminContEquiv = customerRetailerRelationship
						.getTCRMAdminContEquivBObj();

				if (xRetailerRel == null) {
					DWLError error = errHandler
							.getErrorMessage(
									DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
									DWLErrorCode.FIELD_VALIDATION_ERROR,
									DaimlerBusinessServicesErrorReasonCode.BLANK_XRETAILERREL,
									theDWLTxnObj.getTxnControl(), new String[0]);
					vectReqDWLError.add(error);
					throw new BusinessProxyException(error.getErrorMessage());
				} else {
					if (xRetailerRel.getXRETAILERBObj() == null) {
						DWLError error = errHandler
								.getErrorMessage(
										DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
										DWLErrorCode.FIELD_VALIDATION_ERROR,
										DaimlerBusinessServicesErrorReasonCode.BLANK_RETAILER_OBJECT,
										theDWLTxnObj.getTxnControl(),
										new String[0]);
						vectReqDWLError.add(error);
						throw new BusinessProxyException(
								error.getErrorMessage());

					} else if ((xRetailerRel.getXRETAILERBObj()
							.getRETAILER_GSSN_CODE() == null || xRetailerRel
							.getXRETAILERBObj().getRETAILER_GSSN_CODE()
							.isEmpty())
							&& (xRetailerRel.getXRETAILERBObj()
									.getRETAILER_GS_CODE() == null || xRetailerRel
									.getXRETAILERBObj().getRETAILER_GS_CODE()
									.isEmpty())) {
						DWLError error = errHandler
								.getErrorMessage(
										DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
										DWLErrorCode.FIELD_VALIDATION_ERROR,
										DaimlerBusinessServicesErrorReasonCode.GS_CODE_GSSN_CODE_MANDATORY,
										theDWLTxnObj.getTxnControl(),
										new String[0]);
						vectReqDWLError.add(error);
						throw new BusinessProxyException(
								error.getErrorMessage());

					}
				}
				if (adminContEquiv == null
						|| !StringUtils.isNonBlank(adminContEquiv
								.getAdminPartyId())) {
					DWLError error = errHandler
							.getErrorMessage(
									DaimlerBusinessServicesComponentID.MAINTAIN_CUSTOMER_RETAILER_RELATIONSHIP_BUSINESS_PROXY,
									DWLErrorCode.FIELD_VALIDATION_ERROR,
									DaimlerBusinessServicesErrorReasonCode.BLANK_ADMIN_SYS_TYPE,
									theDWLTxnObj.getTxnControl(), new String[0]);
					vectReqDWLError.add(error);
					throw new BusinessProxyException(error.getErrorMessage());
				}
			}
		}
	}

	/**
	 * 
	 * @param globalVIN
	 * @param control
	 * @return
	 */
	private boolean validateGVIN(String globalVIN, DWLControl control) {
		logger.finest("ENTER method validateGVIN");
		Vector<XVEHICLEBObj> resultVehicleBObj = new Vector<XVEHICLEBObj>();
		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(globalVIN));

		try {
			resultVehicleBObj = executeQueryVehicleObject(
					DaimlerCommonSQLConstants.GET_VEHICLE_ID, params, control);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (resultVehicleBObj == null || resultVehicleBObj.isEmpty()) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * 
	 * @param sqlStatement
	 * @param params
	 * @param control
	 * @return
	 * @throws Exception
	 */
	public Vector<XVEHICLEBObj> executeQueryVehicleObject(String sqlStatement,
			List params, DWLControl control) throws Exception {
		logger.finest("ENTER method executeQueryVehicleObject");
		Vector<XVEHICLEBObj> resultContVechRel = new Vector<XVEHICLEBObj>();
		DaimlerAdditionComponent xContactVechRelComp = new DaimlerAdditionComponent();
		// DWLControl control = null;
		String xVehicleId = null;
		SQLQuery query = new SQLQuery();
		ResultSet rs = null;
		try {
			rs = query.executeQuery(sqlStatement, params);
			if (rs.next()) {
				do {
					xVehicleId = rs
							.getString(DaimlerCommonSQLConstants.VEHICLE_ID);

					XVEHICLEBObj contVechRelObj = ((XVEHICLEBObj) (xContactVechRelComp
							.getXVEHICLE(xVehicleId, control)).getData());
					if (contVechRelObj != null) {
						resultContVechRel.add(contVechRelObj);
					}
				} while (rs.next());
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null && !rs.isClosed()) {
				rs.close();
				rs = null;
			}
			query.close();
		}

		return resultContVechRel;

	}

	/**
	 * 
	 * @param contEquiv
	 * @param dwlControl
	 * @return
	 * @throws Exception
	 */
	private String validParty(TCRMAdminContEquivBObj contEquiv,
			DWLControl dwlControl) throws Exception {
		// TODO Auto-generated method stub
		logger.finest("ENTER method validParty");
		SQLQuery sqlQuery = new SQLQuery();
		ResultSet objResultSet = null;
		String getPartyIdSql = DaimlerCommonSQLConstants.SEARCH_CUSTOMER_BY_CONTEQUIV;
		String strContIDActiveParty = null;
		String adminSystemType = null;
		String adminPartyId = null;
		String adminSystemValue = null;
		List<SQLParam> params = new ArrayList<SQLParam>();

		adminSystemType = contEquiv.getAdminSystemType();
		adminPartyId = contEquiv.getAdminPartyId();
		adminSystemValue = contEquiv.getAdminSystemValue();

		if (adminSystemType == null) {
			adminSystemType = DaimlerCustomerRetailerRelHelper.getCodeType(
					adminSystemValue,
					DaimlerCommonSQLConstants.CODETABLE_ADMINSYSTP, dwlControl);
		}

		try {
			params = new ArrayList<SQLParam>();
			params.add(0, new SQLParam(adminPartyId));
			params.add(1, new SQLParam(adminSystemType));

		
			objResultSet = sqlQuery.executeQuery(getPartyIdSql, params);
			
		
			
			while (objResultSet.next()) {
				strContIDActiveParty = objResultSet
						.getString(DaimlerCommonSQLConstants.GET_PARTY_ID);
			}

			objResultSet.close();
		} catch (Exception ex) {

		}

		return strContIDActiveParty;
	}

	/**
	 * 
	 * @param partyId
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XCONTACTRETAILERRELBObj> RetailerRelObjByPartyId(
			String partyId, String retailer_id, DWLControl control)
			throws Exception {
		Vector<XCONTACTRETAILERRELBObj> resultRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();
		List<SQLParam> params = new ArrayList<SQLParam>();
		params.add(0, new SQLParam(partyId));
		params.add(1, new SQLParam(retailer_id));
		resultRetailerRel = executeQueryRetailerRel(
				DaimlerCommonSQLConstants.GET_SOURCE_RETAILER_REL_ID, params,
				control);
		return resultRetailerRel;
	}

	/**
	 * 
	 * @param partyId
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XCONTACTRETAILERRELBObj> retailerRelObjByPartyIdandGSCODE(
			String partyId, String retailer_id, DWLControl control)
			throws Exception {
		Vector<XCONTACTRETAILERRELBObj> resultRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();
		List<SQLParam> params = new ArrayList<SQLParam>();
		params.add(0, new SQLParam(partyId));
		params.add(1, new SQLParam(retailer_id));
		
		long startTime = System.currentTimeMillis();
				
		resultRetailerRel = executeQueryRetailerRel(
				DaimlerCommonSQLConstants.GET_SOURCE_RETAILER_REL_ID_USING_GSCODE,
				params, control);
		
		long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
	   
		 	//control.put("CRR_GET_SOURCE_RETAILER_REL_ID_USING_GSCODE",  elapsedTime);
	
		return resultRetailerRel;
	}

	/**
	 * 
	 * @param partyId
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XCONTACTRETAILERRELBObj> retailerRelObjByPartyIdandGSSNCODE(
			String partyId, String retailer_id, DWLControl control)
			throws Exception {
		Vector<XCONTACTRETAILERRELBObj> resultRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();
		List<SQLParam> params = new ArrayList<SQLParam>();
		params.add(0, new SQLParam(partyId));
		params.add(1, new SQLParam(retailer_id));
		
		long startTime = System.currentTimeMillis();
		
		resultRetailerRel = executeQueryRetailerRel(
				DaimlerCommonSQLConstants.GET_SOURCE_RETAILER_REL_ID_USING_GSSNCODE,
				params, control);
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		//control.put("CRR_GET_SOURCE_RETAILER_REL_ID_USING_GSSNCODE",  elapsedTime);
		
		return resultRetailerRel;
	}

	/**
	 * 
	 * @param sqlStatement
	 * @param params
	 * @param control
	 * @return
	 * @throws Exception
	 */
	public Vector<XCONTACTRETAILERRELBObj> executeQueryRetailerRel(
			String sqlStatement, List params, DWLControl control)
			throws Exception {

		Vector<XCONTACTRETAILERRELBObj> resultRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();
		DaimlerAdditionComponent xRetailerRelComp = new DaimlerAdditionComponent();
		// DWLControl control = null;
		String RetailerRelId = null;
		SQLQuery query = new SQLQuery();
		ResultSet rs = null;
		try {
			rs = query.executeQuery(sqlStatement, params);
			if (rs.next()) {
				do {
					RetailerRelId = rs
							.getString(DaimlerCommonSQLConstants.RETAILER_REL_ID);

					XCONTACTRETAILERRELBObj RetailerRelObj = ((XCONTACTRETAILERRELBObj) (xRetailerRelComp
							.getXCONTACTRETAILERREL(RetailerRelId, control))
							.getData());
					if (RetailerRelObj != null) {
						resultRetailerRel.add(RetailerRelObj);
					}
				} while (rs.next());
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null && !rs.isClosed()) {
				rs.close();
				rs = null;
			}
			query.close();
		}

		return resultRetailerRel;

	}

	/**
	 * 
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XRETAILERBObj> RetailerObjectByRetailerId(
			String retailer_id, DWLControl control) throws Exception {

		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(retailer_id));

		resultRetailerBObj = executeQueryRetailerObject(
				DaimlerCommonSQLConstants.GET_RETAILER_ID, params, control);

		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XRETAILERBObj> retailerObjectByRetailerIdandGSCode(
			String retailer_id, DWLControl control) throws Exception {

		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(retailer_id));

		long startTime = System.currentTimeMillis();
		
		resultRetailerBObj = executeQueryRetailerObject(
				DaimlerCommonSQLConstants.GET_RETAILER_ID_BY_GSCODE, params,
				control);

		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		//control.put("CRR_GET_RETAILER_ID_BY_GSCODE",  elapsedTime);
		
		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param retailer_id
	 * @param control
	 * @return
	 * @throws Exception
	 */
	private Vector<XRETAILERBObj> retailerObjectByRetailerIdandGSSNCode(
			String retailer_id, DWLControl control) throws Exception {

		Vector<XRETAILERBObj> resultRetailerBObj = new Vector<XRETAILERBObj>();

		List<SQLParam> params = new ArrayList<SQLParam>();

		params.add(0, new SQLParam(retailer_id));

		long startTime=  System.currentTimeMillis();
		
		resultRetailerBObj = executeQueryRetailerObject(
				DaimlerCommonSQLConstants.GET_RETAILER_ID_BY_GSSNCODE, params,
				control);
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
			//control.put("CRR_GET_RETAILER_ID_BY_GSSNCODE",  elapsedTime);
		
		return resultRetailerBObj;
	}

	/**
	 * 
	 * @param sqlStatement
	 * @param params
	 * @param control
	 * @return
	 * @throws Exception
	 */

	public Vector<XRETAILERBObj> executeQueryRetailerObject(
			String sqlStatement, List params, DWLControl control)
			throws Exception {

		Vector<XRETAILERBObj> resultRetailerRel = new Vector<XRETAILERBObj>();
		DaimlerAdditionComponent xRetailerRelComp = new DaimlerAdditionComponent();
		// DWLControl control = null;
		String xRetailerId = null;
		SQLQuery query = new SQLQuery();
		ResultSet rs = null;
		try {
			rs = query.executeQuery(sqlStatement, params);
			if (rs.next()) {
				do {
					xRetailerId = rs
							.getString(DaimlerCommonSQLConstants.RETAILER_ID);

					XRETAILERBObj RetailerRelObj = ((XRETAILERBObj) (xRetailerRelComp
							.getXRETAILER(xRetailerId, control)).getData());
					if (RetailerRelObj != null) {
						resultRetailerRel.add(RetailerRelObj);
					}
				} while (rs.next());
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null && !rs.isClosed()) {
				rs.close();
				rs = null;
			}
			query.close();
		}

		return resultRetailerRel;

	}

	/**
	 * 
	 * @param reqVecRetailerRel
	 * @param control
	 * @param partyId
	 * @return
	 * @throws BusinessProxyException
	 * @throws DaimlerCompositeException
	 */

	private Vector<XCONTACTRETAILERRELBObj> fireAddTransaction(
			Vector<XCONTACTRETAILERRELBObj> reqVecRetailerRel,
			DWLControl control, String partyId) throws BusinessProxyException,
			DaimlerCompositeException {
		DWLResponse response = null;

		DWLStatus status = null;
		Vector<DWLError> error = null;
		String serr = null;
		StringBuffer sBuffer = new StringBuffer();
		Vector<XCONTACTRETAILERRELBObj> respRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();

		try {

			for (XCONTACTRETAILERRELBObj RetailerRelObj : reqVecRetailerRel) {
				DWLTransactionPersistent RetailerRelTxnObj = new DWLTransactionPersistent();
				RetailerRelObj.setControl(control);
				RetailerRelObj.setCONT_ID(partyId);

				RetailerRelTxnObj.setTxnType("addXCONTACTRETAILERREL");
				RetailerRelTxnObj.setTxnControl(control);
				RetailerRelTxnObj.setTxnTopLevelObject(RetailerRelObj);

				long startTime = System.currentTimeMillis();
				
				response = (DWLResponse) super.execute(RetailerRelTxnObj);
				
				long stopTime = System.currentTimeMillis();
				long elapsedTime = stopTime - startTime;
			
					//control.put("CRR_addXCONTACTRETAILERREL1",  elapsedTime);
				
				throwExceptionifTXNFail(response, sBuffer);
				respRetailerRel.add((XCONTACTRETAILERRELBObj) response
						.getData());

			}

		}

		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}

		return respRetailerRel;
	}

	/**
	 * 
	 * @param retailerRelObj
	 * @param control
	 */

	/**
	 * 
	 * @param response
	 * @param sBuffer
	 * @throws BusinessProxyException
	 */
	private void throwExceptionifTXNFail(DWLResponse response,
			StringBuffer sBuffer) throws BusinessProxyException {
		DWLStatus status;
		Vector<DWLError> error;
		String serr;
		status = response.getStatus();

		error = status.getDwlErrorGroup();

		if (error != null && error.size() > 0) {
			status.setStatus(DWLStatus.FATAL);

			for (DWLError dw : error) {

				serr = dw.getErrorMessage();

				sBuffer.append("::" + serr);
				vectReqDWLError.add(dw);

			}

			throw new BusinessProxyException(sBuffer.toString());

		}
	}

	/**
	 * 
	 * @param globalVIN
	 * @param control
	 * @return
	 */
	public String getVEHICLEID(String globalVIN, DWLControl control) {

		if (globalVIN != null) {
			Vector<XVEHICLEBObj> resultVehicleBObj = new Vector<XVEHICLEBObj>();
			List<SQLParam> params = new ArrayList<SQLParam>();

			params.add(0, new SQLParam(globalVIN));

			try {
				resultVehicleBObj = executeQueryVehicleObject(
						DaimlerCommonSQLConstants.GET_VEHICLE_ID, params,
						control);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (resultVehicleBObj == null || resultVehicleBObj.isEmpty()) {
				return null;
			} else {
				XVEHICLEBObj xVEHICLEBObj = (XVEHICLEBObj) resultVehicleBObj
						.get(0);
				return xVEHICLEBObj.getXVEHICLEpkId();
			}
		} else {
			return null;
		}

	}

	/**
	 * 
	 * @param reqVecRetailerRel
	 * @param vecDBRetailerBObj
	 * @param control
	 * @param partyId
	 * @return
	 * @throws BusinessProxyException
	 * @throws DaimlerCompositeException
	 */
	private Vector<XCONTACTRETAILERRELBObj> fireAddRetailerTransaction(
			Vector<XCONTACTRETAILERRELBObj> reqVecRetailerRel,
			Vector<XRETAILERBObj> vecDBRetailerBObj, DWLControl control,
			String partyId) throws BusinessProxyException,
			DaimlerCompositeException {
		DWLResponse response = null;
		Vector<XCONTACTRETAILERRELBObj> respRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();

		DWLStatus status1 = null;
		Vector<DWLError> error1 = null;
		String serr1 = null;
		StringBuffer sBuffer1 = new StringBuffer();

		try {

			for (XCONTACTRETAILERRELBObj RetailerRelObj : reqVecRetailerRel) {
				DWLTransactionPersistent RetailerRelTxnObj = new DWLTransactionPersistent();
				RetailerRelObj.setControl(control);
				RetailerRelObj.setCONT_ID(partyId);

				for (XRETAILERBObj RetailerObj : vecDBRetailerBObj) {
					RetailerRelObj.setRETAILER_ID(RetailerObj
							.getXRETAILERpkId());
					if (RetailerRelObj.getXRETAILERBObj() != null) {
						RetailerRelObj.getXRETAILERBObj().setXRETAILERpkId(
								RetailerObj.getXRETAILERpkId());
					}

				}

				RetailerRelTxnObj.setTxnType("addXCONTACTRETAILERREL");

				RetailerRelTxnObj.setTxnControl(control);
				RetailerRelTxnObj.setTxnTopLevelObject(RetailerRelObj);

				long startTime = System.currentTimeMillis();
				
				response = (DWLResponse) super.execute(RetailerRelTxnObj);
				
				long stopTime = System.currentTimeMillis();
				long elapsedTime = stopTime - startTime;
		
				//control.put("CRR_addXCONTACTRETAILERREL2",  elapsedTime);

				throwExceptionifTXNFail(response, sBuffer1);

				for (XRETAILERBObj RetailerObj : vecDBRetailerBObj) {
					((XCONTACTRETAILERRELBObj) response.getData())
							.setXRETAILERBObj(RetailerObj);
				}

				respRetailerRel.add((XCONTACTRETAILERRELBObj) response
						.getData());

			}

		}

		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}

		return respRetailerRel;
	}

	/**
	 * 
	 * @param itemsXRetailerRelBObj
	 * @param vecDBRetailerRelBObj
	 * @param control
	 * @param partyId
	 * @throws BusinessProxyException
	 * @throws DaimlerCompositeException
	 */

	private void resolveIdentity(
			Vector<XCONTACTRETAILERRELBObj> itemsXRetailerRelBObj,
			Vector<XCONTACTRETAILERRELBObj> vecDBRetailerRelBObj,
			DWLControl control, String partyId) throws BusinessProxyException,
			DaimlerCompositeException {

		logger.info("MaintainCustomerRetailerRelationShipCompositeTxnBP:resolveIdentity- ENTER");

		Vector<XCONTACTRETAILERRELBObj> inputRecords = itemsXRetailerRelBObj;
		Vector<XCONTACTRETAILERRELBObj> dbRecords = vecDBRetailerRelBObj;
		DWLStatus status = new DWLStatus();
		// To handle update scenarios

		try {

			HashMap<String, XCONTACTRETAILERRELBObj> resRetailerRelMap = new HashMap<String, XCONTACTRETAILERRELBObj>();
			HashMap<String, XCONTACTRETAILERRELBObj> resRetailerRelMap1 = new HashMap<String, XCONTACTRETAILERRELBObj>();
			HashMap<String, XCONTACTRETAILERRELBObj> nonMatchRetailerRelMap = new HashMap<String, XCONTACTRETAILERRELBObj>();

			boolean RetailerMatch = true;

			if (inputRecords.size() > 0 || inputRecords != null) {
				for (XCONTACTRETAILERRELBObj inputRetailerRel : inputRecords) {

					for (XCONTACTRETAILERRELBObj dbRetailerRel : dbRecords) {

						if ((StringUtils.isNonBlank(inputRetailerRel
								.getXRETAILERBObj().getRETAILER_GSSN_CODE()) && (inputRetailerRel
								.getXRETAILERBObj().getRETAILER_GSSN_CODE())
								.equals(dbRetailerRel.getXRETAILERBObj()
										.getRETAILER_GSSN_CODE()))
								|| (StringUtils.isNonBlank(inputRetailerRel
										.getXRETAILERBObj()
										.getRETAILER_GS_CODE()) && (inputRetailerRel
										.getXRETAILERBObj()
										.getRETAILER_GS_CODE())
										.equals(dbRetailerRel
												.getXRETAILERBObj()
												.getRETAILER_GS_CODE()))) {

							RetailerMatch = false;
							inputRetailerRel.setCONT_ID(partyId);
							if (gscode) {
								resRetailerRelMap1.put(dbRetailerRel
										.getXRETAILERBObj()
										.getRETAILER_GS_CODE(), dbRetailerRel);
								resRetailerRelMap.put(inputRetailerRel
										.getXRETAILERBObj()
										.getRETAILER_GS_CODE(),
										inputRetailerRel);
							} else if (gssncode) {
								resRetailerRelMap1
										.put(dbRetailerRel.getXRETAILERBObj()
												.getRETAILER_GSSN_CODE(),
												dbRetailerRel);
								resRetailerRelMap.put(inputRetailerRel
										.getXRETAILERBObj()
										.getRETAILER_GSSN_CODE(),
										inputRetailerRel);
							}
							for (Map.Entry<String, XCONTACTRETAILERRELBObj> entrySource : resRetailerRelMap
									.entrySet()) {

								for (Map.Entry<String, XCONTACTRETAILERRELBObj> entryDB : resRetailerRelMap1
										.entrySet()) {

									XCONTACTRETAILERRELBObj xRetailerSourceValue = entrySource
											.getValue();
									XCONTACTRETAILERRELBObj xRetailerDBValue = entryDB
											.getValue();
									new DaimlerCustomerRetailerRelHelper()
											.handleUpdateCustomerRetailerRel(
													xRetailerSourceValue,
													xRetailerDBValue, control,
													status, this);
									break;
								}

							}
							break;

						} else {

							RetailerMatch = true;
							continue;

						}
					}

					if (RetailerMatch == true) {
						if (gscode) {
							nonMatchRetailerRelMap.put(inputRetailerRel
									.getXRETAILERBObj().getRETAILER_GS_CODE(),
									inputRetailerRel);
						} else if (gssncode) {
							nonMatchRetailerRelMap.put(
									inputRetailerRel.getXRETAILERBObj()
											.getRETAILER_GSSN_CODE(),
									inputRetailerRel);
						}
						inputRetailerRel.setCONT_ID(partyId);
						for (Map.Entry<String, XCONTACTRETAILERRELBObj> entryNonMatch : nonMatchRetailerRelMap
								.entrySet()) {
							XCONTACTRETAILERRELBObj xRetailerValue = entryNonMatch
									.getValue();
							new DaimlerCustomerRetailerRelHelper()
									.handleAddPartyRetailerRel(xRetailerValue,
											status);

						}
					}

				}

			}

			if (status.getDwlErrorGroup() != null
					&& status.getDwlErrorGroup().size() > 0) {
				status.setStatus(DWLStatus.FATAL);
				throw new DaimlerCompositeException(
						DaimlerCompositeUtils.createResponseByStatus(status));
			}

		} catch (Exception exception) {
			exception.getMessage();
			DaimlerCompositeUtils.handleException(exception);
		} finally {
			logger.info("MaintainCustomerRetailerRelationShipCompositeTxnBP:resolveIdentity- RETURN");
		}
	}

	/**
	 * 
	 * @param reqVecRetailerRel
	 * @param vecDBRetailerRelBObj
	 * @param control
	 * @param partyId
	 * @return
	 * @throws BusinessProxyException
	 * @throws DaimlerCompositeException
	 */

	private Vector<XCONTACTRETAILERRELBObj> fireAddUpdateTransaction(
			Vector<XCONTACTRETAILERRELBObj> reqVecRetailerRel,
			Vector<XCONTACTRETAILERRELBObj> vecDBRetailerRelBObj,
			DWLControl control, String partyId) throws BusinessProxyException,
			DaimlerCompositeException {
		DWLResponse response = null;
		Vector<XCONTACTRETAILERRELBObj> respRetailerRel = new Vector<XCONTACTRETAILERRELBObj>();

		DWLStatus status2 = null;
		Vector<DWLError> error2 = null;
		String serr2 = null;
		StringBuffer sBuffer2 = new StringBuffer();

		try {

			HashMap<String, XCONTACTRETAILERRELBObj> resRetailerRelMap = new HashMap<String, XCONTACTRETAILERRELBObj>();
			HashMap<String, XCONTACTRETAILERRELBObj> nonMatchRetailerRelMap = new HashMap<String, XCONTACTRETAILERRELBObj>();

			boolean RetailerToMatch = true;

			for (XCONTACTRETAILERRELBObj RetailerRelObj : reqVecRetailerRel) {

				DWLTransactionPersistent RetailerRelTxnObj = new DWLTransactionPersistent();

				DaimlerAdditionComponent daimlerAddComponent = new DaimlerAdditionComponent();

				for (XCONTACTRETAILERRELBObj dbRetailerRelObj : vecDBRetailerRelBObj) {

					if ((StringUtils.isNonBlank(RetailerRelObj
							.getXRETAILERBObj().getRETAILER_GSSN_CODE()) && (RetailerRelObj
							.getXRETAILERBObj().getRETAILER_GSSN_CODE())
							.equals(dbRetailerRelObj.getXRETAILERBObj()
									.getRETAILER_GSSN_CODE()))
							|| (StringUtils.isNonBlank(RetailerRelObj
									.getXRETAILERBObj().getRETAILER_GS_CODE()) && (RetailerRelObj
									.getXRETAILERBObj().getRETAILER_GS_CODE())
									.equals(dbRetailerRelObj.getXRETAILERBObj()
											.getRETAILER_GS_CODE()))) {

						RetailerToMatch = false;
						if (gscode) {
							resRetailerRelMap.put(RetailerRelObj
									.getXRETAILERBObj().getRETAILER_GS_CODE(),
									RetailerRelObj);

						} else if (gssncode) {
							resRetailerRelMap.put(
									RetailerRelObj.getXRETAILERBObj()
											.getRETAILER_GSSN_CODE(),
									RetailerRelObj);
						}
						for (Map.Entry<String, XCONTACTRETAILERRELBObj> entrySource : resRetailerRelMap
								.entrySet()) {

							XCONTACTRETAILERRELBObj xRetailerRelSourceValue = entrySource
									.getValue();

							xRetailerRelSourceValue.setControl(control);
							xRetailerRelSourceValue.setCONT_ID(partyId);

							long startTime = System.currentTimeMillis();
							
							response = daimlerAddComponent
									.updateXCONTACTRETAILERREL(xRetailerRelSourceValue);
							
							long stopTime = System.currentTimeMillis();
							long elapsedTime = stopTime - startTime;
					
									//control.put("CRR_updateXCONTACTRETAILERREL",  elapsedTime);

							throwExceptionifTXNFail(response, sBuffer2);

							break;
						}
						break;

					} else {

						RetailerToMatch = true;
						continue;
					}

				}

				if (RetailerToMatch == true) {
					if (gscode) {
						nonMatchRetailerRelMap.put(RetailerRelObj
								.getXRETAILERBObj().getRETAILER_GSSN_CODE(),
								RetailerRelObj);

					} else if (gssncode) {
						nonMatchRetailerRelMap.put(RetailerRelObj
								.getXRETAILERBObj().getRETAILER_GSSN_CODE(),
								RetailerRelObj);
					}

					for (Map.Entry<String, XCONTACTRETAILERRELBObj> entryNonMatch : nonMatchRetailerRelMap
							.entrySet()) {
						XCONTACTRETAILERRELBObj xRetailerRelValue = entryNonMatch
								.getValue();

						long startTime = System.currentTimeMillis();
						
						response = daimlerAddComponent
								.addXCONTACTRETAILERREL(xRetailerRelValue);
						
						long stopTime = System.currentTimeMillis();
						long elapsedTime = stopTime - startTime;		
						//control.put("CRR_addXCONTACTRETAILERREL3",  elapsedTime);

						throwExceptionifTXNFail(response, sBuffer2);

						Vector<XCONTACTRETAILERROLEBObj> xRETAILERROLEVECT = xRetailerRelValue
								.getItemsXCONTACTRETAILERROLEBObj();

					}

				}

				respRetailerRel.add((XCONTACTRETAILERRELBObj) response
						.getData());

			}

		}

		catch (BusinessProxyException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new BusinessProxyException(ex.getMessage());
		}

		return respRetailerRel;
	}
}
