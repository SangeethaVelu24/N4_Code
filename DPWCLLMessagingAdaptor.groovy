import com.navis.argo.ArgoIntegrationField
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.api.GroovyApi
import com.navis.argo.business.atoms.*
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.model.LocPosition
import com.navis.argo.business.xps.model.Che
import com.navis.carina.integrationservice.business.IntegrationService
import com.navis.external.framework.util.EFieldChange
import com.navis.external.framework.util.EFieldChangesView
import com.navis.framework.IntegrationServiceField
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.Junction
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.scope.ScopeCoordinates
import com.navis.inventory.MovesField
import com.navis.inventory.ServicesMovesField
import com.navis.inventory.business.moves.MoveEvent
import com.navis.inventory.business.units.Unit
import com.navis.services.business.event.Event
import com.navis.spatial.business.model.AbstractBin
import com.navis.vessel.VesselField
import com.navis.vessel.business.atoms.BerthSideTypeEnum
import com.navis.vessel.business.operation.Vessel
import com.navis.vessel.business.schedule.VesselStatisticsByCrane
import com.navis.vessel.business.schedule.VesselVisitBerthing
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.yard.business.model.StackBlock
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.http.*
import org.springframework.http.MediaType
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestTemplate

/**
 * @author <a href="mailto:sramsamy@weservetech.com">Ramasamy S</a>, 10/Oct/2021
 *
 * Module: IoT-Rtls
 * Description: This library holds methods for prepare, process and send the messages to third party system.
 * Implementation: Code Extension type is 'LIBRARY'
 */
class DPWCLLMessagingAdaptor extends GroovyApi {

    /**
     * Push Master data and entity changes to RTLS system
     */
    public String recordIsmEntry(String inMessageType, Map inParamMap) {
        LOGGER.setLevel(Level.DEBUG);
        if (inMessageType == null || inParamMap == null) {
            logMsg("MessageType is null, skip it. "+inMessageType + ", "+inParamMap);
            return;
        }

        boolean isRecorded = false;
        try {
            isRecorded = recordISM(inMessageType, inParamMap, getIntegrationServiceNameByMessageType(inMessageType));

        } catch (Exception e) {
            LOGGER.error("Exception in recordIsmEntry: " + e.getMessage());
        }
        return isRecorded.toString();
    }

    /**
     * GET request to receive the TOKEN from RTLS system
     */
    public void getRtlsToken() {
        LOGGER.setLevel(Level.WARN);
        try {
            IntegrationService integrationService = getIntegrationServiceByName(INT_SERV__GET_TOKEN);
            String completeURL = integrationService ? integrationService.getIntservUrl() : null;
            logMsg("completeURL: " + completeURL);
            if (completeURL) {
                Map mapCredential = new HashMap();
                mapCredential.put("userName", integrationService.getIntservUserId());
                mapCredential.put("password", integrationService.getIntservPassword());
                TOKEN_VALUE = extractTokenFromRTLSResponse(sendRequestForToken(completeURL, mapCredential));
            }
            logMsg("TOKEN_VALUE: " + TOKEN_VALUE);

        } catch (Exception e) {
            LOGGER.error("Exception in getRtlsToken: " + e.getMessage());
        }
    }

    private String extractTokenFromRTLSResponse(String inResponseJsonMessage) {
        //Get token from the map
        logMsg("before readValue : " + inResponseJsonMessage)
        Map map;
        try {
            if (inResponseJsonMessage) {
                ObjectMapper objMapper = new ObjectMapper();
                map = objMapper.readValue(inResponseJsonMessage, Map.class);
                logMsg("after readValue");
            }
        } catch (Exception e) {
            LOGGER.error("Exception in extractTokenFromRTLSResponse : " + e.getMessage());
        }
        return (map != null ? map.get(TOKEN_KEY) : null);
    }

    private String sendRequestForToken(String inRestApiURI, Map inJsonMap) {
        String response;
        try {
            logMsg("sendRequestForToken - URI: " + inRestApiURI);
            if (inRestApiURI) {
                IntegrationServiceMessage ismForToken = createIntegrationSrcMsg(INT_SERV__GET_TOKEN, null, null, null);
                //logMsg("ism: " + ismForToken);

                //String jsonRequestStr = "{\"jobid\":\"123456\",\"jobmsg\":\"test message\"}";
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                String jsonStr = null;
                if (inJsonMap) {
                    inJsonMap.put(SEQ_NUMBER, ismForToken.getIsmSeqNbr());
                    //logMsg("inJsonMap: " + inJsonMap);
                    ObjectMapper objMapper = new ObjectMapper();
                    jsonStr = objMapper.writeValueAsString(inJsonMap);
                    logMsg("jsonStr: " + jsonStr);
                    ismForToken.setIsmMessagePayloadBig(jsonStr);
                    ismForToken.setIsmMessagePayload(jsonStr);
                    ismForToken.setIsmUserString4(String.valueOf(ismForToken.getIsmSeqNbr()));
                }

                HttpEntity<String> entity;
                if (jsonStr) {
                    entity = new HttpEntity<String>(jsonStr, headers);
                } else {
                    entity = new HttpEntity<String>(headers);
                }

                ResponseEntity<String> responseEntity;
                try {
                    responseEntity = new RestTemplate().exchange(inRestApiURI, HttpMethod.POST, entity, String.class);
                } catch (Exception ex) {
                    LOGGER.error("Exception while exchange: " + ex.getMessage());
                    ismForToken.setIsmUserString2(ex.getMessage())
                    ismForToken.setIsmUserString5(T_FAILURE);
                    HibernateApi.getInstance().save(ismForToken);
                    //HibernateApi.getInstance().flush();
                }

                //logMsg("token - responseEntity: " + responseEntity);
                if (responseEntity) {
                    String statusCode = responseEntity.getStatusCode();
                    response = responseEntity.getBody();
                    logMsg("token - Response Body : " + response + ", Status code : " + statusCode);
                    if (response.length() > 255) {
                        ismForToken.setIsmUserString2(response.substring(0, 255));
                    } else {
                        ismForToken.setIsmUserString2(response);
                    }
                    //logMsg("response: " + response);

                    //If status of response message is FALSE, then retry with new token
                    if (statusCode.equals("200")) {
                        ismForToken.setIsmUserString5(T_SUCCESS);
                    } else {
                        ismForToken.setIsmUserString5(T_FAILURE);
                    }
                    HibernateApi.getInstance().save(ismForToken);
                    //HibernateApi.getInstance().flush();
                }
            }

        } catch (Exception e) {
            LOGGER.error("Exception in sendRequestForToken: " + e.getMessage());
        }
        return response;
    }

    /**
     * Send POST request to REST api webservice
     */
    public String sendRequestPost() {
        LOGGER.setLevel(Level.DEBUG);
        String response;
        try {
            GeneralReference generalReference = GeneralReference.findUniqueEntryById("DPWCLL", "MESSAGE_EXCHANGE", "RTLS", "LIMIT");
            GET_TOKEN_TRY_LIMIT = generalReference ? Integer.parseInt(generalReference.getRefValue1()) : 1;
            ISM_MSG_PUSH_LIMIT = generalReference ? Integer.parseInt(generalReference.getRefValue2()) : 20;

            List<IntegrationServiceMessage> ismList = getIsmListToBeSend();
            logMsg("ismList::: " + ismList);
            if (ismList)
                logMsg("ismList size: " + ismList.size());

            if (ismList && ismList.size() > 0 && TOKEN_VALUE == null)
                getRtlsToken();

            //String key;
            Set<String> pushJobMessageSet = new HashSet<String>()
            for (IntegrationServiceMessage ismForMessage : ismList) {
                logMsg("ismForMessage :: " + ismForMessage.getIsmSeqNbr());

                if (INT_SERV__PUSH_JOB_DETAIL == ismForMessage.getIsmIntegrationService().getIntservName()) {
                    StringBuffer sbKey = new StringBuffer();
                    //key = ismForMessage.getIsmUserString4() + ismForMessage.getIsmUserString3(); // ismUsrstr3-Remarks, ismUsrstr4-job
                    sbKey.append(ismForMessage.getIsmUserString4()).append(ismForMessage.getIsmUserString3()); // ismUsrstr3-Remarks, ismUsrstr4-job
                    logMsg("sbKey : "+sbKey);

                    if (pushJobMessageSet.contains(sbKey.toString())) {
                        logMsg("skip duplicate ISM : "+ismForMessage);
                        ismForMessage.setIsmUserString5(T_IGNORE);
                        continue;
                    }
                    pushJobMessageSet.add(sbKey.toString())
                }

                String inRestApiURI = ismForMessage.getIsmUserString1();
                if (inRestApiURI) {
                    logMsg("Post request - URI: " + inRestApiURI);
                    int iTryCounter = 0;
                    String statusCode;
                    while (iTryCounter < GET_TOKEN_TRY_LIMIT && (statusCode == null || !statusCode.equals("200"))) {
                        logMsg("iTryCounter :: " + iTryCounter);
                        ++iTryCounter;

                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType(MediaType.APPLICATION_JSON);
                        headers.add(T_AUTHORIZATION, (T_BEARER + TOKEN_VALUE));

                        String jsonStr = ismForMessage.getIsmMessagePayloadBig();
                        HttpEntity<String> entity;
                        if (jsonStr) {
                            entity = new HttpEntity<String>(jsonStr, headers);
                        } else {
                            entity = new HttpEntity<String>(headers);
                        }

                        ResponseEntity<String> responseEntity;
                        try {
                            responseEntity = new RestTemplate().exchange(inRestApiURI, HttpMethod.POST, entity, String.class);
                        } catch (Exception ex) {
                            LOGGER.error("Exception while exchange: " + ex.getMessage());

                            updateIsmSentDate(ismForMessage);
                            ismForMessage.setIsmUserString2(ex.getMessage())
                            ismForMessage.setIsmUserString5(T_FAILURE);
                            //HibernateApi.getInstance().flush();
                        }

                        //logMsg("responseEntity: " + responseEntity);
                        if (responseEntity) {
                            statusCode = responseEntity.getStatusCode();
                            response = responseEntity.getBody();
                            logMsg("Response Body :: " + response + ", Status code : " + statusCode);

                            if (response.length() > 255) {
                                ismForMessage.setIsmUserString2(response.substring(0, 255));
                            } else {
                                ismForMessage.setIsmUserString2(response);
                            }

                            //If status of response message is FALSE, then retry with new token
                            if (statusCode.equals("200")) {
                                updateIsmSentDate(ismForMessage);
                                ismForMessage.setIsmUserString5(T_SUCCESS);
                                HibernateApi.getInstance().save(ismForMessage);
                                //logMsg("usrstr5 persist.")

                            } else { // If response failed, then try again with new token
                                getRtlsToken();
                            }
                        } else {
                            getRtlsToken();
                        }
                        HibernateApi.getInstance().save(ismForMessage);
                    }
                    logMsg("Loop completed.");
                }
            }

        } catch (Exception e) {
            LOGGER.error("Exception in sendRequestPost: " + e.getMessage());
        }
        return response;
    }

    private void updateIsmSentDate(IntegrationServiceMessage inIsm) {
        if (ArgoUtils.isEmpty(inIsm.getIsmUserString5())) {
            Date dateNow = new Date()
            inIsm.setIsmFirstSendTime(dateNow);
            inIsm.setIsmLastSendTime(dateNow);
        } else {
            inIsm.setIsmLastSendTime(new Date());
        }
    }

    /**
     * record Integration Service Message
     */
    private boolean recordISM(String inMessageType, Map inJsonMap, String inIntegrationServiceName) {
        try {
            LOGGER.setLevel(Level.DEBUG)
            logMsg("recordISM : " + inMessageType);
            if (inIntegrationServiceName != null) {
                IntegrationServiceMessage ismForMessage = createIntegrationSrcMsg(inIntegrationServiceName, null, null, null);
                //logMsg("ismForMessage:: " + (ismForMessage?ismForMessage.getIsmSeqNbr() : T_EMPTY) + ", created: "+ismForMessage.getIsmCreated() + ", modified: "+ismForMessage.getIsmChanged());
                LOGGER.setLevel(Level.DEBUG)

                if (ismForMessage != null) {
                    if (ismForMessage.getIsmCreated() == null)
                        ismForMessage.setIsmCreated(new Date());
                    if (ismForMessage.getIsmChanged() == null)
                        ismForMessage.setIsmChanged(new Date());

                    String statusCode;
                    //String jsonRequestStr = "{\"jobid\":\"123456\",\"jobmsg\":\"test message\"}";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    headers.add(T_AUTHORIZATION, (T_BEARER + TOKEN_VALUE));

                    if (inJsonMap) {
                        logMsg("map-seqno: "+ inJsonMap.get(T_JOB))
                        inJsonMap.put(SEQ_NUMBER, String.valueOf(ismForMessage.getIsmSeqNbr()));
                        if (inJsonMap.get(T_JOB) == null)
                            inJsonMap.put(T_JOB, String.valueOf(ismForMessage.getIsmSeqNbr()));

                        logMsg("create jsonstr")
                        ObjectMapper objMapper = new ObjectMapper();
                        String jsonStr = objMapper.writeValueAsString(inJsonMap);

                        logMsg("jsonStr:: " + jsonStr);
                        if (jsonStr != null && ArgoUtils.isNotEmpty(jsonStr.trim())) {
                            ismForMessage.setIsmMessagePayload(jsonStr.length() > 255 ? jsonStr.substring(0, 255) : jsonStr);
                            ismForMessage.setIsmMessagePayloadBig(jsonStr);
                            ismForMessage.setIsmUserString4(inJsonMap.get(T_JOB));

                            if (ArgoUtils.isNotEmpty((String) inJsonMap.get(T__SOURCE_OBJECT)))
                                ismForMessage.setIsmUserString3((String) inJsonMap.get(T__SOURCE_OBJECT));

                            //logMsg("save ismForMessage")
                            HibernateApi.getInstance().save(ismForMessage);
                            HibernateApi.getInstance().flush();
                            //logMsg("before return")
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception in recordISM: " + e.getMessage());
        }
        return false;
    }

    private static SimpleClientHttpRequestFactory getClientHttpRequestFactory() {
        SimpleClientHttpRequestFactory clientHttpRequestFactory = new SimpleClientHttpRequestFactory();
        clientHttpRequestFactory.setConnectTimeout(TIMEOUT);
        clientHttpRequestFactory.setReadTimeout(TIMEOUT);
        return clientHttpRequestFactory;
    }

    private List<IntegrationServiceMessage> getIsmListToBeSend() {
        DomainQuery dq = QueryUtils.createDomainQuery(T_INTEGRATION_SERVICE_MESSAGE)
                .addDqPredicate(nonProcessedDisJunction)
        //.addDqPredicate(nonEligibleConJunction)
                .addDqPredicate(PredicateFactory.not(PredicateFactory.in(ISM_SERV_NAME, RTLS_PROCESS_LIST)))
                .addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_SEQ_NBR))
                .setDqMaxResults(ISM_MSG_PUSH_LIMIT);

        //logMsg(dq);

        return Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);
    }

    public static IntegrationService getIntegrationServiceByName(String inIntegrationServiceName) {
        DomainQuery dq = QueryUtils.createDomainQuery("IntegrationService").addDqPredicate(PredicateFactory.eq(IntegrationServiceField.INTSERV_NAME, inIntegrationServiceName));
        return (IntegrationService) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);
    }

    public IntegrationServiceMessage createIntegrationSrcMsg(String inIntegrationServiceName, String inMessagePayload, Event inEvent, String inEntityId) {
        LOGGER.setLevel(Level.WARN);
        LOGGER.debug("inIntegrationServiceName: " + inIntegrationServiceName);
        IntegrationService integrationService = getIntegrationServiceByName(inIntegrationServiceName);
        String uri = integrationService ? integrationService.getIntservUrl() : null;
        logMsg("uri:: " + uri);

        IntegrationServiceMessage integrationServiceMessage = new IntegrationServiceMessage();
        try {
            integrationServiceMessage.setIsmUserString1(uri);
            if (inEvent) {
                integrationServiceMessage.setIsmEventPrimaryKey((Long) inEvent.getEvntEventType().getPrimaryKey());
                integrationServiceMessage.setIsmEntityClass(inEvent.getEventAppliedToClass());
                integrationServiceMessage.setIsmEventTypeId(inEvent.getEventTypeId());
            }
            if (inEntityId)
                integrationServiceMessage.setIsmEntityNaturalKey(inEntityId);

            if (integrationService) {
                integrationServiceMessage.setIsmIntegrationService(integrationService);
                integrationServiceMessage.setIsmFirstSendTime(ArgoUtils.timeNow());
                integrationServiceMessage.setIsmLastSendTime(ArgoUtils.timeNow());
            }

            if (inMessagePayload) {
                String msg = inMessagePayload.length() > DB_CHAR_LIMIT ? inMessagePayload.substring(0, DB_CHAR_LIMIT) : inMessagePayload;
                integrationServiceMessage.setIsmMessagePayload(msg);

                LOGGER.debug("inMessagePayload length: " + inMessagePayload.length());
                integrationServiceMessage.setIsmMessagePayloadBig(inMessagePayload);
            }
            LOGGER.debug("integrationServiceMessage::: " + integrationServiceMessage);

            integrationServiceMessage.setIsmSeqNbr(new IntegrationServMessageSequenceProvider().getNextSequenceId());

            ScopeCoordinates scopeCoordinates = ContextHelper.getThreadUserContext().getScopeCoordinate();
            Long scopeLevel = ScopeCoordinates.GLOBAL_LEVEL;
            String scopeGkey = null;
            if (!scopeCoordinates.isScopeGlobal()) {
                scopeLevel = new Long(ScopeCoordinates.getScopeId(4));
                scopeGkey = (String) scopeCoordinates.getScopeLevelCoord(scopeLevel.intValue());
            }
            integrationServiceMessage.setIsmScopeGkey(scopeGkey);
            integrationServiceMessage.setIsmScopeLevel(scopeLevel);
            LOGGER.debug("integrationServiceMessage: " + integrationServiceMessage);

            HibernateApi.getInstance().save(integrationServiceMessage);
            //HibernateApi.getInstance().flush();

        } catch (Exception e) {
            LOGGER.error("Exception in createIntegrationSrcMsg: " + e.getMessage());
        }
        return integrationServiceMessage;
    }

    private String getIntegrationServiceNameByMessageType(String inMessageType) {
        String integrationServiceName;
        try {
            switch (inMessageType) {
                case MSG__CHE_LOGIN_LOGOUT:
                    integrationServiceName = INT_SERV__CHE_LOGIN_LOGOUT;
                    break;

                case MSG__JOB_DETAIL:
                    integrationServiceName = INT_SERV__PUSH_JOB_DETAIL;
                    break;

                case MSG__VSL_BERTH_EVENT:
                    integrationServiceName = INT_SERV__PUSH_VSL_BERTH;
                    break;

                case MSG__MACHINE_PRODUCTIVITY:
                    integrationServiceName = INT_SERV__PUSH_MC_PRODUCTIVITY;
                    break;

                case MSG__STS:
                    integrationServiceName = INT_SERV__PUSH_MOVESTOGO_STS;
                    break;

                case MSG__VESSEL_PRODUCTIVITY:
                    integrationServiceName = INT_SERV__PUSH_VSL_PRODUCTIVITY;
                    break;

                case MSG__QC_LANE_ASSIGNMENT:
                    integrationServiceName = INT_SERV__PUSH_QC_LANE_ASSIGN;
                    break;

                case MSG__CHE_LIST:
                    integrationServiceName = INT_SERV__PUSH_CHE_LIST;
                    break;

                case MSG__YARD_BLOCK:
                    integrationServiceName = INT_SERV__PUSH_YARD_BLOCK;
                    break;

                case MSG__OTR_IN_OUT:
                    integrationServiceName = INT_SERV__PUSH_OTR_IN_OUT;
                    break;

                case MSG__REEFER_POWER_SWITCH:
                    integrationServiceName = INT_SERV__PUSH_REEFER_POWER_SWITCH;
                    break;

                default:
                    break;
            }
            logMsg("integrationServiceName: " + integrationServiceName);

        } catch (Exception e) {
            LOGGER.error("Exception in getIntegrationServiceNameByMessageType : " + e.getMessage());
        }
        return integrationServiceName;
    }

    private String convertMapToParamString(Map inMap) {
        StringBuilder sbJsonStr = new StringBuilder();
        if (inMap && inMap.size() > 0) {
            int iCounter = 0;
            for (String key : inMap.keySet()) {
                if (iCounter > 0) {
                    sbJsonStr.append(T_PARAM_SEPERATOR);
                }
                sbJsonStr.append(key);
                sbJsonStr.append(T_EQUALS);
                sbJsonStr.append((String) inMap.get(key));
                ++iCounter;
            }
        }
        return sbJsonStr.toString();
    }

    public static class IntegrationServMessageSequenceProvider extends com.navis.argo.business.model.ArgoSequenceProvider {
        public Long getNextSequenceId() {
            return super.getNextSeqValue(serviceMsgSequence, (Long) ContextHelper.getThreadFacilityKey());
        }
        private String serviceMsgSequence = "INT_SEQ";
    }

    private Object getNewFieldValue(EFieldChangesView inOriginalFieldChanges, MetafieldId inMetaFieldId) {
        EFieldChange fieldChange = inOriginalFieldChanges.findFieldChange(inMetaFieldId);
        return fieldChange ? fieldChange.getNewValue() : null;
    }

    public String getEpochDate(Date inDate) {
        if (inDate != null)
            return inDate.toInstant().getEpochSecond().toString()
        else
            return T_EMPTY;
    }

    // GE: General, RF: Reefer, TK : Tank, FR: Flat Rack, OT: Open Top, BK: Dry Bulk, AS: Air Surface, PF: PlatForm
    public String getEqIsoGroupMatch(EquipIsoGroupEnum inIsoGroupEnum) {
        logMsg("inIsoGroupKey: " + inIsoGroupEnum);
        String isoGrp = T_EMPTY;
        try {
            switch (inIsoGroupEnum) {

                case EquipIsoGroupEnum.GP:
                case EquipIsoGroupEnum.VH:
                    isoGrp = T_GE;
                    break;

                case EquipIsoGroupEnum.BU:
                case EquipIsoGroupEnum.BK:
                    isoGrp = T_BK;
                    break;

                case EquipIsoGroupEnum.RE:
                case EquipIsoGroupEnum.RT:
                case EquipIsoGroupEnum.RS:
                case EquipIsoGroupEnum.RC:
                case EquipIsoGroupEnum.HR:
                    isoGrp = T_RF;
                    break;

                case EquipIsoGroupEnum.UT:
                    isoGrp = T_OT;
                    break;

                case EquipIsoGroupEnum.PL:
                    isoGrp = T_PF;
                    break;

                case EquipIsoGroupEnum.PF:
                case EquipIsoGroupEnum.PC:
                case EquipIsoGroupEnum.PS:
                    isoGrp = T_FR;
                    break;

                case EquipIsoGroupEnum.TN:
                case EquipIsoGroupEnum.TD:
                case EquipIsoGroupEnum.TG:
                    isoGrp = T_TK;
                    break;

                case EquipIsoGroupEnum.AS:
                    isoGrp = T_AS;
                    break;

                default:
                    isoGrp = inIsoGroupEnum.getKey();
                    break;
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getEqIsoGroupMatch : " + e.getMessage());
        }
    }

    //For soap message request process
    /*public CheKindEnum convertTypeToCheKind(inType) {
        CheKindEnum cheKindEnum;
        try {
            switch (inType) {
                case T_YT:
                    cheKindEnum = CheKindEnum.ITV
                    break;

                case T_RS:
                    cheKindEnum = CheKindEnum.RST
                    break;

                case T_FL:
                    cheKindEnum = CheKindEnum.FLT
                    break;

                case T_QC:
                    cheKindEnum = CheKindEnum.QC
                    break;

                default:
                    cheKindEnum = null;
                    break;
            }

        } catch (Exception e) {
            LOGGER.error("Exception in convertTypeToCheKind : "+e.getMessage());
        }
        return cheKindEnum;
    }*/


    // YT, OT, RS, FL, QC
    public String getCheTypeMatch(Che inChe) {
        String cheType = T_EMPTY;
        try {
            //Internal Terminal Truck(Yard Tractor)
            switch (inChe.getCheKindEnum()) {

                case CheKindEnum.ITV:
                case CheKindEnum.TT:
                case CheKindEnum.AGV:
                case CheKindEnum.MTT:
                    cheType = T_YT;
                    break;

                case CheKindEnum.UNKN:
                    cheType = T_OT;
                    break;

                case CheKindEnum.RST:
                    cheType = T_RS;
                    break;

                case CheKindEnum.FLT:
                    cheType = T_FL;
                    break;

                case CheKindEnum.QC:
                    cheType = T_QC;
                    break;

                default:
                    cheType = inChe.getCheKindEnum().getKey();
                    break;
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getCheTypeMatch : " + e.getMessage());
        }

        //logMsg("cheType: "+cheType);
        return cheType;
    }


    //S: Standby, P: Parking, I :Idle or Inactive, A : Active, B:Breakdown, M:Maintenance
    public String getCheStatusMatch(Che inChe) {
        String cheStatus = T_EMPTY;
        try {
            switch (inChe.getCheStatusEnum()) {

                case CheStatusEnum.UNAVAIL:
                    cheStatus = T_I;
                    break;

                case CheStatusEnum.UNKNOWN:
                    cheStatus = T_S;
                    break;

                case CheStatusEnum.OUTOFSERVICE:
                    cheStatus = T_P;
                    break;

                case CheStatusEnum.WORKING:
                    cheStatus = T_A;
                    break;

                case CheStatusEnum.OFFLINEBREAKDOWN:
                    cheStatus = T_B;
                    break;

                case CheStatusEnum.OFFLINEMAINTENANCE:
                    cheStatus = T_M;
                    break;

                default:
                    int length = inChe.getCheStatusEnum().getKey().length();
                    cheStatus = length <= 10 ? inChe.getCheStatusEnum().getKey().substring(0, length) : inChe.getCheStatusEnum().getKey().substring(0, 10);
                    break;
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getCheStatusMatch : " + e.getMessage());
        }

        //logMsg("cheStatus: "+cheStatus);
        return cheStatus;
    }


    //Convert (Block/Bay.Row.Column.Tier) 2C150G1B - BBRRRCCT - 2C.150.G1.B
    //public Map getFormattedSlot(LocPosition inPosition) {
    public String getFormattedSlot(LocPosition inPosition) {
        LOGGER.setLevel(Level.WARN);
        try {
            AbstractBin abstractBin = inPosition.isYardPosition()? inPosition.getPosBin() : null;
            logMsg("abstractBin: "+abstractBin)
            if(abstractBin) { //Yard Slot
                String posSlot = inPosition.getPosSlot(); //2A12E.4
                logMsg("posSlot: "+posSlot);
                int length = posSlot.length();

                logMsg("AbnName: " + abstractBin.getAbnName() + ", abnGkey: " + abstractBin.getAbnGkey());

                AbstractBin parentBin = abstractBin.getAbnParentBin()
                logMsg("parentBin: " + parentBin)
                if (parentBin) {
                    logMsg("Parent-AbnName: " + parentBin.getAbnName() + ", parent-abnGkey: " + parentBin.getAbnGkey());

                    AbstractBin grandParentBin = parentBin.getAbnParentBin()
                    logMsg("grandParentBin: " + grandParentBin)
                    if (grandParentBin) {
                        logMsg("grandParentBin-AbnName: " + grandParentBin.getAbnName() + ", grandParentBin-abnGkey: " + grandParentBin.getAbnGkey());

                        StackBlock stackBlock = StackBlock.hydrate(grandParentBin.getAbnGkey());
                        String labelFormat = stackBlock.getAyblkLabelSchemeHost();
                        logMsg("format: "+stackBlock.getAyblkLabelUIFullPosition() + " : "+ labelFormat);

                        //B2R3C1
                        int blockOffset = Integer.parseInt(String.valueOf(labelFormat.charAt(1))); // 2
                        int rowOffset = Integer.parseInt(String.valueOf(labelFormat.charAt(3)));   // 3
                        int columnOffset = Integer.parseInt(String.valueOf(labelFormat.charAt(5))); // 1

                        System.out.println("blockOffset: "+blockOffset);
                        System.out.println("rowOffset: "+rowOffset);
                        System.out.println("columnOffset: "+columnOffset);

                        //2A12E.4 - length=7
                        String tier = posSlot.substring(length-1); //get last char => 4
                        String block = posSlot.substring(0, blockOffset); //0, 2 => 2A
                        String column = posSlot.substring(length-3, (length-3+columnOffset)); //4, 5 => E
                        String row = posSlot.substring(blockOffset, length-3); //2, 4

                        StringBuilder sbSlot = new StringBuilder();
                        sbSlot.append(block).append(T_DOT);
                        sbSlot.append(row).append(T_DOT);
                        sbSlot.append(column).append(T_DOT);
                        sbSlot.append(tier);

                        logMsg("sbSlot: "+sbSlot);

                        return sbSlot.toString();
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getFormattedSlot : " + e.getMessage());
        }
        return inPosition.getPosSlot();
    }

    //public Map getContainerPositionMap(String inQualifier, String inIdentifier, String inSlot) { // Y - CLL - xxxxx
    public Map getContainerPositionMap(LocPosition inPosition) { // Y - CLL - xxxxx
        Map containerPositionMap = new HashMap();
        try {
            if (inPosition) {
                //LocPosition.getPositionPrefixChar(fromPosition.getPosLocType()).toString(), fromPosition.getPosLocId(), fromPosition.getPosSlot())
                //fromPosition.getPosLocType()).toString(), fromPosition.getPosLocId(), fromPosition.getPosSlot())
                //containerPositionMap.put(T_POSITION_SLOT, inPosition.getPosSlot());
                containerPositionMap.put(T_POSITION_QUALIFIER, LocPosition.getPositionPrefixChar(inPosition.getPosLocType()).toString());
                containerPositionMap.put(T_LOCATION_IDENTIFIER, inPosition.getPosLocId());
                containerPositionMap.put(T_POSITION_SLOT, getFormattedSlot(inPosition));

            } else {
                containerPositionMap.put(T_POSITION_QUALIFIER, T_EMPTY);
                containerPositionMap.put(T_LOCATION_IDENTIFIER, T_EMPTY);
                containerPositionMap.put(T_POSITION_SLOT, T_EMPTY);
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getContainerPositionMap : " + e.getMessage());
        }
        return containerPositionMap;
    }

    public Map getCheMap(Che inChe) {
        Map cheMap = new HashMap();
        try {
            if (inChe != null) {
                cheMap.put(T_CHE_ID, inChe.getCheShortName());
                cheMap.put(T_CHE_TYPE, getCheTypeMatch(inChe));
                cheMap.put(T_STATUS, getCheStatusMatch(inChe));

            } else {
                cheMap.put(T_CHE_ID, T_EMPTY);
                cheMap.put(T_CHE_TYPE, T_EMPTY);
                cheMap.put(T_STATUS, T_EMPTY);
            }
        } catch (Exception e) {
            LOGGER.error("Exception in " + e.getMessage());
        }
        return cheMap;
    }

    public Map getContainerCategoryStatusMap(Unit inUnit) {
        Map map = new HashMap();
        try {
            if (inUnit != null) {
                String freightKind = T_EMPTY;
                String category = T_EMPTY;

                switch (inUnit.getUnitFreightKind()) {
                    case FreightKindEnum.MTY:
                        freightKind = T_E;
                        break;

                    case FreightKindEnum.FCL:
                        freightKind = T_F;
                        break;

                    case FreightKindEnum.LCL:
                        freightKind = T_L;
                        break;

                    default:
                        freightKind = T_O;
                        break;
                }


                switch (inUnit.getUnitCategory()) {
                    case UnitCategoryEnum.EXPORT:
                        category = T_E;
                        break;

                    case UnitCategoryEnum.IMPORT:
                        category = T_I;
                        break;

                    case UnitCategoryEnum.TRANSSHIP:
                        category = T_T;
                        break;

                    case UnitCategoryEnum.STORAGE:
                        category = T_S;
                        break;

                    case UnitCategoryEnum.THROUGH:
                        category = T_H;
                        break;

                    default:
                        category = T_EMPTY;
                        break;
                }

                map.put(T_CATEGORY_STATUS, freightKind);
                map.put(T_CATEGORY, category);
            }
        } catch (Exception e) {
            LOGGER.error("Exception in " + e.getMessage());
        }
        return map;
    }

    public String getMoveKindMatch(WiMoveKindEnum inMoveKind) {
        String resultMoveKind = T_EMPTY;
        try {
            if (inMoveKind != null) {
                if (WiMoveKindEnum.RailLoad == inMoveKind) {
                    resultMoveKind = T_ROLD;
                } else if (WiMoveKindEnum.RailDisch == inMoveKind) {
                    resultMoveKind = T_RIDS;
                } else if (WiMoveKindEnum.YardMove == inMoveKind) {
                    resultMoveKind = T_YARD;
                } else {
                    resultMoveKind = inMoveKind.getKey();
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception in getMoveKindMatch : " + e.getMessage());
        }
        return resultMoveKind;
    }

    /**
     PA--> Pre-arrival
     BP--> Bay Plan Received
     OD--> OK to Discharge
     DC--> Discharge Completed
     OC--> Operation Completed
     DP--> Departed
     SC--> Scheduling
     CL--> Cancelled
     SA--> Shift to Anchorage
     */
    public String getVesselVisitStateMatch(CarrierVisitPhaseEnum inVisitPhaseEnum) {
        String visitState = T_EMPTY;
        try {
            if (inVisitPhaseEnum != null) {

                switch (inVisitPhaseEnum) {
                    case CarrierVisitPhaseEnum.CREATED:
                        visitState = T_PA;  //Pre-arrival
                        break;

                    case CarrierVisitPhaseEnum.INBOUND:
                        visitState = T_BP;  //Bay Plan Received
                        break;

                    case CarrierVisitPhaseEnum.ARRIVED:
                    case CarrierVisitPhaseEnum.WORKING:
                        visitState = T_OD;  //OK to Discharge
                        break;

                    case CarrierVisitPhaseEnum.COMPLETE:
                    case CarrierVisitPhaseEnum.CLOSED:
                        visitState = T_OC;  //Operation Completed
                        break;

                    case CarrierVisitPhaseEnum.DEPARTED:
                        visitState = T_DP;  //Departed
                        break;

                    case CarrierVisitPhaseEnum.CANCELED:
                        visitState = T_CL;
                        break;

                    default:
                        break;
                }

                //Below are not covered in TOS
                //DC--> Discharge Completed
                //SC--> Scheduling
                //SA--> Shift to Anchorage
            }
        } catch (Exception e) {
            LOGGER.error("Exception in getVesselVisitStateMatch : " + e.getMessage());
        }
        logMsg("visitState: " + visitState);
        return visitState;
    }

    public List getMoveEventList(String inVesselVisitId, Date inLastHour) {
        logMsg("In getMoveEventList - inLastHour : "+inLastHour)
        List<MoveEvent> moveEventList = new ArrayList<MoveEvent>();
        try {
            Junction positionDisJunction = PredicateFactory.disjunction()
                    .add(PredicateFactory.eq(FROM_POSITION_LOC_ID, inVesselVisitId))
                    .add(PredicateFactory.eq(TO_POSITION_LOC_ID, inVesselVisitId));

            DomainQuery domainQuery = QueryUtils.createDomainQuery("MoveEvent")
                    .addDqPredicate(PredicateFactory.isNotNull(ServicesMovesField.MVE_CHE_QUAY_CRANE))
                    .addDqPredicate(PredicateFactory.isNotNull(ServicesMovesField.MVE_TIME_PUT))
                    .addDqPredicate(positionDisJunction)
                    .addDqOrdering(Ordering.asc(ServicesMovesField.MVE_TIME_PUT));

            if (inLastHour != null)
                domainQuery.addDqPredicate(PredicateFactory.between(ServicesMovesField.MVE_TIME_PUT, inLastHour, new Date()));

            moveEventList.addAll(HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery));
            //LOGGER.debug("moveEventList :::" + moveEventList);

        } catch (Exception e) {
            LOGGER.error("Exception in getMoveEventList : " + e.getMessage());
        }
        return moveEventList;
    }


    public boolean isYardChe(CheKindEnum inCheKind) {
        return CheKindEnum.RTG.equals(inCheKind) || CheKindEnum.SC.equals(inCheKind) || CheKindEnum.RST.equals(inCheKind) || CheKindEnum.FLT.equals(inCheKind) || CheKindEnum.ASC.equals(inCheKind) || CheKindEnum.RMG.equals(inCheKind);
    }

    // 2.4.	PushVesselBerthingEvent
    public void notifyRTLSForVVBerthing(VesselVisitBerthing vesselVisitBerthing, Map inBerthMap) {
        try {
            Date berthETA, berthETD, berthStartWork, berthEndWork, berthATA, berthATD
            String berthBollardFore, berthBollardAft, berthBollardForeOffset, berthBollardAftOffset
            BerthSideTypeEnum berthSideTo
            Long berthSeq

            if (inBerthMap != null) {
                berthETA = (Date) inBerthMap.get(VesselField.BRTHG_E_T_A)
                berthETD = (Date) inBerthMap.get(VesselField.BRTHG_E_T_D)
                berthStartWork = (Date) inBerthMap.get(VesselField.BRTHG_TIME_START_WORK)
                berthEndWork = (Date) inBerthMap.get(VesselField.BRTHG_TIME_STOPT_WORK)
                berthATA = (Date) inBerthMap.get(VesselField.BRTHG_A_T_A)
                berthATD = (Date) inBerthMap.get(VesselField.BRTHG_A_T_D)
                berthSeq = (Long) inBerthMap.get(VesselField.BRTHG_SEQ)
                berthSideTo = (BerthSideTypeEnum) inBerthMap.get(VesselField.BRTHG_SHIP_SIDE_TO)
                berthBollardFore = (String) inBerthMap.get(VesselField.BRTHG_ABS_BOLLARD_FORE)
                berthBollardAft = (String) inBerthMap.get(VesselField.BRTHG_ABS_BOLLARD_AFT)
                berthBollardForeOffset = (String) inBerthMap.get(VesselField.BRTHG_ABS_BOLLARD_FORE_OFFSET)
                berthBollardAftOffset = (String) inBerthMap.get(VesselField.BRTHG_ABS_BOLLARD_AFT_OFFSET)
            }

            if (vesselVisitBerthing != null) {
                berthETA = berthETA ? berthETA : vesselVisitBerthing.getBrthgETA();
                berthETD = berthETD ? berthETD : vesselVisitBerthing.getBrthgETD();
                berthStartWork = berthStartWork ? berthStartWork : vesselVisitBerthing.getBrthgTimeStartWork();
                berthEndWork = berthEndWork ? berthEndWork : vesselVisitBerthing.getBrthgTimeStoptWork();
                berthATA = berthATA ? berthATA : vesselVisitBerthing.getBrthgATA();
                berthATD = berthATD ? berthATD : vesselVisitBerthing.getBrthgATD();
                berthSeq = berthSeq ? berthSeq : vesselVisitBerthing.getBrthgSeq();
                berthSideTo = berthSideTo ? berthSideTo : vesselVisitBerthing.getBrthgShipSideTo();
                //logMsg("GUI brthgAbsBollardForeFromXps: "+vesselVisitBerthing.getField(VesselGuiMetafield.BRTHG_ABS_BOLLARD_FORE_FROM_XPS));
                //logMsg("BIZ brthgAbsBollardForeFromXps: "+ vesselVisitBerthing.getField(VesselBizMetafield.BRTHG_ABS_BOLLARD_FORE_FROM_XPS));

                berthBollardFore = berthBollardFore ? berthBollardFore : vesselVisitBerthing.getBrthgAbsBollardFore();
                berthBollardAft = berthBollardAft ? berthBollardAft : vesselVisitBerthing.getBrthgAbsBollardAft();
                berthBollardForeOffset = berthBollardForeOffset ? berthBollardForeOffset : vesselVisitBerthing.getBrthgAbsBollardForeOffset();
                berthBollardAftOffset = berthBollardAftOffset ? berthBollardAftOffset : vesselVisitBerthing.getBrthgAbsBollardAftOffset();

                logMsg("Bollard aft: " + vesselVisitBerthing.getBrthgAbsBollardAft())
                logMsg("Bollard fore: " + vesselVisitBerthing.getBrthgAbsBollardFore())
            }

            logMsg("ETA: " + berthETA + ", ETD: " + berthETD + ", start work: " + berthStartWork
                    + ", end work: " + berthEndWork + ", ATA: " + berthATA + ", ATD: " + berthATD
                    + ", berth seq: " + berthSeq + ", Side To: " + berthSideTo + ", Bollard Fore: " + berthBollardFore
                    + ", Bollard Aft: " + berthBollardAft + ", BollardForeOffset: " + berthBollardForeOffset + ", BollardAftOffset: " + berthBollardAftOffset);



            long totalMoves = 0L;
            String rotationNumber, vesselName, serviceCode, loa, width, totalLoadMoves, totalDischargeMoves, totalOtherMoves;
            String eta = berthETA != null ? getEpochDate(berthETA) : null;
            String etd = berthETD != null ? getEpochDate(berthETD) : null;
            String startWorkDate = berthStartWork != null ? getEpochDate(berthStartWork) : null;
            String endWorkDate = berthEndWork != null ? getEpochDate(berthEndWork) : null;
            String portArrivalDate = berthATA != null ? getEpochDate(berthATA) : null;
            String sailedDate = berthATD != null ? getEpochDate(berthATD) : null;

            List<Map> craneDetailsList = new ArrayList<Map>();
            CarrierVisit carrierVisit;
            VesselVisitDetails vvd = vesselVisitBerthing ? vesselVisitBerthing.getBrthgVvd() : null;
            if (vvd) {
                rotationNumber = vvd.getVvdObVygNbr();
                vesselName = vvd.getVvdVessel() ? vvd.getVvdVessel().getVesName() : null;
                logMsg("rotationNumber: " + vvd.getVvdObVygNbr());
                logMsg("vesselName: " + vvd.getVesselId());

                serviceCode = vvd.getCvdService() ? vvd.getCvdService().getSrvcId() : T_EMPTY;
                logMsg("serviceCode: " + serviceCode);

                carrierVisit = vvd.getCvdCv();
                logMsg("carrierVisit: " + carrierVisit);

                Set craneSet = vvd.getVvdStatisticsByCraneSet();
                logMsg("craneSet: " + craneSet);
                if (craneSet) {
                    logMsg("craneSet size: " + craneSet.size())
                    for (VesselStatisticsByCrane vvByCrane : craneSet) {
                        String craneName = vvByCrane.getCstatCrane() ? vvByCrane.getCstatCrane().getCheShortName() : null;
                        logMsg("vvByCrane craneName: " + craneName);
                        logMsg("vvByCrane start time: " + vvByCrane.getCstatStartTime());
                        logMsg("vvByCrane end time: " + vvByCrane.getCstatEndTime());
                        logMsg("vvByCrane total moves:: " + vvByCrane.getCstatTotalMoves());

                        Map<String, String> craneMap = new HashMap<String, String>();
                        craneMap.put(T_CRANE_ID, craneName);
                        craneMap.put(T_CRANE_START_WORK_TIME, vvByCrane.getCstatStartTime());
                        craneMap.put(T_CRANE_END_WORK_TIME, vvByCrane.getCstatEndTime());
                        craneDetailsList.add(craneMap);

                        totalMoves = totalMoves + vvByCrane.getCstatTotalMoves();
                    }
                }
                logMsg("totalMoves: " + totalMoves);
                rotationNumber = vvd.getVvdObVygNbr();
                if (rotationNumber == null || ArgoUtils.isEmpty(rotationNumber))
                    return;

                serviceCode = vvd.getCvdService() ? vvd.getCvdService().getSrvcName() : T_EMPTY;

                Vessel vessel = vvd.getVvdVessel();
                if (vessel != null) {
                    vesselName = vessel.getVesName();
                    Long vesselLength = vessel ? vessel.getVesVesselClass().getVesclassLoaCm() : 0L;
                    loa = vesselLength ? java.lang.String.valueOf(vesselLength / 100L) : 0L;
                    Long vesselWidth = vessel.getVesVesselClass() ? vessel.getVesVesselClass().getVesclassBeamCm() : 0L;
                    width = vesselWidth ? java.lang.String.valueOf(vesselWidth / 100L) : 0L;
                }
                logMsg("vessel: " + vessel);

                //@TODO: total moves count can be taken directly. check

                totalLoadMoves = java.lang.String.valueOf(getLoadDischMoveCount(vvd, T_LOAD));
                totalDischargeMoves = java.lang.String.valueOf(getLoadDischMoveCount(vvd, T_DISCHARGE));
                totalOtherMoves = java.lang.String.valueOf(getLoadDischMoveCount(vvd, T_OTHER));
                if (eta == null)
                    eta = vvd.getCvdETA() ? getEpochDate(vvd.getCvdETA()) : T_EMPTY;
                if (etd == null)
                    etd = vvd.getCvdETD() ? getEpochDate(vvd.getCvdETD()) : T_EMPTY;
                if (startWorkDate == null)
                    startWorkDate = vvd.getVvdTimeStartWork() ? getEpochDate(vvd.getVvdTimeStartWork()) : T_EMPTY;
                if (endWorkDate == null)
                    endWorkDate = vvd.getVvdTimeEndWork() ? getEpochDate(vvd.getVvdTimeEndWork()) : T_EMPTY;
                if (portArrivalDate == null)
                    portArrivalDate = vvd.getVvdTimeOffPortArrive() ? getEpochDate(vvd.getVvdTimeOffPortArrive()) : T_EMPTY;
                if (sailedDate == null)
                    sailedDate = vvd.getVvdTimeOffPortDepart() ? getEpochDate(vvd.getVvdTimeOffPortDepart()) : T_EMPTY;
            }
            logMsg("totalOtherMoves: " + totalOtherMoves);

            Map dataMap = new HashMap();
            //sequenceNumber pick from ISM seq number in library groovy
            dataMap.put(T__MSG_TYPE, MSG_TYPE__VSL_BERTH_EVENT);
            dataMap.put(T_PORT_CODE, ContextHelper.getThreadFacilityId());
            dataMap.put(T__TERMINAL_ID, T_CLL_1);
            dataMap.put(T__SOURCE_SYSTEM, T_TOS);
            dataMap.put(T__DEST_SYSTEM, T_RTLS);
            dataMap.put(T__SOURCE_OBJECT, T_EMPTY);
            dataMap.put(T__ACTION_DATE, getEpochDate(new Date()));
            dataMap.put(T__USER_ID, ContextHelper.getThreadUserId());
            dataMap.put(T__STATUS, T_NEW);

            dataMap.put(T_ROTATION_NO, rotationNumber ? rotationNumber : T_EMPTY);
            dataMap.put(T_VESSEL_NAME, vesselName ? vesselName : T_EMPTY);
            dataMap.put(T_SERVICE_CODE, serviceCode ? serviceCode : T_EMPTY);

            //10-Aug-2023 : sramasamy@weservetech.com - Below line added per request from DT team to sort the unique vessel-visit
            dataMap.put(T_VISIT_ID, carrierVisit? carrierVisit.getCvId() : T_EMPTY);

            dataMap.put(T_LOA, loa ? loa : T_EMPTY);
            dataMap.put(T_TOTAL_MOVES, String.valueOf(totalMoves));
            dataMap.put(T_TOTAL_LOAD_MOVES, totalLoadMoves ? totalLoadMoves : T_EMPTY);
            dataMap.put(T_TOTAL_DISCH_MOVES, totalDischargeMoves ? totalDischargeMoves : T_EMPTY);
            dataMap.put(T_TOTAL_OTHER_MOVES, totalOtherMoves ? totalOtherMoves : T_EMPTY);
            dataMap.put(T_ETA, eta);
            dataMap.put(T_ETD, etd);
            dataMap.put(T_START_WORK_DATE, startWorkDate);
            dataMap.put(T_END_WORK_DATE, endWorkDate);
            dataMap.put(T_PORT_ARRIVE_DATE, portArrivalDate);
            dataMap.put(T_SAILED_DATE, sailedDate);

            if (berthSeq) {
                StringBuilder sbBerthSeq = new StringBuilder();
                sbBerthSeq.append(T_B);
                sbBerthSeq.append(String.valueOf(berthSeq));
                dataMap.put(T_BERTH_NUMBER, sbBerthSeq.toString());
            } else {
                dataMap.put(T_BERTH_NUMBER, T_EMPTY);
            }
            dataMap.put(T_VESSEL_WIDTH, width);

            if (berthSideTo != null) {
                if (BerthSideTypeEnum.PORTSIDE == berthSideTo)
                    dataMap.put(T_ALONG_SIDE, T_P);
                else if (BerthSideTypeEnum.STARBOARD == berthSideTo)
                    dataMap.put(T_ALONG_SIDE, T_S);
                else if (BerthSideTypeEnum.UNKNOWN == berthSideTo || BerthSideTypeEnum.STERNSIDETO == berthSideTo)
                    dataMap.put(T_ALONG_SIDE, T_O);

            } else {
                dataMap.put(T_ALONG_SIDE, T_EMPTY);
            }

            dataMap.put(T_ALLOCATED_CRANES, craneDetailsList);
            dataMap.put(T_VISIT_STATE, carrierVisit ? getVesselVisitStateMatch(carrierVisit.getCvVisitPhase()) : T_EMPTY);
            dataMap.put(T_FROM_BOLLARD, berthBollardAft ? berthBollardAft : T_EMPTY);
            dataMap.put(T_TO_BOLLARD, berthBollardFore ? berthBollardFore : T_EMPTY);
            dataMap.put(T_FROM_BOLLARD_OFFSET, berthBollardForeOffset ? berthBollardForeOffset : T_EMPTY);
            dataMap.put(T_TO_BOLLARD_OFFSET, berthBollardAftOffset ? berthBollardAftOffset : T_EMPTY);

            if (dataMap.size() > 0)
                recordIsmEntry(MSG__VSL_BERTH_EVENT, dataMap);

        } catch (Exception e) {
            LOGGER.error("Exception in notifyRTLS : " + e.getMessage());
        }
    }

    private int getLoadDischMoveCount(VesselVisitDetails inVvd, String inMoveType) {
        try {
            //getCvCustomsId
            String cvId = inVvd.getCvdCv() ? inVvd.getCvdCv().getCvId() : null;
            String cvCustomId = inVvd.getCvdCv() ? inVvd.getCvdCv().getCvCustomsId() : null;
            logMsg("cvId: " + cvId + ", cvCustomId: " + cvCustomId);

            DomainQuery dq;
            if (T_LOAD == inMoveType) {
                dq = QueryUtils.createDomainQuery("WorkInstruction")
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_MOVE_KIND, WiMoveKindEnum.VeslLoad))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_TYPE, LocTypeEnum.VESSEL))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_ID, cvId));

            } else if (T_DISCHARGE == inMoveType) {
                dq = QueryUtils.createDomainQuery("WorkInstruction")
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_MOVE_KIND, WiMoveKindEnum.VeslDisch))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_TYPE, LocTypeEnum.VESSEL))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_ID, cvId));

            } else if (T_OTHER == inMoveType) {
                dq = QueryUtils.createDomainQuery("WorkInstruction")
                        .addDqPredicate(PredicateFactory.not(PredicateFactory.in(MovesField.WI_MOVE_KIND, [WiMoveKindEnum.VeslLoad, WiMoveKindEnum.VeslDisch])))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_TYPE, LocTypeEnum.VESSEL))
                        .addDqPredicate(PredicateFactory.eq(MovesField.WI_CARRIER_LOC_ID, cvId));

            }
            return HibernateApi.getInstance().findCountByDomainQuery(dq);

        } catch (Exception e) {
            LOGGER.error("Exception in getLoadDischMoveCount: " + e.getMessage());
        }
        return 0;
    }

    private void logMsg(Object inMsg) {
        LOGGER.debug(inMsg);
    }

    /*static {
        //LOGGER.setLevel(Level.DEBUG);
        GeneralReference generalReference = GeneralReference.findUniqueEntryById("DPWCLL", "MESSAGE_EXCHANGE", "RTLS", "LIMIT");
        try {
            GET_TOKEN_TRY_LIMIT = generalReference ? Integer.parseInt(generalReference.getRefValue1()) : 1;
            ISM_MSG_PUSH_LIMIT = generalReference ? Integer.parseInt(generalReference.getRefValue2()) : 20;
        } catch (Exception e) {
            LOGGER.error("Provide valid integer value for token-try-limit in General reference : " + e.getMessage());
        }
    }*/

    private static final List<WiMoveKindEnum> MOVE_KIND_LIST = Arrays.asList(WiMoveKindEnum.VeslLoad, WiMoveKindEnum.VeslDisch);
    private static final List<UnitCategoryEnum> STS__CATEGORY_LIST = Arrays.asList(UnitCategoryEnum.EXPORT, UnitCategoryEnum.IMPORT, UnitCategoryEnum.TRANSSHIP);

    private static final MetafieldId FROM_POSITION_LOC_ID = MetafieldIdFactory.getCompoundMetafieldId(ServicesMovesField.MVE_FROM_POSITION, MovesField.POS_LOC_ID);
    private static final MetafieldId TO_POSITION_LOC_ID = MetafieldIdFactory.getCompoundMetafieldId(ServicesMovesField.MVE_TO_POSITION, MovesField.POS_LOC_ID);

    private static final MetafieldId ISM_SERV_NAME = MetafieldIdFactory.getCompoundMetafieldId(ArgoIntegrationField.ISM_INTEGRATION_SERVICE, IntegrationServiceField.INTSERV_NAME);

    private static final Junction nonProcessedDisJunction = PredicateFactory.disjunction()
    /*.add(PredicateFactory.ne(ArgoIntegrationField.ISM_USER_STRING5, T_SUCCESS))*/
            .add(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING5, T_FAILURE))
            .add(PredicateFactory.isNull(ArgoIntegrationField.ISM_USER_STRING5));

    /*private static final Junction nonEligibleConJunction = PredicateFactory.conjunction()
                                    //.add(PredicateFactory.ne(ISM_SERV_NAME, INT_SERV__GET_TOKEN))
                                    //.add(PredicateFactory.ne(ISM_SERV_NAME, T_RTLS_MISMATCH));
                                    .add(PredicateFactory.not(PredicateFactory.in(ISM_SERV_NAME, RTLS_PROCESS_LIST)));*/

    private static final int TIMEOUT = 35000;
    public static final String T_EMPTY = "";
    public static final String T_PARAM_SEPERATOR = "&";
    public static final String T_EQUALS = "=";
    private static final String T_AUTHORIZATION = "Authorization";
    private static final String T_BEARER = "Bearer ";
    private static final String T_INTEGRATION_SERVICE_MESSAGE = "IntegrationServiceMessage"

    private static int GET_TOKEN_TRY_LIMIT;
    private static int ISM_MSG_PUSH_LIMIT;

    private static String INT_SERV__GET_TOKEN = "Rtls_GetToken";
    private static String INT_SERV__CHE_LOGIN_LOGOUT = "Rtls_PushCHELoginLogoutDetail";
    private static String INT_SERV__PUSH_JOB_DETAIL = "Rtls_PushJobDetail";
    private static String INT_SERV__PUSH_VSL_BERTH = "Rtls_PushVesselBerthingEventDetail";
    private static String INT_SERV__PUSH_MC_PRODUCTIVITY = "Rtls_PushMachineProductivityDetail";
    private static String INT_SERV__PUSH_MOVESTOGO_STS = "Rtls_PushMovestogoSTSDetail";
    private static String INT_SERV__PUSH_VSL_PRODUCTIVITY = "Rtls_PushVesselProductivityDetail";
    private static String INT_SERV__PUSH_QC_LANE_ASSIGN = "Rtls_PushQCLaneAssignmentDetail";
    private static String INT_SERV__PUSH_CHE_LIST = "Rtls_PushCHEListDetail";
    private static String INT_SERV__PUSH_YARD_BLOCK = "Rtls_PushYardBlockMapDetail";
    private static String INT_SERV__PUSH_OTR_IN_OUT = "Rtls_PushOTREntryExitDetails";
    private static String INT_SERV__PUSH_REEFER_POWER_SWITCH = "Rtls_PushReeferContainerDetails";

    public static final int DB_CHAR_LIMIT = 3000;
    private static final String T_SUCCESS = "SUCCESS";
    private static final String T_FAILURE = "FAILURE";
    private static final String T_IGNORE = "IGNORE";
    private static final String SEQ_NUMBER = "sequenceNumber";
    private static final String T_JOB = "job";
    //private static final String T_RTLS_MISMATCH = "Rtls_Mismatch";
    //private static final List<String> RTLS_PROCESS_LIST = ["Rtls_ProcessEta", "Rtls_ProcessJobOrder", "Rtls_ProcessArrivalNotification", "Rtls_ProcessRealTimeLocation", "Rtls_ProcessCheAlerts", "Rtls_Mismatch"] as ArrayList;
    private static final String[] RTLS_PROCESS_LIST = ["Rtls_GetToken", "Rtls_ProcessEta", "Rtls_ProcessJobOrder", "Rtls_ProcessArrivalNotification", "Rtls_ProcessRealTimeLocation", "Rtls_ProcessCheAlerts", "Rtls_Mismatch"];


    // RTLS Message types
    //private static final String MSG__GET_TOKEN = "GetToken";
    public static final String MSG__CHE_LOGIN_LOGOUT = "CHELoginLogoutDetail";
    public static final String MSG__JOB_DETAIL = "JobDetail";
    public static final String MSG__VSL_BERTH_EVENT = "VesselBerthingEvent";
    public static final String MSG__MACHINE_PRODUCTIVITY = "MachineProductivityDetail ";
    public static final String MSG__STS = "MovestogoSTSDetail";
    public static final String MSG__VESSEL_PRODUCTIVITY = "VesselProductivityDetail";
    public static final String MSG__QC_LANE_ASSIGNMENT = "QCLaneAssignmentDetail";
    public static final String MSG__CHE_LIST = "CHEListDetail";
    public static final String MSG__YARD_BLOCK = "YardBlockMapDetail";
    public static final String MSG__OTR_IN_OUT = "OTREntryExitDetails";
    public static final String MSG__REEFER_POWER_SWITCH = "ReeferContainerDetails";


    private static final String T_LOAD = "LOAD";
    private static final String T_DISCHARGE = "DISCHARGE";
    private static final String T_OTHER = "OTHER";
    private static final String T_CRANE_ID = "craneId";
    private static final String T_CRANE_START_WORK_TIME = "craneStartWorkTime";
    private static final String T_CRANE_END_WORK_TIME = "craneEndWorkTime";
    private static final String T_ROTATION_NO = "rotationNo";
    private static final String T_VESSEL_NAME = "vesselName";
    private static final String T_SERVICE_CODE = "serviceCode";
    private static final String T_VISIT_ID = "visitId";
    private static final String T_LOA = "loa";
    private static final String T_TOTAL_MOVES = "totalMoves";
    private static final String T_TOTAL_LOAD_MOVES = "totalLoadMoves";
    private static final String T_TOTAL_DISCH_MOVES = "totalDischargeMoves";
    private static final String T_TOTAL_OTHER_MOVES = "totalOtherMoves";
    private static final String T_ETA = "eta";
    private static final String T_ETD = "etd";
    private static final String T_START_WORK_DATE = "startWorkDate";
    private static final String T_END_WORK_DATE = "endWorkDate";
    private static final String T_PORT_ARRIVE_DATE = "portArrivalDate";
    private static final String T_SAILED_DATE = "sailedDate";
    private static final String T_BERTH_NUMBER = "berthNumber";
    private static final String T_VESSEL_WIDTH = "vesselWidth";
    private static final String T_ALONG_SIDE = "alongside";
    private static final String T_ALLOCATED_CRANES = "allocatedCranes";
    private static final String T_VISIT_STATE = "visitState";
    private static final String T_FROM_BOLLARD = "fromBollard";
    private static final String T_TO_BOLLARD = "toBollard";
    private static final String T_FROM_BOLLARD_OFFSET = "fromBollardOffset";
    private static final String T_TO_BOLLARD_OFFSET = "toBollardOffset";


    public static final String MSG_TYPE__CHE_LOGIN_LOGOUT = "202";
    public static final String MSG_TYPE__JOB_DETAIL = "203";
    public static final String MSG_TYPE__VSL_BERTH_EVENT = "204";
    public static final String MSG_TYPE__MACHINE_PRODUCTIVITY = "205";
    public static final String MSG_TYPE__STS = "206";
    public static final String MSG_TYPE__VESSEL_PRODUCTIVITY = "207";
    public static final String MSG_TYPE__QC_LANE_ASSIGNMENT = "208";
    public static final String MSG_TYPE__CHE_LIST = "209";
    public static final String MSG_TYPE__YARD_BLOCK = "210";
    public static final String MSG_TYPE__OTR_IN_OUT = "211";
    public static final String MSG_TYPE__REEFER_POWER_SWITCH = "212";

    private static String TOKEN_VALUE = null;
    private static String TOKEN_KEY = "tokenNumber";

    //Below fields are used in other groovy files such as ELI,..
    public static final String T_NEW = "New"; //New or Update
    public static final String T_TOS = "TOS";
    public static final String T_RTLS = "RTLS";
    public static final String T_CLL = "CLL";
    public static final String T_CLL_1 = "CLL1"; //Per discussion with RTLS team

    public static final String T__MSG_TYPE = "messageType";
    public static final String T_PORT_CODE = "portCode";
    public static final String T__TERMINAL_ID = "terminalId";
    public static final String T__SOURCE_SYSTEM = "sourceSystem";
    public static final String T__DEST_SYSTEM = "destinationSystem";
    public static final String T__SOURCE_OBJECT = "sourceObject";
    public static final String T__ACTION_DATE = "actionDate";
    public static final String T__USER_ID = "userId";
    public static final String T__STATUS = "status";

    private static final String T_OPERATOR_ID = "operatorId";
    private static final String T_OPERATOR_NAME = "operatorName";

    private static final String T_DOT = ".";
    private static final String T_GE = "GE";
    private static final String T_BK = "BK";
    private static final String T_RF = "RF";
    private static final String T_OT = "OT";
    private static final String T_PF = "PF";
    private static final String T_FR = "FR";
    private static final String T_TK = "TK";
    private static final String T_AS = "AS";
    private static final String T_YT = "YT";
    private static final String T_RS = "RS";
    private static final String T_FL = "FL";
    private static final String T_QC = "QC";
    private static final String T_S = "S";
    private static final String T_A = "A";
    private static final String T_B = "B";
    private static final String T_M = "M";
    private static final String T_P = "P";
    private static final String T_I = "I";
    private static final String T_C = "C";
    private static final String T_POSITION_QUALIFIER = "positionQualifier";
    private static final String T_POSITION_SLOT = "positionSlot";
    private static final String T_LOCATION_IDENTIFIER = "locationIdentifier";
    private static final String T_E = "E";
    private static final String T_F = "F";
    private static final String T_L = "L";
    private static final String T_O = "O";
    private static final String T_T = "T";
    private static final String T_H = "H";
    private static final String T_CATEGORY_STATUS = "categoryStatus";
    private static final String T_CATEGORY = "category";
    private static final String T_ROLD = "ROLD";
    private static final String T_RIDS = "RIDS";

    public static final String T_YARD = "YARD";

    private static final String T_PA = "PA";
    private static final String T_BP = "BP";
    //private static final String T_SA = "SA";
    private static final String T_OC = "OC";
    private static final String T_DP = "DP";
    private static final String T_OD = "OD";
    private static final String T_CL = "CL";

    public static final String T_CHE_ID = "cheId";
    public static final String T_CHE_TYPE = "cheType";
    public static final String T_STATUS = "status";

    public static final String T_EVENTTYPE_LGON = "LGON";
    public static final String T_EVENTTYPE_LGOF = "LGOF";
    public static final String T_EVENTTYPE_AVAL = "AVAL";
    public static final String T_EVENTTYPE_UNAV = "UNAV";
    public static final String T_EVENTTYPE_DSPT = "DSPT";
    public static final String T_EVENTTYPE_LIFT = "LIFT";
    public static final String T_EVENTTYPE_DROP = "DROP";

    public static final String T_EVENTTYPE_TVCO = "TVCO";
    public static final String T_EVENTTYPE_TYDR = "TYDR";
    public static final String T_EVENTTYPE_AYCO = "AYCO";
    public static final String T_EVENTTYPE_TVDR = "TVDR";
    public static final String T_EVENTTYPE_QCFL = "QCFL";

    private static final Logger LOGGER = Logger.getLogger(DPWCLLMessagingAdaptor.class);
}
