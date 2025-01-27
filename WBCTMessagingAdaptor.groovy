import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoIntegrationField
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.CarrierModeEnum
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.xps.model.PointOfWork
import com.navis.carina.integrationservice.business.IntegrationService
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.IntegrationServiceField
import com.navis.framework.business.Roastery
import com.navis.framework.business.atoms.IntegrationServiceDirectionEnum
import com.navis.framework.business.atoms.IntegrationServiceTypeEnum
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.scope.ScopeCoordinates
import com.navis.inventory.business.moves.WorkQueue
import com.navis.services.business.event.Event
import com.navis.vessel.business.schedule.VesselVisitDetails
import groovy.transform.CompileStatic
import org.apache.log4j.Logger
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.http.*
import org.springframework.web.client.RestTemplate
import wslite.json.JSONObject

import java.text.SimpleDateFormat

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 10-Oct-2024
*
*  Requirements: WBCT-227 - push Vessel visit and crane data from N4 to API endpoints
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTMessagingAdaptor
*     Code Extension Type: LIBRARY
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTMessagingAdaptor extends AbstractExtensionCallback {
    @CompileStatic
    void sendVesselVisitMessage(VesselVisitDetails vvd) {
        try {
            processVesselVisitDetails(vvd)

        } catch (Exception e) {
            LOGGER.warn("Exception in sendVesselVisitMessage: " + e.getMessage())
        }
    }

    void processCraneDetails(WorkQueue wq) {
        if (wq != null) {
            PointOfWork pow = wq?.getWqFirstRelatedShift()?.getWorkshiftOwnerPow() != null ? wq?.getWqFirstRelatedShift()?.getWorkshiftOwnerPow() : null
            String vesselId = wq.getWqPosLocId() != null ? wq.getWqPosLocId() : null
            CarrierVisit cv = vesselId != null ? CarrierVisit.resolveCv(ContextHelper.getThreadFacility(), CarrierModeEnum.VESSEL, vesselId) : null
            String phase = cv != null ? cv.getCvVisitPhase().getKey().substring(2) : null
            String powId = pow != null ? (pow.getPointofworkName() != null ? pow.getPointofworkName() : null) : null
            if (vesselId != null && phase != null && powId != null) {
                Map craneMap = new HashMap();
                craneMap.put("visitId", vesselId)
                craneMap.put("craneId", powId)
                craneMap.put("status", phase)
                LOGGER.warn("vessel map " + craneMap)
                IntegrationService integrationService = getIntegrationServiceByName(INT_SERV__CRANE);
                if (integrationService) {
                    recordISM(craneMap, integrationService.getIntservName())
                }
            }
        }
    }
    /**
     * record Integration Service Message
     */
    private IntegrationServiceMessage recordISM(Map inJsonMap, String inIntegrationServiceName) {
        try {
            if (inIntegrationServiceName != null) {
                IntegrationServiceMessage ismForMessage = createIntegrationSrcMsg(inIntegrationServiceName, null, null, null);
                LOGGER.warn("ism for message " + ismForMessage)
                if (ismForMessage != null) {
                    if (ismForMessage.getIsmCreated() == null)
                        ismForMessage.setIsmCreated(new Date());

                    String statusCode;
                    if (inJsonMap) {
                        ObjectMapper objMapper = new ObjectMapper();
                        String jsonStr = objMapper.writeValueAsString(inJsonMap);
                        if (jsonStr != null && ArgoUtils.isNotEmpty(jsonStr.trim())) {
                            ismForMessage.setIsmMessagePayload(jsonStr.length() > 255 ? jsonStr.substring(0, 255) : jsonStr);
                            ismForMessage.setIsmMessagePayloadBig(jsonStr);
                            HibernateApi.getInstance().save(ismForMessage);
                            HibernateApi.getInstance().flush();
                            return ismForMessage
                        }
                    }
                }
                return ismForMessage
            }
        } catch (Exception e) {
            LOGGER.error("Exception in recordISM: " + e.getMessage());
        }
    }

    void processVesselVisitDetails(VesselVisitDetails vvd) {
        if (vvd != null) {
            if (vvd != null) {
                String vesselId = vvd?.getCvdCv()?.getCvId() != null ? vvd?.getCvdCv()?.getCvId() : null
                String phase = vvd.getVvdVisitPhase().getKey().substring(2)
                Map vesselMap = new HashMap();
                vesselMap.put("visitId", vesselId)
                vesselMap.put("status", phase)
                LOGGER.warn("vessel map " + vesselMap)
                ObjectMapper objMapper = new ObjectMapper();
                String jsonStr = objMapper.writeValueAsString(vesselMap);
                IntegrationService integrationService = getIntegrationServiceByName(INT_SERV__VESSEL_VISIT);
                if (integrationService) {
                    IntegrationServiceMessage ism = recordISM(vesselMap, integrationService.getIntservName())
                    LOGGER.warn("ism " + ism)
                    if (ism != null) {
                        getBearerToken(false)
                        processMessage(ism, jsonStr)
                    }
                }
            }
        }
    }

    void processCraneDetailsFromISM(List<IntegrationServiceMessage> ismList) {
        if (ismList != null && ismList.size() > 0) {
            getBearerToken(false)
            for (IntegrationServiceMessage ism : ismList) {
                String jsonStr = ism.getIsmMessagePayloadBig()
                try {
                    processMessage(ism, jsonStr)
                } catch (Exception e) {
                    LOGGER.warn("processCraneDetailsFromISM catch executing.." + e.getMessage())
                }
            }

        }
    }

    void processMessage(IntegrationServiceMessage ism, String jsonStr) {
        LOGGER.warn("process message executing...")
        if (ism != null && ArgoUtils.isNotEmpty(jsonStr)) {
            IntegrationService integrationService = ism.getIsmIntegrationService()
            if (integrationService != null) {
                String url = integrationService.getIntservUrl() != null ? integrationService.getIntservUrl() : ""
                if (ArgoUtils.isNotEmpty(url)) {
                    ism.setIsmMessagePayloadBig(jsonStr); ism.setIsmMessagePayload(jsonStr);
                    LOGGER.warn("token value outside loop " + TOKEN_VALUE)
                    int iCount = 0
                    while (iCount < 2) {
                        LOGGER.warn("inside loop count " + iCount)
                        LOGGER.warn("token value " + TOKEN_VALUE)
                        HttpHeaders headers = new HttpHeaders()
                        headers.setContentType(MediaType.APPLICATION_JSON)
                        headers.set(T_AUTHORIZATION, T_BEARER + TOKEN_VALUE)
                        HttpEntity<String> entity;
                        if (jsonStr) {
                            entity = new HttpEntity<String>(jsonStr, headers)
                        } else {
                            entity = new HttpEntity<String>(headers);
                        }
                        LOGGER.warn("request " + jsonStr)
                        ResponseEntity<String> responseEntity
                        try {
                            responseEntity = new RestTemplate().exchange(url, HttpMethod.PUT, entity, Object.class)
                            LOGGER.warn("response entity " + responseEntity)

                            if (responseEntity) {
                                String statusCode = responseEntity.getStatusCode()
                                String response = responseEntity.getBody()
                                LOGGER.warn("token - Response Body : " + response + ", Status code : " + statusCode)
                                if (response.length() > 255) {
                                    ism.setIsmUserString2(response.substring(0, 255))
                                } else {
                                    ism.setIsmUserString2(response)
                                }
                                //If status of response message is FALSE, then retry with new token
                                if (statusCode.equals("200")) {
                                    ism.setIsmUserString5(T_SUCCESS)
                                    HibernateApi.getInstance().save(ism)
                                    break
                                } else { // If response failed, then try again with new token
                                    getBearerToken(true)
                                    HibernateApi.getInstance().save(ism)
                                }
                            } else {
                                getBearerToken(true)
                            }

                        } catch (Exception ex) {
                            LOGGER.error("Exception while exchange: " + ex.getMessage());
                            ism.setIsmUserString2(ex.getMessage())
                            ism.setIsmUserString5(T_FAILURE)
                            HibernateApi.getInstance().save(ism)
                            getBearerToken(true)
                        }
                        iCount++
                    } //While loop ends
                    LOGGER.warn("outside loop count " + iCount)
                }
            }
        }
    }

    public void getBearerToken(boolean isFailureCall) {
        LOGGER.warn("get bearer token executing...")
        try {
            IntegrationServiceMessage ism = findISM(INT_SERV__GET_TOKEN)
            if (ism == null || isFailureCall) {
                ism = updateISM(ism)
            }
            if (ism != null && ism.getIsmMessagePayloadBig() != null) {
                JSONObject jObject = new JSONObject(ism.getIsmMessagePayloadBig());
                TOKEN_VALUE = jObject.has("token") ? jObject.get("token") : null
                return
            }

        } catch (Exception e) {
            LOGGER.error("Exception in getBearerToken: " + e.getMessage());
        }
    }

    private IntegrationServiceMessage findISM(String ismName) {
        IntegrationServiceMessage ism = null
        DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ismIntegrationService.intservType"), IntegrationServiceTypeEnum.WEB_SERVICE))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ismIntegrationService.intservDirection"), IntegrationServiceDirectionEnum.OUTBOUND))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ismIntegrationService.intservName"), ismName))
                .addDqPredicate(PredicateFactory.isNotNull(ArgoIntegrationField.ISM_MESSAGE_PAYLOAD_BIG))
                .addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_ENTITY_PRIMARY_KEY))
        List<IntegrationServiceMessage> ismList = HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
        if (ismList != null && ismList.size() > 0) {
            ism = (IntegrationServiceMessage) ismList.get(0)
        }
        return ism
    }

    private IntegrationServiceMessage updateISM(IntegrationServiceMessage ism) {
        IntegrationService integrationService = getIntegrationServiceByName(INT_SERV__GET_TOKEN);
        String completeURL = integrationService ? integrationService.getIntservUrl() : null;
        LOGGER.warn("complete url " + completeURL)
        if (completeURL) {
            Map mapCredential = new HashMap();
            mapCredential.put("username", integrationService.getIntservUserId());
            mapCredential.put("password", integrationService.getIntservPassword());
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String jsonStr = null
            if (mapCredential != null) {
                ObjectMapper objMapper = new ObjectMapper();
                jsonStr = objMapper.writeValueAsString(mapCredential);
                LOGGER.warn("json str " + jsonStr)
                HttpEntity<String> entity;
                if (jsonStr) {
                    entity = new HttpEntity<String>(jsonStr, headers);
                } else {
                    entity = new HttpEntity<String>(headers);
                }
                ResponseEntity<String> responseEntity;
                try {
                    responseEntity = new RestTemplate().exchange(completeURL, HttpMethod.POST, entity, HashMap.class);
                    LOGGER.warn("responseEntity: " + responseEntity);
                } catch (Exception ex) {
                    LOGGER.error("Exception while exchange: " + ex.getMessage());
                }
                if (responseEntity) {
                    String statusCode = responseEntity.getStatusCode()
                    LOGGER.warn("status code : " + statusCode + ", res Body : " + responseEntity.getBody())
                    Map<String, String> resBodyMap = responseEntity.getBody()
                    if (ism == null) {
                        return recordISM(resBodyMap, INT_SERV__GET_TOKEN)
                    } else {
                        if (resBodyMap.get("token") != null) {
                            ism.setIsmMessagePayloadBig(resBodyMap.get("token"))
                        }
                    }
                    HibernateApi.getInstance().save(ism)
                    return ism
                }
            }
        }
    }


    public IntegrationServiceMessage createIntegrationSrcMsg(String inIntegrationServiceName, String inMessagePayload, Event inEvent, String inEntityId) {
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
                //integrationServiceMessage.setIsmLastSendTime(ArgoUtils.timeNow());
            }

            if (inMessagePayload) {
                String msg = inMessagePayload.length() > DB_CHAR_LIMIT ? inMessagePayload.substring(0, DB_CHAR_LIMIT) : inMessagePayload;
                integrationServiceMessage.setIsmMessagePayload(msg);
                integrationServiceMessage.setIsmMessagePayloadBig(inMessagePayload);
            }
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

    public static class IntegrationServMessageSequenceProvider extends com.navis.argo.business.model.ArgoSequenceProvider {
        public Long getNextSequenceId() {
            return super.getNextSeqValue(serviceMsgSequence, (Long) ContextHelper.getThreadFacilityKey());
        }
        private String serviceMsgSequence = "HC_SEQ";
    }

    public static IntegrationService getIntegrationServiceByName(String inIntegrationServiceName) {
        DomainQuery dq = QueryUtils.createDomainQuery("IntegrationService").addDqPredicate(PredicateFactory.eq(IntegrationServiceField.INTSERV_NAME, inIntegrationServiceName));
        return (IntegrationService) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);
    }

    private void logMsg(Object inMsg) {
        LOGGER.debug(inMsg);
    }

    public static final int DB_CHAR_LIMIT = 3000;
    private static String INT_SERV__GET_TOKEN = "HATCHCLERK_TOKEN";
    private static String INT_SERV__VESSEL_VISIT = "HATCHCLERK_VESSEL_VISIT"
    private static String INT_SERV__CRANE = "HATCHCLERK_CRANE"
    private static final String T_SUCCESS = "SUCCESS";
    private static final String T_FAILURE = "FAILURE";
    private static String TOKEN_VALUE = null;
    private static final String T_AUTHORIZATION = "Authorization";
    private static final String T_BEARER = "Bearer ";
    private static final SimpleDateFormat desiredFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final Logger LOGGER = Logger.getLogger(WBCTMessagingAdaptor.class);
}
