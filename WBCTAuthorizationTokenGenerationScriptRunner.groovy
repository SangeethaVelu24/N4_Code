package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.carina.integrationservice.business.IntegrationService
import com.navis.framework.IntegrationServiceField
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.scope.ScopeCoordinates
import com.navis.services.business.event.Event
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.client.RestTemplate

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 
*
*  Requirements: 
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: 
*     Code Extension Type: 
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTAuthorizationTokenGenerationScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()

        IntegrationService integrationService = getIntegrationServiceByName(INT_SERV__GET_TOKEN);
        String completeURL = integrationService ? integrationService.getIntservUrl() : null;
        String userName = integrationService ? integrationService.getIntservUserId() : null
        String password = integrationService ? integrationService.getIntservPassword() : null
        sb.append("integration service " + integrationService).append("\n")
        sb.append("complete url " + completeURL).append("\n")
        sb.append("user name " + userName).append("\n")
        sb.append("password " + password).append("\n")

        if (completeURL) {
            Map mapCredential = new HashMap();
            mapCredential.put("userName", integrationService.getIntservUserId());
            mapCredential.put("password", integrationService.getIntservPassword());
            // TOKEN_VALUE = extractTokenFromResponse(sendRequestForToken(completeURL, mapCredential));
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity;
            String jsonStr = null;
            String response;

            ObjectMapper objMapper = new ObjectMapper();
            jsonStr = objMapper.writeValueAsString(mapCredential);
            sb.append("json str " + jsonStr).append("\n")
            if (jsonStr) {
                entity = new HttpEntity<String>(jsonStr, headers);
            } else {
                entity = new HttpEntity<String>(headers);
            }

            ResponseEntity<String> responseEntity;

            try {
                responseEntity = new RestTemplate().exchange(completeURL, HttpMethod.POST, entity, String.class);
                sb.append("response entity " + responseEntity).append("\n")
            } catch (Exception ex) {
                //  LOGGER.error("Exception while exchange: " + ex.getMessage());
                /* ismForToken.setIsmUserString2(ex.getMessage())
                 ismForToken.setIsmUserString5(T_FAILURE);
                 HibernateApi.getInstance().save(ismForToken);*/
                //HibernateApi.getInstance().flush();
            }
            if (responseEntity) {
                String statusCode = responseEntity.getStatusCode();
                sb.append("status code " + statusCode).append("\n")
                response = responseEntity.getBody();
                sb.append("response " + response).append("\n")
            }
            sb.append("token value " + TOKEN_VALUE).append("\n")
        }

        return sb.toString()
    }

    private String extractTokenFromResponse(String inResponseJsonMessage) {
        //Get token from the map
        //   logMsg("before readValue : " + inResponseJsonMessage)
        Map map;
        try {
            if (inResponseJsonMessage) {
                ObjectMapper objMapper = new ObjectMapper();
                map = objMapper.readValue(inResponseJsonMessage, Map.class);
                //  logMsg("after readValue");
            }
        } catch (Exception e) {

            // LOGGER.error("Exception in extractTokenFromRTLSResponse : " + e.getMessage());
        }
        return (map != null ? map.get(TOKEN_KEY) : null);
    }

    private String sendRequestForToken(String inRestApiURI, Map inJsonMap) {
        String response;
        try {
            // logMsg("sendRequestForToken - URI: " + inRestApiURI);
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
                    // logMsg("jsonStr: " + jsonStr);
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
                    //  LOGGER.error("Exception while exchange: " + ex.getMessage());
                    ismForToken.setIsmUserString2(ex.getMessage())
                    ismForToken.setIsmUserString5(T_FAILURE);
                    HibernateApi.getInstance().save(ismForToken);
                    //HibernateApi.getInstance().flush();
                }

                //logMsg("token - responseEntity: " + responseEntity);
                if (responseEntity) {
                    String statusCode = responseEntity.getStatusCode();
                    response = responseEntity.getBody();
                    // logMsg("token - Response Body : " + response + ", Status code : " + statusCode);
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
            // catch statement
            // LOGGER.error("Exception in sendRequestForToken: " + e.getMessage());
        }
        return response;
    }

    public IntegrationServiceMessage createIntegrationSrcMsg(String inIntegrationServiceName, String inMessagePayload, Event inEvent, String inEntityId) {
        IntegrationService integrationService = getIntegrationServiceByName(inIntegrationServiceName);
        String uri = integrationService ? integrationService.getIntservUrl() : null;

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

            HibernateApi.getInstance().save(integrationServiceMessage);
            //HibernateApi.getInstance().flush();

        } catch (Exception e) {

            // catch statement
        }
        return integrationServiceMessage;
    }


    public static class IntegrationServMessageSequenceProvider extends com.navis.argo.business.model.ArgoSequenceProvider {
        public Long getNextSequenceId() {
            return super.getNextSeqValue(serviceMsgSequence, (Long) ContextHelper.getThreadFacilityKey());
        }
        private String serviceMsgSequence = "INT_SEQ";
    }

    public static IntegrationService getIntegrationServiceByName(String inIntegrationServiceName) {
        DomainQuery dq = QueryUtils.createDomainQuery("IntegrationService").addDqPredicate(PredicateFactory.eq(IntegrationServiceField.INTSERV_NAME, inIntegrationServiceName));
        return (IntegrationService) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);
    }
    private static String TOKEN_VALUE = null;
    private static String TOKEN_KEY = "tokenNumber";
    public static final int DB_CHAR_LIMIT = 3000;
    private static final String T_SUCCESS = "SUCCESS";
    private static final String T_FAILURE = "FAILURE";
    private static final String SEQ_NUMBER = "sequenceNumber";
    private static String INT_SERV__GET_TOKEN = "HATCHCLERK_TOKEN";
}
