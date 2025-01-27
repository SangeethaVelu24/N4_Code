import com.navis.argo.ArgoIntegrationField
import com.navis.argo.ContextHelper
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.external.framework.util.ExtensionUtils
import com.navis.framework.IntegrationServiceField
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.Junction
import com.navis.framework.portal.query.PredicateFactory
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 25-Oct-2024
*
*  Requirements:  WBCT-227 - push Vessel visit and crane data from N4 to API endpoints
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTSendCraneUpdateMessageGroovyJob
*     Code Extension Type: GROOVY_JOB_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTSendCraneUpdateMessageGroovyJob extends AbstractGroovyJobCodeExtension {

    @Override
    void execute(Map<String, Object> inParams) {
        LOGGER.warn("WBCTSendCraneUpdateMessageGroovyJob executing...")
        List<IntegrationServiceMessage> ismList = getIsmListToBeSend()
        LOGGER.warn("ism list count "+ismList.size())
        library.processCraneDetailsFromISM(ismList)

    }

    private List<IntegrationServiceMessage> getIsmListToBeSend() {
        DomainQuery dq = QueryUtils.createDomainQuery(T_INTEGRATION_SERVICE_MESSAGE)
                .addDqPredicate(nonProcessedDisJunction)
                .addDqPredicate(PredicateFactory.eq(ISM_SERV_NAME, INT_SERV__CRANE))
                .addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_SEQ_NBR))
                .setDqMaxResults(ISM_MSG_PUSH_LIMIT);
        return Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);
    }

    private static final String T_INTEGRATION_SERVICE_MESSAGE = "IntegrationServiceMessage"
    private static final Junction nonProcessedDisJunction = PredicateFactory.disjunction()
            .add(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING5, T_FAILURE))
            .add(PredicateFactory.isNull(ArgoIntegrationField.ISM_USER_STRING5));
    private static final String T_FAILURE = "FAILURE";
    private static final MetafieldId ISM_SERV_NAME = MetafieldIdFactory.getCompoundMetafieldId(ArgoIntegrationField.ISM_INTEGRATION_SERVICE, IntegrationServiceField.INTSERV_NAME);
    private static String INT_SERV__CRANE = "HATCHCLERK_CRANE"
    private static int ISM_MSG_PUSH_LIMIT = 30;
    def library = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), LIBRARY);
    private static final String LIBRARY = "WBCTMessagingAdaptor";

    private static final Logger LOGGER = Logger.getLogger(WBCTSendCraneUpdateMessageGroovyJob.class);

}
