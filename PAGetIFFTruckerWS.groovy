package WBCT

import com.navis.argo.ArgoRefEntity
import com.navis.argo.ArgoRefField
import com.navis.argo.business.reference.Group
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 27-11-2024
*
*  Requirements: WBCT-335- send list of IFF (Import Free Flow) truckers to TWP.
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PAGetIFFTruckerWS
*     Code Extension Type: GROOVY_WS_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PAGetIFFTruckerWS extends AbstractGroovyWSCodeExtension {
    @Override
    String execute(Map<String, String> inParameters) {
        LOGGER.warn("PAGetIFFTruckerWS executing...")
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();

        DomainQuery dq = QueryUtils.createDomainQuery(ArgoRefEntity.GROUP)
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_FLEX_STRING01, IS_IFF_TRUCKER))
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))

        List<Group> groupList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        Set<String> trkList = new HashSet<>()
        for (Group group : groupList) {

            Set<ScopedBizUnit> scopedBizUnitSet = group.getGrpBzuSet()
            if (scopedBizUnitSet != null && scopedBizUnitSet.size() > 0) {
                for (ScopedBizUnit scopedBizUnit : scopedBizUnitSet) {
                    trkList.add(scopedBizUnit.getBzuId())
                }
            }
        }


        if (trkList != null && trkList.size() > 0) {
            for (String trkCompany : trkList) {
                Map<String, Object> resultMapping = new HashMap<>();
                resultMapping[ID] = trkCompany
                trkCompanyGrpList.add(resultMapping)
            }
        }

        return JsonOutput.toJson(trkCompanyGrpList)
    }

    private static final String ID = "ID"
    private static final String IS_IFF_TRUCKER = "Y"
    private static final Logger LOGGER = Logger.getLogger(PAGetIFFTruckerWS.class)
}
