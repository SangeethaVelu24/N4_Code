package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.reference.Group
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.road.business.model.TruckingCompany
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 18-Sep-2024
*
*  Requirements: checking for free-flow or not
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PAGetGroupCodesWS
*     Code Extension Type: GROOVY_WS_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PAGetGroupCodesWS extends AbstractGroovyWSCodeExtension {

    @Override
    String execute(Map<String, String> inParameters) {
        LOGGER.warn("PAGetGroupCodesWS executing...")

        String truckingCoId = inParameters.get(TRKCID)
        LOGGER.warn("truckingCoId " + truckingCoId)
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();
        if (ArgoUtils.isNotEmpty(truckingCoId)) {
            trkCompanyGrpList = addGroupsFromTruckingCompany(truckingCoId)
        } else {
            trkCompanyGrpList = addGroupsWithoutTruckingCompany()
        }
        return JsonOutput.toJson(trkCompanyGrpList)
    }

    private List<Map<String, Object>> addGroupsFromTruckingCompany(String truckingCoId) {
        TruckingCompany trkCompany = TruckingCompany.findTruckingCompany(truckingCoId)
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();

        if (trkCompany != null) {
            Set<Group> groupSet = trkCompany.getBzuGrpSet()
            if (groupSet != null && !groupSet.isEmpty() && groupSet.size() > 0) {
                for (Group group : groupSet) {
                    if (IS_FREE_FLOW.equalsIgnoreCase(group.getGrpFlexString01())) {
                        if (hasDeckedUnits(group)) {
                            Map<String, Object> resultMapping = new HashMap<>();
                            resultMapping[GROUP_ID] = group.getGrpId()
                            trkCompanyGrpList.add(resultMapping)

                        }
                    }
                }
            }
        }
        return trkCompanyGrpList
    }

    private List<Map<String, Object>> addGroupsWithoutTruckingCompany() {
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();

        DomainQuery dq = QueryUtils.createDomainQuery("Group")
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("grpFlexString01"), IS_FREE_FLOW))
                .addDqOrdering(Ordering.asc(MetafieldIdFactory.valueOf("grpId")))

        List<Group> groupList = (List<Group>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        LOGGER.warn("group list " + groupList)
        if (groupList != null && !groupList.isEmpty() && groupList.size() > 0) {
            for (Group group : groupList) {
                Map<String, Object> resultMapping = new HashMap<>();
                resultMapping[GROUP_ID] = group.getGrpId()
                trkCompanyGrpList.add(resultMapping)
            }
        }
        return trkCompanyGrpList
    }


    private boolean hasDeckedUnits(Group group) {
        DomainQuery dq = QueryUtils.createDomainQuery("UnitFacilityVisit")
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvUnit.unitRouting.rtgGroup.grpId"), group.getGrpId()))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvFlexString02"), IS_DECKED))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvTransitState"), UfvTransitStateEnum.S40_YARD))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvUnit.unitCategory"), UnitCategoryEnum.IMPORT))
        int count = HibernateApi.getInstance().findCountByDomainQuery(dq)
        if (count > 0) {
            return true
        }
        return false
    }

    private static final String IS_DECKED = "D"
    private static final String IS_FREE_FLOW = "Y"
    private static final String GROUP_ID = "GROUP_ID"
    private static final String TRKCID = "PARM_TRKCID"
    private static final Logger LOGGER = Logger.getLogger(PAGetGroupCodesWS.class)

}
