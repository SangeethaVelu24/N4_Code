package WBCT

import com.navis.argo.business.reference.Group
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.road.business.model.TruckingCompany
import groovy.json.JsonOutput
import org.apache.commons.lang.StringUtils

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

class ScriptRunner {

    String execute() {


          //    StringBuilder sb = new StringBuilder()

          String truckingCoId = "AAAF"
          String isFreeFlow = "Yes"
          String isDecked = "D"
          sb.append("truckingCoId " + truckingCoId).append("\n")
          List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();
          if (truckingCoId != null && StringUtils.isNotEmpty(truckingCoId)) {
              trkCompanyGrpList = addGroupsFromTruckingCompany(truckingCoId)
          } else {
              trkCompanyGrpList = addGroupsWithoutTruckingCompany()
          }
          sb.append("json output " + JsonOutput.toJson(trkCompanyGrpList)).append("\n")
        return sb.toString()
        //return sb.toString()
    }

    private List<Map<String, Object>> addGroupsFromTruckingCompany(String truckingCoId) {
        sb.append("addGroupsFromTruckingCompany executing...").append("\n")
        TruckingCompany trkCompany = TruckingCompany.findTruckingCompany(truckingCoId)
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();
        String groupId = null
        if (trkCompany != null) {
            Set<Group> groupSet = trkCompany.getBzuGrpSet()
            sb.append("group set " + groupSet).append("\n")
            if (groupSet != null && !groupSet.isEmpty() && groupSet.size() > 0) {
                for (Group group : groupSet) {
                    if (IS_FREE_FLOW.equalsIgnoreCase(group.getGrpFlexString01())) {
                        if (hasDeckedUnits(group)) {
                            Map<String, Object> resultMapping = new HashMap<>();
                            groupId = group.getGrpId()
                            sb.append("group id " + groupId).append("\n")
                            //  resultMapping["GROUP_ID"] = groupId
                            resultMapping.put("GROUP_ID", groupId)
                            trkCompanyGrpList.add(resultMapping)
                            sb.append("trkCompanyGrpList inside loop " + trkCompanyGrpList).append("\n")

                        }
                    }
                }
            }
        }
        sb.append("trkCompanyGrpList " + trkCompanyGrpList).append("\n")
        return trkCompanyGrpList
    }

    private List<Map<String, Object>> addGroupsWithoutTruckingCompany() {
        List<Map<String, Object>> trkCompanyGrpList = new ArrayList<Map<String, Object>>();

        DomainQuery dq = QueryUtils.createDomainQuery("Group")
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("grpFlexString01"), IS_FREE_FLOW))
                .addDqOrdering(Ordering.asc(MetafieldIdFactory.valueOf("grpId")))

        List<Group> groupList = (List<Group>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        if (groupList != null && !groupList.isEmpty()) {
            if (groupList != null && !groupList.isEmpty() && groupList.size() > 0) {
                for (Group group : groupList) {
                    if (IS_FREE_FLOW.equalsIgnoreCase(group.getGrpFlexString01())) {
                        Map<String, Object> resultMapping = new HashMap<>();

                        resultMapping["GROUP_ID"] = group.getGrpId()
                        trkCompanyGrpList.add(resultMapping)
                    }
                }
            }
        }
        return trkCompanyGrpList
    }

    private boolean hasDeckedUnits(Group group) {
        DomainQuery dq = QueryUtils.createDomainQuery("UnitFacilityVisit")
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvUnit.unitRouting.rtgGroup.grpId"), group.getGrpId()))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("ufvFlexString02"), IS_DECKED))

        int count = HibernateApi.getInstance().findCountByDomainQuery(dq)
        if (count > 0) {
            return true
        }
        return false
    }

    private static final String IS_DECKED = "D"
    private static final String IS_FREE_FLOW = "Yes"
    private static StringBuilder sb = new StringBuilder()
}
